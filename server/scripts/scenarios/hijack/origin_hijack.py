#!/usr/bin/env python3
import argparse
import json
from datetime import datetime, timedelta, timezone
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
from sqlalchemy import create_engine
import os

# ===== 내부 파라미터 =====
MIN_PEERS         = 2            # 서로 다른 peer 최소 수 (전체 윈도 기준)
MIN_EVENTS        = 5            # 전체 윈도 announce 최소 수
LOOKBACK_DAYS     = 7            # baseline 산정 과거 기간
NEW_ORIGIN_RATIO  = 0.60         # 새 origin 우세 비율(>= 이면 교체로 간주)
REQUIRE_BASELINE  = True         # baseline 없으면 스킵할지 여부

# ===== 원본/출력 =====
TABLE_PREFIX = "update_entries_"
OUT_TABLE    = "hijack_events"
EVENT_TYPE   = "ORIGIN"
TIMESCALE_URI = os.getenv('TIMESCALE_URI')

def parse_args():
    p = argparse.ArgumentParser(description="Origin hijack detector (whole-window, no buckets)")
    p.add_argument("--start_time", type=str, required=True, help="ISO8601 e.g. 2025-05-25T00:00:00")
    p.add_argument("--end_time",   type=str, required=True, help="ISO8601 e.g. 2025-05-25T07:00:00")
    return p.parse_args()

# ---------- 유틸 ----------
def day_range(start_dt, end_dt):
    d = start_dt.date()
    while d <= end_dt.date():
        yield d
        d += timedelta(days=1)

def extract_origin(as_path):
    if not as_path:
        return None
    return as_path[-1]

# ---------- ANNOUNCE 적재 ----------
def load_announces(start_dt, end_dt) -> pd.DataFrame:
    engine = create_engine(TIMESCALE_URI)
    frames = []
    for d in day_range(start_dt, end_dt):
        tbl = f"{TABLE_PREFIX}{d.strftime('%Y%m%d')}"
        q = f"""
        SELECT timestamp, peer_as, as_path, announce_prefixes
        FROM {tbl}
        WHERE timestamp >= %s AND timestamp < %s
          AND announce_prefixes IS NOT NULL
        ORDER BY timestamp ASC
        """
        try:
            df = pd.read_sql_query(q, engine, params=(start_dt, end_dt), parse_dates=['timestamp'])
        except Exception as e:
            print(f"[warn] fetch {tbl} failed: {e}")
            continue
        if df.empty:
            continue
        a = df.explode('announce_prefixes').rename(columns={'announce_prefixes':'prefix'})
        a['as_path'] = a['as_path'].apply(lambda x: x if isinstance(x, list) else (list(x) if x is not None else []))
        frames.append(a[['timestamp','peer_as','as_path','prefix']])

    if not frames:
        return pd.DataFrame(columns=['timestamp','peer_as','as_path','prefix'])

    out = pd.concat(frames, ignore_index=True)
    out['timestamp'] = pd.to_datetime(out['timestamp'], utc=True)
    return out.sort_values('timestamp')

# ---------- baseline(lookback 최빈 origin) ----------
def build_baseline(df_lookback: pd.DataFrame) -> pd.DataFrame:
    if df_lookback.empty:
        return pd.DataFrame(columns=['prefix','baseline_origin','count'])
    base = df_lookback.copy()
    base['origin_as'] = base['as_path'].apply(extract_origin)
    base = base[base['origin_as'].notna()]
    if base.empty:
        return pd.DataFrame(columns=['prefix','baseline_origin','count'])
    cnt = base.groupby(['prefix','origin_as']).size().reset_index(name='cnt')
    idx = cnt.groupby('prefix')['cnt'].idxmax()
    winners = cnt.loc[idx].rename(columns={'origin_as':'baseline_origin','cnt':'count'})
    return winners[['prefix','baseline_origin','count']]

# ---------- 전체 윈도 ORIGIN HIJACK 판정 ----------
def detect_origin_hijack_whole_window(df_current: pd.DataFrame, baseline_df: pd.DataFrame):
    if df_current.empty:
        return []

    cur = df_current.copy()
    cur['origin_as'] = cur['as_path'].apply(extract_origin)
    cur = cur[cur['origin_as'].notna()]

    baseline_map = baseline_df.set_index('prefix')['baseline_origin'].to_dict() if not baseline_df.empty else {}

    events = []
    for prefix, g in cur.groupby('prefix', sort=False):
        total_events = len(g)
        if total_events < MIN_EVENTS:
            continue
        distinct_peers = g['peer_as'].nunique()
        if distinct_peers < MIN_PEERS:
            continue

        counts = g['origin_as'].value_counts()
        top_origin = int(counts.idxmax())
        top_ratio  = float(counts.max() / total_events)

        baseline_origin = int(baseline_map[prefix]) if prefix in baseline_map else None
        if REQUIRE_BASELINE and baseline_origin is None:
            continue

        # 기준과 다르고 새 origin이 우세할 때만 이벤트 생성
        if (baseline_origin is None) or (top_origin != baseline_origin and top_ratio >= NEW_ORIGIN_RATIO):
            # origin별 증거
            per_origin = {}
            for o, gg in g.groupby('origin_as'):
                per_origin[int(o)] = {
                    "peers": sorted(map(int, gg['peer_as'].unique().tolist())),
                    "events": int(len(gg))
                }

            first_update = g['timestamp'].min()
            last_update  = g['timestamp'].max()

            evidence = {
                "window": {"start": first_update.isoformat(), "end": last_update.isoformat()},
                "baseline_origin": baseline_origin,
                "top_origin": int(top_origin),
                "top_ratio": round(top_ratio, 3),
                "per_origin": per_origin
            }

            summary = (
                f"[{first_update:%Y-%m-%d %H:%M:%S} ~ {last_update:%Y-%m-%d %H:%M:%S}] "
                f"Origin change for {prefix} | baseline={baseline_origin} → new={int(top_origin)} "
                f"({int(100*top_ratio)}% window share) | peers={distinct_peers} | events={total_events}"
            )

            events.append({
                "time": first_update,                 # 대표 시각: 최초 관측
                "prefix": str(prefix),
                "event_type": EVENT_TYPE,
                "origin_asns": [int(top_origin)],     # 규격 일치: 집합 형태
                "distinct_peers": int(distinct_peers),
                "total_events": int(total_events),

                "first_update": first_update,
                "last_update": last_update,

                "baseline_origin": baseline_origin,
                "top_origin": int(top_origin),
                "top_ratio": round(top_ratio, 6),

                "parent_prefix": None,
                "more_specific": None,

                "evidence_json": json.dumps(evidence, ensure_ascii=False),
                "summary": summary,
                "analyzed_at": datetime.now(timezone.utc)
            })

    return events

# ---------- 저장 ----------
def save_events(rows):
    if not rows:
        print("no ORIGIN hijack events to save")
        return
    conn = psycopg2.connect(TIMESCALE_URI)
    cur = conn.cursor()
    sql = f"""
    INSERT INTO {OUT_TABLE}
    (time, prefix, event_type,
     origin_asns, distinct_peers, total_events,
     first_update, last_update,
     baseline_origin, top_origin, top_ratio,
     parent_prefix, more_specific,
     evidence_json, summary, analyzed_at)
    VALUES %s
    """
    data = [(
        r["time"], r["prefix"], r["event_type"],
        r["origin_asns"], r["distinct_peers"], r["total_events"],
        r["first_update"], r["last_update"],
        r["baseline_origin"], r["top_origin"], r["top_ratio"],
        r["parent_prefix"], r["more_specific"],
        r["evidence_json"], r["summary"], r["analyzed_at"]
    ) for r in rows]
    execute_values(cur, sql, data)
    conn.commit()
    cur.close()
    conn.close()
    print(f"saved {len(rows)} ORIGIN hijack events")

# ---------- main ----------
def main():
    args = parse_args()
    start_dt = pd.to_datetime(args.start_time, utc=True)
    end_dt   = pd.to_datetime(args.end_time,   utc=True)

    # baseline: lookback 윈도에서 최빈 origin 산정
    lookback_start = start_dt - timedelta(days=LOOKBACK_DAYS)
    df_lookback = load_announces(lookback_start, start_dt) 
    baseline_df = build_baseline(df_lookback)

    # 1시간 청크 단위로 로드→탐지→즉시 저장 (메모리 사용량 최소화)
    total_saved = 0
    current_time = start_dt
    while current_time < end_dt:
        chunk_end = min(current_time + pd.Timedelta(hours=1), end_dt)
        print(f"Processing chunk: {current_time} to {chunk_end}")

        df_current = load_announces(current_time, chunk_end)
        if not df_current.empty:
            events = detect_origin_hijack_whole_window(df_current, baseline_df)
            if events:
                print(f"Found {len(events)} origin hijack events in this chunk")
                save_events(events)  # 청크별 즉시 저장
                total_saved += len(events)
            else:
                print("No origin hijack events in this chunk")
        else:
            print("No announces in this chunk")

        current_time = chunk_end

    print(f"Total saved: {total_saved} origin hijack events")

if __name__ == "__main__":
    main()