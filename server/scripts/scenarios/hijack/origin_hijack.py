#!/usr/bin/env python3
import argparse
import json
from datetime import datetime, timedelta, timezone
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
from sqlalchemy import create_engine
import os

# ===== 내부 파라미터(코드 내부에서만 사용) =====
BUCKET_MIN        = 5           # 5분 버킷
MIN_PEERS         = 2           # 버킷 내 서로 다른 peer 최소 수
MIN_EVENTS        = 5           # 버킷 내 최소 이벤트 수
LOOKBACK_DAYS     = 7           # baseline 산정을 위한 과거 기간
NEW_ORIGIN_RATIO  = 0.60        # 새 origin 우세 비율(>= 이면 교체로 간주)
REQUIRE_BASELINE  = True        # baseline 없으면 스킵

# ===== 원본/출력 =====
TABLE_PREFIX = "update_entries_"
OUT_TABLE    = "hijack_events"
EVENT_TYPE   = "ORIGIN"
TIMESCALE_URI = os.getenv('TIMESCALE_URI')

def parse_args():
    p = argparse.ArgumentParser(description="Origin hijack detector (bucket-only, minimal schema)")
    p.add_argument("--start_time", type=str, required=True,
                   help="ISO8601 e.g. 2025-05-25T00:00:00")
    p.add_argument("--end_time",   type=str, required=True,
                   help="ISO8601 e.g. 2025-05-25T07:00:00")
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

# ---------- 원본 ANNOUNCE 적재 ----------
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
        except Exception:
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

# ---------- baseline(lookback 최빈 origin) 산정 ----------
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

# ---------- 5분 버킷 단일 판정 ----------
def detect_origin_hijack_bucket(df_current: pd.DataFrame, baseline_df: pd.DataFrame):
    if df_current.empty:
        return []

    cur = df_current.copy()
    cur['bucket']    = cur['timestamp'].dt.floor(f'{BUCKET_MIN}min')
    cur['origin_as'] = cur['as_path'].apply(extract_origin)
    cur = cur[cur['origin_as'].notna()]

    baseline_map = {}
    has_baseline = not baseline_df.empty
    if has_baseline:
        baseline_map = baseline_df.set_index('prefix')['baseline_origin'].to_dict()

    events = []
    for (prefix, bucket), g in cur.groupby(['prefix','bucket'], sort=False):
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

        # 기준과 다르고 새 origin이 충분히 우세할 때만 기록
        if (baseline_origin is None) or (top_origin != baseline_origin and top_ratio >= NEW_ORIGIN_RATIO):
            # origin별 증거 요약
            per_origin = {}
            for o, gg in g.groupby('origin_as'):
                peers = sorted(map(int, gg['peer_as'].unique().tolist()))
                per_origin[int(o)] = {
                    "peers": peers,
                    "events": int(len(gg))
                }

            first_update = g['timestamp'].min()
            last_update  = g['timestamp'].max()

            evidence = {
                "bucket_time": bucket.isoformat(),
                "baseline_origin": baseline_origin,
                "top_origin": int(top_origin),
                "top_ratio": round(top_ratio, 3),
                "per_origin": per_origin
            }

            summary = (
                f"[{first_update:%Y-%m-%d %H:%M:%S} ~ {last_update:%Y-%m-%d %H:%M:%S}] "
                f"Origin change for {prefix} | baseline={baseline_origin} → new={int(top_origin)} "
                f"({int(100*top_ratio)}% in bucket) | peers={distinct_peers} | events={total_events}"
            )

            events.append({
                "time": bucket,
                "prefix": prefix,
                "event_type": EVENT_TYPE,
                "origin_asns": [int(top_origin)],      # 주도 origin만 저장(집합 필드 규격 유지)
                "distinct_peers": int(distinct_peers),
                "total_events": int(total_events),

                "first_update": first_update,
                "last_update": last_update,

                "baseline_origin": baseline_origin,
                "top_origin": int(top_origin),
                "top_ratio": round(top_ratio, 6),

                "parent_prefix": None,                 # SUBPREFIX 전용
                "more_specific": None,                 # SUBPREFIX 전용

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

    # baseline 산정용 lookback
    lookback_start = start_dt - timedelta(days=LOOKBACK_DAYS)
    df_lookback = load_announces(lookback_start, start_dt)
    baseline_df = build_baseline(df_lookback)

    # 현재 구간
    df_current = load_announces(start_dt, end_dt)
    if df_current.empty:
        print("no announces in given range")
        return

    events = detect_origin_hijack_bucket(df_current, baseline_df)
    save_events(events)

if __name__ == "__main__":
    main()