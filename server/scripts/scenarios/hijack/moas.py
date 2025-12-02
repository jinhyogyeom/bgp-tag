#!/usr/bin/env python3
import argparse
import json
from datetime import datetime, timezone, timedelta
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
from sqlalchemy import create_engine
import os

# ===== 탐지 임계 (창=전체 기간) =====
MIN_PEERS   = 2   # 서로 다른 peer 최소 수
MIN_EVENTS  = 5   # 관측 이벤트(announce) 최소 수

# ===== 원본/출력 =====
TABLE_PREFIX = "update_entries_"
OUT_TABLE    = "hijack_events"
EVENT_TYPE   = "MOAS"
TIMESCALE_URI = os.getenv('TIMESCALE_URI')

def parse_args():
    p = argparse.ArgumentParser(description="MOAS detector (no buckets; whole-window)")
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
        except Exception as e:
            print(f"[warn] fetch {tbl} failed: {e}")
            continue
        if df.empty:
            continue
        a = df.explode('announce_prefixes').rename(columns={'announce_prefixes':'prefix'})
        # as_path를 list로 정규화
        a['as_path'] = a['as_path'].apply(
            lambda x: x if isinstance(x, list) else (list(x) if x is not None else [])
        )
        frames.append(a[['timestamp','peer_as','as_path','prefix']])

    if not frames:
        return pd.DataFrame(columns=['timestamp','peer_as','as_path','prefix'])

    out = pd.concat(frames, ignore_index=True)
    out['timestamp'] = pd.to_datetime(out['timestamp'], utc=True)
    return out.sort_values('timestamp')

# ---------- 전체 기간 단일 윈도 MOAS 판정 ----------
def detect_moas_whole_window(df: pd.DataFrame):
    if df.empty:
        return []

    # origin_as 붙이기
    cur = df.copy()
    cur['origin_as'] = cur['as_path'].apply(extract_origin)
    cur = cur[cur['origin_as'].notna()]

    events = []
    for prefix, g in cur.groupby('prefix', sort=False):
        origins = g['origin_as'].dropna().unique()
        if len(origins) < 2:
            continue

        distinct_peers = g['peer_as'].nunique()
        total_events   = len(g)
        if distinct_peers < MIN_PEERS or total_events < MIN_EVENTS:
            continue

        # 기간 요약
        first_update = g['timestamp'].min()
        last_update  = g['timestamp'].max()

        # origin별 증거 요약
        per_origin = {}
        for o, gg in g.groupby('origin_as'):
            per_origin[int(o)] = {
                "peers": sorted(map(int, gg['peer_as'].unique().tolist())),
                "events": int(len(gg)),
                "sample_as_paths": [gg['as_path'].iloc[0]] if len(gg) else []
            }

        evidence = {
            "window": {"start": first_update.isoformat(), "end": last_update.isoformat()},
            "per_origin": per_origin
        }

        summary = (
            f"[{first_update:%Y-%m-%d %H:%M:%S} ~ {last_update:%Y-%m-%d %H:%M:%S}] "
            f"MOAS for {prefix} | origins={sorted(map(int, origins.tolist()))} | "
            f"peers={distinct_peers} | events={total_events}"
        )

        events.append({
            # 버킷이 없으니 대표 시간은 최초 관측 시각으로 저장
            "time": first_update,
            "prefix": str(prefix),
            "event_type": EVENT_TYPE,
            "origin_asns": sorted(map(int, origins.tolist())),
            "distinct_peers": int(distinct_peers),
            "total_events": int(total_events),

            "first_update": first_update,
            "last_update": last_update,

            # 다른 이벤트 타입용 컬럼은 None
            "baseline_origin": None,
            "top_origin": None,
            "top_ratio": None,
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
        print("no MOAS events to save")
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
        None, None, None,
        None, None,
        r["evidence_json"], r["summary"], r["analyzed_at"]
    ) for r in rows]

    execute_values(cur, sql, data)
    conn.commit()
    cur.close()
    conn.close()
    print(f"saved {len(rows)} MOAS events")

# ---------- main ----------
def main():
    args = parse_args()
    start_dt = pd.to_datetime(args.start_time, utc=True)
    end_dt   = pd.to_datetime(args.end_time,   utc=True)

    # 1시간 청크 단위로 로드→탐지→즉시 저장 (메모리 사용량 최소화)
    total_saved = 0
    current_time = start_dt
    while current_time < end_dt:
        chunk_end = min(current_time + pd.Timedelta(hours=1), end_dt)
        print(f"Processing chunk: {current_time} to {chunk_end}")

        df = load_announces(current_time, chunk_end)
        if not df.empty:
            events = detect_moas_whole_window(df)
            if events:
                print(f"Found {len(events)} MOAS events in this chunk")
                save_events(events)  # 청크별 즉시 저장
                total_saved += len(events)
            else:
                print("No MOAS events in this chunk")
        else:
            print("No announces in this chunk")

        current_time = chunk_end

    print(f"Total saved: {total_saved} MOAS events")

if __name__ == "__main__":
    main()