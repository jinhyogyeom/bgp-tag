#!/usr/bin/env python3
import argparse
import json
from datetime import datetime, timedelta, timezone
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
from sqlalchemy import create_engine
import os

# ===== 내부 탐지 파라미터(코드 내부에서만 사용) =====
BUCKET_MIN   = 5          # 5분 버킷
MIN_PEERS    = 2          # 버킷 내 서로 다른 peer 최소 수
MIN_EVENTS   = 5          # 버킷 내 최소 이벤트 수

# ===== 원본/출력 =====
TABLE_PREFIX = "update_entries_"
OUT_TABLE    = "hijack_events"
EVENT_TYPE   = "MOAS"
TIMESCALE_URI = os.getenv('TIMESCALE_URI')

def parse_args():
    p = argparse.ArgumentParser(description="MOAS detector (bucket-only, minimal schema)")
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
def load_announces(start_iso, end_iso) -> pd.DataFrame:
    start_dt = pd.to_datetime(start_iso, utc=True)
    end_dt   = pd.to_datetime(end_iso,   utc=True)
    engine   = create_engine(TIMESCALE_URI)
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
        # as_path를 list로 정규화
        a['as_path'] = a['as_path'].apply(lambda x: x if isinstance(x, list) else (list(x) if x is not None else []))
        frames.append(a[['timestamp','peer_as','as_path','prefix']])
    if not frames:
        return pd.DataFrame(columns=['timestamp','peer_as','as_path','prefix'])
    out = pd.concat(frames, ignore_index=True)
    out['timestamp'] = pd.to_datetime(out['timestamp'], utc=True)
    return out.sort_values('timestamp')

# ---------- 5분 버킷 단일 판정 ----------
def detect_moas_bucket_only(df: pd.DataFrame):
    if df.empty:
        return []
    dfb = df.copy()
    dfb['bucket'] = dfb['timestamp'].dt.floor(f'{BUCKET_MIN}min')
    dfb['origin_as'] = dfb['as_path'].apply(extract_origin)
    dfb = dfb[dfb['origin_as'].notna()]

    events = []
    for (prefix, bucket), g in dfb.groupby(['prefix','bucket'], sort=False):
        origins = g['origin_as'].dropna().unique()
        if len(origins) < 2:
            continue
        distinct_peers = g['peer_as'].nunique()
        if distinct_peers < MIN_PEERS:
            continue
        total_events = len(g)
        if total_events < MIN_EVENTS:
            continue

        # origin별 증거 요약
        per_origin = {}
        peers_union = set()
        for o, gg in g.groupby('origin_as'):
            peers = sorted(map(int, gg['peer_as'].unique().tolist()))
            per_origin[int(o)] = {
                "peers": peers,
                "events": int(len(gg)),
                # 경로 샘플(있으면)
                "sample_as_paths": [gg['as_path'].iloc[0]] if len(gg) else []
            }
            peers_union.update(peers)

        first_update = g['timestamp'].min()
        last_update  = g['timestamp'].max()

        # 분석용 최소 evidence (탐지 파라미터는 포함하지 않음)
        evidence = {
            "bucket_time": bucket.isoformat(),
            "per_origin": per_origin
        }

        summary = (
            f"[{first_update:%Y-%m-%d %H:%M:%S} ~ {last_update:%Y-%m-%d %H:%M:%S}] "
            f"MOAS for {prefix} | origins={sorted(map(int, origins.tolist()))} | "
            f"peers={distinct_peers} | events={total_events}"
        )

        # 스키마에 맞춰, MOAS에 불필요한 칼럼은 None으로 채움
        events.append({
            "time": bucket,
            "prefix": prefix,
            "event_type": EVENT_TYPE,
            "origin_asns": sorted(map(int, origins.tolist())),
            "distinct_peers": int(distinct_peers),
            "total_events": int(total_events),

            "first_update": first_update,
            "last_update": last_update,

            "baseline_origin": None,  # ORIGIN 전용
            "top_origin": None,       # ORIGIN 전용
            "top_ratio": None,        # ORIGIN 전용

            "parent_prefix": None,    # SUBPREFIX 전용
            "more_specific": None,    # SUBPREFIX 전용

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

    # 스키마와 동일한 컬럼 순서로 INSERT
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
    print(f"saved {len(rows)} MOAS events")

# ---------- main ----------
def main():
    args = parse_args()
    df = load_announces(args.start_time, args.end_time)
    if df.empty:
        print("no announces in given range")
        return
    events = detect_moas_bucket_only(df)
    save_events(events)

if __name__ == "__main__":
    main()