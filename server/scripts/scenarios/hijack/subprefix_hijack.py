#!/usr/bin/env python3
import argparse
import json
from datetime import datetime, timedelta, timezone
import ipaddress
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
from sqlalchemy import create_engine
import os

# ===== 내부 파라미터 =====
BUCKET_MIN   = 5
MIN_PEERS    = 1
MIN_EVENTS   = 1

# ===== 원본/출력 =====
TABLE_PREFIX = "update_entries_"
OUT_TABLE    = "hijack_events"
EVENT_TYPE   = "SUBPREFIX"
TIMESCALE_URI = os.getenv('TIMESCALE_URI')

def parse_args():
    p = argparse.ArgumentParser(description="Subprefix hijack detector (bucket-only, minimal schema)")
    p.add_argument("--start_time", type=str, required=True)
    p.add_argument("--end_time",   type=str, required=True)
    return p.parse_args()

def day_range(start_dt, end_dt):
    d = start_dt.date()
    while d <= end_dt.date():
        yield d
        d += timedelta(days=1)

def extract_origin(as_path):
    if not as_path:
        return None
    return as_path[-1]

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

def detect_subprefix_hijack(df: pd.DataFrame):
    if df.empty:
        return []

    dfb = df.copy()
    dfb['bucket']    = dfb['timestamp'].dt.floor(f'{BUCKET_MIN}min')
    dfb['origin_as'] = dfb['as_path'].apply(extract_origin)
    dfb = dfb[dfb['origin_as'].notna()]

    events = []
    # 버킷별 검사
    for bucket, g in dfb.groupby('bucket', sort=False):
        prefixes = {}
        for row in g.itertuples(index=False):
            try:
                pfx = ipaddress.ip_network(row.prefix, strict=False)
            except Exception:
                continue
            prefixes.setdefault(pfx, set()).add(int(row.origin_as))

        # prefix 간 상하관계 검사
        for pfx in sorted(prefixes, key=lambda x: (x.prefixlen, x.network_address)):
            for sup in sorted(prefixes, key=lambda x: x.prefixlen):
                if sup.prefixlen < pfx.prefixlen and sup.supernet_of(pfx):
                    sup_origins = prefixes[sup]
                    sub_origins = prefixes[pfx]
                    # 상위와 다른 Origin에서 광고 → Subprefix Hijack
                    if not sub_origins.issubset(sup_origins):
                        first_update = g['timestamp'].min()
                        last_update  = g['timestamp'].max()
                        evidence = {
                            "bucket_time": bucket.isoformat(),
                            "super_prefix": str(sup),
                            "super_origins": sorted(list(sup_origins)),
                            "sub_prefix": str(pfx),
                            "sub_origins": sorted(list(sub_origins))
                        }
                        summary = (
                            f"[{first_update:%Y-%m-%d %H:%M:%S} ~ {last_update:%Y-%m-%d %H:%M:%S}] "
                            f"Subprefix hijack: {pfx} (origins={list(sub_origins)}) "
                            f"under {sup} (origins={list(sup_origins)})"
                        )
                        events.append({
                            "time": bucket,
                            "prefix": str(pfx),
                            "event_type": EVENT_TYPE,
                            "origin_asns": sorted(list(sub_origins)),
                            "distinct_peers": int(g['peer_as'].nunique()),
                            "total_events": int(len(g)),

                            "first_update": first_update,
                            "last_update": last_update,

                            "baseline_origin": None,
                            "top_origin": None,
                            "top_ratio": None,

                            "parent_prefix": str(sup),
                            "more_specific": str(pfx),

                            "evidence_json": json.dumps(evidence, ensure_ascii=False),
                            "summary": summary,
                            "analyzed_at": datetime.now(timezone.utc)
                        })
    return events

def save_events(rows):
    if not rows:
        print("no SUBPREFIX events to save")
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
    print(f"saved {len(rows)} SUBPREFIX events")

def main():
    args = parse_args()
    start_dt = pd.to_datetime(args.start_time, utc=True)
    end_dt   = pd.to_datetime(args.end_time,   utc=True)

    df = load_announces(start_dt, end_dt)
    if df.empty:
        print("no announces in given range")
        return

    events = detect_subprefix_hijack(df)
    save_events(events)

if __name__ == "__main__":
    main()