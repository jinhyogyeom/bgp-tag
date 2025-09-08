#!/usr/bin/env python3
import argparse
from datetime import datetime, timedelta
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
from sqlalchemy import create_engine
import os

# ===== 원본/출력 =====
TABLE_PREFIX = "update_entries_"
OUT_TABLE    = "loop_analysis_results"
TIMESCALE_URI = os.getenv('TIMESCALE_URI')

def parse_args():
    p = argparse.ArgumentParser(description="BGP Loop detector (no buckets, per-update, non-consecutive repeats)")
    p.add_argument("--start_time", type=str, required=True, help="ISO8601 e.g. 2025-05-25T00:00:00Z")
    p.add_argument("--end_time",   type=str, required=True, help="ISO8601 e.g. 2025-05-25T07:00:00Z")
    return p.parse_args()

def day_range(start_dt, end_dt):
    d = start_dt.date()
    while d <= end_dt.date():
        yield d
        d += timedelta(days=1)

def load_announces(start_dt, end_dt) -> pd.DataFrame:
    """
    기간 내 ANNOUNCE만 로드 → (timestamp, prefix, peer_as, as_path) 정규화
    """
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
        # as_path를 list[int]로 정규화
        def norm_path(x):
            if isinstance(x, list):
                return [int(v) for v in x]
            if x is None:
                return []
            try:
                return [int(v) for v in list(x)]
            except Exception:
                return []
        a['as_path'] = a['as_path'].apply(norm_path)
        frames.append(a[['timestamp','peer_as','as_path','prefix']])

    if not frames:
        return pd.DataFrame(columns=['timestamp','peer_as','as_path','prefix'])

    out = pd.concat(frames, ignore_index=True)
    out['timestamp'] = pd.to_datetime(out['timestamp'], utc=True)
    return out.sort_values('timestamp')

def find_nonconsecutive_repeat(as_path):
    """
    비연속 반복(A ... B ... A) 발견 시 반복 ASN과 위치를 반환.
    연속 반복(AS prepending)은 정상으로 간주하고 무시.
    반환: None 또는 {"asn": ASN, "i": first_idx, "j": second_idx}
    """
    if not as_path or len(as_path) < 3:
        return None
    last_pos = {}
    for idx, asn in enumerate(as_path):
        try:
            asn = int(asn)
        except Exception:
            return None
        if asn in last_pos and idx - last_pos[asn] > 1:
            return {"asn": asn, "i": last_pos[asn], "j": idx}
        last_pos[asn] = idx
    return None

def detect_loops(df: pd.DataFrame):
    """
    버킷팅 없이, 각 ANNOUNCE 레코드 단위로 비연속 반복이 있으면 곧장 수집.
    """
    if df.empty:
        return []

    rows = []
    now = datetime.now()
    for row in df.itertuples(index=False):
        path = row.as_path if isinstance(row.as_path, list) else []
        info = find_nonconsecutive_repeat(path)
        if not info:
            continue

        path_str = " ".join(map(str, path))
        summary = (
            f"[{row.timestamp:%Y-%m-%d %H:%M:%S}] BGP loop for {row.prefix} | "
            f"peer_as={int(row.peer_as)} | repeat_as={info['asn']} "
            f"(pos {info['i']}→{info['j']}) | as_path=[{path_str}]"
        )

        rows.append((
            row.timestamp,               # time
            str(row.prefix),             # prefix
            int(row.peer_as),            # peer_as
            int(info['asn']),            # repeat_as
            int(info['i']),              # first_idx
            int(info['j']),              # second_idx
            path,                        # as_path :: int[]
            int(len(path)),              # path_len
            summary,                     # summary
            now                          # analyzed_at
        ))
    return rows

def save_rows(rows):
    if not rows:
        print("no LOOP events to save"); return
    conn = psycopg2.connect(TIMESCALE_URI)
    cur = conn.cursor()
    sql = f"""
    INSERT INTO {OUT_TABLE}
    (time, prefix, peer_as, repeat_as, first_idx, second_idx, as_path, path_len, summary, analyzed_at)
    VALUES %s
    ON CONFLICT (time, prefix, peer_as, repeat_as, first_idx, second_idx) DO NOTHING
    """
    execute_values(cur, sql, rows)
    conn.commit()
    cur.close()
    conn.close()
    print(f"saved {len(rows)} LOOP events")

def main():
    args = parse_args()
    start_dt = pd.to_datetime(args.start_time, utc=True)
    end_dt   = pd.to_datetime(args.end_time,   utc=True)

    df = load_announces(start_dt, end_dt)
    if df.empty:
        print("no announces in given range"); return

    rows = detect_loops(df)
    save_rows(rows)

if __name__ == "__main__":
    main()