#!/usr/bin/env python3
import argparse
from datetime import datetime, timezone
import pandas as pd
from jinja2 import Template
import gc
from sqlalchemy import create_engine
import os

TIMESCALE_URI = os.getenv('TIMESCALE_URI')

FLAP_THRESHOLD_SECONDS = 10
MIN_FLAP_TRANSITIONS = 5

def parse_arguments():
    parser = argparse.ArgumentParser(description="BGP Flap Analysis Summarization by Prefix+Peer for RAG")
    parser.add_argument("--start_time", type=str, required=True)
    parser.add_argument("--end_time", type=str, required=True)
    parser.add_argument("--consider_path_change", action="store_true")
    return parser.parse_args()

def fetch_bgp_updates(start_time: str, end_time: str) -> pd.DataFrame:
    target_date = pd.to_datetime(start_time).strftime('%Y%m%d')
    query = f"""
    SELECT 
        entry_id,
        timestamp,
        peer_as,
        as_path,
        announce_prefixes,
        withdraw_prefixes
    FROM update_entries_{target_date}
    WHERE timestamp BETWEEN %s AND %s
    ORDER BY timestamp ASC
    """
    print(f"[DEBUG] Fetching data from update_entries_{target_date} between {start_time} and {end_time}")
    engine = create_engine(TIMESCALE_URI)
    df = pd.read_sql_query(
        query,
        engine,
        params=(start_time, end_time),
        parse_dates=['timestamp']
    )
    print(f"[DEBUG] Raw rows fetched: {len(df)}")

    announces = df[df['announce_prefixes'].notna()]
    withdraws = df[df['withdraw_prefixes'].notna()]
    print(f"[DEBUG] announces before explode: {len(announces)}, withdraws before explode: {len(withdraws)}")

    announces = announces.explode('announce_prefixes')
    withdraws = withdraws.explode('withdraw_prefixes')
    print(f"[DEBUG] announces after explode: {len(announces)}, withdraws after explode: {len(withdraws)}")

    announces['event'] = 'A'
    withdraws['event'] = 'W'
    announces = announces.rename(columns={'announce_prefixes': 'prefix'})
    withdraws = withdraws.rename(columns={'withdraw_prefixes': 'prefix'})
    announces['as_path'] = announces['as_path'].apply(lambda x: x if x is not None else [])
    withdraws['as_path'] = withdraws['as_path'].apply(lambda x: x if x is not None else [])

    combined = pd.concat([
        announces[['entry_id', 'timestamp', 'peer_as', 'as_path', 'prefix', 'event']],
        withdraws[['entry_id', 'timestamp', 'peer_as', 'as_path', 'prefix', 'event']]
    ])
    print(f"[DEBUG] Combined rows: {len(combined)}")

    return combined.sort_values('timestamp')

def analyze_flap_anomalies(
    df, 
    flap_threshold_seconds=FLAP_THRESHOLD_SECONDS, 
    min_flap_transitions=MIN_FLAP_TRANSITIONS, 
    consider_path_change=False
):
    print(f"[DEBUG] analyze_flap_anomalies input rows: {len(df)}")
    if df.empty:
        return []
    use_cols = ['timestamp','prefix','peer_as','event','as_path']
    gdf = df[use_cols].copy()
    gdf['timestamp'] = pd.to_datetime(gdf['timestamp'])
    gdf = gdf.sort_values(['prefix','peer_as','timestamp'])

    gdf['prev_event'] = gdf.groupby(['prefix','peer_as'])['event'].shift(1)
    gdf['prev_ts'] = gdf.groupby(['prefix','peer_as'])['timestamp'].shift(1)
    gdf['prev_as_path'] = gdf.groupby(['prefix','peer_as'])['as_path'].shift(1)

    dt = (gdf['timestamp'] - gdf['prev_ts']).dt.total_seconds()

    # 1. Classical flap: A↔W
    classical_flap = (
        gdf['prev_event'].notna() &
        (gdf['event'] != gdf['prev_event']) &
        (dt <= flap_threshold_seconds)
    )

    # 2. Path flap: A→A but path changes
    prev_path_str = gdf['prev_as_path'].apply(lambda x: ','.join(map(str,x)) if isinstance(x,(list,tuple)) else str(x))
    curr_path_str = gdf['as_path'].apply(lambda x: ','.join(map(str,x)) if isinstance(x,(list,tuple)) else str(x))
    path_flap = (
        (gdf['prev_event'] == 'A') &
        (gdf['event'] == 'A') &
        (dt <= flap_threshold_seconds) &
        (curr_path_str != prev_path_str)
    )

    print(f"[DEBUG] Classical: {classical_flap.sum()}, Path: {path_flap.sum()}")

    gdf['flap_type'] = None
    gdf.loc[classical_flap, 'flap_type'] = 1
    gdf.loc[path_flap, 'flap_type'] = 2

    gdf['is_flip'] = gdf['flap_type'].notna()

    agg = gdf.groupby(['prefix','peer_as']).agg(
        total_events=('event','size'),
        flap_count=('is_flip','sum'),
        first_update=('timestamp','min'),
        last_update=('timestamp','max')
    ).reset_index()

    hit = agg[agg['flap_count'] >= min_flap_transitions].copy()
    print(f"[DEBUG] Flap candidates found: {len(hit)}")

    # 벡터화 최적화: flap_types를 한 번에 계산
    flap_summary = gdf[gdf['is_flip']].groupby(['prefix', 'peer_as'])['flap_type'].apply(
        lambda x: ','.join(map(str, sorted(x.unique())))
    ).reset_index()
    flap_summary.columns = ['prefix', 'peer_as', 'flap_types_str']
    
    # hit와 flap_summary 조인
    hit_with_types = hit.merge(flap_summary, on=['prefix', 'peer_as'], how='left')
    hit_with_types['flap_types_str'] = hit_with_types['flap_types_str'].fillna('')
    
    now_utc = datetime.now(timezone.utc).isoformat()
    
    # 벡터화된 summary 생성
    summaries = []
    for row in hit_with_types.itertuples(index=False):
        summaries.append({
            "prefix": row.prefix,
            "peer_as": int(row.peer_as),
            "total_events": int(row.total_events),
            "flap_count": int(row.flap_count),
            "first_update": row.first_update.isoformat(),
            "last_update": row.last_update.isoformat(),
            "summary": generate_summary_with_peer(
                row.prefix, row.peer_as, row.total_events,
                row.first_update, row.last_update, row.flap_count
            ) + f"\n- Flap types observed: {row.flap_types_str}",
            "analyzed_at": now_utc
        })
    return summaries

def generate_summary_with_peer(prefix, peer_as, total, first, last, count):
    tpl = Template("""
[{{ tr }} BGP Updates – Prefix: {{ prefix }} (peer_as: {{ peer_as }})
- Total updates: {{ total }}
- Update time range: {{ first }} ~ {{ last }}
- Flap (rapid A/W) count: {{ count }}
""".strip())
    tr = f"{first.strftime('%Y-%m-%d %H:%M:%S')} ~ {last.strftime('%Y-%m-%d %H:%M:%S')}"
    return tpl.render(tr=tr,
                      prefix=prefix,
                      peer_as=peer_as,
                      total=total,
                      first=first.strftime('%Y-%m-%d %H:%M:%S'),
                      last=last.strftime('%Y-%m-%d %H:%M:%S'),
                      count=count)

def save_to_timescale(summaries):
    if not summaries:
        print("[DEBUG] No summaries to save")
        return
    engine = create_engine(TIMESCALE_URI)
    data = [(
        datetime.fromisoformat(s['first_update']),
        s['prefix'],
        s['peer_as'],
        s['total_events'],
        s['flap_count'],
        datetime.fromisoformat(s['first_update']),
        datetime.fromisoformat(s['last_update']),
        s['summary'],
        datetime.fromisoformat(s['analyzed_at'])
    ) for s in summaries]
    df = pd.DataFrame(data, columns=[
        'time','prefix','peer_as','total_events','flap_count',
        'first_update','last_update','summary','analyzed_at'
    ])
    print(f"[DEBUG] Saving {len(df)} summaries to TimescaleDB")
    
    # 배치 크기를 줄여서 안정적으로 삽입
    batch_size = 100
    for i in range(0, len(df), batch_size):
        batch = df.iloc[i:i+batch_size]
        try:
            batch.to_sql('flap_analysis_results', engine, if_exists='append', index=False)
            print(f"[DEBUG] Saved batch {i//batch_size + 1}/{(len(df)-1)//batch_size + 1}")
        except Exception as e:
            print(f"[ERROR] Failed to save batch {i//batch_size + 1}: {e}")
            continue

def main():
    args = parse_arguments()
    start_dt = pd.to_datetime(args.start_time)
    end_dt = pd.to_datetime(args.end_time)
    total_saved = 0
    current_time = start_dt
    while current_time < end_dt:
        chunk_end = min(current_time + pd.Timedelta(hours=1), end_dt)
        print(f"[INFO] Processing chunk: {current_time} to {chunk_end}")
        df = fetch_bgp_updates(current_time.isoformat(), chunk_end.isoformat())
        print(f"[INFO] Data fetched: {len(df)} rows")
        if not df.empty:
            summaries = analyze_flap_anomalies(df, consider_path_change=args.consider_path_change)
            if summaries:
                print(f"[INFO] Found {len(summaries)} summaries in this chunk")
                save_to_timescale(summaries)
                total_saved += len(summaries)
            else:
                print("[INFO] No flap events in this chunk")
        else:
            print("[INFO] No data found in this chunk")
        current_time = chunk_end
        del df
        gc.collect()
    print(f"[INFO] Total saved: {total_saved} flap summaries")

if __name__ == "__main__":
    main()