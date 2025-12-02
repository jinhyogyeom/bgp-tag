#!/usr/bin/env python3
import argparse
from datetime import datetime, timezone
import pandas as pd
from jinja2 import Template
import gc
from sqlalchemy import create_engine
import os

TIMESCALE_URI = os.getenv('TIMESCALE_URI')

FLAP_THRESHOLD_SECONDS = 60
MIN_FLAP_TRANSITIONS = 2

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
    engine = create_engine(TIMESCALE_URI)
    df = pd.read_sql_query(
        query,
        engine,
        params=(start_time, end_time),
        parse_dates=['timestamp']
    )
    announces = df[df['announce_prefixes'].notna()].explode('announce_prefixes')
    withdraws = df[df['withdraw_prefixes'].notna()].explode('withdraw_prefixes')
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
    return combined.sort_values('timestamp')

def analyze_flap_anomalies(df, flap_threshold_seconds=FLAP_THRESHOLD_SECONDS, min_flap_transitions=MIN_FLAP_TRANSITIONS, consider_path_change=False):
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
    flip_aw = (gdf['prev_event'].notna()) & (gdf['event'] != gdf['prev_event']) & (dt <= flap_threshold_seconds)
    if consider_path_change:
        prev_path_str = gdf['prev_as_path'].apply(lambda x: ','.join(map(str,x)) if isinstance(x, (list,tuple)) else str(x))
        curr_path_str = gdf['as_path'].apply(lambda x: ','.join(map(str,x)) if isinstance(x, (list,tuple)) else str(x))
        flip_path = (gdf['prev_event'] == 'A') & (gdf['event'] == 'A') & (dt <= flap_threshold_seconds) & (curr_path_str != prev_path_str)
    else:
        flip_path = False
    gdf['is_flip'] = flip_aw | flip_path
    agg = gdf.groupby(['prefix','peer_as']).agg(
        total_events=('event','size'),
        flap_count=('is_flip','sum'),
        first_update=('timestamp','min'),
        last_update=('timestamp','max')
    ).reset_index()
    hit = agg[agg['flap_count'] >= min_flap_transitions].copy()
    summaries = []
    now_utc = datetime.now(timezone.utc).isoformat()
    for row in hit.itertuples(index=False):
        summaries.append({
            "prefix": row.prefix,
            "peer_as": int(row.peer_as),
            "total_events": int(row.total_events),
            "flap_count": int(row.flap_count),
            "first_update": row.first_update.isoformat(),
            "last_update": row.last_update.isoformat(),
            "summary": generate_summary_with_peer(row.prefix, row.peer_as, row.total_events, row.first_update, row.last_update, row.flap_count),
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
    df.to_sql('flap_analysis_results', engine, if_exists='append', index=False)

def main():
    args = parse_arguments()
    start_dt = pd.to_datetime(args.start_time)
    end_dt = pd.to_datetime(args.end_time)
    all_summaries = []
    current_time = start_dt
    while current_time < end_dt:
        chunk_end = min(current_time + pd.Timedelta(hours=6), end_dt)
        print(f"Processing chunk: {current_time} to {chunk_end}")
        df = fetch_bgp_updates(current_time.isoformat(), chunk_end.isoformat())
        if not df.empty:
            summaries = analyze_flap_anomalies(df, consider_path_change=args.consider_path_change)
            all_summaries.extend(summaries)
            print(f"Found {len(summaries)} summaries in this chunk")
        else:
            print("No data found in this chunk")
        current_time = chunk_end
        del df
        gc.collect()
    if all_summaries:
        print(f"Saving {len(all_summaries)} total summaries to database...")
        save_to_timescale(all_summaries)
        print("Save completed")
    else:
        print("No summaries to save")

if __name__ == "__main__":
    main()