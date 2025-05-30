#!/usr/bin/env python3
import argparse
from datetime import datetime, timezone
import pandas as pd
from jinja2 import Template
import gc
from sqlalchemy import create_engine
import os

TIMESCALE_URI = os.getenv('TIMESCALE_URI')

# flap settings
FLAP_THRESHOLD_SECONDS = 60  # max interval to count as flap
MIN_FLAP_TRANSITIONS   = 2   # minimum transitions to flag flap

def parse_arguments():
    parser = argparse.ArgumentParser(description="BGP Flap Analysis Summarization by Prefix for RAG")
    parser.add_argument("--start_time",  type=str, required=True,
                        help="start time (ISO format, e.g. '2021-10-25T00:00:00')")
    parser.add_argument("--end_time",    type=str, required=True,
                        help="end time   (ISO format, e.g. '2021-10-25T07:00:00')")
    return parser.parse_args()

def fetch_bgp_updates(start_time: str, end_time: str) -> pd.DataFrame:
    """
    PostgreSQL에서 BGP 업데이트 데이터를 조회하여 플랩 분석에 필요한 형태로 변환
    """
    
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
    
    # SQLAlchemy 엔진 생성
    engine = create_engine(TIMESCALE_URI)
    
    # SQLAlchemy를 사용하여 데이터 조회
    df = pd.read_sql_query(
        query,
        engine,
        params=(start_time, end_time),
        parse_dates=['timestamp']
    )
    
    # announce와 withdraw를 분리하여 처리
    announces = df[df['announce_prefixes'].notna()].explode('announce_prefixes')
    withdraws = df[df['withdraw_prefixes'].notna()].explode('withdraw_prefixes')
    
    # withdraw_prefixes에 CIDR 표기법 추가
    withdraws['withdraw_prefixes'] = withdraws['withdraw_prefixes'].apply(lambda x: f"{x}/24" if x and '/' not in x else x)
    
    # 상태 정보 추가
    announces['event'] = 'A'
    withdraws['event'] = 'W'
    
    # 컬럼명 통일
    announces = announces.rename(columns={'announce_prefixes': 'prefix'})
    withdraws = withdraws.rename(columns={'withdraw_prefixes': 'prefix'})
    
    # as_path가 None인 경우 빈 리스트로 처리
    announces['as_path'] = announces['as_path'].apply(lambda x: x if x is not None else [])
    withdraws['as_path'] = withdraws['as_path'].apply(lambda x: x if x is not None else [])
    
    # 데이터 통합
    combined = pd.concat([
        announces[['entry_id', 'timestamp', 'peer_as', 'as_path', 'prefix', 'event']],
        withdraws[['entry_id', 'timestamp', 'peer_as', 'as_path', 'prefix', 'event']]
    ])
    
    return combined.sort_values('timestamp')

def analyze_flap_anomalies(df):
    """
    플랩 이상 현상 분석
    """
    summaries = []
    
    for prefix, group in df.groupby('prefix', sort=False):
        events = group.sort_values('timestamp')[['timestamp', 'event']].to_records(index=False)
        prev_evt, prev_ts = None, None
        transitions = 0
        
        for ts, evt in events:
            if prev_evt and evt != prev_evt:
                delta = (pd.to_datetime(ts) - pd.to_datetime(prev_ts)).total_seconds()
                if delta <= FLAP_THRESHOLD_SECONDS:
                    transitions += 1
            prev_evt, prev_ts = evt, ts
        
        if transitions >= MIN_FLAP_TRANSITIONS:
            first = group['timestamp'].min()
            last = group['timestamp'].max()
            summaries.append({
                "prefix": prefix,
                "total_events": int(group.shape[0]),
                "flap_count": transitions,
                "first_update": first.isoformat(),
                "last_update": last.isoformat(),
                "summary": generate_summary(prefix, group.shape[0], first, last, transitions),
                "analyzed_at": datetime.now(timezone.utc).isoformat()
            })
    
    return summaries

def generate_summary(prefix, total, first, last, count):
    tpl = Template("""
[{{ time_range }} BGP Updates – Prefix: {{ prefix }}]
- Total updates: {{ total }}
- Update time range: {{ first }} ~ {{ last }}
- Flap (rapid A/W) count: {{ count }}
""".strip())
    tr = f"{first.strftime('%Y-%m-%d %H:%M:%S')} ~ {last.strftime('%Y-%m-%d %H:%M:%S')}"
    return tpl.render(time_range=tr,
                      prefix=prefix,
                      total=total,
                      first=first.strftime('%Y-%m-%d %H:%M:%S'),
                      last=last.strftime('%Y-%m-%d %H:%M:%S'),
                      count=count)

def save_to_timescale(summaries):
    """
    분석 결과를 TimescaleDB에 저장
    """
    if not summaries:
        return
    
    engine = create_engine(TIMESCALE_URI)
    
    try:
        data = [(
            datetime.fromisoformat(s['first_update']),
            s['prefix'],
            s['total_events'],
            s['flap_count'],
            datetime.fromisoformat(s['first_update']),
            datetime.fromisoformat(s['last_update']),
            s['summary'],
            datetime.fromisoformat(s['analyzed_at'])
        ) for s in summaries]
        
        df = pd.DataFrame(data, columns=[
            'time', 'prefix', 'total_events', 'flap_count',
            'first_update', 'last_update', 'summary', 'analyzed_at'
        ])
        
        df.to_sql('flap_analysis_results', engine, if_exists='append', index=False)
        
    except Exception as e:
        raise

def main():
    args = parse_arguments()
    
    try:
        # BGP 업데이트 데이터 조회
        df = fetch_bgp_updates(args.start_time, args.end_time)
        if df.empty:
            return
            
        # 플랩 분석 수행
        summaries = analyze_flap_anomalies(df)

        # TimescaleDB에 결과 저장
        if summaries:
            save_to_timescale(summaries)
            
    except Exception as e:
        raise

if __name__ == "__main__":
    main()