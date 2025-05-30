#!/usr/bin/env python3
import argparse
import json
from datetime import datetime, timezone
import pandas as pd
from jinja2 import Template
import gc
import psycopg2
from psycopg2.extras import execute_values
from sqlalchemy import create_engine
import os

# PostgreSQL connection
TIMESCALE_URI = os.getenv('TIMESCALE_URI')

def parse_arguments():
    parser = argparse.ArgumentParser(description="BGP Loop Analysis Summarization by Prefix for RAG")
    parser.add_argument("--start_time",  type=str, required=True,
                        help="start time (ISO format, e.g. '2021-10-25T00:00:00')")
    parser.add_argument("--end_time",    type=str, required=True,
                        help="end time   (ISO format, e.g. '2021-10-25T07:00:00')")
    return parser.parse_args()

def get_db_connection():
    try:
        return psycopg2.connect(TIMESCALE_URI)
    except psycopg2.Error as e:
        print(f"Database connection error: {e}")
        raise

def fetch_bgp_updates(start_time: str, end_time: str) -> pd.DataFrame:
    """
    PostgreSQL에서 BGP 업데이트 데이터를 조회하여 루프 분석에 필요한 형태로 변환
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
    
    # 상태 정보 추가
    announces['state'] = 'announce'
    withdraws['state'] = 'withdraw'
    
    # 컬럼명 통일
    announces = announces.rename(columns={'announce_prefixes': 'prefix'})
    withdraws = withdraws.rename(columns={'withdraw_prefixes': 'prefix'})
    
    # 데이터 통합
    combined = pd.concat([
        announces[['entry_id', 'timestamp', 'peer_as', 'as_path', 'prefix', 'state']],
        withdraws[['entry_id', 'timestamp', 'peer_as', 'as_path', 'prefix', 'state']]
    ])
    
    return combined.sort_values('timestamp')

def has_as_loop(as_path):
    """
    AS 경로에 루프가 있는지 확인
    """
    if not as_path or not isinstance(as_path, list):
        return False
    
    # AS 경로에서 중복된 AS 번호가 있는지 확인
    return len(as_path) != len(set(as_path))

def analyze_loop_anomalies(df):
    """
    루프 이상 현상 분석
    """
    summaries = []
    for prefix, group in df.groupby('prefix', sort=False):
        # AS 루프가 있는 경로 찾기
        loop_paths = group[group['as_path'].apply(has_as_loop)]
        
        if not loop_paths.empty:
            first = group['timestamp'].min()
            last = group['timestamp'].max()
            summaries.append({
                "prefix": prefix,
                "as_path": [str(asn) for asn in loop_paths['as_path'].iloc[0]],  # TEXT[] 타입으로 변환
                "total_events": int(group.shape[0]),
                "first_update": first.isoformat(),
                "last_update": last.isoformat(),
                "summary": generate_summary(prefix, group.shape[0], first, last, loop_paths['as_path'].iloc[0]),
                "analyzed_at": datetime.now(timezone.utc).isoformat()

            })
    return summaries

def generate_summary(prefix, total, first, last, as_path):
    tpl = Template("""
[{{ time_range }} BGP Updates – Prefix: {{ prefix }}]
- Total updates: {{ total }}
- Update time range: {{ first }} ~ {{ last }}
- AS Path with loop: {{ as_path|join(' ') }}
⚠️ BGP Loop detected! AS path contains repeated AS numbers.
""".strip())
    tr = f"{first.strftime('%Y-%m-%d %H:%M:%S')} ~ {last.strftime('%Y-%m-%d %H:%M:%S')}"
    return tpl.render(time_range=tr,
                      prefix=prefix,
                      total=total,
                      first=first.strftime('%Y-%m-%d %H:%M:%S'),
                      last=last.strftime('%Y-%m-%d %H:%M:%S'),
                      as_path=as_path)

def save_to_timescale(conn, summaries):
    """
    분석 결과를 TimescaleDB에 저장
    """
    if not summaries:
        return
    
    cursor = conn.cursor()
    try:
        data = [(
            datetime.fromisoformat(s['first_update']),
            s['prefix'],
            s['as_path'],  # 이미 TEXT[] 타입으로 변환됨
            s['total_events'],
            datetime.fromisoformat(s['first_update']),
            datetime.fromisoformat(s['last_update']),
            s['summary'],
            datetime.fromisoformat(s['analyzed_at'])
        ) for s in summaries]
        
        execute_values(cursor, """
            INSERT INTO loop_analysis_results 
            (time, prefix, as_path, total_events, first_update, 
             last_update, summary, analyzed_at)
            VALUES %s
        """, data)
        
        conn.commit()
        print(f"✅ Inserted {len(summaries)} records to TimescaleDB")
        
    except Exception as e:
        conn.rollback()
        print(f"❌ Failed to insert to TimescaleDB: {e}")
    finally:
        cursor.close()

def main():
    args = parse_arguments()
    
    try:
        # BGP 업데이트 데이터 조회
        df = fetch_bgp_updates(args.start_time, args.end_time)
        if df.empty:
            print("No BGP updates found in the specified time range")
            return
            
        # 루프 분석 수행
        summaries = analyze_loop_anomalies(df)
        
        # TimescaleDB에 결과 저장
        with get_db_connection() as conn:
            save_to_timescale(conn, summaries)
            
    except Exception as e:
        print(f"Error: {e}")
        raise

if __name__ == "__main__":
    main()