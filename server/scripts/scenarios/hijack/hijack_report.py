#!/usr/bin/env python3
import argparse
import json
import os
import logging
from datetime import datetime, timedelta
import psycopg2
from psycopg2.extras import RealDictCursor
from typing import List, Dict, Any, Optional
from dataclasses import dataclass
from concurrent.futures import ThreadPoolExecutor
import pytz

# -----------------------------
# Constants
# -----------------------------
TIMESCALE_URI = os.getenv('TIMESCALE_URI')
TOP_N = 5  # 상위 5개 프리픽스로 고정

# -----------------------------
# Logging
# -----------------------------
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# -----------------------------
# Data Structures
# -----------------------------
@dataclass
class HijackRecord:
    time: datetime
    prefix: str
    unique_origin_asns: List[str]
    total_events: int
    first_update: datetime
    last_update: datetime
    summary: str
    analyzed_at: datetime

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> Optional['HijackRecord']:
        try:
            # 숫자 필드 변환 - 문자열 정리 후 변환
            def safe_int_convert(value: Any) -> int:
                if value is None:
                    return 0
                if isinstance(value, int):
                    return value
                # 문자열에서 숫자만 추출
                cleaned = ''.join(c for c in str(value) if c.isdigit() or c == '.')
                return int(float(cleaned)) if cleaned else 0

            total_events = safe_int_convert(data.get('total_events'))
            
            return cls(
                time=parse_datetime(data['time']),
                prefix=data['prefix'],
                unique_origin_asns=data['unique_origin_asns'],
                total_events=total_events,
                first_update=parse_datetime(data['first_update']),
                last_update=parse_datetime(data['last_update']),
                summary=data['summary'],
                analyzed_at=parse_datetime(data['analyzed_at'])
            )
        except (KeyError, ValueError, TypeError) as e:
            logger.error(f"Error parsing record: {e}")
            return None

# -----------------------------
# Arg parsing
# -----------------------------
def parse_args():
    parser = argparse.ArgumentParser(
        description="Generate 10-minute interval natural language JSONL reports from TimescaleDB hijack analysis results"
    )
    parser.add_argument(
        '--output_file', '-o', default='hijack_10min_nl_reports.jsonl',
        help="Where to write 10-minute interval natural language report JSONL"
    )
    parser.add_argument(
        '--start_time', type=str, required=True,
        help="Start time for analysis (ISO format, e.g., 2024-03-20T00:00:00)"
    )
    parser.add_argument(
        '--end_time', type=str, required=True,
        help="End time for analysis (ISO format, e.g., 2024-03-20T01:00:00)"
    )
    parser.add_argument(
        '--batch_size', type=int, default=1000,
        help="Number of records to fetch per batch (default: 1000)"
    )
    parser.add_argument(
        '--timezone', type=str, default='UTC',
        help="Timezone for report generation (default: UTC)"
    )
    return parser.parse_args()

# -----------------------------
# Database
# -----------------------------
def get_db_connection():
    try:
        return psycopg2.connect(
            TIMESCALE_URI,
            cursor_factory=RealDictCursor
        )
    except psycopg2.Error as e:
        logger.error(f"Database connection error: {e}")
        raise

def fetch_hijack_records(start_time: str, end_time: str, batch_size: int) -> List[HijackRecord]:
    """
    TimescaleDB에서 hijack 분석 결과를 배치 단위로 조회
    """
    records = []
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            offset = 0
            while True:
                cur.execute("""
                    SELECT 
                        time,
                        prefix,
                        unique_origin_asns,
                        total_events,
                        first_update,
                        last_update,
                        summary,
                        analyzed_at
                    FROM hijack_analysis_results
                    WHERE time BETWEEN %s AND %s
                    ORDER BY time ASC
                    LIMIT %s OFFSET %s
                """, (start_time, end_time, batch_size, offset))
                
                batch = cur.fetchall()
                if not batch:
                    break
                    
                for record in batch:
                    if hijack_record := HijackRecord.from_dict(record):
                        records.append(hijack_record)
                
                offset += batch_size
                logger.info(f"Fetched {len(records)} records so far...")
    
    return records

# -----------------------------
# Utilities
# -----------------------------
def parse_datetime(val):
    if isinstance(val, datetime):
        return val
    try:
        # 문자열일 때만 Z 치환
        return datetime.fromisoformat(val.replace('Z', '+00:00'))
    except Exception as e:
        logger.error(f"Error parsing datetime {val}: {e}")
        return None

def floor_to_10min(dt: datetime) -> datetime:
    """
    Round down to the nearest 10-minute interval.
    e.g. 00:07 -> 00:00; 00:13 -> 00:10
    """
    return dt.replace(minute=(dt.minute // 10) * 10, second=0, microsecond=0)

def calculate_risk_score(events: int, origin_asn_count: int, duration_hours: float) -> float:
    """
    Calculate normalized risk score based on event density, origin ASN count, and duration
    """
    # 최소 1분으로 제한하여 event_density가 비정상적으로 높아지는 것 방지
    duration_hours = max(duration_hours, 1/60)
    event_density = events / duration_hours
    
    # 각 지표 정규화
    normalized_density = min(event_density * 0.4, 100)  # 이벤트 밀도 제한
    normalized_asns = min(origin_asn_count * 0.3, 30)   # origin ASN 수 제한
    normalized_duration = min(duration_hours * 0.3, 24) # 지속 시간 제한
    
    return normalized_density + normalized_asns + normalized_duration

def process_bucket(records: List[HijackRecord], timezone: str) -> Dict[str, Any]:
    """
    10분 단위 버킷의 데이터를 처리하여 리포트 생성
    """
    if not records:
        return None
        
    # 시간대 변환
    tz = pytz.timezone(timezone)
    
    # Aggregate overall metrics
    total_prefixes = len({r.prefix for r in records})
    total_events = sum(r.total_events for r in records)
    total_origin_asns = len(set().union(*[set(r.unique_origin_asns) for r in records]))

    # Aggregate per-prefix metrics
    prefix_groups: Dict[str, List[HijackRecord]] = {}
    for rec in records:
        if rec.prefix not in prefix_groups:
            prefix_groups[rec.prefix] = []
        prefix_groups[rec.prefix].append(rec)

    agg_list = []
    for prefix, group in prefix_groups.items():
        events = sum(r.total_events for r in group)
        origin_asns = set().union(*[set(r.unique_origin_asns) for r in group])
        first = min(r.first_update for r in group)
        last = max(r.last_update for r in group)
        duration_hours = (last - first).total_seconds() / 3600
        
        risk_score = calculate_risk_score(events, len(origin_asns), duration_hours)
        
        agg_list.append({
            'prefix': prefix,
            'total_events': events,
            'origin_asn_count': len(origin_asns),
            'origin_asns': list(origin_asns),
            'duration_hours': duration_hours,
            'risk_score': risk_score,
            'first_update': first.astimezone(tz).isoformat(),
            'last_update': last.astimezone(tz).isoformat()
        })

    # Sort prefixes by risk score descending and select top 5
    sorted_agg = sorted(agg_list, key=lambda x: x['risk_score'], reverse=True)
    top_agg = sorted_agg[:TOP_N]

    # Build natural language report
    top_list = ", ".join(
        f"{d['prefix']} (risk: {d['risk_score']:.1f})" for d in top_agg
    )
    details = "; ".join(
        f"{d['prefix']} had {d['total_events']} updates from {d['origin_asn_count']} origin ASNs ({', '.join(d['origin_asns'])}) over {d['duration_hours']:.1f} hours" 
        for d in top_agg
    )
    
    first_record = records[0]
    start_time = floor_to_10min(first_record.first_update)
    end_time = start_time + timedelta(minutes=10)
    
    rpt = (
        f"Between {start_time.astimezone(tz).isoformat()} and {end_time.astimezone(tz).isoformat()}, "
        f"{total_prefixes} unique prefixes showed hijack activity, totaling {total_events} updates from {total_origin_asns} unique origin ASNs. "
        f"Top {TOP_N} prefixes by risk score: {top_list}. "
        f"Details for top {TOP_N}: {details}."
    )

    return {
        'timestamp': start_time.astimezone(tz).isoformat(),
        'scenario_type': 'bgp_hijack',
        'report': rpt
    }

# -----------------------------
# Main
# -----------------------------
def main():
    args = parse_args()
    
    try:
        # TimescaleDB에서 데이터 조회
        records = fetch_hijack_records(args.start_time, args.end_time, args.batch_size)
        if not records:
            logger.error(f"No records found between {args.start_time} and {args.end_time}")
            return

        # Group records by 10-minute buckets
        buckets: Dict[str, List[HijackRecord]] = {}
        for rec in records:
            if not rec.first_update:
                continue
            start = floor_to_10min(rec.first_update)
            key = start.isoformat()
            if key not in buckets:
                buckets[key] = []
            buckets[key].append(rec)

        # Write natural language JSONL reports
        with open(args.output_file, 'w') as out_f:
            # 병렬 처리로 버킷 처리
            with ThreadPoolExecutor() as executor:
                futures = []
                for bucket_records in buckets.values():
                    futures.append(
                        executor.submit(
                            process_bucket,
                            bucket_records,
                            args.timezone
                        )
                    )
                
                # 결과 수집 및 저장
                for future in futures:
                    if result := future.result():
                        out_f.write(json.dumps(result) + "\n")
        
        logger.info(f"✅ Written {len(buckets)} natural language reports to {args.output_file}")
        
    except Exception as e:
        logger.error(f"Error generating reports: {e}")
        raise

if __name__ == '__main__':
    main() 