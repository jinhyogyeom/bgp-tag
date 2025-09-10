import asyncio
import json
import os
import pandas as pd
import psycopg2
from typing import Optional, List, Dict, Any
from fastmcp import FastMCP
from sqlalchemy import create_engine, text
import logging
from datetime import datetime, timedelta
import re

# FastMCP 서버 초기화
mcp = FastMCP(
    name="BGP Analysis Server",
    instructions="이 서버는 BGP 이상 탐지 데이터를 분석하고 자연어 질의를 SQL 쿼리로 변환하여 답변을 제공합니다."
)

# 데이터베이스 연결 설정
TIMESCALE_URI = os.getenv('TIMESCALE_URI', 'postgresql://postgres:postgres@timescaledb:5432/bgp_timeseries')

def get_db_connection():
    """데이터베이스 연결"""
    return psycopg2.connect(TIMESCALE_URI)

def get_sqlalchemy_engine():
    """SQLAlchemy 엔진 생성"""
    return create_engine(TIMESCALE_URI)

# ===== 1단계: 질의 생성 (자연어 → SQL) =====

def parse_time_range(query: str) -> tuple:
    """자연어에서 시간 범위 추출"""
    now = datetime.now()
    
    # 오늘, 어제, 최근 N일 등 패턴 매칭
    if "오늘" in query or "today" in query.lower():
        start = now.replace(hour=0, minute=0, second=0, microsecond=0)
        end = now.replace(hour=23, minute=59, second=59, microsecond=999999)
    elif "어제" in query or "yesterday" in query.lower():
        start = (now - timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
        end = (now - timedelta(days=1)).replace(hour=23, minute=59, second=59, microsecond=999999)
    elif "최근" in query or "recent" in query.lower():
        days = 7  # 기본값
        if "시간" in query:
            hours = int(re.search(r'(\d+)시간', query).group(1))
            start = now - timedelta(hours=hours)
            end = now
        else:
            days_match = re.search(r'(\d+)일', query)
            if days_match:
                days = int(days_match.group(1))
            start = now - timedelta(days=days)
            end = now
    else:
        # 기본값: 최근 24시간
        start = now - timedelta(hours=24)
        end = now
    
    return start, end

def generate_sql_query(query: str) -> str:
    """자연어 질의를 SQL 쿼리로 변환"""
    query_lower = query.lower()
    
    # 시간 범위 추출
    start_time, end_time = parse_time_range(query)
    
    # 기본 SELECT 절
    base_select = """
    SELECT 
        time,
        prefix,
        event_type,
        origin_asns,
        distinct_peers,
        total_events,
        first_update,
        last_update,
        summary
    FROM hijack_events
    WHERE time >= %s AND time <= %s
    """
    
    # 이벤트 타입별 필터링
    if "moas" in query_lower or "moas" in query_lower:
        base_select += " AND event_type = 'MOAS'"
    elif "origin" in query_lower or "오리진" in query:
        base_select += " AND event_type = 'ORIGIN'"
    elif "subprefix" in query_lower or "서브프리픽스" in query:
        base_select += " AND event_type = 'SUBPREFIX'"
    
    # 정렬 및 제한
    if "많은" in query or "상위" in query or "top" in query_lower:
        base_select += " ORDER BY total_events DESC"
    elif "최근" in query or "recent" in query_lower:
        base_select += " ORDER BY time DESC"
    else:
        base_select += " ORDER BY time DESC"
    
    # 결과 수 제한
    if "몇 개" in query or "개수" in query:
        base_select = f"SELECT COUNT(*) as count FROM ({base_select}) as subquery"
    elif "요약" in query or "summary" in query_lower:
        base_select += " LIMIT 10"
    else:
        base_select += " LIMIT 50"
    
    return base_select, start_time, end_time

# ===== 2단계: 질의 실행 =====

def execute_query(sql_query: str, params: tuple = None) -> pd.DataFrame:
    """SQL 쿼리 실행 및 결과 반환"""
    try:
        engine = get_sqlalchemy_engine()
        if params:
            df = pd.read_sql_query(sql_query, engine, params=params)
        else:
            df = pd.read_sql_query(sql_query, engine)
        return df
    except Exception as e:
        print(f"❌ 쿼리 실행 실패: {str(e)}")
        return pd.DataFrame()

# ===== 3단계: 응답 생성 =====

def generate_response(query: str, df: pd.DataFrame) -> str:
    """쿼리 결과를 기반으로 자연어 응답 생성"""
    if df.empty:
        return "❌ 해당 조건에 맞는 데이터를 찾을 수 없습니다."
    
    query_lower = query.lower()
    
    # 개수 조회인 경우
    if "count" in df.columns:
        count = df['count'].iloc[0]
        return f"📊 총 {count}개의 이벤트가 발견되었습니다."
    
    # 이벤트 타입별 분석
    if 'event_type' in df.columns:
        event_counts = df['event_type'].value_counts()
        response = f"📈 BGP 이상 탐지 결과 (총 {len(df)}개 이벤트):\n\n"
        
        for event_type, count in event_counts.items():
            response += f"• {event_type}: {count}개\n"
        
        # 상세 정보
        if len(df) <= 10:
            response += "\n📋 상세 정보:\n"
            for _, row in df.iterrows():
                response += f"  - {row['time']}: {row['prefix']} ({row['event_type']})\n"
                response += f"    {row['summary']}\n\n"
        
        return response
    
    return "📊 데이터 분석이 완료되었습니다."

# ===== MCP 도구들 =====

@mcp.tool()
def analyze_bgp_events(query: str) -> str:
    """BGP 이상 탐지 데이터를 분석합니다.
    
    Args:
        query: 자연어 질의 (예: "오늘 MOAS 이벤트가 몇 개 발생했나?", "최근 Origin hijack 패턴을 보여줘")
    """
    try:
        print(f"🔍 [BGP] 질의 분석: {query}")
        
        # 1단계: 질의 생성
        sql_query, start_time, end_time = generate_sql_query(query)
        print(f"🔍 [BGP] 생성된 SQL: {sql_query}")
        
        # 2단계: 질의 실행
        df = execute_query(sql_query, (start_time, end_time))
        print(f"🔍 [BGP] 쿼리 결과: {len(df)}개 행")
        
        # 3단계: 응답 생성
        response = generate_response(query, df)
        print(f"🔍 [BGP] 응답 생성 완료")
        
        return response
    except Exception as e:
        error_msg = f"❌ BGP 분석 실패: {str(e)}"
        print(f"🔍 [BGP] 오류: {error_msg}")
        return error_msg

@mcp.tool()
def get_bgp_statistics() -> str:
    """BGP 이상 탐지 통계를 조회합니다."""
    try:
        # 전체 통계 쿼리
        stats_query = """
        SELECT 
            event_type,
            COUNT(*) as total_events,
            COUNT(DISTINCT prefix) as unique_prefixes,
            AVG(total_events) as avg_events_per_prefix,
            MIN(time) as first_event,
            MAX(time) as last_event
        FROM hijack_events 
        WHERE time >= NOW() - INTERVAL '7 days'
        GROUP BY event_type
        ORDER BY total_events DESC
        """
        
        df = execute_query(stats_query)
        
        if df.empty:
            return "❌ 최근 7일간 BGP 이상 탐지 데이터가 없습니다."
        
        response = "📊 BGP 이상 탐지 통계 (최근 7일):\n\n"
        
        for _, row in df.iterrows():
            response += f"🔸 {row['event_type']}:\n"
            response += f"  - 총 이벤트: {row['total_events']}개\n"
            response += f"  - 고유 프리픽스: {row['unique_prefixes']}개\n"
            response += f"  - 평균 이벤트/프리픽스: {row['avg_events_per_prefix']:.1f}\n"
            response += f"  - 첫 이벤트: {row['first_event']}\n"
            response += f"  - 마지막 이벤트: {row['last_event']}\n\n"
        
        return response
    except Exception as e:
        return f"❌ 통계 조회 실패: {str(e)}"

@mcp.tool()
def search_specific_prefix(prefix: str, hours: int = 24) -> str:
    """특정 프리픽스의 BGP 이벤트를 검색합니다.
    
    Args:
        prefix: 검색할 프리픽스 (예: "192.168.1.0/24")
        hours: 검색할 시간 범위 (기본값: 24시간)
    """
    try:
        start_time = datetime.now() - timedelta(hours=hours)
        end_time = datetime.now()
        
        query = """
        SELECT 
            time,
            prefix,
            event_type,
            origin_asns,
            distinct_peers,
            total_events,
            summary
        FROM hijack_events 
        WHERE prefix = %s 
        AND time >= %s AND time <= %s
        ORDER BY time DESC
        LIMIT 20
        """
        
        df = execute_query(query, (prefix, start_time, end_time))
        
        if df.empty:
            return f"❌ 프리픽스 '{prefix}'에 대한 최근 {hours}시간 이벤트가 없습니다."
        
        response = f"🔍 프리픽스 '{prefix}' 검색 결과 (최근 {hours}시간, {len(df)}개 이벤트):\n\n"
        
        for _, row in df.iterrows():
            response += f"⏰ {row['time']}\n"
            response += f"   타입: {row['event_type']}\n"
            response += f"   Origin AS: {row['origin_asns']}\n"
            response += f"   피어 수: {row['distinct_peers']}\n"
            response += f"   이벤트 수: {row['total_events']}\n"
            response += f"   요약: {row['summary']}\n\n"
        
        return response
    except Exception as e:
        return f"❌ 프리픽스 검색 실패: {str(e)}"

@mcp.tool()
def get_top_anomalies(limit: int = 10) -> str:
    """가장 많은 이벤트가 발생한 이상 탐지 결과를 조회합니다.
    
    Args:
        limit: 조회할 상위 개수 (기본값: 10)
    """
    try:
        query = """
        SELECT 
            prefix,
            event_type,
            total_events,
            distinct_peers,
            time,
            summary
        FROM hijack_events 
        WHERE time >= NOW() - INTERVAL '24 hours'
        ORDER BY total_events DESC
        LIMIT %s
        """
        
        df = execute_query(query, (limit,))
        
        if df.empty:
            return "❌ 최근 24시간 BGP 이상 탐지 데이터가 없습니다."
        
        response = f"🔥 상위 {len(df)}개 BGP 이상 탐지 이벤트 (최근 24시간):\n\n"
        
        for i, (_, row) in enumerate(df.iterrows(), 1):
            response += f"{i}. {row['prefix']} ({row['event_type']})\n"
            response += f"   이벤트 수: {row['total_events']}개\n"
            response += f"   피어 수: {row['distinct_peers']}개\n"
            response += f"   시간: {row['time']}\n"
            response += f"   요약: {row['summary']}\n\n"
        
        return response
    except Exception as e:
        return f"❌ 상위 이상 탐지 조회 실패: {str(e)}"

if __name__ == "__main__":
    print("🚀 BGP Analysis MCP 서버 시작 (포트: 8001)")
    print("📊 제공 기능:")
    print("  - analyze_bgp_events: 자연어 질의로 BGP 데이터 분석")
    print("  - get_bgp_statistics: BGP 이상 탐지 통계 조회")
    print("  - search_specific_prefix: 특정 프리픽스 검색")
    print("  - get_top_anomalies: 상위 이상 탐지 이벤트 조회")
    
    # streamable-http 모드로 서버 실행
    mcp.run(transport="http", host="0.0.0.0", port=8001) 