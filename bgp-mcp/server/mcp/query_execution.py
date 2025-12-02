import os
import pandas as pd
from sqlalchemy import create_engine
from typing import Optional, Tuple

# 데이터베이스 연결 설정
TIMESCALE_URI = os.getenv('TIMESCALE_URI', 'postgresql://postgres:postgres@timescaledb:5432/bgp_timeseries')

def execute_query(sql_query: str, params: Tuple = None) -> pd.DataFrame:
    """SQL 쿼리 실행 및 결과 반환"""
    try:
        print(f"SQL: {sql_query}")
        
        engine = create_engine(TIMESCALE_URI)
        if params:
            df = pd.read_sql_query(sql_query, engine, params=params)
        else:
            df = pd.read_sql_query(sql_query, engine)
        
        print(f"결과: {len(df)}개 행")
        
        return df
    except Exception as e:
        print(f"쿼리 실행 실패: {str(e)}")
        return pd.DataFrame()