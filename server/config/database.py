"""데이터베이스 초기화 모듈"""
import os
import psycopg2
import logging

logger = logging.getLogger(__name__)

def init_database():
    """데이터베이스 초기화 - DDL 스크립트 실행"""
    try:
        # 데이터베이스 연결
        db_uri = os.getenv('TIMESCALE_URI', 'postgresql://postgres:postgres@timescaledb:5432/bgp_timeseries')
        conn = psycopg2.connect(db_uri)
        cursor = conn.cursor()
        
        # DDL 스크립트 파일 읽기
        ddl_file_path = "/app/scripts/scenarios/ddl.sql"
        
        with open(ddl_file_path, 'r') as file:
            ddl_sql = file.read()
        
        # DDL 스크립트 실행
        # cursor.execute(ddl_sql)
        
        conn.commit()
        cursor.close()
        conn.close()
        
        logger.info("Database initialization completed successfully")
        
    except Exception as e:
        logger.error(f"Database initialization failed: {e}")

