from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from routers import chat
import dotenv
import os
import psycopg2
from psycopg2.extras import execute_values
import logging

dotenv.load_dotenv()

# 로깅 설정
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI()

# CORS 설정
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

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
        cursor.execute(ddl_sql)
        
        conn.commit()
        cursor.close()
        conn.close()
        
        logger.info("Database initialization completed successfully")
        
    except Exception as e:
        logger.error(f"Database initialization failed: {e}")

# 앱 시작 시 데이터베이스 초기화
@app.on_event("startup")
async def startup_event():
    init_database()

app.include_router(chat.router)
