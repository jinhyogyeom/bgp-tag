from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from fastapi.middleware.cors import CORSMiddleware
from routers import chat
from dotenv import load_dotenv
import asyncio
import uvicorn
from langchain_mcp_adapters.client import MultiServerMCPClient
from langgraph.prebuilt import create_react_agent
from typing import Optional
import dotenv
import os
import psycopg2
from psycopg2.extras import execute_values
import logging
import subprocess

load_dotenv()

# 로깅 설정
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(
    title="🌐 BGP Anomaly Detection & Analysis API",
    description="BGP 이상 탐지 및 분석을 위한 API with MCP Agent",
    version="1.0.0"
)

# CORS 설정
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# 전역 변수로 에이전트 저장
agent = None

class MessageRequest(BaseModel):
    message: Optional[str] = None
    messages: Optional[str] = None

class MessageResponse(BaseModel):
    response: str
    success: bool
    error: str = None

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

async def get_agent():
    """MCP 에이전트를 초기화하고 반환합니다."""
    global agent
    if agent is None:
        try:
            client = MultiServerMCPClient(
                {
                    "bgp_analysis": {
                        "transport": "streamable_http",
                        "url": "http://localhost:8001/mcp/"
                    }
                }
            )
            tools = await client.get_tools()
            
            # BGP 네트워크 분석 전문가 시스템 프롬프트
            system_prompt = """
당신은 BGP(Border Gateway Protocol) 네트워크 분석 전문가입니다.

역할:
- BGP 이상 탐지 및 네트워크 보안 분석 전문가
- 사용자의 질문을 분석하여 적절한 SQL 쿼리 작성
- 쿼리 결과를 전문적으로 해석하고 인사이트 제공
- BGP 관련 용어와 개념을 쉽게 설명

분석 과정:
1. 먼저 get_bgp_schema()로 테이블 구조 파악
2. 사용자 질문에 맞는 SQL 쿼리 작성
3. execute_bgp_query()로 데이터 조회
4. 결과를 전문적으로 분석하고 설명

BGP 핵심 개념:
- Origin Hijack: 프리픽스 하이재킹
- MOAS: 다중 Origin AS 이상
- AS Path Loop: 라우팅 루프
- Prefix Flapping: 경로 불안정

항상 데이터 기반으로 정확하고 전문적인 분석을 제공하세요.
(시간대, prefix, as 등이 일치하는 데이터가 존재하지 않는 경우 없는 결과를 지어내지 말고 관측된 데이터가 없다고 명시하세요.)
"""
            
            agent = create_react_agent("openai:gpt-4o-mini", tools, system_prompt=system_prompt)
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"에이전트 초기화 실패: {str(e)}")
    return agent

# 앱 시작 시 데이터베이스 초기화 및 MCP 서버 시작
@app.on_event("startup")
async def startup_event():
    init_database()
    subprocess.Popen(["python", "mcp/server.py"], cwd="/app")

@app.get("/")
async def root():
    """API 루트 엔드포인트"""
    return {
        "message": "🌐 BGP Anomaly Detection & Analysis API",
        "version": "1.0.0",
        "endpoints": {
            "/": "API 정보",
            "/invoke": "자연어 명령 처리",
            "/health": "서버 상태 확인",
            "/examples": "사용 예제 목록",
            "/chat": "BGP 채팅 인터페이스"
        }
    }

@app.get("/health")
async def health_check():
    """서버 상태 확인"""
    try:
        agent = await get_agent()
        return {
            "status": "healthy",
            "message": "서버가 정상적으로 작동 중입니다",
            "agent_initialized": agent is not None,
            "database_connected": True
        }
    except Exception as e:
        return {
            "status": "unhealthy",
            "message": f"서버 오류: {str(e)}",
            "agent_initialized": False,
            "database_connected": False
        }

@app.get("/examples")
async def get_examples():
    """사용 가능한 예제 목록을 반환합니다."""
    examples = [
        {
            "category": "BGP 분석",
            "examples": [
                "오늘 BGP 이상 탐지 결과를 보여줘",
                "MOAS 이벤트가 얼마나 발생했나?",
                "Origin hijack 패턴을 분석해줘",
                "BGP flap 현황을 확인해줘"
            ]
        },
        {
            "category": "데이터 조회",
            "examples": [
                "2025-05-25 데이터를 분석해줘",
                "최근 24시간 BGP 이벤트를 보여줘",
                "특정 AS의 BGP 행동을 분석해줘",
                "프리픽스별 이상 패턴을 찾아줘"
            ]
        },
        {
            "category": "복합 명령",
            "examples": [
                "BGP 이상 탐지 결과를 요약하고 주요 패턴을 설명해줘",
                "MOAS와 Origin hijack의 연관성을 분석해줘",
                "BGP 데이터를 시각화해서 보여줘",
                "BGP 보안 위협을 평가하고 대응 방안을 제시해줘"
            ]
        }
    ]
    return {"examples": examples}


@app.post("/invoke", response_model=MessageResponse)
async def invoke(request: MessageRequest):
    """자연어 명령을 처리하고 응답을 반환합니다."""
    try:
        agent = await get_agent()
        # message 또는 messages 필드 사용
        user_message = request.message or request.messages
        if not user_message:
            return MessageResponse(
                response="",
                success=False,
                error="메시지가 제공되지 않았습니다."
            )
        response = await agent.ainvoke({"messages": user_message})
        
        # 응답에서 마지막 메시지 추출
        last_message = response['messages'][-1].content if response['messages'] else "응답이 없습니다."
        
        return MessageResponse(
            response=last_message,
            success=True
        )
    except Exception as e:
        return MessageResponse(
            response="",
            success=False,
            error=str(e)
        )

# 기존 BGP 채팅 라우터 포함
app.include_router(chat.router)

if __name__ == "__main__":
    print("🚀 BGP Anomaly Detection & Analysis API 서버를 시작합니다...")
    print("🌍 서버 URL: http://localhost:8080")
    print("📚 API 문서: http://localhost:8080/docs")
    print("📖 사용 예제: http://localhost:8080/examples")
    print("💚 서버 상태: http://localhost:8080/health")
    print("💬 BGP 채팅: http://localhost:8080/chat")
    
    uvicorn.run(app, host="0.0.0.0", port=8080)
