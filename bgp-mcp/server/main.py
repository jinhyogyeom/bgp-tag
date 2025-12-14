from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from fastapi.middleware.cors import CORSMiddleware
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

# 불필요한 로그 제거
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("httpcore").setLevel(logging.WARNING)
logging.getLogger("uvicorn.access").setLevel(logging.WARNING)
logging.getLogger("mcp.client").setLevel(logging.WARNING)

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
            
            agent = create_react_agent("openai:gpt-4o", tools)
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"에이전트 초기화 실패: {str(e)}")
    return agent

# 앱 시작 시 데이터베이스 초기화 및 MCP 서버 시작
@app.on_event("startup")
async def startup_event():
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
        
        # 첫 번째 요청에 시스템 지침 포함하도록 메시지 구성
        enhanced_message = f"먼저 get_system_instructions()를 호출하여 당신의 역할과 지침을 확인한 후, 다음 사용자 질문에 답해주세요: {user_message}"
        #langgraph로 호출 강제?

        response = await agent.ainvoke({"messages": enhanced_message})
        
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
if __name__ == "__main__":
    print("🚀 BGP Anomaly Detection & Analysis API 서버를 시작합니다...")
    print("🌍 서버 URL: http://localhost:8080")
    print("📚 API 문서: http://localhost:8080/docs")
    print("📖 사용 예제: http://localhost:8080/examples")
    print("💚 서버 상태: http://localhost:8080/health")
    print("💬 BGP 채팅: http://localhost:8080/chat")
    
    uvicorn.run(app, host="0.0.0.0", port=8080)
