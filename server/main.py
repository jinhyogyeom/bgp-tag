"""BGP Anomaly Detection & Analysis API - Main Application"""
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from dotenv import load_dotenv
import uvicorn
import subprocess

from config import setup_logging, init_database
from routers import chat
from routers.invoke import router as invoke_router
from services.agent_service import get_agent

load_dotenv()

# 로깅 설정
logger = setup_logging()

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

# 라우터 포함
app.include_router(invoke_router)
app.include_router(chat.router)

if __name__ == "__main__":
    print("🚀 BGP Anomaly Detection & Analysis API 서버를 시작합니다...")
    print("🌍 서버 URL: http://localhost:8080")
    print("📚 API 문서: http://localhost:8080/docs")
    print("📖 사용 예제: http://localhost:8080/examples")
    print("💚 서버 상태: http://localhost:8080/health")
    print("💬 BGP 채팅: http://localhost:8080/chat")
    
    uvicorn.run(app, host="0.0.0.0", port=8080, log_level="critical")
