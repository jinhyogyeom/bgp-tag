"""MCP 에이전트 관리 서비스"""
from fastapi import HTTPException
from langchain_mcp_adapters.client import MultiServerMCPClient
from langgraph.prebuilt import create_react_agent

# 전역 변수로 에이전트 저장
agent = None

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

