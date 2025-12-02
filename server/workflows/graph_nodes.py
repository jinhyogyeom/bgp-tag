"""LangGraph 노드 함수들"""
import asyncio
import httpx
from models.schemas import GraphState
from services.agent_service import get_agent

async def node_1_invoke_current_server(state: GraphState) -> GraphState:
    """노드 1: 현재 서버의 invoke 과정 (기존 MCP agent 호출)"""
    try:
        print("\n[노드 1] 현재 MCP 서버 호출 시작...")
        
        agent = await get_agent()
        user_message = state["user_message"]
        
        # 시스템 지침 포함한 메시지 구성
        enhanced_message = f"먼저 get_system_instructions()를 호출하여 당신의 역할과 지침을 확인한 후, 다음 사용자 질문에 답해주세요: {user_message}"
        
        # MCP agent 호출
        response = await agent.ainvoke({"messages": enhanced_message})
        
        # 응답 추출
        mcp_response = response['messages'][-1].content if response['messages'] else "응답이 없습니다."
        
        print(f"[노드 1] MCP 응답: {mcp_response[:100]}...")
        
        return {
            **state,
            "enhanced_message": enhanced_message,
            "mcp_response": mcp_response
        }
    except Exception as e:
        print(f"[노드 1] 오류 발생: {str(e)}")
        return {
            **state,
            "error": f"노드 1 오류: {str(e)}"
        }

async def node_2_call_other_mcp_server(state: GraphState) -> GraphState:
    """노드 2: 여러 MCP 서버를 병렬로 호출"""
    try:
        print("\n[노드 2] 여러 MCP 서버로 병렬 요청 시작...")
        
        user_message = state["user_message"]
        
        # 호출할 MCP 서버 목록 (필요에 따라 수정)
        mcp_servers = [
            {"name": "서버1", "url": "http://localhost:8002/invoke"},
            {"name": "서버2", "url": "http://localhost:8003/invoke"},
            {"name": "서버3", "url": "http://localhost:8004/invoke"},
            {"name": "서버4", "url": "http://localhost:8005/invoke"},
        ]
        
        async def call_server(server_info):
            """개별 서버 호출"""
            try:
                async with httpx.AsyncClient(timeout=60.0) as client:
                    response = await client.post(
                        server_info["url"],
                        json={"message": user_message}
                    )
                    if response.status_code == 200:
                        result = response.json()
                        return {
                            "name": server_info["name"],
                            "response": result.get("response", "응답 없음"),
                            "success": True
                        }
                    else:
                        return {
                            "name": server_info["name"],
                            "response": f"HTTP {response.status_code} 오류",
                            "success": False
                        }
            except Exception as e:
                return {
                    "name": server_info["name"],
                    "response": f"오류: {str(e)}",
                    "success": False
                }
        
        # 병렬 호출
        results = await asyncio.gather(*[call_server(server) for server in mcp_servers])
        
        # 결과 정리
        other_mcp_response = "\n\n".join([
            f"[{r['name']}]\n{r['response']}" for r in results
        ])
        
        print(f"[노드 2] {len(results)}개 서버 응답 완료")
        
        return {
            **state,
            "other_mcp_response": other_mcp_response
        }
    except Exception as e:
        print(f"[노드 2] 오류 발생: {str(e)}")
        return {
            **state,
            "other_mcp_response": f"[오류: {str(e)}]"
        }

async def node_3_generate_response(state: GraphState) -> GraphState:
    """노드 3: 응답 생성 및 후처리"""
    try:
        print("\n[노드 3] 최종 응답 생성 시작...")
        
        mcp_response = state.get("mcp_response", "")
        other_mcp_response = state.get("other_mcp_response", "")
        
        # 두 응답을 결합하여 최종 응답 생성
        final_response = f"""[BGP 분석 서버 응답]
{mcp_response}

[추가 MCP 서버 응답들]
{other_mcp_response}
"""
        
        print(f"[노드 3] 최종 응답 생성 완료")
        
        return {
            **state,
            "final_response": final_response.strip()
        }
    except Exception as e:
        print(f"[노드 3] 오류 발생: {str(e)}")
        return {
            **state,
            "error": f"노드 3 오류: {str(e)}"
        }

