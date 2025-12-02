"""Invoke 엔드포인트 라우터"""
from fastapi import APIRouter
from models.schemas import MessageRequest, MessageResponse, GraphState
from workflows.workflow import create_workflow

router = APIRouter()

@router.post("/invoke", response_model=MessageResponse)
async def invoke(request: MessageRequest):
    """자연어 명령을 처리하고 응답을 반환합니다. (LangGraph 워크플로우 사용)"""
    try:
        # message 또는 messages 필드 사용
        user_message = request.message or request.messages
        if not user_message:
            return MessageResponse(
                response="",
                success=False,
                error="메시지가 제공되지 않았습니다."
            )
        
        # 깔끔한 출력을 위한 구분선
        print("\n" + "="*80)
        print(f"자연어 질의 : {user_message}")
        print("="*80)
        print("\n🔄 LangGraph 워크플로우 시작...")
        
        # LangGraph 워크플로우 생성 및 실행
        workflow = create_workflow()
        
        # 초기 상태 설정
        initial_state: GraphState = {
            "user_message": user_message,
            "enhanced_message": "",
            "mcp_response": "",
            "other_mcp_response": "",
            "final_response": "",
            "error": None
        }
        
        # 워크플로우 실행
        result = await workflow.ainvoke(initial_state)
        
        # 오류 체크
        if result.get("error"):
            print(f"\n❌ 워크플로우 오류: {result['error']}\n")
            return MessageResponse(
                response="",
                success=False,
                error=result["error"]
            )
        
        # 최종 응답 추출
        final_response = result.get("final_response", "응답이 없습니다.")
        
        # 깔끔한 응답 출력
        print(f"\n✅ AI 응답 : {final_response}")
        print("="*80 + "\n")
        
        return MessageResponse(
            response=final_response,
            success=True
        )
    except Exception as e:
        print(f"\n❌ 오류 발생: {str(e)}\n")
        return MessageResponse(
            response="",
            success=False,
            error=str(e)
        )

@router.get("/examples")
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

