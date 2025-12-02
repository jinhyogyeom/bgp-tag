"""LangGraph 워크플로우 생성"""
from langgraph.graph import StateGraph, END
from models.schemas import GraphState
from .graph_nodes import (
    node_1_invoke_current_server,
    node_2_call_other_mcp_server,
    node_3_generate_response
)

def create_workflow():
    """LangGraph 워크플로우를 생성합니다."""
    workflow = StateGraph(GraphState)
    
    # 노드 추가
    workflow.add_node("node_1_invoke", node_1_invoke_current_server)
    workflow.add_node("node_2_other_mcp", node_2_call_other_mcp_server)
    workflow.add_node("node_3_response", node_3_generate_response)
    
    # 엣지 연결: node_1 -> node_2 -> node_3 -> END
    workflow.set_entry_point("node_1_invoke")
    workflow.add_edge("node_1_invoke", "node_2_other_mcp")
    workflow.add_edge("node_2_other_mcp", "node_3_response")
    workflow.add_edge("node_3_response", END)
    
    return workflow.compile()

