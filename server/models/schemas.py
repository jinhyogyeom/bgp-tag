"""Pydantic 모델 및 State 정의"""
from pydantic import BaseModel
from typing import Optional, TypedDict

class MessageRequest(BaseModel):
    message: Optional[str] = None
    messages: Optional[str] = None

class MessageResponse(BaseModel):
    response: str
    success: bool
    error: str = None

# LangGraph State 정의
class GraphState(TypedDict):
    user_message: str
    enhanced_message: str
    mcp_response: str
    other_mcp_response: str
    final_response: str
    error: Optional[str]

