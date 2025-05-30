from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
import httpx
from retriever import rag_chain
from models.chat_room import (
    create_chat_room,
    get_chat_room,
    ChatRoom,
    ChatRoomListItem,
    get_all_chat_rooms,
)

from datetime import datetime, timedelta

router = APIRouter()

# 메모리에 채팅 기록 저장
chat_history = []


class ChatRequest(BaseModel):
    message: str
    room_id: str


class NewChatRequest(BaseModel):
    entity: str
    entity_type: str
    start_datetime: str = None
    end_datetime: str = None


class ChatResponse(BaseModel):
    response: str


class NewChatResponse(BaseModel):
    room_id: str


# OLLAMA_URL = "http://host.docker.internal:11434/api/chat"
# OLLAMA_MODEL = "gemma3:12b"


@router.get("/chatrooms", response_model=list[ChatRoomListItem])
async def get_chat_rooms():
    return get_all_chat_rooms()


@router.post("/chatrooms", response_model=NewChatResponse)
async def create_new_chat(req: NewChatRequest):
    yesterday = datetime.now() - timedelta(days=1)

    start_datetime = (
        datetime.fromisoformat(req.start_datetime)
        if req.start_datetime
        else yesterday.replace(hour=0, minute=0, second=0, microsecond=0)
    )
    end_datetime = (
        datetime.fromisoformat(req.end_datetime)
        if req.end_datetime
        else yesterday.replace(hour=0, minute=59, second=59, microsecond=0)
    )

    if start_datetime.date() != end_datetime.date():
        raise HTTPException(
            status_code=400, detail="Start and end datetime must be on the same date"
        )

    room = create_chat_room(
        req.entity,
        req.entity_type,
        start_datetime="20250525T000000Z",
        end_datetime="20250525T005959Z",
    )

    return NewChatResponse(room_id=room.id)


@router.get("/chatrooms/{room_id}", response_model=ChatRoom)
async def get_chat_room_details(room_id: str):
    try:
        room = get_chat_room(room_id)
    except KeyError:
        raise HTTPException(status_code=404, detail="Chat room not found")
    return room


@router.post("/chats", response_model=ChatResponse)
async def chat_with_bot(req: ChatRequest):
    try:
        result = rag_chain(
            query=req.message,
            embedding_model="all-MiniLM-L6-v2",
            # llm_model="llama3:8b",
            llm_model="gpt-4o-mini",
            k=100,
            target_date="20250525",
        )
        return {"response": result}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
