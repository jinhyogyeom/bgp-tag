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
    update_chat_room_history,
)

from datetime import datetime, timedelta

router = APIRouter()

# 메모리에 채팅 기록 저장
chat_history = []


class ChatRequest(BaseModel):
    message: str
    room_id: str


class NewChatRequest(BaseModel):
    entity: str = None
    entity_type: str = None
    start_datetime: str
    end_datetime: str


class ChatResponse(BaseModel):
    response: str


class NewChatResponse(BaseModel):
    room_id: str


@router.get("/chatrooms", response_model=list[ChatRoomListItem])
async def get_chat_rooms():
    return get_all_chat_rooms()


@router.post("/chatrooms", response_model=NewChatResponse)
async def create_new_chat(req: NewChatRequest):
    try:
        start_datetime = datetime.strptime(req.start_datetime, "%Y-%m-%dT%H:%M")
        end_datetime = datetime.strptime(req.end_datetime, "%Y-%m-%dT%H:%M")
    except ValueError:
        raise HTTPException(
            status_code=400,
            detail="Invalid datetime format. Expected format: YYYY-MM-DDThh:mm",
        )

    if start_datetime.date() != end_datetime.date():
        raise HTTPException(
            status_code=400, detail="Start and end datetime must be on the same date"
        )

    room = create_chat_room(
        req.entity,
        req.entity_type,
        start_datetime=req.start_datetime,
        end_datetime=req.end_datetime,
    )

    update_chat_room_history(
        role="assistant",
        room_id=room.id,
        message=f"{req.start_datetime}부터 {req.end_datetime}까지의 데이터가 적재되었습니다.",
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
    room_id = req.room_id
    if room_id not in [room.id for room in get_all_chat_rooms()]:
        raise HTTPException(status_code=404, detail="Chat room not found")

    chatroom = get_chat_room(room_id)

    try:
        result = rag_chain(
            query=req.message,
            embedding_model="all-MiniLM-L6-v2",
            # llm_model="gemma3:1b",
            llm_model="gpt-4o-mini",
            k=100,
            target_date="20250525",
            start_datetime=chatroom.start_datetime,
            end_datetime=chatroom.end_datetime,
        )

        update_chat_room_history(
            role="user",
            room_id=room_id,
            message=req.message,
        )
        update_chat_room_history(
            role="assistant",
            room_id=room_id,
            message=result,
        )

        return {"response": result}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
