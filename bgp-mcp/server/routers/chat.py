from fastapi import APIRouter, HTTPException
import os
from retriever import rag_chain
from models.chat_room import (
    create_chat_room,
    get_chat_room,
    ChatRoom,
    ChatRoomListItem,
    get_all_chat_rooms,
    update_chat_room_history,
)
from models.chat import ChatRequest, NewChatRequest, ChatResponse, NewChatResponse, chat

from datetime import datetime

router = APIRouter()

# 메모리에 채팅 기록 저장
chat_history = []


@router.get("/chatrooms", response_model=list[ChatRoomListItem])
async def get_chat_rooms():
    return get_all_chat_rooms()


@router.post("/chatrooms", response_model=NewChatResponse)
async def create_new_chat(req: NewChatRequest):
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
        result = chat(
            query=req.message,
            target_date="20250525",
            start_datetime=chatroom.start_datetime,
            end_datetime=chatroom.end_datetime,
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

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
