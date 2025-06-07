from typing import Dict, List
from pydantic import BaseModel
import uuid


class ChatRoom(BaseModel):
    id: str
    entity: str
    entity_type: str
    start_datetime: str
    end_datetime: str
    history: List[dict]


class ChatRoomListItem(BaseModel):
    id: str
    entity: str
    entity_type: str
    start_datetime: str
    end_datetime: str


# 전역 채팅방 저장소
chat_rooms: Dict[str, ChatRoom] = {}


def create_chat_room(
    entity: str, entity_type: str, start_datetime: str, end_datetime: str
) -> ChatRoom:
    room_id = str(uuid.uuid4())
    room = ChatRoom(
        id=room_id,
        entity=entity,
        entity_type=entity_type,
        start_datetime=start_datetime,
        end_datetime=end_datetime,
        history=[],
    )
    chat_rooms[room_id] = room
    return room


def get_chat_room(room_id: str) -> ChatRoom:
    return chat_rooms[room_id]


def get_all_chat_rooms() -> List[ChatRoomListItem]:
    return [
        ChatRoomListItem(
            id=room.id,
            entity=room.entity,
            entity_type=room.entity_type,
            start_datetime=room.start_datetime,
            end_datetime=room.end_datetime,
        )
        for room in chat_rooms.values()
    ]


def update_chat_room_history(role: str, room_id: str, message: dict) -> None:
    if room_id in chat_rooms:
        chat_rooms[room_id].history.append(
            {
                "role": role,
                "message": message,
            }
        )
    else:
        raise ValueError(f"Chat room with id {room_id} does not exist.")
