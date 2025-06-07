from pydantic import BaseModel
from datetime import datetime


class ChatRequest(BaseModel):
    message: str
    room_id: str


class NewChatRequest(BaseModel):
    entity: str = None
    entity_type: str = None
    start_datetime: str
    end_datetime: str

    def __init__(self, **data):
        super().__init__(**data)
        try:
            start_datetime = datetime.strptime(self.start_datetime, "%Y-%m-%dT%H:%M")
            end_datetime = datetime.strptime(self.end_datetime, "%Y-%m-%dT%H:%M")
        except ValueError:
            raise ValueError(
                "Invalid datetime format. Expected format: YYYY-MM-DDThh:mm"
            )

        if start_datetime.date() != end_datetime.date():
            raise ValueError("Start and end datetime must be on the same date")


class ChatResponse(BaseModel):
    response: str


class NewChatResponse(BaseModel):
    room_id: str
