from pydantic import BaseModel, Field
from typing import List, Optional
from datetime import datetime

class MessageIn(BaseModel):
    content: str = Field(..., min_length=1)

class Message(BaseModel):
    id: int = Field(ge=1)
    content: str
    ts: datetime

class Ack(BaseModel):
    ok: bool
    node: str
    detail: Optional[str] = None
