from datetime import datetime
from typing import Optional

from pydantic import BaseModel, Field


class MessageIn(BaseModel):
    content: str = Field(..., min_length=1)
    w: int = Field(
        default=1,
        ge=1,
        description="Write concern: num of ACK required (1 = master only)",
    )


class Message(BaseModel):
    id: int = Field(ge=1)
    content: str
    ts: datetime


class MessageOut(BaseModel):
    """
    response model for POST /messages, excludes internal timestamp
    """

    id: int = Field(ge=1)
    content: str


class Ack(BaseModel):
    """
    Acknowledgement
    """

    ok: bool
    node: str
    detail: Optional[str] = None


class ReplicatePayload(BaseModel):
    """
    Internal payload sent from master to secondaries during replication

    just to make clearer code, i.e. we know where master sends to secondaries vs the clients send to master
    """

    id: int = Field(ge=1)
    content: str
    ts: datetime
