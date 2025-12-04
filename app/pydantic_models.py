from datetime import datetime
from enum import Enum
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


class SecondaryHealth(str, Enum):
    HEALTHY = "healthy"
    SUSPECTED = "suspected"
    UNHEALTHY = "unhealthy"


class HealthResponse(BaseModel):
    ok: bool
    role: str
    message_count: int
    pending_out_of_order: int
    secondaries: Optional[dict] = None
    has_quorum: Optional[bool] = None
