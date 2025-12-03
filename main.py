import logging
from typing import Dict

from fastapi import FastAPI

from app.pydantic_models import Message
from app.routers import health, messages, replication
from app.storage import LogStore
from settings import settings

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s %(levelname)s [%(name)s] %(message)s"
)

log = logging.getLogger(settings.role.upper())

app = FastAPI(
    title="Replicated Log – MASTER"
    if settings.role == "master"
    else "Replicated Log – SECONDARY",
    description=(
        "Master node: accepts client POSTs and performs blocking replication to all secondaries."
        if settings.role == "master"
        else "Secondary node: read-only for clients; accepts internal replication from master."
    ),
    version="2.0.0",  # iteration of the project
)


app.include_router(health.router)
app.include_router(messages.router)
app.include_router(replication.router)


store = LogStore()

# track pending messages that arrived out of order on secondaries
# maps message_id -> Message waiting to be committed
pending_buffer: Dict[int, Message] = {}
