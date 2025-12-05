import logging

# found the info from the Ramalho 2022 book
from contextlib import (
    asynccontextmanager,  # that one is kinda cool
)
from typing import Dict

from fastapi import FastAPI

from app.pydantic_models import Message
from app.routers import health, messages, replication
from app.services.health_tracker import health_tracker
from app.services.replication_manager import replication_manager
from app.storage import LogStore
from settings import settings

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s %(levelname)s [%(name)s] %(message)s"
)

log = logging.getLogger(settings.role.upper())


@asynccontextmanager
async def lifespan(app: FastAPI):
    await health_tracker.start()
    await replication_manager.start()
    yield


app = FastAPI(
    title="Replicated Log — MASTER"
    if settings.role == "master"
    else "Replicated Log — SECONDARY",
    description=(
        "Master node: accepts client POSTs and performs blocking replication to all secondaries."
        if settings.role == "master"
        else "Secondary node: read-only for clients; accepts internal replication from master."
    ),
    version="3.0.0",  # third iteration!
    lifespan=lifespan,
)


app.include_router(health.router)
app.include_router(messages.router)
app.include_router(replication.router)


store = LogStore()

# track pending messages that arrived out of order on secondaries
# maps message_id -> Message waiting to be committed
pending_buffer: Dict[int, Message] = {}
