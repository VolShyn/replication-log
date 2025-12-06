import asyncio
import logging
from datetime import datetime

from fastapi import APIRouter, HTTPException

from app.pydantic_models import Message, MessageIn, MessageOut
from app.services.replication import replicate_one
from settings import settings

router = APIRouter()
log = logging.getLogger(settings.role.upper())


@router.get("/messages", response_model=list[MessageOut])
async def get_messages():
    """
    GET messages is available at any role!

    return: list of all messages
    """
    from main import store

    return await store.list_all()


@router.post("/messages", response_model=MessageOut)
async def append_message(payload: MessageIn):
    """
    Append a new message to the log (master only)
    Supports write concern for replication
    """
    from main import store

    if settings.role != "master":
        raise HTTPException(status_code=405, detail="POST only allowed on master")

    write_concern = payload.w
    num_secondaries = len(settings.secondaries)

    # validate write concern (master + secondaries)
    max_w = 1 + num_secondaries
    if write_concern > max_w:
        raise HTTPException(
            status_code=400,
            detail=f"Write concern w={write_concern} exceeds available nodes ({max_w}) ",
        )

    # 1) reserve id and create message
    msg_id = await store.reserve_id()
    msg = Message(
        id=msg_id, content=payload.content, ts=datetime.now()
    )  # .now == utcnow()

    # 2) write locally first (write-ahead)
    await store.commit(msg)
    log.info(
        f"Committed locally id={msg.id} content_length={len(msg.content)} ts={msg.ts.isoformat()}"
    )

    # 3) replication logic
    # required_acks = w - 1
    required_acks = write_concern - 1

    # start all replication tasks concurrently
    tasks = [
        asyncio.create_task(replicate_one(str(url), msg))
        for url in settings.secondaries
    ]

    # w=1: return immediately after master commit (fire-and-forget replication)
    if required_acks == 0:
        log.info(f"w=1 satisfied: master commit for id={msg.id}")
        return msg

    # w>1: wait for required secondary ACKs
    acks = 0
    for coro in asyncio.as_completed(tasks):
        if await coro:
            acks += 1
            if acks >= required_acks:
                log.info(f"w={write_concern} satisfied: {acks} ACKs for id={msg.id}")
                return msg

    log.error(
        f"w={write_concern} failed: got {acks}/{required_acks} ACKs for id={msg.id}"
    )
    raise HTTPException(
        status_code=502,
        detail=f"Replication failed: got {acks}/{required_acks} secondary ACKs",
    )
