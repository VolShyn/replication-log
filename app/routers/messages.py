import asyncio
import logging
from datetime import datetime

from fastapi import APIRouter, HTTPException

from app.pydantic_models import Message, MessageIn
from app.services.replication import replicate_on_background, replicate_one
from settings import settings

router = APIRouter()
log = logging.getLogger(settings.role.upper())


@router.get("/messages", response_model=list[Message])
async def get_messages():
    """
    GET messages is available at any role!

    return: list of all messages
    """
    from main import store

    return await store.list_all()


@router.post("/messages", response_model=Message)
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

    # 3) write concern = 1 is an eventual consistency example
    if write_concern == 1:
        log.info(f"w=1: returning after master commit for id={msg.id}")
        # still replicate to secondaries in background
        if settings.secondaries:
            # start background task
            asyncio.create_task(replicate_on_background(msg))
        return msg

    # 4) for w > 1, we need (w - 1) secondary ACKs before responding
    required_acks = write_concern - 1

    ack_event = asyncio.Event()  # each replica task increments a counter when succeeds
    ack_count = {"value": 0}

    # start all replication tasks concurrently
    tasks = [
        asyncio.create_task(
            replicate_one(str(u), msg, ack_count, required_acks, ack_event)
        )
        for u in settings.secondaries
    ]

    # wait until we have enough ACKs or all tasks complete
    done_waiting = asyncio.create_task(ack_event.wait())
    all_done = asyncio.gather(*tasks, return_exceptions=True)

    # wait for either: required ACKs received OR all tasks finished
    await asyncio.wait([done_waiting, all_done], return_when=asyncio.FIRST_COMPLETED)

    # check if we got enough ACKs
    if ack_count["value"] >= required_acks:
        log.info(
            f"Write concern w={write_concern} satisfied: {ack_count['value']} secondary ACKs for id={msg.id}"
        )
        return msg

    # not enough ACKs - this is a failure for the requested write concern
    log.error(
        f"Failed to satisfy w={write_concern}: only got {ack_count['value']} secondary ACKs for id={msg.id}"
    )
    raise HTTPException(
        status_code=502,
        detail=f"Replication failed: required {required_acks} secondary ACKs, got {ack_count['value']}",
    )
