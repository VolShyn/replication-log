import asyncio
import logging

from fastapi import APIRouter, HTTPException

from app.pydantic_models import Message, ReplicatePayload
from settings import settings

router = APIRouter()
log = logging.getLogger(settings.role.upper())


@router.post("/replicate", include_in_schema=False)
async def receive_replication(msg: ReplicatePayload):
    """
    Internal endpoint for receiving replicated messages from master
    Implements deduplication and total ordering
    """

    from main import pending_buffer, store

    if settings.role == "master":
        raise HTTPException(
            status_code=405, detail="replicate endpoint only for secondaries"
        )

    # simulate artificial delay to demonstrate eventual consistency
    if settings.repl_delay_secs > 0:
        log.info(f"Simulating delay of {settings.repl_delay_secs}s for msg id={msg.id}")
        await asyncio.sleep(settings.repl_delay_secs)

    # convert to Message obj for storage
    incoming_msg = Message(id=msg.id, content=msg.content, ts=msg.ts)

    # DEDUPLICATION: Check if we already have this message
    existing = await store.get_by_id(msg.id)
    if existing is not None:
        if existing.content == msg.content and existing.ts == msg.ts:
            log.info(f"Dedup: message id={msg.id} already exists, returning OK")
            return {"status": "ok", "id": msg.id, "dedup": True}
        # same ID but different content - conflict
        raise HTTPException(
            status_code=409,
            detail=f"Conflict: message id={msg.id} exists with different content",
        )

    # TOTAL ORDERING: only accept messages in sequence
    expected_id = await store.reserve_id()

    if msg.id == expected_id:
        # this is the next expected message - commit it
        await store.commit(incoming_msg)
        log.info(f"Committed id={msg.id} (expected={expected_id})")

        # check if any buffered messages can now be committed
        await flush_pending_buffer()

        return {"status": "ok", "id": msg.id}

    elif msg.id > expected_id:
        # out of order - buffer for later
        pending_buffer[msg.id] = incoming_msg
        log.warning(
            f"Out-of-order: got id={msg.id}, expected={expected_id}. goes to buffer bye-bye"
        )
        return {"status": "ok", "id": msg.id, "buffered": True}

    else:
        # msg.id < expected_id means we already processed this or there's a gap issue
        # This shouldn't happen with proper dedup, but handle defensively
        raise HTTPException(
            status_code=409, detail=f"Unexpected id={msg.id}, expected >= {expected_id}"
        )


async def flush_pending_buffer() -> None:
    """commit pending messages that are now in sequence"""

    from main import pending_buffer, store

    while True:
        next_expected = await store.reserve_id()
        if next_expected in pending_buffer:
            msg = pending_buffer.pop(next_expected)
            await store.commit(msg)
            log.info(f"Flushed buffered message id={msg.id}")
        else:
            break
