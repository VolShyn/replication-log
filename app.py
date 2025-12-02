import asyncio
import logging
from datetime import datetime

import httpx
from fastapi import FastAPI, HTTPException
from fastapi.encoders import jsonable_encoder

from pydantic_models import Message, MessageIn
from settings import settings
from state import LogStore

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
    version="1.0.0",
)

store = LogStore()


# added self-check acceptance test
@app.get("/health")
async def health():
    message_count = len(await store.list_all())
    return {
        "ok": True,
        "role": settings.role,
        "message_count": message_count,
        "self_check": "passed",
    }


@app.get("/messages", response_model=list[Message])
async def get_messages():
    return await store.list_all()


@app.post("/messages", response_model=Message)
async def append_message(payload: MessageIn):
    if settings.role != "master":
        raise HTTPException(status_code=405, detail="POST only allowed on master")

    # 1) reserve id and create message
    msg_id = await store.reserve_id()
    msg = Message(id=msg_id, content=payload.content, ts=datetime.utcnow())

    # 2) write locally first (write-ahead)
    await store.commit(msg)
    log.info(
        f"Committed locally id={msg.id} content_length={len(msg.content)} ts={msg.ts.isoformat()}"
    )

    # 3) replicate synchronously to all secondaries; wait for ACKs
    if not settings.secondaries:
        log.info("No secondaries configured; returning immediately.")
        return msg

    # helper to post with retries
    async def replicate_one(url: str) -> None:
        target = url.rstrip("/") + "/replicate"
        attempt = 0

        # to test blocking without errors, set REPL_DELAY_SECS < REPL_TIMEOUT_SECS
        timeout = httpx.Timeout(settings.repl_timeout_secs)
        async with httpx.AsyncClient(timeout=timeout) as client:
            while True:
                attempt += 1
                try:
                    payload = jsonable_encoder(msg)  # otherwise we'll get error
                    # because datetime is not serializable
                    r = await client.post(target, json=payload)
                    r.raise_for_status()
                    # expecting an ACK-like body
                    ack = r.json()
                    if ack.get("status") == "ok":
                        log.info(f"ACK from {url} for id={msg.id} attempt={attempt}")
                        return
                    raise RuntimeError(f"Bad ACK from {url}: {ack}")
                except Exception as e:
                    if attempt > settings.repl_retries:
                        log.error(
                            f"Replication failed to {url} after {attempt} attempts: {e}"
                        )
                        raise
                    backoff = min(0.25 * attempt, 1.0)
                    log.warning(f"Retry {attempt} to {url} in {backoff:.2f}s ... ({e})")
                    await asyncio.sleep(backoff)

    # run replications concurrently but wait for ALL (blocking)
    try:
        await asyncio.gather(*(replicate_one(str(u)) for u in settings.secondaries))
    except Exception:
        raise HTTPException(status_code=502, detail="Replication failed")

    log.info(
        f"Replication complete for id={msg.id} secondaries={len(settings.secondaries)}"
    )

    return msg


# internal; If we'll call it by hand, it will create entries that the master never saw
@app.post("/replicate", include_in_schema=False)
async def receive_replication(msg: Message):
    if settings.role == "master":
        raise HTTPException(
            status_code=405, detail="replicate endpoint only for secondaries"
        )

    if settings.repl_delay_secs > 0:
        log.info(
            f"Simulating replication delay of {settings.repl_delay_secs}s for msg id={msg.id}"
        )
        await asyncio.sleep(
            settings.repl_delay_secs
        )  # changed to parse it from the settings

    # idempotency: if already have this id, ensure it's the same record
    existing = {m.id: m for m in await store.list_all()}
    if msg.id in existing:
        prev = existing[msg.id]
        if prev.content == msg.content and (msg.ts is None or prev.ts == msg.ts):
            return {"status": "ok", "id": msg.id}  # duplicate apply = OK
        raise HTTPException(
            status_code=409, detail="conflicting replication payload for existing id"
        )

    # strict ordering: only accept the exact next id
    expected = await store.reserve_id()
    if msg.id != expected:
        raise HTTPException(
            status_code=409,
            detail=f"out-of-order id: expected {expected}, got {msg.id}",
        )

    # commit with master ts if provided, else assign now (for manual tests)
    ts = msg.ts or datetime.utcnow()
    await store.commit(Message(id=msg.id, content=msg.content, ts=ts))
    return {"status": "ok", "id": msg.id}
