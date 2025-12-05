import asyncio
import logging

import httpx
from fastapi.encoders import jsonable_encoder

from app.pydantic_models import Message
from settings import settings

log = logging.getLogger(settings.role.upper())


async def replicate_one(url: str, msg: Message) -> bool:
    """
    Replicate a message to a single secondary with retries.
    Returns True if replication succeeded, False otherwise.
    """
    target = url.rstrip("/") + "/replicate"
    attempt = 0

    # to test blocking without errors, set REPL_DELAY_SECS < REPL_TIMEOUT_SECS
    timeout = httpx.Timeout(settings.repl_timeout_secs, connect=5.0)
    async with httpx.AsyncClient(timeout=timeout) as client:
        # strictly less, because we'll run +1 loop, which is implicit
        while attempt < settings.repl_retries:
            attempt += 1
            try:
                data = jsonable_encoder(msg)
                r = await client.post(target, json=data)
                r.raise_for_status()
                ack = r.json()
                if ack.get("status") == "ok":
                    log.info(f"ACK from {url} for id={msg.id} attempt={attempt}")
                    return True
                else:
                    log.warning(f"Unexpected ACK format from {url}: {ack}")
                    return False

            # separate timeout exceptions from the other exceptions
            except httpx.TimeoutException as e:
                log.warning(f"Timeout to {url} attempt {attempt}: {e}")
                if attempt >= settings.repl_retries:
                    log.error(
                        f"Replication to {url} failed after {attempt} attempts (timeout)"
                    )
                    return False

                # don't sleep long on timeout, just retry
                await asyncio.sleep(0.5)
            except Exception as e:
                if attempt >= settings.repl_retries:
                    log.error(f"Replication to {url} failed: {e}")
                    return False
                backoff = min(0.25 * attempt, 1.0)
                log.warning(f"Retry {attempt} to {url}: {e}")
                await asyncio.sleep(backoff)
    return False
