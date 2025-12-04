import asyncio
import logging

import httpx
from fastapi.encoders import jsonable_encoder

from app.pydantic_models import Message, SecondaryHealth
from settings import settings

log = logging.getLogger(settings.role.upper())


async def replicate_one(
    url: str,
    msg: Message,
    ack_count: dict,
    required_acks: int,
    ack_event: asyncio.Event,
) -> bool:
    from app.services.health_tracker import health_tracker
    from app.services.replication_manager import replication_manager

    target = url.rstrip("/") + "/replicate"
    attempt = 0
    ack_lock = asyncio.Lock()

    while True:  # infinite retries for w > 1
        attempt += 1

        # smart delay based on health status
        health_status = await health_tracker.get_status(url)

        if attempt > 1:
            if health_status == SecondaryHealth.UNHEALTHY:
                delay = min(5.0 * attempt, 30.0)  # longer waits for unhealthy nodes
            elif health_status == SecondaryHealth.SUSPECTED:
                delay = min(1.0 * attempt, 10.0)
            else:
                delay = min(0.5 * attempt, 5.0)

            log.info(
                f"Retry {attempt} to {url} (status={health_status.value}), waiting {delay}s"
            )
            await asyncio.sleep(delay)

        # to test blocking without errors, set REPL_DELAY_SECS < REPL_TIMEOUT_SECS
        timeout = httpx.Timeout(settings.repl_timeout_secs, connect=5.0)
        try:
            async with httpx.AsyncClient(timeout=timeout) as client:
                data = jsonable_encoder(msg)
                r = await client.post(target, json=data)
                r.raise_for_status()
                ack = r.json()

                if ack.get("status") == "ok":
                    log.info(f"ACK from {url} for id={msg.id} attempt={attempt}")
                    await health_tracker.mark_successful_replication(url)
                    await replication_manager.mark_delivered(url, msg.id)

                    async with ack_lock:
                        ack_count["value"] += 1
                        if ack_count["value"] >= required_acks:
                            ack_event.set()
                    return True
                else:
                    log.warning(f"Unexpected ACK format from {url}: {ack}")

        # separate timeout exceptions from the other exceptions
        except httpx.TimeoutException as e:
            log.warning(f"Timeout to {url} attempt {attempt}: {e}")
        except httpx.ConnectError as e:
            log.warning(f"Connection failed to {url} attempt {attempt}: {e}")
        except Exception as e:
            log.warning(f"Replication error to {url} attempt {attempt}: {e}")


# I wont change the background one, because, this can have limited retries, since it is only for eventual consistency,
# so we do not guarantee delivery during the request
async def replicate_on_background(msg: Message) -> None:
    """Fire-and-forget replication for w=1 scenarios (eventual consistency)"""
    for url in settings.secondaries:
        target = str(url).rstrip("/") + "/replicate"
        attempt = 0
        timeout = httpx.Timeout(settings.repl_timeout_secs)

        async with httpx.AsyncClient(timeout=timeout) as client:
            while attempt <= settings.repl_retries:
                attempt += 1
                try:
                    data = jsonable_encoder(msg)
                    r = await client.post(target, json=data)
                    r.raise_for_status()
                    log.info(
                        f"Background replication to {url} succeeded for id={msg.id}"
                    )
                    break
                except Exception as e:
                    if attempt > settings.repl_retries:
                        log.error(
                            f"Background replication to {url} failed for id={msg.id}: {e}"
                        )
                        break
                    backoff = min(0.25 * attempt, 1.0)
                    await asyncio.sleep(backoff)
