import asyncio
import logging

import httpx
from fastapi.encoders import jsonable_encoder

from app.pydantic_models import SecondaryHealth
from settings import settings

log = logging.getLogger("REPLICATION_MANAGER")

# I've encountered a problem, when everything seems to be working fine,
# nonetheless, after restarting one of the secondary servers, I saw that it has no messages at all, moreover,
# after POST new message haven't arrived.
# This is partially because my storage is in-memory, but even If it would be on an external drive,
# we still want to be sure, that the data is indeed the same.
# I also like the idea of having some redis server holding this, instead of my coding, however


class ReplicationManager:
    """
    some kind of a storage tracker,
    considers that the master is our sourse of truth


    each secondary has a set tracking which message IDs it has acknowledged,
    a background loop periodically compares these and sends missing messages (difference)

    When a secondary crashes, any in-flight retry loops will die with the request context, nonetheless
    a persistent background loop survives and will catch up the secondary when it recovers,
    regardless of when the original POST happened
    """

    def __init__(self):
        self._running = False

        # track secondaries ACK's as url: set[ids]
        self._delivered: dict[str, set[int]] = {
            str(url): set() for url in settings.secondaries
        }
        # prevent race conditions and corruptions
        self._locks: dict[str, asyncio.Lock] = {
            str(url): asyncio.Lock() for url in settings.secondaries
        }

    async def start(self):
        """
        start background sync loops for all the secondaries

        should be called once during application startup, yet, idempotent
        """
        if settings.role != "master" or self._running:
            return
        self._running = True
        for url in settings.secondaries:
            # task per secondary
            asyncio.create_task(self._sync_loop(str(url)))

        log.info("Replication manager started")

    async def mark_delivered(self, url: str, msg_id: int):
        """
        record that secondary ACK'ed a message
        prevents duplicate sends

        Args:
            url: (e.g. "http://secondary1:8000/")
            msg_id: ID of the successfully delivered message
        """
        async with self._locks[url]:
            self._delivered[url].add(msg_id)

    async def get_pending_count(self, url: str) -> int:
        """
        health router needs to get info about pending list, it's just an async way to provide it
        also, this type of logic must be separated from the router
        """
        from main import store

        all_messages = await store.list_all()
        async with self._locks[url]:
            return len([m for m in all_messages if m.id not in self._delivered[url]])

    async def _sync_loop(self, url: str):
        from app.services.health_tracker import health_tracker
        from main import store

        while True:
            # polling interval is a balance between responsiveness and CPU usage
            await asyncio.sleep(2.0)

            # don't bother trying if the secondary is known to be down (either spams errors)
            health_status = await health_tracker.get_status(url)
            if health_status == SecondaryHealth.UNHEALTHY:
                continue

            #  determine which messages this secondary is missing
            all_messages = await store.list_all()
            async with self._locks[url]:
                missing = [m for m in all_messages if m.id not in self._delivered[url]]

            if not missing:
                continue

            # total ordering, sort by id
            missing.sort(key=lambda m: m.id)

            # below is an attempt to deliver missing data
            target = url.rstrip("/") + "/replicate"
            timeout = httpx.Timeout(settings.repl_timeout_secs, connect=5.0)

            async with httpx.AsyncClient(timeout=timeout) as client:
                for msg in missing:
                    try:
                        r = await client.post(target, json=jsonable_encoder(msg))
                        r.raise_for_status()
                        if r.json().get("status") == "ok":
                            await self.mark_delivered(url, msg.id)
                            await health_tracker.mark_successful_replication(url)
                            log.info(f"Sync delivered id={msg.id} to {url}")
                    except Exception as e:
                        log.debug(f"Sync failed for id={msg.id} to {url}: {e}")
                        break


# module-level instance acts as a singleton (python caches modules)
replication_manager = ReplicationManager()
