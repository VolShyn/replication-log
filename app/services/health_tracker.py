import asyncio
import logging
from datetime import datetime
from typing import Dict

import httpx

from app.pydantic_models import SecondaryHealth
from settings import settings

log = logging.getLogger("HEALTH_TRACKER")


class HealthTracker:
    def __init__(self):
        self._status: Dict[str, SecondaryHealth] = {}
        self._missed_beats: Dict[str, int] = {}
        self._last_seen: Dict[str, datetime] = {}
        self._lock = asyncio.Lock()
        self._running = False

        for url in settings.secondaries:
            url_str = str(url)
            self._status[url_str] = SecondaryHealth.HEALTHY
            self._missed_beats[url_str] = 0
            self._last_seen[url_str] = datetime.now()

    async def start(self):
        if settings.role != "master" or self._running:
            return
        self._running = True
        asyncio.create_task(self._heartbeat_loop())
        log.info("Heartbeat tracker started")

    async def _heartbeat_loop(self):
        while self._running:
            await asyncio.sleep(settings.heartbeat_interval_secs)
            await self._check_all_secondaries()

    async def _check_all_secondaries(self):
        timeout = httpx.Timeout(settings.heartbeat_timeout_secs)
        async with httpx.AsyncClient(timeout=timeout) as client:
            for url in settings.secondaries:
                url_str = str(url)
                target = url_str.rstrip("/") + "/health"
                try:
                    r = await client.get(target)
                    r.raise_for_status()
                    await self._mark_healthy(url_str)
                except Exception as e:
                    await self._mark_missed(url_str)
                    log.debug(f"Heartbeat failed for {url_str}: {e}")

    async def _mark_healthy(self, url: str):
        async with self._lock:
            self._missed_beats[url] = 0
            self._last_seen[url] = datetime.now()
            if self._status[url] != SecondaryHealth.HEALTHY:
                log.info(f"{url} is now HEALTHY")
            self._status[url] = SecondaryHealth.HEALTHY

    async def _mark_missed(self, url: str):
        async with self._lock:
            self._missed_beats[url] += 1
            missed = self._missed_beats[url]

            if missed >= settings.unhealthy_threshold:
                if self._status[url] != SecondaryHealth.UNHEALTHY:
                    log.warning(f"{url} is now UNHEALTHY (missed {missed} heartbeats)")
                self._status[url] = SecondaryHealth.UNHEALTHY
            elif missed >= settings.suspect_threshold:
                if self._status[url] != SecondaryHealth.SUSPECTED:
                    log.warning(f"{url} is now SUSPECTED (missed {missed} heartbeats)")
                self._status[url] = SecondaryHealth.SUSPECTED

    async def get_status(self, url: str) -> SecondaryHealth:
        async with self._lock:
            return self._status.get(str(url), SecondaryHealth.UNHEALTHY)

    async def get_all_status(self) -> Dict[str, dict]:
        async with self._lock:
            return {
                url: {
                    "status": self._status[url].value,
                    "missed_heartbeats": self._missed_beats[url],
                    "last_seen": self._last_seen[url].isoformat(),
                }
                for url in self._status
            }

    async def mark_successful_replication(self, url: str):
        """Call this when replication succeeds - acts as implicit heartbeat"""
        await self._mark_healthy(str(url))

    # prevents writing when too many nodes are down
    async def has_quorum(self) -> bool:
        """Check if we have quorum (majority of nodes are healthy or suspected)"""
        async with self._lock:
            total_nodes = 1 + len(settings.secondaries)  # master + secondaries
            healthy_count = 1  # master is always healthy from its own perspective

            for url in self._status:
                if self._status[url] in (
                    SecondaryHealth.HEALTHY,
                    SecondaryHealth.SUSPECTED,
                ):
                    healthy_count += 1

            return healthy_count > total_nodes // 2


health_tracker = HealthTracker()
