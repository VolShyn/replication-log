import asyncio
from datetime import datetime
from typing import Dict, List

from pydantic_models import Message


class LogStore:
    def __init__(self) -> None:
        self._messages: Dict[int, Message] = {}
        self._lock = asyncio.Lock()
        self._next_id = 1

    async def reserve_id(self) -> int:
        async with self._lock:
            return self._next_id

    async def commit(self, msg: Message) -> None:
        async with self._lock:
            self._messages[msg.id] = msg
            self._next_id = max(self._next_id, msg.id + 1)

    async def list_all(self) -> List[Message]:
        async with self._lock:
            # return in ID order
            return [self._messages[k] for k in sorted(self._messages.keys())]
