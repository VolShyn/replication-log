import asyncio
from typing import Dict, List, Optional

from app.pydantic_models import Message


class LogStore:
    """
    Simple message storage logic, some kind of a wrapper around the dict

    explicit Lock is provided, because between the yield and writing to some memory, another
    coroutine can work with the data, e.g:
        val = await ...
        *another coroutine runs and changes val*
        _ = [for i in val]
    """

    def __init__(self) -> None:
        self._messages: Dict[int, Message] = {}
        self._lock = asyncio.Lock()  # use explicit lock to prevent data corruption
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

    # get_by_id to implement the deduplication
    async def get_by_id(self, msg_id: int) -> Optional[Message]:
        async with self._lock:
            return self._messages.get(msg_id)
