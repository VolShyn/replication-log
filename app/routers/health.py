from fastapi import APIRouter

from app.pydantic_models import HealthResponse
from settings import settings

router = APIRouter()


@router.get("/health", response_model=HealthResponse)
async def health():
    from main import pending_buffer, store

    message_count = len(await store.list_all())
    pending_count = len(pending_buffer)

    result = HealthResponse(
        ok=True,
        role=settings.role,
        message_count=message_count,
        pending_out_of_order=pending_count,
    )

    if settings.role == "master":
        from app.services.health_tracker import health_tracker
        from app.services.replication_manager import replication_manager

        # verify how many messages are pending for the secondary (easy to track, considering the delay)
        secondaries_status = await health_tracker.get_all_status()
        for url in secondaries_status:
            secondaries_status[url][
                "pending_messages"
            ] = await replication_manager.get_pending_count(url)

        result.secondaries = secondaries_status
        result.has_quorum = await health_tracker.has_quorum()

    return result
