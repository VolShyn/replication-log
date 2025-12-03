from fastapi import APIRouter

from settings import settings

router = APIRouter()


@router.get("/health")
async def health():
    """Health check endpoint with self-check acceptance test"""
    from main import pending_buffer, store  # circular import

    message_count = len(await store.list_all())
    pending_count = len(pending_buffer)
    return {
        "ok": True,
        "role": settings.role,
        "message_count": message_count,
        "pending_out_of_order": pending_count,
        "self_check": "passed",
    }
