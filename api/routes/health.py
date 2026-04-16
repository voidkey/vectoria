import logging

from fastapi import APIRouter
from sqlalchemy import text

from db.base import SessionLocal
from parsers.registry import registry

logger = logging.getLogger(__name__)

router = APIRouter()


async def _check_db() -> bool:
    try:
        async with SessionLocal() as session:
            await session.execute(text("SELECT 1"))
        return True
    except Exception:
        logger.warning("Health check: database unreachable", exc_info=True)
        return False


async def _check_storage() -> bool:
    try:
        from storage import get_storage
        store = await get_storage()
        # A lightweight HEAD on a non-existent key — succeeds if credentials
        # and bucket are valid (even though the key doesn't exist).
        await store.exists("_health_check_probe")
        return True
    except Exception:
        logger.warning("Health check: object storage unreachable", exc_info=True)
        return False


@router.get("/health")
async def health():
    db_ok = await _check_db()
    storage_ok = await _check_storage()
    all_ok = db_ok and storage_ok
    return {
        "status": "ok" if all_ok else "degraded",
        "checks": {
            "database": db_ok,
            "storage": storage_ok,
        },
        "supported_types": registry.supported_types(),
    }
