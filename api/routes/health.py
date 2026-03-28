from fastapi import APIRouter
from parsers.registry import registry

router = APIRouter()


@router.get("/health")
async def health():
    return {
        "status": "ok",
        "supported_types": registry.supported_types(),
    }
