from fastapi import APIRouter
from parsers.registry import registry

router = APIRouter()


@router.get("/health")
async def health():
    return {
        "status": "ok",
        "parsers": registry.list_engines(),
    }
