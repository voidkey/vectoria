from fastapi import Depends, FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.trustedhost import TrustedHostMiddleware

from api.deps import verify_api_key
from api.routes.analyze import router as analyze_router
from api.routes.documents import router as docs_router
from api.routes.health import router as health_router
from api.routes.images import router as images_router
from api.routes.knowledgebase import router as kb_router
from api.routes.query import router as query_router
from config import get_settings

settings = get_settings()

app = FastAPI(title="Vectoria", version="0.1.0")

# --- CORS ---
app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.cors_origins,
    allow_methods=["*"],
    allow_headers=["*"],
)

# --- Public routes (no auth) ---
app.include_router(health_router)

# --- Protected routes (require API key when configured) ---
_auth = [Depends(verify_api_key)]

app.include_router(analyze_router, dependencies=_auth)
app.include_router(kb_router, dependencies=_auth)
app.include_router(docs_router, dependencies=_auth)
app.include_router(query_router, dependencies=_auth)
app.include_router(images_router, dependencies=_auth)


@app.get("/")
async def root():
    return {"service": "vectoria", "version": "0.1.0"}
