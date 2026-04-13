import logging

from fastapi import Depends, FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.trustedhost import TrustedHostMiddleware
from fastapi.responses import JSONResponse

from api.deps import verify_api_key
from api.errors import AppError, ErrorCode
from api.routes.analyze import router as analyze_router
from api.routes.documents import router as docs_router
from api.routes.health import router as health_router
from api.routes.images import router as images_router
from api.routes.knowledgebase import router as kb_router
from api.routes.query import router as query_router
from config import get_settings

settings = get_settings()

app = FastAPI(title="Vectoria", version="0.1.0", root_path="/vectoria")

# --- CORS ---
# TODO: 部署生产环境前将 allow_origins 从 "*" 改为具体的域名列表
app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.cors_origins,
    allow_methods=["*"],
    allow_headers=["*"],
)

# --- Public routes (no auth, no version prefix) ---
app.include_router(health_router)

# --- Protected routes (require API key when configured) ---
_auth = [Depends(verify_api_key)]

app.include_router(analyze_router, prefix="/v1", dependencies=_auth)
app.include_router(kb_router, prefix="/v1", dependencies=_auth)
app.include_router(docs_router, prefix="/v1", dependencies=_auth)
app.include_router(query_router, prefix="/v1", dependencies=_auth)
app.include_router(images_router, prefix="/v1", dependencies=_auth)


logger = logging.getLogger(__name__)


@app.exception_handler(AppError)
async def app_error_handler(request: Request, exc: AppError):
    return JSONResponse(
        status_code=exc.status_code,
        content={"code": exc.code, "detail": exc.detail},
    )


@app.exception_handler(RequestValidationError)
async def validation_error_handler(request: Request, exc: RequestValidationError):
    return JSONResponse(
        status_code=422,
        content={"code": ErrorCode.VALIDATION_ERROR, "detail": str(exc)},
    )


@app.exception_handler(Exception)
async def unhandled_error_handler(request: Request, exc: Exception):
    logger.exception("Unhandled error")
    return JSONResponse(
        status_code=500,
        content={"code": ErrorCode.INTERNAL_ERROR, "detail": "Internal server error"},
    )


@app.get("/")
async def root():
    return {"service": "vectoria", "version": "0.1.0"}
