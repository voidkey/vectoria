from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from pathlib import Path
from config import get_settings
from api.routes.analyze import router as analyze_router
from api.routes.knowledgebase import router as kb_router
from api.routes.documents import router as docs_router
from api.routes.query import router as query_router
from api.routes.health import router as health_router

settings = get_settings()

app = FastAPI(title="Vectoria", version="0.1.0")

Path(settings.storage_path).mkdir(parents=True, exist_ok=True)
app.mount("/files", StaticFiles(directory=settings.storage_path), name="files")

app.include_router(analyze_router)
app.include_router(kb_router)
app.include_router(docs_router)
app.include_router(query_router)
app.include_router(health_router)


@app.get("/")
async def root():
    return {"service": "vectoria", "version": "0.1.0"}
