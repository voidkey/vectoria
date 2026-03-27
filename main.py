from contextlib import asynccontextmanager
from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from pathlib import Path
from config import get_settings
from api.routes.analyze import router as analyze_router
from api.routes.knowledgebase import router as kb_router
from api.routes.documents import router as docs_router

settings = get_settings()

# Must create before StaticFiles mount (FastAPI requires directory to exist)
Path(settings.storage_path).mkdir(parents=True, exist_ok=True)


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Additional startup work goes here in future tasks
    yield


app = FastAPI(title="Vectoria", version="0.1.0", lifespan=lifespan)
app.mount("/files", StaticFiles(directory=settings.storage_path), name="files")
app.include_router(analyze_router)
app.include_router(kb_router)
app.include_router(docs_router)


@app.get("/")
async def root():
    return {"service": "vectoria", "version": "0.1.0"}
