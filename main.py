from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from pathlib import Path
from config import get_settings

settings = get_settings()

app = FastAPI(title="Vectoria", version="0.1.0")

# Serve stored images
Path(settings.storage_path).mkdir(parents=True, exist_ok=True)
app.mount("/files", StaticFiles(directory=settings.storage_path), name="files")


@app.get("/")
async def root():
    return {"service": "vectoria", "version": "0.1.0"}
