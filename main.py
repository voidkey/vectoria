from fastapi import FastAPI
from api.routes.analyze import router as analyze_router
from api.routes.knowledgebase import router as kb_router
from api.routes.documents import router as docs_router
from api.routes.query import router as query_router
from api.routes.health import router as health_router

app = FastAPI(title="Vectoria", version="0.1.0")

app.include_router(analyze_router)
app.include_router(kb_router)
app.include_router(docs_router)
app.include_router(query_router)
app.include_router(health_router)


@app.get("/")
async def root():
    return {"service": "vectoria", "version": "0.1.0"}
