from pydantic import BaseModel
from typing import Optional


class ImageInfo(BaseModel):
    id: str
    url: str
    context: str = ""
    type: str = "unknown"


class AnalyzeResponse(BaseModel):
    title: str
    source: str
    markdown: str
    images: list[ImageInfo] = []


class KnowledgeBaseCreate(BaseModel):
    name: str
    description: str = ""


class KnowledgeBaseResponse(BaseModel):
    id: str
    name: str
    description: str
    created_at: str


class DocumentResponse(BaseModel):
    id: str
    kb_id: str
    title: str
    source: str
    engine: str
    chunk_count: int
    created_at: str


class QueryRequest(BaseModel):
    query: str
    top_k: int = 5
    rerank: bool = False
    query_rewrite: bool = True


class QueryResponse(BaseModel):
    answer: str
    sources: list[dict] = []
