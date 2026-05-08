from pydantic import BaseModel, Field


class AnalyzeURLRequest(BaseModel):
    url: str
    extract_images: bool = True


class DocumentURLRequest(BaseModel):
    url: str


class DocumentTextRequest(BaseModel):
    text: str = Field(..., min_length=1)
    title: str | None = None


class OutlineItem(BaseModel):
    level: int
    title: str


class ImageInfo(BaseModel):
    id: str
    url: str
    context: str = ""
    type: str = "unknown"


class AnalyzeResponse(BaseModel):
    title: str
    source: str
    content: str
    outline: list[OutlineItem] = []
    image_count: int = 0
    images: list[ImageInfo] = []


class KnowledgeBaseCreate(BaseModel):
    name: str = Field(..., min_length=1, max_length=255)
    description: str = Field(default="", max_length=2000)


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
    chunk_count: int
    status: str
    error_msg: str = ""
    created_at: str


class DocumentIngestResponse(DocumentResponse):
    """Sync response for document upload (file/url).

    No ``image_count`` here: parsing is async, so any value we could put
    in this body would either be 0 (queued/parsing — almost always) or
    only correct under ``?wait=true``. Callers that need the count fetch
    ``GET /documents/{id}`` (returns ``DocumentDetailResponse``) or
    ``GET /documents/{id}/images``.
    """
    content: str = ""
    outline: list[OutlineItem] = []
    image_status: str = "none"


class DocumentDetailResponse(DocumentIngestResponse):
    """Response for GET /documents/{id}. Adds ``image_count`` since by
    the time a caller GETs a doc, parse has had a chance to populate it.
    """
    image_count: int = 0


class DocumentImageResponse(BaseModel):
    id: str
    url: str
    filename: str
    index: int
    width: int | None = None
    height: int | None = None
    aspect_ratio: str = ""
    description: str = ""
    vision_status: str = "pending"
    alt: str = ""
    context: str = ""
    section_title: str = ""
    page: int | None = None


class DocumentImagesListResponse(BaseModel):
    doc_id: str
    images: list[DocumentImageResponse] = []


class DocumentSourceURLResponse(BaseModel):
    doc_id: str
    source_type: str  # "file" or "url"
    url: str
    object_key: str | None = None  # raw storage key for "file" type, None for "url" type


class PaginatedResponse(BaseModel):
    """Generic paginated wrapper."""
    total: int
    offset: int
    limit: int


class KnowledgeBaseListResponse(PaginatedResponse):
    items: list[KnowledgeBaseResponse] = []


class DocumentListResponse(PaginatedResponse):
    items: list[DocumentResponse] = []


class QueryRequest(BaseModel):
    query: str = Field(..., min_length=1, max_length=2000)
    top_k: int = Field(default=5, ge=1, le=100)
    rerank: bool = False
    # Default flipped to False in W6-8 after eval/reports/baseline-2026-04-22.json
    # showed the LLM-driven query rewrite dropped CJK hit@1 from 0.70
    # → 0.55 on the Jaspers-philosophy test KB. Left as a per-request opt-in for
    # cases where the caller has evidence it helps (very short queries,
    # non-Chinese traffic, …).
    query_rewrite: bool = False
    # When True, skip the LLM answer generation and return an empty
    # ``answer`` field; ``sources`` still contains the retrieved chunks.
    # Used by the retrieval evaluation harness (eval/run.py) so one
    # eval cycle drops from ~11 min to under a minute. End users who
    # want to build their own prompts on top of raw retrieval can also
    # opt in.
    retrieve_only: bool = False


class QueryResponse(BaseModel):
    answer: str
    sources: list[dict] = []
