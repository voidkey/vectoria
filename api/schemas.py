from pydantic import BaseModel, Field


class AnalyzeURLRequest(BaseModel):
    url: str
    extract_images: bool = True


class DocumentURLRequest(BaseModel):
    url: str


class DocumentTextRequest(BaseModel):
    text: str = Field(..., min_length=1)
    title: str | None = None


class CreateUploadRequest(BaseModel):
    filename: str = Field(..., min_length=1)
    # Advisory hints only (early-reject / skip-upload). Never trusted for
    # correctness — the real gates run on the actual bytes at `complete`.
    sha256: str | None = None
    size: int | None = None


class CreateCaptureRequest(BaseModel):
    url: str
    max_screenshots: int | None = None


class CaptureResponse(BaseModel):
    id: str
    kb_id: str
    status: str
    image_status: str = "none"
    error_msg: str = ""
    # Structured error, derived from Document.error_code via ERROR_META.
    # All three are None on success / legacy rows.
    error_code: int | None = None
    retryable: bool | None = None
    suggested_action: str | None = None
    profile: dict | None = None
    created_at: str


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
    index_status: str = "pending"
    error_msg: str = ""
    # Structured error, derived from Document.error_code via ERROR_META.
    # All three are None on success / legacy rows.
    error_code: int | None = None
    retryable: bool | None = None
    suggested_action: str | None = None
    created_at: str
    # Caller-supplied edited rendition, orthogonal to ``content``.
    # ``has_edit`` is the field to branch on — ``edited_revision`` is
    # monotonic and stays non-zero after a withdrawn edit, so it is NOT a
    # "has an edit" signal. Defaults here mean the ingest-time builders
    # (which can never have an edit yet) need no extra plumbing.
    has_edit: bool = False
    edited_revision: int = 0
    edited_at: str | None = None


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


class CreateUploadResponse(BaseModel):
    # When a duplicate is detected up front, no URL is minted and `document`
    # carries the existing doc. Otherwise the upload_url/upload_id/expires_at
    # fields are populated for a direct PUT.
    dedup_hit: bool = False
    document: DocumentIngestResponse | None = None
    upload_id: str | None = None
    upload_url: str | None = None
    method: str | None = None
    expires_at: str | None = None


class DocumentDetailResponse(DocumentIngestResponse):
    """Response for GET /documents/{id}. Adds ``image_count`` since by
    the time a caller GETs a doc, parse has had a chance to populate it.
    """
    image_count: int = 0
    # PDF pages / PPTX slides. ``None`` for non-paginated sources
    # (docx, html, plain text) and for legacy binary .doc — Word's
    # notion of "page" is a render-time concept with no honest static
    # answer. Also ``None`` until the upload-time gate (PDF/PPTX) or
    # parse-time count (PPT) has populated it.
    page_count: int | None = None


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


class EditedContentRequest(BaseModel):
    """Body for ``PUT /documents/{id}/edited`` — the text/markdown form.

    ``base_revision`` is an optional optimistic lock: when supplied it must
    equal the document's current ``edited_revision`` or the write is
    rejected with 409. Batch callers should always send it; ad-hoc callers
    can omit it and accept last-writer-wins.
    """
    content: str = Field(..., min_length=1)
    filename: str | None = None
    base_revision: int | None = None


class EditedContentResponse(BaseModel):
    """Metadata for a document's current edited rendition.

    ``url`` is a freshly-minted presigned GET — short-lived by design, so
    treat it as a download handle, not something to persist. ``content`` is
    populated only when the caller asked for it AND the artifact decodes as
    UTF-8 (binary uploads leave it null; fetch ``url`` instead).
    """
    doc_id: str
    kb_id: str
    revision: int
    filename: str
    object_key: str
    url: str
    edited_at: str | None = None
    content: str | None = None


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
    # → 0.55 on the evaluation KB. Left as a per-request opt-in for
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
