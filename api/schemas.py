from pydantic import BaseModel, Field


class AnalyzeURLRequest(BaseModel):
    url: str
    extract_images: bool = True


class DocumentURLRequest(BaseModel):
    url: str


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
    """Extended response for document upload -- includes parsed content."""
    content: str = ""
    outline: list[OutlineItem] = []
    image_count: int = 0
    image_status: str = "none"


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


class DocumentImagesListResponse(BaseModel):
    doc_id: str
    images: list[DocumentImageResponse] = []


class DocumentSourceURLResponse(BaseModel):
    doc_id: str
    source_type: str  # "file" or "url"
    url: str


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
    query_rewrite: bool = True


class QueryResponse(BaseModel):
    answer: str
    sources: list[dict] = []
