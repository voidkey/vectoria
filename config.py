from pydantic import SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict
from typing import Literal
from functools import lru_cache


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    # LLM
    openai_base_url: str = "https://api.openai.com/v1"
    openai_api_key: SecretStr = SecretStr("")
    llm_model: str = "gpt-4o"

    # Embedding (falls back to LLM settings if not set)
    embedding_base_url: str = ""
    embedding_api_key: SecretStr = SecretStr("")
    embedding_model: str = "text-embedding-3-small"
    embedding_dimensions: int = 1536
    embedding_batch_size: int = 4

    @property
    def effective_embedding_base_url(self) -> str:
        return self.embedding_base_url or self.openai_base_url

    @property
    def effective_embedding_api_key(self) -> str:
        key = self.embedding_api_key.get_secret_value()
        return key if key else self.openai_api_key.get_secret_value()

    # Vector store
    vector_store: Literal["pgvector", "chroma"] = "pgvector"
    database_url: SecretStr = SecretStr("postgresql+asyncpg://postgres:postgres@localhost/vectoria")

    # Object storage
    storage_type: str = "s3"
    s3_endpoint: str = "http://localhost:9000"
    s3_region: str = ""
    s3_access_key: str = "minioadmin"
    s3_secret_key: SecretStr = SecretStr("minioadmin")
    s3_bucket: str = "vectoria"
    s3_addressing_style: str = "auto"  # auto|virtual|path
    s3_presign_expires: int = 3600

    # Parse engine
    default_parse_engine: str = "auto"

    # Hard cap on parsed document content (characters). Anything larger is
    # rejected with 413 before splitting/embedding to avoid OOM — the splitter
    # and embedding pipeline hold the full content in memory and fan out into
    # many intermediate copies.
    max_content_chars: int = 5_000_000

    # Hard cap on raw upload size (bytes). Rejected at the HTTP entry before
    # the file is buffered in memory.
    max_upload_bytes: int = 50 * 1024 * 1024

    # Per-parse wall-clock timeout (seconds). Parsers run in a subprocess
    # pool; after this timeout the worker is terminated so a stuck convert()
    # can't block the API thread indefinitely.
    parser_timeout: float = 120.0

    # Whether heavy parsers run in a subprocess pool. Defaults on for prod
    # isolation; tests that rely on in-process patching (mocking
    # DocumentConverter etc.) flip this off via monkeypatch.
    parser_isolation: bool = True

    # Max concurrent ingestions (file + URL combined) in the API process.
    # Each ingestion holds the uploaded file bytes + parsed content in memory;
    # unbounded concurrency lets N × 50MB pile up and OOM the API.
    max_concurrent_ingestions: int = 3

    # MinerU remote API
    mineru_api_url: str = ""
    mineru_backend: str = "pipeline"
    mineru_language: str = "ch"

    # Vision LLM (for image description)
    vision_base_url: str = ""
    vision_api_key: SecretStr = SecretStr("")
    vision_model: str = "gpt-4o"

    # Security
    api_key: SecretStr = SecretStr("")
    cors_origins: list[str] = []

    # JWT auth (optional; enables X-Authorization-Token and Authorization: Bearer
    # alongside X-API-Key). Must match the signing secret/algorithm of whatever
    # service issues the tokens.
    jwt_secret: SecretStr = SecretStr("")
    # Restricted to go-atlas's supported set; rejects `none` and other algos at load time.
    jwt_algorithm: Literal["HS256", "HS384", "HS512"] = "HS256"
    # When set, tokens must carry a matching `iss` claim. When empty, issuer is not
    # verified — tokens with any (or no) issuer are accepted.
    jwt_issuer: str = ""

    # RAG pipeline toggles
    enable_query_rewrite: bool = True
    enable_reranker: bool = False
    reranker_base_url: str = ""


@lru_cache
def get_settings() -> Settings:
    return Settings()
