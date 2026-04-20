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

    # Legacy setting retained for config-shape compatibility. The API
    # no longer parses in-process (W1 Task 4), so this semaphore no
    # longer gates anything. Will be removed after one release cycle.
    max_concurrent_ingestions: int = 3

    # POST /documents/{file,url}?wait=true polls the Document row for up
    # to this many seconds before returning so backward-compat clients
    # can still receive content in the response body. Past the timeout
    # we return whatever state the Document is in — queued is a valid
    # outcome for clients that didn't opt in.
    ingest_wait_timeout_seconds: float = 30.0
    ingest_wait_poll_interval_seconds: float = 0.25

    # MinerU remote API
    mineru_api_url: str = ""
    mineru_backend: str = "pipeline"
    mineru_language: str = "ch"
    # Breaker: open after N consecutive 5xx/timeout/network failures; stay
    # open for reset_timeout seconds, then probe with one request.
    mineru_breaker_threshold: int = 5
    mineru_breaker_reset_timeout: float = 300.0

    # Vision LLM (for image description)
    vision_base_url: str = ""
    vision_api_key: SecretStr = SecretStr("")
    vision_model: str = "gpt-4o"
    vision_breaker_threshold: int = 5
    vision_breaker_reset_timeout: float = 300.0

    # Embedding reliability. Threshold is higher than mineru/vision because
    # the embedder already retries internally with backoff; the breaker is
    # the last resort when retries can't drain the outage.
    embedding_breaker_threshold: int = 10
    embedding_breaker_reset_timeout: float = 60.0

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

    # Observability
    # Port the worker process binds for prometheus_client stdlib HTTP server.
    # API exposes /metrics on the main uvicorn port via fastapi-instrumentator,
    # so this only applies to worker pods. Intentionally different from the
    # default API port (8001 in scripts/deploy-host.sh) so host-mode deploys
    # don't collide — override in K8s if 9001 is needed elsewhere.
    worker_metrics_port: int = 9001

    # Redis URL for distributed state (rate-limit token buckets today;
    # shared circuit-breaker state / dedupe caches as future use cases).
    # SecretStr so password-bearing URLs (``redis://:pw@host:6379/0``)
    # don't show up in debug dumps or error traces.
    redis_url: SecretStr = SecretStr("redis://localhost:6379/0")

    # Worker runtime limits
    # RSS self-kill threshold in bytes. When a worker's resident memory
    # exceeds this between tasks, it exits cleanly and K8s restarts it.
    # 0 disables the check (dev default — no /proc on macOS anyway).
    # Production recommendation: 2 GiB, leaving headroom under a 4 GiB
    # container limit to absorb one more large task before OOM-killer hits.
    worker_rss_limit_bytes: int = 0

    # Comma-separated list of ``task_type`` values this worker instance
    # will consume from the queue. Empty = accept all task types (default).
    # Drives future multi-deployment sharding: e.g. ``WORKER_QUEUES=url_render``
    # in one K8s Deployment, everything-else in another — same image, same code,
    # different env. See docs for the roster.
    worker_queues: str = ""

    # Reserved for W5 multi-deployment work. The current runner processes
    # tasks serially; this value is stored only so env/config can be shaped
    # ahead of the concurrent implementation.
    worker_concurrency: int = 1


@lru_cache
def get_settings() -> Settings:
    return Settings()
