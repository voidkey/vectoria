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

    # Vector store — pgvector is the only supported backend; removed
    # the chroma enum option in W6-6 because no chroma adapter was
    # ever implemented (vectorstore/ has only pgvector.py + base.py).
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

    # Chunking knobs. Chunks are what gets embedded + indexed and also
    # what the LLM receives as retrieval context. Defaults are tuned
    # for mixed CJK/Latin content at 1024 chars — large enough to
    # carry a paragraph's worth of context but still under the
    # embedding model's token limits at the char-to-token expansion
    # ratio typical of CJK.
    splitter_chunk_size: int = 1024
    splitter_chunk_overlap: int = 64

    # Hard cap on raw upload size (bytes). Rejected at the HTTP entry before
    # the file is buffered in memory.
    max_upload_bytes: int = 50 * 1024 * 1024

    # Hard cap on PDF page count. The byte cap above doesn't catch
    # "small file, many pages" — a 19 MB scanned PDF can hide 1000+
    # pages that mineru can't OCR within its 120 s per-call timeout,
    # burning 3 retries × 120 s of GPU time before fallback. Rejecting
    # at upload (after pypdfium2 reads the xref, ~ms) costs nothing.
    # 200 covers the long tail of business documents (reports, slide
    # exports, manuals); larger inputs should be split or routed via
    # a dedicated long-doc pipeline.
    max_pdf_pages: int = 200

    # Hard cap on PPTX slide count. Same shape of attack as PDF —
    # text-only slides compress small but each slide still pays the
    # full per-slide parse + image-extraction + vision cost. Counting
    # is a zip directory listing, no XML parse, microseconds.
    max_pptx_slides: int = 200

    # Per-parse wall-clock timeout (seconds). Parsers run in a subprocess
    # pool; after this timeout the worker is terminated so a stuck convert()
    # can't block the API thread indefinitely.
    parser_timeout: float = 120.0

    # Whether heavy parsers run in a subprocess pool. Defaults on for prod
    # isolation; tests that rely on in-process patching (mocking
    # DocumentConverter etc.) flip this off via monkeypatch.
    parser_isolation: bool = True

    # --- Phase 1 ingest-quality knobs -----------------------------------
    # Min extracted content length (chars, post-.strip()) for a document
    # to count as "non-empty". Shorter → fail (or image_only rescue if
    # a structured-source URL handler flagged allow_image_only=True and
    # image_urls is non-empty). Rule is strict-less-than, so a value
    # exactly equal to this threshold passes.
    min_content_chars: int = 50

    # Cap on number of image URLs any URL handler will carry per
    # document. Set to a large number (e.g. 9999) to effectively disable.
    url_image_cap: int = 50

    # Comma-separated domain suffixes that are unreachable from this deployment
    # region and should fast-fail immediately. Default empty = no effect in any
    # environment. Set e.g. "wikipedia.org" for deployments behind a firewall
    # that blocks certain domains.
    # Env var: UNREACHABLE_DOMAINS (no prefix; pydantic-settings reads it directly).
    unreachable_domains: str = ""

    # When True, reject uploads whose magic-byte-sniffed MIME family
    # does NOT match the claimed file extension. When False, log +
    # metric but let the upload through (safe rollback during rollout).
    strict_mime_check: bool = True

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

    # PaddleOCR-VL remote API (PDF primary; MinerU stays as fallback B).
    # Both URL and key required; either being empty makes the parser
    # advertise unavailable so the registry falls straight through to
    # mineru. Gateway accepts JSON+base64 PDF (see docs in
    # ``parsers/paddle_parser.py``).
    paddle_api_url: str = ""
    paddle_api_key: SecretStr = SecretStr("")
    # Wall-clock per VL call (s). VL gateway's own ceiling is 600 s;
    # we stay close to that — long PDFs (~50-200 pages) routinely sit
    # at 60-90 s, and a 120 s client-side cut (the value MinerU uses)
    # would prematurely fail them when the gateway is still working.
    paddle_timeout: float = 600.0
    # Per-process cap on concurrent VL requests. Single-card GPU
    # serializes; >3 concurrent on image-heavy PDFs has been observed
    # to drop connections (see VL gateway docs §5). Multi-worker hosts
    # get N × ceiling; tune at worker count level.
    paddle_concurrency: int = 3
    paddle_breaker_threshold: int = 5
    paddle_breaker_reset_timeout: float = 300.0
    # Optional file relay for deployments far from the Paddle gateway.
    # POSTing inline base64 over a long (e.g. cross-continent) link is
    # slow and timeout-prone; with a relay configured, the parser
    # uploads the PDF to this S3-compatible bucket and sends the
    # gateway a presigned download URL instead. Point the endpoint at
    # an accelerated domain near the app, and the (optional) download
    # endpoint at the regional domain near the gateway. Relay objects
    # are deleted after each call; add a 1-day bucket lifecycle rule as
    # backstop. All empty = relay off, wire format unchanged.
    paddle_relay_endpoint: str = ""
    paddle_relay_download_endpoint: str = ""  # defaults to endpoint
    paddle_relay_region: str = ""
    paddle_relay_access_key: str = ""
    paddle_relay_secret_key: SecretStr = SecretStr("")
    paddle_relay_bucket: str = ""
    paddle_relay_addressing_style: str = "virtual"
    paddle_relay_prefix: str = "paddle-relay/"
    # Presign TTL must cover semaphore + gateway queue wait, not just
    # the download itself — the gateway fetches when the job starts.
    paddle_relay_url_expires: int = 3600

    # Vision LLM (for image description + vision-native parser)
    vision_base_url: str = ""
    vision_api_key: SecretStr = SecretStr("")
    vision_model: str = "gpt-4o"
    # Default output language for vision results when a request doesn't
    # specify one. "zh" preserves the original Chinese behavior; overseas
    # deployments set VISION_DEFAULT_LANGUAGE=en (per-request locale is the
    # primary driver there).
    vision_default_language: str = "zh"
    vision_breaker_threshold: int = 5
    vision_breaker_reset_timeout: float = 300.0
    # Rough per-call USD cost estimate, used by the cost counter and
    # daily-budget guardrail. Real cost depends on tokens; a flat
    # estimate is a small, conservative approximation. Adjust per
    # vendor: gpt-4o-mini ≈ 0.005, gpt-4o ≈ 0.02, qwen-vl ≈ 0.002.
    vision_cost_per_call_usd: float = 0.005
    # Soft daily budget. When today's accumulated estimated spend
    # crosses this, vision-native parser advertises is_available()=False
    # and registry falls back to ocr-native (rapidocr). 0 = no cap.
    # Per-process state — multi-worker hosts get N×budget effective
    # ceiling, conservative tune accordingly.
    vision_daily_budget_usd: float = 0.0

    # Embedding reliability. Threshold is higher than mineru/vision because
    # the embedder already retries internally with backoff; the breaker is
    # the last resort when retries can't drain the outage.
    embedding_breaker_threshold: int = 10
    embedding_breaker_reset_timeout: float = 60.0

    # Security
    api_key: SecretStr = SecretStr("")
    cors_origins: list[str] = []

    # Explicit gate for "no auth configured" mode. When both ``api_key``
    # and ``jwt_secret`` are empty, ``verify_auth`` lets everything
    # through — convenient for local dev but dangerous in prod if a
    # sed mistake wipes the .env secrets. Default ``False`` means that
    # combination instead raises 503 at request time so the leak is
    # loud. Flip to ``True`` only in dev / CI.
    allow_unauthenticated: bool = False

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
    # Query rewrite: ``eval/reports/baseline-2026-04-22.json`` showed
    # the LLM rewriter drops CJK hit@1 from 0.70 → 0.55 on real
    # philosophy-text queries; 5/20 queries miss entirely with
    # rewrite on. Leaving the knob in place for opt-in experimentation
    # (short queries / English traffic may behave differently) but
    # the default is off.
    enable_query_rewrite: bool = False
    enable_reranker: bool = False
    reranker_base_url: str = ""

    # RAG write side. When False, parsed documents are NOT embedded/indexed
    # into pgvector — text + image extraction still run, so documents stay
    # fully usable via GET /documents/{id}. /query returns 503 because there
    # is no maintained index to serve. Default True preserves existing behavior.
    enable_indexing: bool = True

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

    # URL fetch hardening (S2)
    url_page_fetch_rate: int = 1          # tokens per window, per-host page pacing
    url_page_fetch_per: int = 2           # window seconds (=> ~0.5 req/s/host)
    url_block_cooldown_seconds: int = 900  # anti-bot cooldown per domain (15 min)
    url_max_redirects: int = 5
    max_url_response_bytes: int = 50 * 1024 * 1024  # 50 MiB — aligned with max_upload_bytes so URL-fetched PDFs aren't capped tighter than uploads

    # Inbound rate limits (per principal, per minute). Principal = JWT
    # sub/uid, else hashed X-API-Key, else client IP (XFF-aware). Set to
    # 0 to disable a limiter without redeploying — kill-switch during
    # incident response. These are PER-END-USER caps (tokens carry a distinct
    # sub per user), sized as burst ceilings against scripted/CLI abuse —
    # normal interactive users never approach them. They do NOT cap aggregate
    # load across users (N users = N×limit); aggregate/GPU protection is a
    # separate concern. kb_create matches doc_ingest because the deployment
    # creates one KB per document (KB-per-doc), so KB creation is as frequent
    # as ingest. Retune via env without redeploy.
    ratelimit_kb_create_per_min: int = 20
    ratelimit_doc_ingest_per_min: int = 20
    # /query is the most expensive endpoint (embedding + 1-2 LLM calls +
    # rerank per request); capped tightest. 0 disables (kill-switch).
    ratelimit_query_per_min: int = 15

    # Worker runtime limits
    # RSS self-kill threshold in bytes. When a worker's resident memory
    # exceeds this between tasks, it exits cleanly and K8s restarts it.
    # 0 disables the check (dev default — no /proc on macOS anyway).
    # Production recommendation: 2 GiB, leaving headroom under a 4 GiB
    # container limit to absorb one more large task before OOM-killer hits.
    worker_rss_limit_bytes: int = 0

    # Comma-separated list of ``task_type`` values this worker instance
    # will consume from the queue. Empty = accept all task types (default).
    # Drives multi-deployment sharding: e.g. ``WORKER_QUEUES=url_render``
    # in one K8s Deployment, everything-else in another — same image, same
    # code, different env.
    worker_queues: str = ""


@lru_cache
def get_settings() -> Settings:
    return Settings()
