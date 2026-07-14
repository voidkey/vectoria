# Vectoria

A lightweight RAG (Retrieval-Augmented Generation) backend service built with FastAPI and pgvector. Provides knowledge base management, document ingestion, and a hybrid search pipeline via a simple REST API.

## Features

- **Multi-format document ingestion** — PDF, DOCX, PPTX, XLSX, CSV, Markdown, plain text, images, and URLs
- **Async document processing** — persistent PG-backed task queue with status tracking, auto-retry, and separate worker process (no Redis needed)
- **Image extraction & vision** — automatically extracts images from documents, stores them in S3-compatible object storage, and optionally describes them via vision LLM
- **Hybrid search** — combines vector similarity search with BM25 keyword search via Reciprocal Rank Fusion
- **Modular RAG pipeline** — Query Rewrite → Retrieve → Fusion → Rerank → Context Expand → Generate
- **OpenAI-compatible** — works with any OpenAI-compatible LLM/embedding endpoint (OpenAI, DeepSeek, Ollama, etc.)
- **Pluggable parsers** — native Office (mammoth/python-pptx/openpyxl), [PaddleOCR-VL](https://github.com/PaddlePaddle/PaddleOCR) HTTP gateway as primary PDF engine (layout + OCR + tables + formulas), MinerU remote API as PDF fallback B, [pypdfium2](https://github.com/pypdfium2-team/pypdfium2) text-layer fallback, [rapidocr](https://github.com/RapidAI/RapidOCR) for image OCR, [markitdown](https://github.com/microsoft/markitdown) as last resort. Default PDF chain: `[paddle, mineru, pdfium, markitdown]` — failures cascade automatically. Heavy parsers run isolated in subprocesses.
- **Multiple vector stores** — pgvector (default), ChromaDB (optional)

## Requirements

- Python 3.11+
- PostgreSQL with [pgvector](https://github.com/pgvector/pgvector) extension
- S3-compatible object storage (MinIO, Volcengine TOS, AWS S3, etc.)
- An OpenAI-compatible API key

## Quick Start

### Local development (uv on host, infra in Docker)

`compose.yaml` ships only the infrastructure — postgres is always started, MinIO only when using the `local` profile. The app runs on the host via uv for fast reload.

```bash
cp .env.example .env          # fill in your API key
./scripts/dev.sh              # starts db/minio, migrates, runs uvicorn --reload
```

API at `http://localhost:8000`, docs at `/docs`, MinIO console at `http://localhost:9001` (minioadmin/minioadmin).

To start infra manually without the convenience script:

```bash
docker compose --profile local up -d --wait   # postgres + minio
uv sync
uv run alembic upgrade head
uv run uvicorn main:app --reload --host 0.0.0.0 --port 8000
```

### Production — Docker (recommended)

Two-step workflow: **build locally**, **pull on prod**. The prod host never builds (no source, no docker build memory spikes).

**Once per release** (local machine):
```bash
docker login                  # first time only
./scripts/build-push.sh       # builds and pushes voidkey/vectoria:{sha,latest}
```

**On the production host:**
```bash
cp .env.example .env.prod     # first time only — fill in production values
./scripts/deploy.sh           # git pull + docker pull + migrate + up -d
```

Uses `compose.yaml + compose.prod.yaml` with two containers: **app** (API server, 1.5 GB limit) and **worker** (background task processing, 4 GB limit). Image defaults to `voidkey/vectoria:latest` but can be pinned: `VECTORIA_IMAGE=voidkey/vectoria:abc1234 ./scripts/deploy.sh`. Logs: `docker compose -f compose.yaml -f compose.prod.yaml logs -f app worker`.

### Production — Host mode (alternative)

If you prefer running the app directly on the host via uv (e.g. shared server with multiple services):

```bash
./scripts/deploy-host.sh      # pulls, syncs deps, migrates, runs uvicorn in background
```

Logs: `logs/uvicorn-<timestamp>.log` (one file per deploy, never overwritten). Override the port via `PORT=8002 ./scripts/deploy-host.sh`.

## API Overview

### Document Parsing

```
POST /v1/analyze/file   # internal / API-key only (X-API-Key); JWT callers get 403
POST /v1/analyze/url    # internal / API-key only (X-API-Key); JWT callers get 403
```

Parse a file or URL into Markdown without storing it. Returns parsed Markdown along with extracted images.

### Knowledge Bases

```
POST   /v1/knowledgebases           # create
GET    /v1/knowledgebases           # list
DELETE /v1/knowledgebases/{kb_id}   # delete
```

### Documents

```
POST   /v1/knowledgebases/{kb_id}/documents/file       # ingest an uploaded file (multipart)
POST   /v1/knowledgebases/{kb_id}/documents/url        # ingest a web URL
POST   /v1/knowledgebases/{kb_id}/documents/text       # ingest raw text
GET    /v1/knowledgebases/{kb_id}/documents            # list
GET    /v1/knowledgebases/{kb_id}/documents/{doc_id}   # get status
DELETE /v1/knowledgebases/{kb_id}/documents/{doc_id}   # delete
```

Document ingestion is asynchronous — the API returns immediately with `status: "queued"`. Poll the single-document endpoint to check progress (`completed` or `failed`).

#### Direct upload (presigned)

For large files, upload straight to object storage instead of streaming the bytes through the API. Two steps:

```
POST /v1/knowledgebases/{kb_id}/documents/uploads                      # mint a presigned PUT URL
POST /v1/knowledgebases/{kb_id}/documents/uploads/{upload_id}/complete  # validate the staged file and ingest it
```

1. `POST .../uploads` with `{"filename": "...", "sha256": "...", "size": ...}` (`sha256`/`size` optional). Returns `{ upload_id, upload_url, method: "PUT", expires_at }`. If `sha256` matches an existing document, it returns `{ dedup_hit: true, document }` and mints no URL.
2. `PUT` the file bytes directly to `upload_url`.
3. `POST .../uploads/{upload_id}/complete` (optionally `?wait=true`). The server HEAD-checks the size, runs the same validation gates as `/file`, promotes the object, and enqueues ingestion — returning the same response shape as `/file`.

`upload_id` is the returned opaque handle; pass it back verbatim. The multipart `/documents/file` endpoint remains the universal fallback and is what you get a `501` pointer to on storage backends that can't presign.

> **Operator setup (required).** The bucket needs two one-time configurations or the flow silently fails — see [Object storage bucket configuration](#object-storage-bucket-configuration).

### Images

```
GET /v1/knowledgebases/{kb_id}/documents/{doc_id}/images                          # list images
GET /v1/knowledgebases/{kb_id}/documents/{doc_id}/images/{img_id}/presigned-url   # get presigned URL
```

### Website capture

Render a URL and extract a deterministic **SiteProfile** — brand colors (with roles), typography, spacing tokens, page sections, key text, and downloaded assets (logo / hero / og image / favicon / background video / Lottie) plus desktop screenshots — for downstream generation agents. Captures are not indexed for RAG and don't show up in the document list.

```
POST /v1/knowledgebases/{kb_id}/captures                      # enqueue a capture -> 202 {id, status:"queued"}
GET  /v1/knowledgebases/{kb_id}/captures/{id}                 # poll status + SiteProfile (presigned asset/screenshot URLs)
GET  /v1/knowledgebases/{kb_id}/captures/{id}/export?format=hyperframes   # download a hyperframes-compatible capture/ zip
```

Async: `POST` returns immediately; a worker renders the page (shared Chromium pool), extracts the profile, stores assets/screenshots, and vision descriptions for logo/hero backfill into `profile.assets[].vision_status`. A captured font that matches a deployment-provided catalog (`FONT_CATALOG_PATH`) is referenced by its CDN URL instead of re-stored; unmatched fonts are downloaded to this deployment's bucket under the `captures/` prefix (add a lifecycle rule for that prefix).

### Query

```
POST /v1/knowledgebases/{kb_id}/query
```

```json
{
  "query": "What is the refund policy?",
  "top_k": 5,
  "query_rewrite": true,
  "rerank": false
}
```

## Configuration

All settings are configured via environment variables (see [`.env.example`](.env.example)).

| Variable | Default | Description |
|---|---|---|
| `OPENAI_BASE_URL` | `https://api.openai.com/v1` | LLM API base URL |
| `OPENAI_API_KEY` | — | API key |
| `LLM_MODEL` | `gpt-4o` | Model for generation and query rewrite |
| `EMBEDDING_BASE_URL` | *(falls back to OPENAI_BASE_URL)* | Embedding API base URL |
| `EMBEDDING_API_KEY` | *(falls back to OPENAI_API_KEY)* | Embedding API key |
| `EMBEDDING_MODEL` | `text-embedding-3-small` | Embedding model |
| `EMBEDDING_DIMENSIONS` | `1536` | Embedding vector dimensions |
| `DATABASE_URL` | `postgresql+asyncpg://...` | PostgreSQL connection string |
| `STORAGE_TYPE` | `s3` | Object storage backend type |
| `S3_ENDPOINT` | `http://localhost:9000` | S3-compatible endpoint URL |
| `S3_REGION` | — | Region (required for TOS, e.g. `cn-beijing`) |
| `S3_ACCESS_KEY` | `minioadmin` | Access key |
| `S3_SECRET_KEY` | `minioadmin` | Secret key |
| `S3_BUCKET` | `vectoria` | Bucket name |
| `S3_ADDRESSING_STYLE` | `auto` | `auto`, `virtual`, or `path` |
| `S3_PRESIGN_EXPIRES` | `3600` | Presigned download URL expiry (seconds) |
| `S3_PRESIGN_UPLOAD_EXPIRES` | `600` | Presigned **upload** (PUT) URL expiry (seconds) — direct-upload path |
| `DEFAULT_PARSE_ENGINE` | `auto` | Parser engine (`auto`, `docx-native`, `pptx-native`, `xlsx-native`, `pdfium`, `ocr-native`, `paddle`, `mineru`, `markitdown`, `url`) |
| `ENABLE_QUERY_REWRITE` | `true` | Rewrite queries with LLM before retrieval |
| `ENABLE_RERANKER` | `false` | Enable cross-encoder reranking |
| `RERANKER_BASE_URL` | — | Reranker API URL |
| `VISION_BASE_URL` | — | Vision LLM API URL (optional, for image description) |
| `VISION_API_KEY` | — | Vision LLM API key |
| `VISION_MODEL` | `gpt-4o` | Vision model |
| `PADDLE_API_URL` | — | PaddleOCR-VL gateway URL (e.g. `https://your-gateway/vl`); empty disables the paddle engine, chain falls through to mineru/pdfium |
| `PADDLE_API_KEY` | — | Bearer token for the PaddleOCR-VL gateway |
| `MINERU_API_URL` | — | MinerU remote API URL (PDF fallback B, kicks in when paddle is unavailable or returns empty) |
| `MINERU_BACKEND` | `pipeline` | MinerU backend mode |
| `MINERU_LANGUAGE` | `ch` | MinerU OCR language |
| `API_KEY` | *(blank = public)* | API key for client authentication (`X-API-Key` header) |
| `CORS_ORIGINS` | `["*"]` | Allowed CORS origins |

### Object storage bucket configuration

The [presigned direct-upload](#direct-upload-presigned) path needs two one-time bucket configurations. Without them the feature fails in ways that are hard to diagnose (a browser `PUT` dies on CORS; abandoned uploads accumulate forever), so set both up front. Both are object-storage configuration, not application settings — apply them with your provider's console or CLI (AWS S3, Volcengine TOS, MinIO `mc`, etc. all accept these shapes).

**1. CORS** — required only when a **browser** uploads directly to the bucket (server-to-server clients can skip it). Set `AllowedOrigins` to your real client origins:

```json
[{
  "AllowedOrigins": ["https://app.example.com"],
  "AllowedMethods": ["PUT"],
  "AllowedHeaders": ["*"],
  "ExposeHeaders": ["ETag"],
  "MaxAgeSeconds": 3600
}]
```

**2. Lifecycle expiry on the staging prefix** — reclaims uploads that were minted but never completed. Scope it to `upload_staging/` **only** — the final `upload_files/` prefix holds live documents and must not be covered:

```json
{ "Rules": [{
  "ID": "expire-upload-staging",
  "Filter": { "Prefix": "upload_staging/" },
  "Status": "Enabled",
  "Expiration": { "Days": 1 }
}] }
```

## A note on "PaddleOCR"

Two PaddleOCR-derived runtimes show up in this project — they look related by name but serve different roles:

- **[rapidocr](https://github.com/RapidAI/RapidOCR)** — bundled in the base dependencies (no extra install needed). ONNX-runtime port of PaddleOCR's detection + recognition models, used by the `ocr-native` parser for image files (`.png`/`.jpg`/`.tiff`/...). Runs in-process on CPU.
- **[PaddleOCR-VL](https://github.com/PaddlePaddle/PaddleOCR) HTTP gateway** — the primary PDF parser, called via `PADDLE_API_URL`. A separate service you deploy yourself (GPU recommended) that handles layout + OCR + tables + formulas for PDFs. Vectoria only ships the HTTP client; the gateway is out-of-process.

## Acknowledgements

Inspired by the architecture and design ideas from the [WeKnora](https://github.com/tencent/WeKnora) project.

## License

MIT
