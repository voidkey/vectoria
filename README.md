# Vectoria

A lightweight RAG (Retrieval-Augmented Generation) backend service built with FastAPI and pgvector. Provides knowledge base management, document ingestion, and a hybrid search pipeline via a simple REST API.

## Features

- **Multi-format document ingestion** — PDF, DOCX, PPTX, XLSX, CSV, Markdown, plain text, images, and URLs
- **Async document processing** — persistent PG-backed task queue with status tracking, auto-retry, and separate worker process (no Redis needed)
- **Image extraction & vision** — automatically extracts images from documents, stores them in S3-compatible object storage, and optionally describes them via vision LLM
- **Hybrid search** — combines vector similarity search with BM25 keyword search via Reciprocal Rank Fusion
- **Modular RAG pipeline** — Query Rewrite → Retrieve → Fusion → Rerank → Context Expand → Generate
- **OpenAI-compatible** — works with any OpenAI-compatible LLM/embedding endpoint (OpenAI, DeepSeek, Ollama, etc.)
- **Pluggable parsers** — native Office (mammoth/python-pptx/openpyxl), [pypdfium2](https://github.com/pypdfium2-team/pypdfium2) PDF fallback, [rapidocr](https://github.com/RapidAI/RapidOCR) image OCR, MinerU remote API for layout-heavy PDFs, [markitdown](https://github.com/microsoft/markitdown) as last resort; heavy parsers run isolated in subprocesses
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
POST /v1/analyze/file   # upload a file (multipart/form-data)
POST /v1/analyze/url    # provide a URL (JSON body)
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
POST   /v1/knowledgebases/{kb_id}/documents            # ingest file or URL
GET    /v1/knowledgebases/{kb_id}/documents            # list
GET    /v1/knowledgebases/{kb_id}/documents/{doc_id}   # get status
DELETE /v1/knowledgebases/{kb_id}/documents/{doc_id}   # delete
```

Document ingestion is asynchronous — the API returns immediately with `status: "processing"`. Poll the single-document endpoint to check progress (`completed` or `failed`).

### Images

```
GET /v1/knowledgebases/{kb_id}/documents/{doc_id}/images                          # list images
GET /v1/knowledgebases/{kb_id}/documents/{doc_id}/images/{img_id}/presigned-url   # get presigned URL
```

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
| `VECTOR_STORE` | `pgvector` | Vector store backend (`pgvector` or `chroma`) |
| `DATABASE_URL` | `postgresql+asyncpg://...` | PostgreSQL connection string |
| `STORAGE_TYPE` | `s3` | Object storage backend type |
| `S3_ENDPOINT` | `http://localhost:9000` | S3-compatible endpoint URL |
| `S3_REGION` | — | Region (required for TOS, e.g. `cn-beijing`) |
| `S3_ACCESS_KEY` | `minioadmin` | Access key |
| `S3_SECRET_KEY` | `minioadmin` | Secret key |
| `S3_BUCKET` | `vectoria` | Bucket name |
| `S3_ADDRESSING_STYLE` | `auto` | `auto`, `virtual`, or `path` |
| `S3_PRESIGN_EXPIRES` | `3600` | Presigned URL expiry (seconds) |
| `DEFAULT_PARSE_ENGINE` | `auto` | Parser engine (`auto`, `docx-native`, `pptx-native`, `xlsx-native`, `pdfium`, `ocr-native`, `markitdown`, `mineru`, `url`) |
| `ENABLE_QUERY_REWRITE` | `true` | Rewrite queries with LLM before retrieval |
| `ENABLE_RERANKER` | `false` | Enable cross-encoder reranking |
| `RERANKER_BASE_URL` | — | Reranker API URL |
| `VISION_BASE_URL` | — | Vision LLM API URL (optional, for image description) |
| `VISION_API_KEY` | — | Vision LLM API key |
| `VISION_MODEL` | `gpt-4o` | Vision model |
| `MINERU_API_URL` | — | MinerU remote API URL (optional, for GPU-based PDF OCR) |
| `MINERU_BACKEND` | `pipeline` | MinerU backend mode |
| `MINERU_LANGUAGE` | `ch` | MinerU OCR language |
| `API_KEY` | *(blank = public)* | API key for client authentication (`X-API-Key` header) |
| `CORS_ORIGINS` | `["*"]` | Allowed CORS origins |

## Optional: OCR with PaddleOCR

For local OCR support:

```bash
uv sync --extra ocr
```

## Acknowledgements

Inspired by the architecture and design ideas from the [WeKnora](https://github.com/tencent/WeKnora) project.

## License

MIT
