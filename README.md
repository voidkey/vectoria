# Vectoria

A lightweight RAG (Retrieval-Augmented Generation) backend service built with FastAPI and pgvector. Provides knowledge base management, document ingestion, and a hybrid search pipeline via a simple REST API.

## Features

- **Multi-format document ingestion** — PDF, DOCX, PPTX, XLSX, CSV, Markdown, plain text, images, and URLs
- **Async document processing** — documents are ingested asynchronously with status tracking (processing → completed / failed)
- **Image extraction** — automatically extracts images from documents and stores them in S3-compatible object storage
- **Hybrid search** — combines vector similarity search with BM25 keyword search via Reciprocal Rank Fusion
- **Modular RAG pipeline** — Query Rewrite → Retrieve → Fusion → Rerank → Context Expand → Generate
- **OpenAI-compatible** — works with any OpenAI-compatible LLM/embedding endpoint (OpenAI, DeepSeek, Ollama, etc.)
- **Pluggable parsers** — [docling](https://github.com/DS4SD/docling), [markitdown](https://github.com/microsoft/markitdown), MinerU (optional GPU-based OCR)
- **Multiple vector stores** — pgvector (default), ChromaDB (optional)

## Requirements

- Python 3.11+
- PostgreSQL with [pgvector](https://github.com/pgvector/pgvector) extension
- S3-compatible object storage (MinIO, Volcengine TOS, AWS S3, etc.)
- An OpenAI-compatible API key

## Quick Start

### Docker (recommended)

```bash
cp .env.example .env
# Edit .env with your API key and settings

docker compose up -d
```

That's it. The API is available at `http://localhost:8000`, interactive docs at `http://localhost:8000/docs`. MinIO console is at `http://localhost:9001` (minioadmin/minioadmin).

### Local development

**1. Start the database and MinIO**

```bash
docker compose up -d db minio minio-init
```

**2. Install dependencies**

```bash
pip install uv
uv sync
playwright install --with-deps chromium
```

**3. Configure environment**

```bash
cp .env.example .env
# Edit .env with your API key and settings
```

**4. Run migrations**

```bash
uv run alembic upgrade head
```

**5. Start the server**

```bash
uv run uvicorn main:app --reload
```

The API is now available at `http://localhost:8000`. Interactive docs at `http://localhost:8000/docs`.

## API Overview

### Document Parsing

```
POST /analyze/file   # upload a file (multipart/form-data)
POST /analyze/url    # provide a URL (JSON body)
```

Parse a file or URL into Markdown without storing it. Returns parsed Markdown along with extracted images.

### Knowledge Bases

```
POST   /knowledgebases           # create
GET    /knowledgebases           # list
DELETE /knowledgebases/{kb_id}   # delete
```

### Documents

```
POST   /knowledgebases/{kb_id}/documents            # ingest file or URL
GET    /knowledgebases/{kb_id}/documents            # list
GET    /knowledgebases/{kb_id}/documents/{doc_id}   # get status
DELETE /knowledgebases/{kb_id}/documents/{doc_id}   # delete
```

Document ingestion is asynchronous — the API returns immediately with `status: "processing"`. Poll the single-document endpoint to check progress (`completed` or `failed`).

### Query

```
POST /knowledgebases/{kb_id}/query
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
| `DEFAULT_PARSE_ENGINE` | `auto` | Parser engine (`auto`, `docling`, `markitdown`, `mineru`) |
| `ENABLE_QUERY_REWRITE` | `true` | Rewrite queries with LLM before retrieval |
| `ENABLE_RERANKER` | `false` | Enable cross-encoder reranking |
| `RERANKER_BASE_URL` | — | Reranker API URL |
| `MINERU_API_URL` | — | MinerU remote API URL (optional, for GPU-based PDF OCR) |
| `MINERU_BACKEND` | `pipeline` | MinerU backend mode |
| `MINERU_LANGUAGE` | `ch` | MinerU OCR language |

## Optional: OCR with PaddleOCR

For local OCR support:

```bash
uv sync --extra ocr
```

## Acknowledgements

Inspired by the architecture and design ideas from the [WeKnora](https://github.com/tencent/WeKnora) project.

## License

MIT
