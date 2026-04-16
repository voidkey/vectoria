# syntax=docker/dockerfile:1.7

# --- Stage 1: builder ---
FROM python:3.12.7-slim AS builder

COPY --from=ghcr.io/astral-sh/uv:0.5.4 /uv /uvx /usr/local/bin/

ENV UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    UV_PYTHON_DOWNLOADS=never \
    PLAYWRIGHT_BROWSERS_PATH=/opt/pw-browsers

WORKDIR /app

RUN --mount=type=cache,target=/root/.cache/uv \
    --mount=type=bind,source=pyproject.toml,target=pyproject.toml \
    --mount=type=bind,source=uv.lock,target=uv.lock \
    uv sync --frozen --no-dev --no-install-project

RUN --mount=type=cache,target=/root/.cache/uv \
    uv run playwright install chromium

# Pre-download docling layout/OCR models so the first real request doesn't
# balloon memory and latency while fetching weights from HuggingFace.
# HF_HOME pins the cache into the image layer; docling reads from the same root.
ENV HF_HOME=/opt/hf-cache
RUN --mount=type=cache,target=/root/.cache/uv \
    uv run python -c "from docling.utils.model_downloader import download_models; download_models()" \
    || echo "docling model preload skipped"


# --- Stage 2: runtime ---
FROM python:3.12.7-slim AS runtime

RUN apt-get update && apt-get install -y --no-install-recommends \
        libgl1 \
        libglib2.0-0 \
        libreoffice-writer \
        libreoffice-impress \
        libnss3 libnspr4 libatk1.0-0 libatk-bridge2.0-0 libcups2 \
        libxcomposite1 libxdamage1 libxrandr2 libgbm1 libxkbcommon0 \
        libpango-1.0-0 libcairo2 libasound2 libatspi2.0-0 \
        tini \
    && rm -rf /var/lib/apt/lists/*

ENV PATH="/app/.venv/bin:$PATH" \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PLAYWRIGHT_BROWSERS_PATH=/opt/pw-browsers \
    HF_HOME=/opt/hf-cache

RUN groupadd --system --gid 1000 app \
    && useradd --system --uid 1000 --gid app --home-dir /app --shell /sbin/nologin app \
    && mkdir -p /app/.config/libreoffice /app/.cache/dconf \
    && chown -R app:app /app/.config /app/.cache

WORKDIR /app

COPY --from=builder --chown=app:app /app/.venv /app/.venv
COPY --from=builder --chown=app:app /opt/pw-browsers /opt/pw-browsers
COPY --from=builder --chown=app:app /opt/hf-cache /opt/hf-cache

COPY --chown=app:app . .

USER app

EXPOSE 8000

HEALTHCHECK --interval=30s --timeout=5s --start-period=40s --retries=3 \
    CMD python -c "import urllib.request,sys; sys.exit(0 if urllib.request.urlopen('http://localhost:8000/health',timeout=3).status==200 else 1)"

ENTRYPOINT ["tini", "--"]
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
