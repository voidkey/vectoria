FROM python:3.12-slim

# System deps for docling (libgl1, libglib2.0) and general build tools
RUN apt-get update && apt-get install -y --no-install-recommends \
    libgl1 \
    libglib2.0-0 \
    && rm -rf /var/lib/apt/lists/*

# Install uv
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /usr/local/bin/

WORKDIR /app

# Install Python deps (cached layer)
COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-dev

# Install Playwright Chromium + its system deps
RUN uv run playwright install --with-deps chromium

# Copy application code
COPY . .

EXPOSE 8000

CMD ["uv", "run", "uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
