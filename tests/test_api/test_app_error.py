"""AppError header passthrough.

Why this is its own test file
-----------------------------
The 429 rate-limit response and any future error-with-headers case
(401 challenge, 503 Retry-After) all depend on this passthrough. Lock
the contract in once instead of asserting it inside every consumer.
"""
import pytest
from fastapi import FastAPI
from fastapi.responses import JSONResponse
from httpx import ASGITransport, AsyncClient

from api.errors import AppError, ErrorCode


def _build_app() -> FastAPI:
    """Minimal app that wires the same exception handler main.py uses."""
    app = FastAPI()

    @app.exception_handler(AppError)
    async def _handler(request, exc: AppError):
        return JSONResponse(
            status_code=exc.status_code,
            content={"code": exc.code, "detail": exc.detail},
            headers=exc.headers,
        )

    @app.get("/raise-with-headers")
    async def _r():
        raise AppError(
            429,
            ErrorCode.RATE_LIMITED,
            "too many",
            headers={"Retry-After": "60", "X-RateLimit-Limit": "10"},
        )

    @app.get("/raise-without-headers")
    async def _r2():
        raise AppError(400, ErrorCode.VALIDATION_ERROR, "bad")

    return app


async def test_app_error_carries_headers_to_response():
    async with AsyncClient(transport=ASGITransport(app=_build_app()), base_url="http://t") as c:
        resp = await c.get("/raise-with-headers")
    assert resp.status_code == 429
    assert resp.headers["retry-after"] == "60"
    assert resp.headers["x-ratelimit-limit"] == "10"


async def test_app_error_without_headers_still_works():
    """Back-compat: existing callers that don't pass headers must not break."""
    async with AsyncClient(transport=ASGITransport(app=_build_app()), base_url="http://t") as c:
        resp = await c.get("/raise-without-headers")
    assert resp.status_code == 400
    # No custom headers, but content-type still set by JSONResponse.
    assert resp.headers["content-type"].startswith("application/json")
