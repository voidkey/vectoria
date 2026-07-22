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


# --- Response body contract (real main.py handler) -------------------------
#
# The upload-reject path (AppError → app_error_handler) and the poll path
# (DocumentDetailResponse → error_fields) historically disagreed on the code
# field name (`code` vs `error_code`). The frontend keys on `error_code`
# everywhere; the handler must emit it alongside the legacy `code` so both
# paths carry the same field. See vectoria-parse-error-contract.md §2.
import json


async def test_handler_emits_both_code_and_error_code():
    from main import app_error_handler

    exc = AppError(413, ErrorCode.PDF_TOO_MANY_PAGES, "PDF has 363 pages; max allowed is 200")
    resp = await app_error_handler(None, exc)
    body = json.loads(resp.body)
    assert body["code"] == ErrorCode.PDF_TOO_MANY_PAGES
    assert body["error_code"] == ErrorCode.PDF_TOO_MANY_PAGES


async def test_handler_includes_error_data_when_present():
    """Over-limit errors ship structured numbers so the frontend can render
    "363 / 200" without parsing the English detail string. See contract §3."""
    from main import app_error_handler

    exc = AppError(
        413, ErrorCode.PDF_TOO_MANY_PAGES,
        "PDF has 363 pages; max allowed is 200",
        error_data={"current": 363, "limit": 200},
    )
    resp = await app_error_handler(None, exc)
    body = json.loads(resp.body)
    assert body["error_data"] == {"current": 363, "limit": 200}


async def test_handler_omits_error_data_when_absent():
    """Errors without structured data don't grow a null key — keep the
    common error body shape unchanged for the majority of codes."""
    from main import app_error_handler

    exc = AppError(400, ErrorCode.VALIDATION_ERROR, "bad")
    resp = await app_error_handler(None, exc)
    body = json.loads(resp.body)
    assert "error_data" not in body


# The frontend keys on `error_code` for EVERY error response, so the field
# must be present on the non-AppError handlers too (422 validation, generic
# HTTPException, unhandled 500) — not just the upload-reject AppError path.
# Otherwise those codes silently fall to the frontend's generic fallback.


async def test_validation_handler_emits_error_code():
    from main import validation_error_handler
    from fastapi.exceptions import RequestValidationError

    resp = await validation_error_handler(None, RequestValidationError([]))
    body = json.loads(resp.body)
    assert body["code"] == ErrorCode.VALIDATION_ERROR
    assert body["error_code"] == ErrorCode.VALIDATION_ERROR


async def test_http_exception_handler_emits_error_code():
    from main import http_error_handler
    from fastapi import HTTPException

    resp = await http_error_handler(None, HTTPException(status_code=404, detail="nope"))
    body = json.loads(resp.body)
    assert body["code"] == 404
    assert body["error_code"] == 404


async def test_unhandled_handler_emits_error_code():
    from main import unhandled_error_handler

    resp = await unhandled_error_handler(None, RuntimeError("boom"))
    body = json.loads(resp.body)
    assert body["code"] == ErrorCode.INTERNAL_ERROR
    assert body["error_code"] == ErrorCode.INTERNAL_ERROR
