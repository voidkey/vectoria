"""Per-principal inbound rate limiting for FastAPI routes.

Why
---
``infra.ratelimit`` is the underlying token bucket (shared with the
outbound CDN limiter). This module pins a *bucket per caller* so a
single buggy or hostile client cannot amplify writes by sharing one
token allowance across many connections. Without per-principal
metering a write endpoint is trivially amplified by any loop or
parallel HTTP client.

Principal derivation
--------------------
In priority order:

1. **JWT ``sub`` / ``uid`` claim** — strongest identity.
2. **X-API-Key value (sha256-truncated)** — when callers share a single
   service key. The hash is in the bucket id (and therefore in
   metric labels) so the raw secret never leaks via ``/metrics``.
3. **X-Forwarded-For first hop** — behind a reverse proxy or load
   balancer the direct peer is an internal address; the real client
   IP rides in XFF. The leftmost hop is the originator per RFC 7239.
4. **request.client.host** — last resort for direct (non-proxied) hits.

Failure mode
------------
Inherits ``infra.ratelimit``'s fail-open behaviour: on Redis outage we
serve from a per-process in-memory bucket. Better to over-serve briefly
than to lock everyone out on a Redis blip.
"""
from __future__ import annotations

import hashlib
import math
import time
from typing import Callable

from fastapi import Depends, Request, Response

from api.auth import API_KEY_HEADER, verify_auth
from api.errors import AppError, ErrorCode
from infra.ratelimit import acquire, get_window_stats


# OpenAPI fragment for routes that wire the limiter. Splice into the
# decorator's ``responses=`` so /docs documents the 429 contract
# (status + ``code`` field + the standard headers).
RATE_LIMITED_RESPONSE = {
    429: {
        "description": "Rate limit exceeded for this caller.",
        "headers": {
            "Retry-After": {
                "description": "Seconds to wait before retrying.",
                "schema": {"type": "integer"},
            },
            "X-RateLimit-Limit": {
                "description": "Request quota per window.",
                "schema": {"type": "integer"},
            },
            "X-RateLimit-Remaining": {
                "description": "Requests remaining in the current window.",
                "schema": {"type": "integer"},
            },
            "X-RateLimit-Reset": {
                "description": "Unix timestamp when the window resets.",
                "schema": {"type": "integer"},
            },
        },
        "content": {
            "application/json": {
                "example": {
                    "code": ErrorCode.RATE_LIMITED,
                    "detail": "Too many requests; retry after the window resets.",
                }
            }
        },
    }
}


def _principal_key(request: Request, claims: dict | None) -> str:
    """Derive a stable bucket id for the caller."""
    if claims:
        ident = claims.get("sub") or claims.get("uid")
        if ident:
            return f"jwt:{ident}"

    api_key = request.headers.get(API_KEY_HEADER)
    if api_key:
        digest = hashlib.sha256(api_key.encode()).hexdigest()[:16]
        return f"key:{digest}"

    fwd = request.headers.get("x-forwarded-for", "")
    if fwd:
        return f"ip:{fwd.split(',')[0].strip()}"

    client = request.client
    if client and client.host:
        return f"ip:{client.host}"
    return "ip:unknown"


def rate_limit(bucket: str, *, rate: int | Callable[[], int], per_seconds: int = 60):
    """FastAPI dependency factory: limit ``rate`` requests per ``per_seconds``
    per principal under ``bucket``.

    ``rate`` accepts an int or a zero-arg callable. The callable form lets
    operators retune via env without redeploy (route wiring passes
    ``lambda: get_settings().some_attr``).

    ``rate <= 0`` disables enforcement (kill-switch for ops).

    Headers emitted (industry standard, GitHub/Stripe convention):

    * ``X-RateLimit-Limit``    — the configured rate
    * ``X-RateLimit-Remaining`` — remaining tokens after this request
    * ``X-RateLimit-Reset``    — Unix timestamp when the window rolls
    * ``Retry-After`` (429 only) — seconds the client should wait

    These ride on every response that crosses the limiter, so a polite
    client never needs to *hit* 429 to know it's approaching a limit.

    Metric: writes ``RATELIMIT_CHECKS_TOTAL{key="inbound:<bucket>"}``
    with the bucket name only — never the per-principal hash — so the
    /metrics surface stays low-cardinality even with thousands of
    distinct callers.
    """

    def _resolve_rate() -> int:
        return rate() if callable(rate) else rate

    metric_label = f"inbound:{bucket}"

    async def _enforce(
        request: Request,
        response: Response,
        claims: dict | None = Depends(verify_auth),
    ) -> None:
        current_rate = _resolve_rate()
        if current_rate <= 0:
            return
        principal = _principal_key(request, claims)
        bucket_key = f"{bucket}:{principal}"

        ok = await acquire(
            bucket_key,
            rate=current_rate,
            per_seconds=per_seconds,
            metric_label=metric_label,
        )
        reset_ts, remaining = await get_window_stats(
            bucket_key, rate=current_rate, per_seconds=per_seconds,
        )

        rl_headers = {
            "X-RateLimit-Limit": str(current_rate),
            "X-RateLimit-Remaining": str(max(remaining, 0)),
            "X-RateLimit-Reset": str(reset_ts),
        }

        if not ok:
            retry_after = max(1, math.ceil(reset_ts - time.time()))
            raise AppError(
                429,
                ErrorCode.RATE_LIMITED,
                "Too many requests; retry after the window resets.",
                headers={"Retry-After": str(retry_after), **rl_headers},
            )

        for name, value in rl_headers.items():
            response.headers[name] = value

    return _enforce
