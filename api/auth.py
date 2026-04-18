"""Dual-mode authentication: JWT (preferred) and X-API-Key (fallback).

When ``JWT_SECRET`` is configured, requests carrying ``X-Authorization-Token``
are validated against it. When ``API_KEY`` is configured, requests carrying
``X-API-Key`` are validated against it. Either mechanism alone is sufficient;
both may be enabled at once to let ecosystem services use JWT while
standalone/open-source deployments keep using a static key.

If neither secret is configured, requests pass through (local dev).
"""
import hmac
from typing import Any

import jwt as pyjwt
from fastapi import Request

from api.errors import AppError, ErrorCode
from config import get_settings


JWT_HEADER = "X-Authorization-Token"
API_KEY_HEADER = "X-API-Key"


async def verify_auth(request: Request) -> dict[str, Any] | None:
    """Validate the request and return JWT claims (if any).

    Returns claims dict when authenticated via JWT, ``None`` when authenticated
    via API key or when auth is disabled. Raises ``AppError(401)`` on failure.
    """
    settings = get_settings()
    jwt_secret = settings.jwt_secret.get_secret_value()
    api_key = settings.api_key.get_secret_value()

    if not jwt_secret and not api_key:
        return None

    token = request.headers.get(JWT_HEADER)
    key = request.headers.get(API_KEY_HEADER)

    if token and jwt_secret:
        try:
            return pyjwt.decode(
                token,
                jwt_secret,
                algorithms=[settings.jwt_algorithm],
                issuer=settings.jwt_issuer or None,
                options={"verify_aud": False},
            )
        except pyjwt.PyJWTError:
            raise AppError(401, ErrorCode.UNAUTHORIZED, "Invalid or expired token")

    if key and api_key:
        if hmac.compare_digest(key, api_key):
            return None
        raise AppError(401, ErrorCode.UNAUTHORIZED, "Invalid API key")

    raise AppError(401, ErrorCode.UNAUTHORIZED, "Missing authentication credentials")
