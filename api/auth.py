"""Dual-mode authentication: JWT (preferred) and X-API-Key (fallback).

When ``JWT_SECRET`` is configured, requests carrying ``X-Authorization-Token``
are validated against it. When service keys are configured (``API_KEYS``,
plus the legacy single ``API_KEY``), requests carrying ``X-API-Key`` are
validated against them. Either mechanism alone is sufficient; both may be
enabled at once to let ecosystem services use JWT while standalone /
open-source deployments keep using a static key.

JWT validation follows the go-atlas ``pillar/auth`` verifier: algorithm
pinned, ``exp`` required, a non-empty ``uid``, ``typ == "access"`` (refresh
tokens are not request credentials), ``iss`` in the accepted list, and a
clock-drift leeway. Verification is stateless — session revocation lives
with the issuer, and short access-token lifetimes bound it, as on every
other verify-only service.

The authenticated caller is stored on ``request.state.principal`` (see
:class:`Principal`) for ownership checks, rate limiting and logs.

If neither secret is configured, requests are rejected with 503 unless
``allow_unauthenticated=True`` is set — the explicit gate prevents a
silent sed-cleared ``.env.prod`` from putting the API online without
any authentication.
"""
import hmac
import logging
from dataclasses import dataclass
from typing import Any, Literal

import jwt as pyjwt
from fastapi import Request

from api.errors import AppError, ErrorCode
from config import get_settings

logger = logging.getLogger(__name__)


JWT_HEADER = "X-Authorization-Token"
API_KEY_HEADER = "X-API-Key"
# Lets a trusted service act for one end user: the KB ownership check then
# applies as if that user had called. Honoured only with a service key — a
# JWT caller's identity always comes from its token.
ON_BEHALF_HEADER = "X-Vectoria-On-Behalf-Of"

ACCESS_TOKEN_TYPE = "access"


@dataclass(frozen=True)
class Principal:
    """Who is calling.

    ``uid`` set means the call is subject to KB ownership: an end user (JWT)
    or a service acting on behalf of one. A service without ``uid`` is
    trusted with every KB.
    """
    kind: Literal["jwt", "service", "anonymous"]
    uid: str | None = None
    team_id: str | None = None
    service: str | None = None

    @property
    def is_scoped(self) -> bool:
        return self.uid is not None

    def label(self) -> str:
        if self.kind == "jwt":
            return f"jwt:{self.uid}"
        if self.kind == "service":
            return f"key:{self.service}" + (f"@{self.uid}" if self.uid else "")
        return "anonymous"


_ANONYMOUS = Principal(kind="anonymous")


def get_principal(request: Request) -> Principal:
    return getattr(request.state, "principal", _ANONYMOUS)


def _extract_jwt(request: Request) -> str | None:
    """Pull a JWT from the request.

    Checks ``X-Authorization-Token`` first (alternative custom header) and
    falls back to the standard ``Authorization: Bearer <token>`` scheme so
    OAuth2/OIDC clients work out of the box. A ``Bearer `` prefix on the
    custom header is tolerated, as go-atlas does.
    """
    token = request.headers.get(JWT_HEADER, "").strip()
    if token:
        scheme, _, value = token.partition(" ")
        return value.strip() if scheme.lower() == "bearer" and value else token
    scheme, _, value = request.headers.get("Authorization", "").partition(" ")
    if scheme.lower() == "bearer" and value:
        return value.strip()
    return None


def _match_service_key(presented: str | None) -> str | None:
    """Name of the service whose key was presented, or None. Every key is
    compared (no early exit) so timing doesn't reveal which one matched."""
    if not presented:
        return None
    matched = None
    for name, key in get_settings().named_api_keys().items():
        if hmac.compare_digest(presented.encode(), key.encode()):
            matched = name
    return matched


def _decode_access_token(token: str, secret: str) -> dict[str, Any]:
    s = get_settings()
    try:
        claims = pyjwt.decode(
            token,
            secret,
            algorithms=[s.jwt_algorithm],
            leeway=s.jwt_leeway_seconds,
            options={"require": ["exp"], "verify_aud": False},
        )
    except pyjwt.PyJWTError:
        raise AppError(401, ErrorCode.UNAUTHORIZED, "Invalid or expired token")
    issuers = s.effective_jwt_issuers()
    if issuers and claims.get("iss") not in issuers:
        raise AppError(401, ErrorCode.UNAUTHORIZED, "Invalid or expired token")
    uid = claims.get("uid")
    if not isinstance(uid, str) or not uid.strip():
        raise AppError(401, ErrorCode.UNAUTHORIZED, "Invalid or expired token")
    if claims.get("typ") != ACCESS_TOKEN_TYPE:
        # Refresh tokens share the signing key; they are exchanged at the
        # issuer, never accepted as a request credential.
        raise AppError(401, ErrorCode.UNAUTHORIZED, "Invalid or expired token")
    return claims


def _team_id(claims: dict[str, Any]) -> str | None:
    meta = claims.get("meta")
    stid = meta.get("stid") if isinstance(meta, dict) else None
    return str(stid) if stid not in (None, "", 0) else None


async def verify_auth(request: Request) -> dict[str, Any] | None:
    """Validate the request and return JWT claims (if any).

    Returns claims dict when authenticated via JWT, ``None`` when authenticated
    via API key or when auth is disabled. Raises ``AppError(401)`` on failure.
    Also records the caller on ``request.state.principal``.
    """
    settings = get_settings()
    jwt_secret = settings.jwt_secret.get_secret_value()
    has_keys = bool(settings.named_api_keys())

    if not jwt_secret and not has_keys:
        if settings.allow_unauthenticated:
            request.state.principal = _ANONYMOUS
            return None
        # Misconfiguration: both secrets empty but the explicit gate
        # was never flipped. Refuse every request so nobody notices
        # they're serving the API naked in production.
        logger.error(
            "auth misconfigured: API_KEY(S) and JWT_SECRET are both empty but "
            "ALLOW_UNAUTHENTICATED is false — refusing request"
        )
        raise AppError(
            503, ErrorCode.INTERNAL_ERROR,
            "Authentication is not configured (set API_KEYS or JWT_SECRET, "
            "or ALLOW_UNAUTHENTICATED=true for dev)",
        )

    token = _extract_jwt(request)
    key = request.headers.get(API_KEY_HEADER)

    if token and jwt_secret:
        claims = _decode_access_token(token, jwt_secret)
        request.state.principal = Principal(
            kind="jwt", uid=claims["uid"].strip(), team_id=_team_id(claims))
        return claims

    if key and has_keys:
        service = _match_service_key(key)
        if service is None:
            raise AppError(401, ErrorCode.UNAUTHORIZED, "Invalid API key")
        on_behalf = (request.headers.get(ON_BEHALF_HEADER) or "").strip() or None
        request.state.principal = Principal(kind="service", service=service, uid=on_behalf)
        return None

    raise AppError(401, ErrorCode.UNAUTHORIZED, "Missing authentication credentials")


async def require_api_key(request: Request) -> None:
    """Authorize endpoints restricted to internal/operator use.

    Unlike ``verify_auth`` (which accepts JWT *or* API key), this requires a
    valid ``X-API-Key`` and rejects JWT-only callers. Used to keep the
    synchronous, queue-bypassing analyze endpoints off the end-user surface.

    In dev mode (``allow_unauthenticated``) it lets everything through so
    local development and the test suite work without secrets. Otherwise a
    request must present an ``X-API-Key`` matching a configured service key;
    everything else — JWT-only callers, wrong/missing keys, or no key
    configured — is closed with 403.
    """
    settings = get_settings()
    if settings.allow_unauthenticated:
        request.state.principal = _ANONYMOUS
        return
    service = _match_service_key(request.headers.get(API_KEY_HEADER))
    if service is not None:
        request.state.principal = Principal(kind="service", service=service)
        return
    raise AppError(
        403, ErrorCode.FORBIDDEN,
        "This endpoint requires API-key authentication",
    )


def auth_config_warnings() -> list[str]:
    """Startup checks for settings that are accepted but probably wrong."""
    s = get_settings()
    warnings = []
    if s.jwt_secret.get_secret_value() and not s.effective_jwt_issuers():
        warnings.append(
            "JWT_SECRET is set but JWT_VALID_ISSUERS is empty: tokens from any "
            "issuer holding the secret are accepted")
    if s.jwt_secret.get_secret_value() and len(s.jwt_secret.get_secret_value()) < 32:
        warnings.append("JWT_SECRET is shorter than 32 bytes (RFC 7518 §3.2)")
    if s.api_key.get_secret_value() and s.api_keys:
        warnings.append(
            "both API_KEY and API_KEYS are set: API_KEY is accepted as service "
            "'default'; drop it once its callers have their own keys")
    return warnings
