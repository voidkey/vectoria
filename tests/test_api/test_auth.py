"""Tests for dual-mode auth: JWT (X-Authorization-Token) + X-API-Key."""
import time

import jwt as pyjwt
import pytest
from fastapi import Request

from api.auth import verify_auth
from api.errors import AppError
from config import get_settings


def _make_request(headers: dict[str, str] | None = None) -> Request:
    """Build a minimal ASGI Request for unit testing."""
    headers = headers or {}
    scope = {
        "type": "http",
        "method": "GET",
        "path": "/v1/test",
        "headers": [(k.lower().encode(), v.encode()) for k, v in headers.items()],
    }
    return Request(scope)


@pytest.fixture
def settings(monkeypatch):
    """Yield a Settings object with auth fields reset; restore after test."""
    s = get_settings()
    # Reset to known state
    monkeypatch.setattr(s.api_key, "get_secret_value", lambda: "")
    monkeypatch.setattr(s.jwt_secret, "get_secret_value", lambda: "")
    monkeypatch.setattr(s, "jwt_algorithm", "HS256")
    monkeypatch.setattr(s, "jwt_issuer", "")
    return s


def _set_api_key(monkeypatch, settings, value: str):
    monkeypatch.setattr(settings.api_key, "get_secret_value", lambda: value)


def _set_jwt_secret(monkeypatch, settings, value: str):
    monkeypatch.setattr(settings.jwt_secret, "get_secret_value", lambda: value)


def _make_jwt(secret: str, **claims) -> str:
    payload = {"uid": "user-1", "iat": int(time.time()), "exp": int(time.time()) + 300}
    payload.update(claims)
    return pyjwt.encode(payload, secret, algorithm="HS256")


# --- Dev mode: nothing configured ---

@pytest.mark.asyncio
async def test_dev_mode_allows_all(settings):
    """When no secrets are set, any request passes (local dev)."""
    req = _make_request()
    result = await verify_auth(req)
    assert result is None


@pytest.mark.asyncio
async def test_dev_mode_ignores_headers(settings):
    """Even with headers present, dev mode just lets them through."""
    req = _make_request({"X-API-Key": "anything", "X-Authorization-Token": "garbage"})
    result = await verify_auth(req)
    assert result is None


# --- API key only ---

@pytest.mark.asyncio
async def test_api_key_valid(monkeypatch, settings):
    _set_api_key(monkeypatch, settings, "secret-key")
    req = _make_request({"X-API-Key": "secret-key"})
    result = await verify_auth(req)
    assert result is None  # api_key mode doesn't return user identity


@pytest.mark.asyncio
async def test_api_key_missing(monkeypatch, settings):
    _set_api_key(monkeypatch, settings, "secret-key")
    req = _make_request()
    with pytest.raises(AppError) as exc:
        await verify_auth(req)
    assert exc.value.status_code == 401


@pytest.mark.asyncio
async def test_api_key_wrong(monkeypatch, settings):
    _set_api_key(monkeypatch, settings, "secret-key")
    req = _make_request({"X-API-Key": "wrong"})
    with pytest.raises(AppError) as exc:
        await verify_auth(req)
    assert exc.value.status_code == 401


# --- JWT only ---

@pytest.mark.asyncio
async def test_jwt_valid(monkeypatch, settings):
    _set_jwt_secret(monkeypatch, settings, "a-jwt-secret-at-least-32-bytes-long!")
    token = _make_jwt("a-jwt-secret-at-least-32-bytes-long!", uid="alice")
    req = _make_request({"X-Authorization-Token": token})
    result = await verify_auth(req)
    assert result is not None
    assert result["uid"] == "alice"


@pytest.mark.asyncio
async def test_jwt_missing(monkeypatch, settings):
    _set_jwt_secret(monkeypatch, settings, "a-jwt-secret-at-least-32-bytes-long!")
    req = _make_request()
    with pytest.raises(AppError) as exc:
        await verify_auth(req)
    assert exc.value.status_code == 401


@pytest.mark.asyncio
async def test_jwt_wrong_signature(monkeypatch, settings):
    _set_jwt_secret(monkeypatch, settings, "a-jwt-secret-at-least-32-bytes-long!")
    token = _make_jwt("a-different-secret-at-least-32-bytes-!", uid="alice")
    req = _make_request({"X-Authorization-Token": token})
    with pytest.raises(AppError) as exc:
        await verify_auth(req)
    assert exc.value.status_code == 401


@pytest.mark.asyncio
async def test_jwt_expired(monkeypatch, settings):
    _set_jwt_secret(monkeypatch, settings, "a-jwt-secret-at-least-32-bytes-long!")
    token = _make_jwt("a-jwt-secret-at-least-32-bytes-long!", uid="alice", exp=int(time.time()) - 1)
    req = _make_request({"X-Authorization-Token": token})
    with pytest.raises(AppError) as exc:
        await verify_auth(req)
    assert exc.value.status_code == 401


@pytest.mark.asyncio
async def test_jwt_malformed(monkeypatch, settings):
    _set_jwt_secret(monkeypatch, settings, "a-jwt-secret-at-least-32-bytes-long!")
    req = _make_request({"X-Authorization-Token": "not.a.jwt"})
    with pytest.raises(AppError) as exc:
        await verify_auth(req)
    assert exc.value.status_code == 401


# --- Issuer validation ---

@pytest.mark.asyncio
async def test_jwt_issuer_match(monkeypatch, settings):
    _set_jwt_secret(monkeypatch, settings, "a-jwt-secret-at-least-32-bytes-long!")
    monkeypatch.setattr(settings, "jwt_issuer", "go-account")
    token = _make_jwt("a-jwt-secret-at-least-32-bytes-long!", uid="alice", iss="go-account")
    req = _make_request({"X-Authorization-Token": token})
    result = await verify_auth(req)
    assert result["uid"] == "alice"


@pytest.mark.asyncio
async def test_jwt_issuer_mismatch(monkeypatch, settings):
    _set_jwt_secret(monkeypatch, settings, "a-jwt-secret-at-least-32-bytes-long!")
    monkeypatch.setattr(settings, "jwt_issuer", "go-account")
    token = _make_jwt("a-jwt-secret-at-least-32-bytes-long!", uid="alice", iss="impersonator")
    req = _make_request({"X-Authorization-Token": token})
    with pytest.raises(AppError) as exc:
        await verify_auth(req)
    assert exc.value.status_code == 401


# --- Both configured: either header works ---

@pytest.mark.asyncio
async def test_both_configured_jwt_works(monkeypatch, settings):
    _set_api_key(monkeypatch, settings, "api-key")
    _set_jwt_secret(monkeypatch, settings, "a-jwt-secret-at-least-32-bytes-long!")
    token = _make_jwt("a-jwt-secret-at-least-32-bytes-long!", uid="alice")
    req = _make_request({"X-Authorization-Token": token})
    result = await verify_auth(req)
    assert result["uid"] == "alice"


@pytest.mark.asyncio
async def test_both_configured_api_key_works(monkeypatch, settings):
    _set_api_key(monkeypatch, settings, "api-key")
    _set_jwt_secret(monkeypatch, settings, "a-jwt-secret-at-least-32-bytes-long!")
    req = _make_request({"X-API-Key": "api-key"})
    result = await verify_auth(req)
    assert result is None


@pytest.mark.asyncio
async def test_both_configured_neither_header(monkeypatch, settings):
    _set_api_key(monkeypatch, settings, "api-key")
    _set_jwt_secret(monkeypatch, settings, "a-jwt-secret-at-least-32-bytes-long!")
    req = _make_request()
    with pytest.raises(AppError) as exc:
        await verify_auth(req)
    assert exc.value.status_code == 401


@pytest.mark.asyncio
async def test_jwt_preferred_when_both_present(monkeypatch, settings):
    """If caller sends both headers, JWT takes precedence (it identifies a user)."""
    _set_api_key(monkeypatch, settings, "api-key")
    _set_jwt_secret(monkeypatch, settings, "a-jwt-secret-at-least-32-bytes-long!")
    token = _make_jwt("a-jwt-secret-at-least-32-bytes-long!", uid="alice")
    req = _make_request({"X-Authorization-Token": token, "X-API-Key": "api-key"})
    result = await verify_auth(req)
    assert result["uid"] == "alice"


@pytest.mark.asyncio
async def test_invalid_jwt_rejected_even_with_valid_api_key(monkeypatch, settings):
    """JWT precedence locks in: a bad JWT fails auth even if API key is valid.

    Prevents a future refactor from silently letting API key rescue a forged token.
    """
    _set_api_key(monkeypatch, settings, "api-key")
    _set_jwt_secret(monkeypatch, settings, "a-jwt-secret-at-least-32-bytes-long!")
    forged = _make_jwt("a-different-secret-at-least-32-bytes-!", uid="attacker")
    req = _make_request({"X-Authorization-Token": forged, "X-API-Key": "api-key"})
    with pytest.raises(AppError) as exc:
        await verify_auth(req)
    assert exc.value.status_code == 401


# --- Authorization: Bearer (OAuth2/OIDC standard) ---

@pytest.mark.asyncio
async def test_jwt_via_bearer_header(monkeypatch, settings):
    """`Authorization: Bearer <token>` works — standard OAuth2/OIDC scheme."""
    _set_jwt_secret(monkeypatch, settings, "a-jwt-secret-at-least-32-bytes-long!")
    token = _make_jwt("a-jwt-secret-at-least-32-bytes-long!", uid="alice")
    req = _make_request({"Authorization": f"Bearer {token}"})
    result = await verify_auth(req)
    assert result["uid"] == "alice"


@pytest.mark.asyncio
async def test_bearer_scheme_case_insensitive(monkeypatch, settings):
    """RFC 7235: auth scheme is case-insensitive."""
    _set_jwt_secret(monkeypatch, settings, "a-jwt-secret-at-least-32-bytes-long!")
    token = _make_jwt("a-jwt-secret-at-least-32-bytes-long!", uid="alice")
    req = _make_request({"Authorization": f"bearer {token}"})
    result = await verify_auth(req)
    assert result["uid"] == "alice"


@pytest.mark.asyncio
async def test_non_bearer_authorization_ignored(monkeypatch, settings):
    """`Authorization: Basic/Digest/...` must not be parsed as a JWT."""
    _set_jwt_secret(monkeypatch, settings, "a-jwt-secret-at-least-32-bytes-long!")
    req = _make_request({"Authorization": "Basic dXNlcjpwYXNz"})
    with pytest.raises(AppError) as exc:
        await verify_auth(req)
    assert exc.value.status_code == 401


@pytest.mark.asyncio
async def test_x_auth_token_preferred_over_bearer(monkeypatch, settings):
    """When both present, X-Authorization-Token wins (go-atlas convention)."""
    _set_jwt_secret(monkeypatch, settings, "a-jwt-secret-at-least-32-bytes-long!")
    primary = _make_jwt("a-jwt-secret-at-least-32-bytes-long!", uid="primary")
    secondary = _make_jwt("a-jwt-secret-at-least-32-bytes-long!", uid="secondary")
    req = _make_request({
        "X-Authorization-Token": primary,
        "Authorization": f"Bearer {secondary}",
    })
    result = await verify_auth(req)
    assert result["uid"] == "primary"


# --- Integration: dep actually wired onto protected routes ---

@pytest.mark.asyncio
async def test_protected_route_rejects_unauthed_request(client, monkeypatch):
    """Hitting a v1 route without auth must return 401 when API_KEY is set.

    Guards against a future router being registered without `_auth` in main.py.
    """
    s = get_settings()
    monkeypatch.setattr(s.api_key, "get_secret_value", lambda: "live-key")
    monkeypatch.setattr(s.jwt_secret, "get_secret_value", lambda: "")
    resp = await client.get("/v1/knowledgebases")
    assert resp.status_code == 401
