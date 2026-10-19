"""JWT verification parity with the go-atlas pillar/auth verifier, named
service keys, and the principal recorded for downstream checks."""
import time

import jwt as pyjwt
import pytest
from fastapi import Request

from api.auth import (
    ON_BEHALF_HEADER, auth_config_warnings, get_principal, require_api_key, verify_auth,
)
from api.errors import AppError
from config import get_settings

SECRET = "a-jwt-secret-at-least-32-bytes-long!"


def _req(headers: dict[str, str] | None = None) -> Request:
    headers = headers or {}
    return Request({
        "type": "http", "method": "GET", "path": "/v1/test",
        "headers": [(k.lower().encode(), v.encode()) for k, v in headers.items()],
    })


def _tok(**claims) -> str:
    now = int(time.time())
    payload = {"uid": "42", "typ": "access", "iss": "go-account", "iat": now, "exp": now + 300}
    payload.update(claims)
    return pyjwt.encode({k: v for k, v in payload.items() if v is not None}, SECRET, algorithm="HS256")


@pytest.fixture
def settings(monkeypatch):
    s = get_settings()
    monkeypatch.setattr(s.api_key, "get_secret_value", lambda: "")
    monkeypatch.setattr(s.jwt_secret, "get_secret_value", lambda: SECRET)
    monkeypatch.setattr(s, "jwt_algorithm", "HS256")
    monkeypatch.setattr(s, "jwt_issuer", "")
    monkeypatch.setattr(s, "jwt_valid_issuers", ["go-account"])
    monkeypatch.setattr(s, "jwt_leeway_seconds", 30)
    monkeypatch.setattr(s, "api_keys", {})
    monkeypatch.setattr(s, "allow_unauthenticated", False)
    return s


async def _jwt(token: str) -> Request:
    req = _req({"X-Authorization-Token": token})
    await verify_auth(req)
    return req


@pytest.mark.parametrize("claims", [
    {"typ": "refresh", "exp": int(time.time()) + 7 * 86400},   # refresh token as credential
    {"typ": None},                                            # pre-v0.6 token, no typ
    {"exp": None},                                            # never-expiring
    {"uid": None},
    {"uid": ""},
    {"uid": 42},                                              # go-atlas uid is a string
    {"iss": None},                                            # iss required when issuers set
    {"iss": "someone-else"},
])
async def test_rejects_tokens_go_atlas_rejects(settings, claims):
    with pytest.raises(AppError) as ei:
        await _jwt(_tok(**claims))
    assert ei.value.status_code == 401


async def test_leeway_absorbs_issuer_clock_ahead(settings):
    # Issuer's clock 2s ahead of ours: iat is "in the future" here.
    req = await _jwt(_tok(iat=int(time.time()) + 2))
    assert get_principal(req).uid == "42"


async def test_leeway_is_bounded(settings):
    with pytest.raises(AppError):
        await _jwt(_tok(exp=int(time.time()) - 120))


async def test_valid_issuers_is_a_list(settings, monkeypatch):
    monkeypatch.setattr(settings, "jwt_valid_issuers", ["go-account", "renamed"])
    await _jwt(_tok(iss="renamed"))


async def test_legacy_jwt_issuer_still_honoured(settings, monkeypatch):
    monkeypatch.setattr(settings, "jwt_valid_issuers", [])
    monkeypatch.setattr(settings, "jwt_issuer", "go-account")
    await _jwt(_tok())
    with pytest.raises(AppError):
        await _jwt(_tok(iss="other"))


async def test_bearer_prefix_on_custom_header(settings):
    req = await _jwt("Bearer " + _tok())
    assert get_principal(req).kind == "jwt"


async def test_principal_carries_uid_and_team_scope(settings):
    req = await _jwt(_tok(meta={"stid": 7, "src": "web"}))
    p = get_principal(req)
    assert (p.kind, p.uid, p.team_id, p.is_scoped) == ("jwt", "42", "7", True)


async def test_jwt_caller_cannot_claim_on_behalf(settings, monkeypatch):
    monkeypatch.setattr(settings, "api_keys", {"figlens": _secret("svc-key-figlens")})
    req = _req({"X-Authorization-Token": _tok(), ON_BEHALF_HEADER: "999"})
    await verify_auth(req)
    assert get_principal(req).uid == "42"


def _secret(v):
    from pydantic import SecretStr
    return SecretStr(v)


async def test_named_service_keys(settings, monkeypatch):
    monkeypatch.setattr(settings, "api_keys", {
        "figlens": _secret("svc-key-figlens"), "ops": _secret("svc-key-ops")})
    req = _req({"X-API-Key": "svc-key-ops"})
    assert await verify_auth(req) is None
    p = get_principal(req)
    assert (p.kind, p.service, p.is_scoped) == ("service", "ops", False)

    with pytest.raises(AppError) as ei:
        await verify_auth(_req({"X-API-Key": "nope"}))
    assert ei.value.status_code == 401


async def test_legacy_api_key_is_service_default(settings, monkeypatch):
    monkeypatch.setattr(settings.api_key, "get_secret_value", lambda: "legacy-key")
    req = _req({"X-API-Key": "legacy-key"})
    await verify_auth(req)
    assert get_principal(req).service == "default"


async def test_service_on_behalf_is_scoped(settings, monkeypatch):
    monkeypatch.setattr(settings, "api_keys", {"figlens": _secret("svc-key-figlens")})
    req = _req({"X-API-Key": "svc-key-figlens", ON_BEHALF_HEADER: " 42 "})
    await verify_auth(req)
    p = get_principal(req)
    assert (p.service, p.uid, p.is_scoped, p.label()) == ("figlens", "42", True, "key:figlens@42")


async def test_require_api_key_accepts_named_key(settings, monkeypatch):
    monkeypatch.setattr(settings, "api_keys", {"ops": _secret("svc-key-ops")})
    req = _req({"X-API-Key": "svc-key-ops"})
    await require_api_key(req)
    assert get_principal(req).service == "ops"
    with pytest.raises(AppError) as ei:
        await require_api_key(_req({"X-Authorization-Token": _tok()}))
    assert ei.value.status_code == 403


def test_config_warnings(settings, monkeypatch):
    assert auth_config_warnings() == []
    monkeypatch.setattr(settings, "jwt_valid_issuers", [])
    assert any("JWT_VALID_ISSUERS" in w for w in auth_config_warnings())
