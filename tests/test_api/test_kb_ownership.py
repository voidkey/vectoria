"""KB ownership: JWT callers (and services acting for a user) reach only
the KBs they own; foreign KBs look missing."""
import time
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import jwt as pyjwt
import pytest
from fastapi.routing import APIRoute

from api.auth import Principal
from api.errors import AppError
from api.kb_access import authorize_kb
from config import get_settings

SECRET = "a-jwt-secret-at-least-32-bytes-long!"


def test_every_kb_scoped_route_checks_ownership():
    """Guard: a new /knowledgebases/{kb_id}/... route must not skip the check."""
    from main import app

    def deps(dependant):
        for d in dependant.dependencies:
            yield d.call
            yield from deps(d)

    scoped = [r for r in app.routes if isinstance(r, APIRoute) and "{kb_id}" in r.path]
    assert scoped, "expected kb-scoped routes"
    missing = [r.path for r in scoped if authorize_kb not in set(deps(r.dependant))]
    assert missing == []


def _request(kb_id, principal):
    return SimpleNamespace(path_params={"kb_id": kb_id} if kb_id else {},
                           state=SimpleNamespace(principal=principal))


def _owner_row(owner):
    result = MagicMock()
    result.first.return_value = None if owner == "<missing>" else SimpleNamespace(id="kb1", owner_uid=owner)
    session = AsyncMock()
    session.execute = AsyncMock(return_value=result)
    cm = MagicMock()
    cm.__aenter__ = AsyncMock(return_value=session)
    cm.__aexit__ = AsyncMock(return_value=False)
    return patch("api.kb_access.get_session", return_value=cm)


JWT_42 = Principal(kind="jwt", uid="42")
SVC = Principal(kind="service", service="figlens")
SVC_FOR_42 = Principal(kind="service", service="figlens", uid="42")


@pytest.mark.parametrize("principal,owner,allowed", [
    (JWT_42, "42", True),
    (JWT_42, "7", False),
    (SVC_FOR_42, "42", True),
    (SVC_FOR_42, "7", False),
    (SVC, "7", True),            # trusted service, no user
    (JWT_42, "<missing>", True),  # left to the route's own 404
])
async def test_authorize_kb(principal, owner, allowed):
    with _owner_row(owner):
        if allowed:
            await authorize_kb(_request("kb1", principal))
        else:
            with pytest.raises(AppError) as ei:
                await authorize_kb(_request("kb1", principal))
            assert ei.value.status_code == 404


@pytest.mark.parametrize("legacy_on,allowed", [(True, True), (False, False)])
async def test_unowned_legacy_kb(monkeypatch, legacy_on, allowed):
    monkeypatch.setattr(get_settings(), "kb_legacy_unowned_access", legacy_on)
    with _owner_row(None):
        if allowed:
            await authorize_kb(_request("kb1", JWT_42))
        else:
            with pytest.raises(AppError):
                await authorize_kb(_request("kb1", JWT_42))


async def test_routes_without_kb_id_skip_check():
    with _owner_row("7") as gs:
        await authorize_kb(_request(None, JWT_42))
    gs.assert_not_called()


@pytest.fixture
def jwt_mode(monkeypatch):
    s = get_settings()
    monkeypatch.setattr(s, "allow_unauthenticated", False)
    monkeypatch.setattr(s.jwt_secret, "get_secret_value", lambda: SECRET)
    monkeypatch.setattr(s, "jwt_valid_issuers", ["go-account"])
    monkeypatch.setattr(s, "api_keys", {})
    monkeypatch.setattr(s.api_key, "get_secret_value", lambda: "")


def _hdr(uid):
    now = int(time.time())
    tok = pyjwt.encode({"uid": uid, "typ": "access", "iss": "go-account",
                        "iat": now, "exp": now + 300}, SECRET, algorithm="HS256")
    return {"X-Authorization-Token": tok}


async def test_foreign_kb_is_404_over_http(client, jwt_mode):
    with _owner_row("7"):
        resp = await client.delete("/v1/knowledgebases/kb1", headers=_hdr("42"))
    assert resp.status_code == 404


async def test_create_records_owner_and_list_is_scoped(client, jwt_mode):
    created = {}

    def _add(kb):
        created["owner_uid"] = kb.owner_uid
        kb.id, kb.created_at = "kb-new", __import__("datetime").datetime(2026, 1, 1)

    with patch("api.routes.knowledgebase.get_session") as gs:
        session = AsyncMock()
        session.add = MagicMock(side_effect=_add)
        session.refresh = AsyncMock()
        result = MagicMock()
        result.scalars.return_value.all.return_value = []
        session.execute = AsyncMock(return_value=result)
        session.scalar = AsyncMock(return_value=0)
        gs.return_value.__aenter__.return_value = session

        resp = await client.post("/v1/knowledgebases", json={"name": "n"}, headers=_hdr("42"))
        assert resp.status_code == 201
        assert created["owner_uid"] == "42"

        resp = await client.get("/v1/knowledgebases", headers=_hdr("42"))
        assert resp.status_code == 200
        list_sql = str(session.execute.call_args.args[0])
        assert "owner_uid" in list_sql
