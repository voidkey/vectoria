"""Inbound per-principal rate limiter contract.

Why this layer exists separately from infra.ratelimit
-----------------------------------------------------
``infra.ratelimit`` is a token-bucket primitive — caller-agnostic.
This module turns it into a FastAPI ``Depends`` factory that
identifies *who* is calling so abuse from one principal can't
amplify writes by sharing a bucket across the whole service.

The principal-derivation order matters: JWT ``sub`` is the strongest
identity, then a hash of the static API key (never the raw secret),
then client IP (honouring X-Forwarded-For because behind the SLB the
direct peer is a 100.64.x.x internal hop).
"""
import hashlib

import pytest
from fastapi import Request, Response
from limits.aio.storage import MemoryStorage

from api.errors import AppError, ErrorCode
from infra import ratelimit


def _make_request(headers: dict[str, str] | None = None, client: tuple[str, int] | None = None) -> Request:
    headers = headers or {}
    scope = {
        "type": "http",
        "method": "POST",
        "path": "/v1/knowledgebases",
        "headers": [(k.lower().encode(), v.encode()) for k, v in headers.items()],
    }
    if client is not None:
        scope["client"] = client
    return Request(scope)


def _make_response() -> Response:
    """Stand-in for the FastAPI-injected response the dep mutates."""
    return Response()


@pytest.fixture(autouse=True)
def _memory_backend():
    """Reset the ratelimit singleton between tests so buckets don't bleed."""
    ratelimit._reset_for_tests()
    ratelimit._set_storage_for_tests(MemoryStorage())
    yield
    ratelimit._reset_for_tests()


# --- _principal_key resolution ---

def test_principal_key_prefers_jwt_sub():
    from api.rate_limit import _principal_key
    req = _make_request({"X-API-Key": "alice"})
    assert _principal_key(req, {"sub": "user-7"}) == "jwt:user-7"


def test_principal_key_uses_uid_when_sub_missing():
    """go-atlas-issued tokens carry uid instead of sub; treat as identity."""
    from api.rate_limit import _principal_key
    req = _make_request()
    assert _principal_key(req, {"uid": "alice"}) == "jwt:alice"


def test_principal_key_falls_back_to_api_key_hash_when_no_claims():
    from api.rate_limit import _principal_key
    req = _make_request({"X-API-Key": "secret-key"})
    expected = f"key:{hashlib.sha256(b'secret-key').hexdigest()[:16]}"
    assert _principal_key(req, None) == expected


def test_principal_key_never_leaks_raw_api_key():
    """The raw API key must not appear in the bucket id — buckets land in
    metric labels and would otherwise expose the secret to anyone reading
    /metrics."""
    from api.rate_limit import _principal_key
    raw = "super-secret-token"
    req = _make_request({"X-API-Key": raw})
    key = _principal_key(req, None)
    assert raw not in key


def test_principal_key_uses_x_forwarded_for_when_no_auth():
    from api.rate_limit import _principal_key
    req = _make_request({"X-Forwarded-For": "203.0.113.7"})
    assert _principal_key(req, None) == "ip:203.0.113.7"


def test_principal_key_takes_first_xff_hop():
    """X-Forwarded-For is a chain; the leftmost address is the original client."""
    from api.rate_limit import _principal_key
    req = _make_request({"X-Forwarded-For": "203.0.113.7, 100.64.0.1, 10.0.0.1"})
    assert _principal_key(req, None) == "ip:203.0.113.7"


def test_principal_key_falls_back_to_client_host():
    from api.rate_limit import _principal_key
    req = _make_request(client=("198.51.100.42", 12345))
    assert _principal_key(req, None) == "ip:198.51.100.42"


# --- rate_limit dep behaviour ---

async def test_rate_limit_allows_up_to_threshold_then_blocks():
    from api.rate_limit import rate_limit
    dep = rate_limit("bucket-a", rate=2, per_seconds=60)
    req = _make_request({"X-API-Key": "alice"})

    await dep(req, _make_response(), claims=None)
    await dep(req, _make_response(), claims=None)
    with pytest.raises(AppError) as exc:
        await dep(req, _make_response(), claims=None)
    assert exc.value.status_code == 429
    assert exc.value.code == ErrorCode.RATE_LIMITED


async def test_rate_limit_different_principals_independent():
    from api.rate_limit import rate_limit
    dep = rate_limit("bucket-a", rate=1, per_seconds=60)

    alice = _make_request({"X-API-Key": "alice"})
    bob = _make_request({"X-API-Key": "bob"})

    await dep(alice, _make_response(), claims=None)
    with pytest.raises(AppError):
        await dep(alice, _make_response(), claims=None)

    # Bob still has a fresh bucket — alice's spam doesn't affect him.
    await dep(bob, _make_response(), claims=None)


async def test_rate_limit_different_buckets_independent():
    """Same principal, two separate route buckets — limits don't cross-talk."""
    from api.rate_limit import rate_limit
    dep_a = rate_limit("bucket-a", rate=1, per_seconds=60)
    dep_b = rate_limit("bucket-b", rate=1, per_seconds=60)
    req = _make_request({"X-API-Key": "alice"})

    await dep_a(req, _make_response(), claims=None)
    with pytest.raises(AppError):
        await dep_a(req, _make_response(), claims=None)

    # bucket-b is a separate counter even for the same caller.
    await dep_b(req, _make_response(), claims=None)


async def test_rate_limit_zero_rate_disables_enforcement():
    """rate=0 is the operator kill-switch: pass everything through."""
    from api.rate_limit import rate_limit
    dep = rate_limit("bucket-a", rate=0, per_seconds=60)
    req = _make_request({"X-API-Key": "alice"})

    # Many calls all succeed even though rate "would be" 0.
    for _ in range(100):
        await dep(req, _make_response(), claims=None)


async def test_rate_limit_accepts_callable_rate():
    """Wiring at module import time can't read live settings — accept a
    zero-arg callable so the limit is resolved at request time, letting
    operators retune via env without redeploy."""
    from api.rate_limit import rate_limit

    current_rate = [2]
    dep = rate_limit("bucket-a", rate=lambda: current_rate[0], per_seconds=60)
    req = _make_request({"X-API-Key": "alice"})

    await dep(req, _make_response(), claims=None)
    await dep(req, _make_response(), claims=None)
    with pytest.raises(AppError):
        await dep(req, _make_response(), claims=None)


async def test_rate_limit_jwt_principal_separated_from_api_key_principal():
    """A user authenticated by JWT and the same API key share NO bucket.
    They're different identities even if the caller is one process."""
    from api.rate_limit import rate_limit
    dep = rate_limit("bucket-a", rate=1, per_seconds=60)

    req = _make_request({"X-API-Key": "k"})
    await dep(req, _make_response(), claims={"sub": "alice"})  # JWT bucket
    # Same request but treated as API-key (no claims) → independent bucket
    await dep(req, _make_response(), claims=None)


# --- Integration: dep is actually wired onto protected routes ---
#
# The unit tests above exercise the dep in isolation. These hit the
# real FastAPI app to catch regressions where someone adds a new write
# route under /v1 and forgets to attach the limiter, or where the route
# wiring resolves the wrong config knob.

from unittest.mock import AsyncMock, MagicMock, patch
from datetime import datetime

from config import get_settings


def _mock_kb_session():
    """Stand-in for the create_kb DB session — returns the mocked KB row
    with a predictable id so the response shape is well-formed."""
    sess_patch = patch("api.routes.knowledgebase.get_session")
    mock = sess_patch.start()
    session = AsyncMock()
    session.add = MagicMock()
    session.commit = AsyncMock()

    def _refresh(obj):
        obj.id = "kb-test"
        obj.created_at = datetime(2026, 1, 1)

    session.refresh = AsyncMock(side_effect=_refresh)
    mock.return_value.__aenter__.return_value = session
    return sess_patch


async def test_create_kb_returns_429_after_threshold(client, monkeypatch):
    monkeypatch.setattr(get_settings(), "ratelimit_kb_create_per_min", 2)
    sess_patch = _mock_kb_session()
    try:
        for _ in range(2):
            resp = await client.post(
                "/v1/knowledgebases",
                json={"name": "X", "description": ""},
                headers={"X-API-Key": "alice"},
            )
            assert resp.status_code == 201, resp.text

        resp = await client.post(
            "/v1/knowledgebases",
            json={"name": "X", "description": ""},
            headers={"X-API-Key": "alice"},
        )
        assert resp.status_code == 429, resp.text
        assert resp.json()["code"] == ErrorCode.RATE_LIMITED
    finally:
        sess_patch.stop()


async def test_create_kb_different_api_keys_have_independent_limits(client, monkeypatch):
    """A rogue alice spamming creates must not lock bob out — the whole
    point of per-principal buckets."""
    monkeypatch.setattr(get_settings(), "ratelimit_kb_create_per_min", 1)
    sess_patch = _mock_kb_session()
    try:
        # alice eats her bucket
        assert (await client.post(
            "/v1/knowledgebases",
            json={"name": "X", "description": ""},
            headers={"X-API-Key": "alice"},
        )).status_code == 201
        assert (await client.post(
            "/v1/knowledgebases",
            json={"name": "X", "description": ""},
            headers={"X-API-Key": "alice"},
        )).status_code == 429

        # bob is unaffected
        assert (await client.post(
            "/v1/knowledgebases",
            json={"name": "X", "description": ""},
            headers={"X-API-Key": "bob"},
        )).status_code == 201
    finally:
        sess_patch.stop()


async def test_create_kb_rate_zero_disables_limit(client, monkeypatch):
    """ratelimit_kb_create_per_min=0 is the documented kill-switch."""
    monkeypatch.setattr(get_settings(), "ratelimit_kb_create_per_min", 0)
    sess_patch = _mock_kb_session()
    try:
        for _ in range(20):
            resp = await client.post(
                "/v1/knowledgebases",
                json={"name": "X", "description": ""},
                headers={"X-API-Key": "alice"},
            )
            assert resp.status_code == 201
    finally:
        sess_patch.stop()


async def test_create_kb_success_response_carries_rate_limit_headers(client, monkeypatch):
    """Standard ratelimit headers (GitHub/Stripe convention) on every
    response so clients can self-pace before hitting 429.

    Asserts: X-RateLimit-Limit, X-RateLimit-Remaining, X-RateLimit-Reset.
    """
    monkeypatch.setattr(get_settings(), "ratelimit_kb_create_per_min", 3)
    sess_patch = _mock_kb_session()
    try:
        resp = await client.post(
            "/v1/knowledgebases",
            json={"name": "X", "description": ""},
            headers={"X-API-Key": "alice"},
        )
        assert resp.status_code == 201
        assert resp.headers["x-ratelimit-limit"] == "3"
        # After one hit on a 3-rate bucket, 2 remain.
        assert resp.headers["x-ratelimit-remaining"] == "2"
        # Reset is a future unix timestamp.
        import time
        reset = int(resp.headers["x-ratelimit-reset"])
        now = int(time.time())
        assert now <= reset <= now + 65
    finally:
        sess_patch.stop()


async def test_create_kb_429_response_carries_retry_after_and_ratelimit_headers(client, monkeypatch):
    """429 must include Retry-After (RFC 6585 SHOULD) plus the same
    X-RateLimit-* headers so a backing-off client can plan its retry."""
    monkeypatch.setattr(get_settings(), "ratelimit_kb_create_per_min", 1)
    sess_patch = _mock_kb_session()
    try:
        # Burn the bucket.
        assert (await client.post(
            "/v1/knowledgebases",
            json={"name": "X", "description": ""},
            headers={"X-API-Key": "alice"},
        )).status_code == 201

        resp = await client.post(
            "/v1/knowledgebases",
            json={"name": "X", "description": ""},
            headers={"X-API-Key": "alice"},
        )
        assert resp.status_code == 429
        assert resp.headers["retry-after"].isdigit()
        assert int(resp.headers["retry-after"]) >= 1
        assert resp.headers["x-ratelimit-limit"] == "1"
        assert resp.headers["x-ratelimit-remaining"] == "0"
        assert "x-ratelimit-reset" in resp.headers
    finally:
        sess_patch.stop()


async def test_create_kb_429_message_does_not_leak_internal_bucket_name(client, monkeypatch):
    """Public-facing 429 detail stays generic; the internal bucket name
    (``kb_create``) is an implementation detail and must not leak in the
    error body."""
    monkeypatch.setattr(get_settings(), "ratelimit_kb_create_per_min", 1)
    sess_patch = _mock_kb_session()
    try:
        await client.post(
            "/v1/knowledgebases",
            json={"name": "X", "description": ""},
            headers={"X-API-Key": "alice"},
        )
        resp = await client.post(
            "/v1/knowledgebases",
            json={"name": "X", "description": ""},
            headers={"X-API-Key": "alice"},
        )
        assert resp.status_code == 429
        body = resp.json()
        assert "kb_create" not in body["detail"].lower()
    finally:
        sess_patch.stop()


async def test_inbound_metric_uses_bucket_label_only(client, monkeypatch):
    """Prometheus cardinality safety: the metric label must be the
    bucket name, never the per-principal hash, otherwise /metrics grows
    one series per distinct caller forever."""
    from prometheus_client import REGISTRY

    monkeypatch.setattr(get_settings(), "ratelimit_kb_create_per_min", 10)

    def _read(label_key: str) -> float:
        v = REGISTRY.get_sample_value(
            "vectoria_ratelimit_checks_total",
            {"key": label_key, "result": "allowed"},
        )
        return float(v or 0)

    sess_patch = _mock_kb_session()
    try:
        before_bucket = _read("inbound:kb_create")
        await client.post(
            "/v1/knowledgebases",
            json={"name": "X", "description": ""},
            headers={"X-API-Key": "alice"},
        )

        # The bucket-only label increments; any principal-bearing variant
        # must stay zero (the principal hash is per-caller and would
        # explode label cardinality).
        assert _read("inbound:kb_create") == before_bucket + 1
        principal_hash = hashlib.sha256(b"alice").hexdigest()[:16]
        assert _read(f"inbound:kb_create:key:{principal_hash}") == 0
        assert _read(f"kb_create:key:{principal_hash}") == 0
    finally:
        sess_patch.stop()


async def test_openapi_schema_documents_429_on_limited_routes(client):
    """OpenAPI is the contract; if a route silently returns 429 without
    declaring it, codegen clients won't handle it. Locks in
    ``responses=RATE_LIMITED_RESPONSE`` on all four wired routes."""
    resp = await client.get("/openapi.json")
    assert resp.status_code == 200
    schema = resp.json()
    paths = schema["paths"]

    limited = [
        ("/v1/knowledgebases", "post"),
        ("/v1/knowledgebases/{kb_id}/documents/file", "post"),
        ("/v1/knowledgebases/{kb_id}/documents/url", "post"),
        ("/v1/knowledgebases/{kb_id}/documents/text", "post"),
        ("/v1/knowledgebases/{kb_id}/query", "post"),
    ]
    for path, method in limited:
        responses = paths[path][method]["responses"]
        assert "429" in responses, f"{method.upper()} {path} missing 429 in OpenAPI"
        headers = responses["429"].get("headers", {})
        assert "Retry-After" in headers
        assert "X-RateLimit-Limit" in headers


async def test_doc_ingest_text_returns_429_after_threshold(client, monkeypatch):
    """All three ingest verbs share the same bucket — pick the cheapest
    (text) to prove the dep is wired. If this 429s, the shared
    ``_ingest_limiter`` constant is correctly attached."""
    monkeypatch.setattr(get_settings(), "ratelimit_doc_ingest_per_min", 2)

    with (
        patch("api.routes.documents._validate_kb", new=AsyncMock()),
        patch("api.routes.documents.get_storage") as mock_storage,
        patch("api.routes.documents.get_session") as mock_sess,
        patch("worker.queue.enqueue", new=AsyncMock()),
        patch("api.routes.documents._find_existing_by_hash", new=AsyncMock(return_value=None)),
    ):
        storage = AsyncMock()
        mock_storage.return_value = storage

        session = AsyncMock()
        session.add = MagicMock()
        session.commit = AsyncMock()

        def _refresh(obj):
            obj.id = "doc-test"
            obj.created_at = datetime(2026, 1, 1)

        session.refresh = AsyncMock(side_effect=_refresh)
        mock_sess.return_value.__aenter__.return_value = session

        # Each text body is unique so dedup doesn't short-circuit the path.
        for i in range(2):
            resp = await client.post(
                "/v1/knowledgebases/kb-x/documents/text",
                json={"text": f"sample doc body {i}"},
                headers={"X-API-Key": "alice"},
            )
            assert resp.status_code == 201, resp.text

        resp = await client.post(
            "/v1/knowledgebases/kb-x/documents/text",
            json={"text": "third body"},
            headers={"X-API-Key": "alice"},
        )
        assert resp.status_code == 429, resp.text
        assert resp.json()["code"] == ErrorCode.RATE_LIMITED


async def test_query_returns_429_after_threshold(client, monkeypatch):
    """/query is the expensive RAG endpoint; prove the per-principal limiter
    is wired. The pipeline is fully mocked — we're testing the gate, not RAG."""
    monkeypatch.setattr(get_settings(), "ratelimit_query_per_min", 2)

    pipeline = MagicMock()
    pipeline.steps = []
    ctx = MagicMock()
    ctx.answer = "stub answer"
    ctx.sources = []
    pipeline.run = AsyncMock(return_value=ctx)

    with (
        patch("api.routes.query.PgVectorStore.create", new=AsyncMock()),
        patch("api.routes.query.get_embedder", new=MagicMock()),
        patch("api.routes.query.build_default_pipeline", return_value=pipeline),
    ):
        for _ in range(2):
            resp = await client.post(
                "/v1/knowledgebases/kb-x/query",
                json={"query": "hello"},
                headers={"X-API-Key": "alice"},
            )
            assert resp.status_code == 200, resp.text

        resp = await client.post(
            "/v1/knowledgebases/kb-x/query",
            json={"query": "hello"},
            headers={"X-API-Key": "alice"},
        )
        assert resp.status_code == 429, resp.text
        assert resp.json()["code"] == ErrorCode.RATE_LIMITED
