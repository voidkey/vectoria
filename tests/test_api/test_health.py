"""Tests for the deep health check endpoint."""

import pytest
from unittest.mock import patch, AsyncMock


@pytest.mark.asyncio
async def test_health_all_ok(client):
    """When DB and storage are reachable, status should be 'ok'."""
    with patch("api.routes.health._check_db", new_callable=AsyncMock, return_value=True), \
         patch("api.routes.health._check_storage", new_callable=AsyncMock, return_value=True):
        resp = await client.get("/health")

    assert resp.status_code == 200
    body = resp.json()
    assert body["status"] == "ok"
    assert body["checks"]["database"] is True
    assert body["checks"]["storage"] is True


@pytest.mark.asyncio
async def test_health_db_down(client):
    """When DB is unreachable, status should be 'degraded'."""
    with patch("api.routes.health._check_db", new_callable=AsyncMock, return_value=False), \
         patch("api.routes.health._check_storage", new_callable=AsyncMock, return_value=True):
        resp = await client.get("/health")

    assert resp.status_code == 200
    body = resp.json()
    assert body["status"] == "degraded"
    assert body["checks"]["database"] is False
    assert body["checks"]["storage"] is True


@pytest.mark.asyncio
async def test_health_storage_down(client):
    """When storage is unreachable, status should be 'degraded'."""
    with patch("api.routes.health._check_db", new_callable=AsyncMock, return_value=True), \
         patch("api.routes.health._check_storage", new_callable=AsyncMock, return_value=False):
        resp = await client.get("/health")

    assert resp.status_code == 200
    body = resp.json()
    assert body["status"] == "degraded"
    assert body["checks"]["database"] is True
    assert body["checks"]["storage"] is False
