"""Tests for request-id middleware."""

import pytest
from unittest.mock import patch, AsyncMock


@pytest.mark.asyncio
async def test_response_has_request_id(client):
    """Every response should include an X-Request-Id header."""
    with patch("api.routes.health._check_db", new_callable=AsyncMock, return_value=True), \
         patch("api.routes.health._check_storage", new_callable=AsyncMock, return_value=True):
        resp = await client.get("/health")

    assert "x-request-id" in resp.headers
    assert len(resp.headers["x-request-id"]) > 0


@pytest.mark.asyncio
async def test_request_id_passthrough(client):
    """If client sends X-Request-Id, the same value should be echoed."""
    with patch("api.routes.health._check_db", new_callable=AsyncMock, return_value=True), \
         patch("api.routes.health._check_storage", new_callable=AsyncMock, return_value=True):
        resp = await client.get("/health", headers={"X-Request-Id": "my-custom-id"})

    assert resp.headers["x-request-id"] == "my-custom-id"
