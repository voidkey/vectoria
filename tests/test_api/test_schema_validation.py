"""Tests for Pydantic schema boundary validation."""

import pytest
from unittest.mock import patch, AsyncMock, MagicMock


@pytest.mark.asyncio
async def test_query_top_k_too_large(client):
    """top_k > 100 should be rejected."""
    resp = await client.post(
        "/v1/knowledgebases/kb1/query",
        json={"query": "hello", "top_k": 999},
    )
    assert resp.status_code == 422


@pytest.mark.asyncio
async def test_query_top_k_zero(client):
    """top_k < 1 should be rejected."""
    resp = await client.post(
        "/v1/knowledgebases/kb1/query",
        json={"query": "hello", "top_k": 0},
    )
    assert resp.status_code == 422


@pytest.mark.asyncio
async def test_query_too_long(client):
    """Query exceeding 2000 chars should be rejected."""
    resp = await client.post(
        "/v1/knowledgebases/kb1/query",
        json={"query": "x" * 2001},
    )
    assert resp.status_code == 422


@pytest.mark.asyncio
async def test_kb_name_empty(client):
    """KB name must not be empty."""
    resp = await client.post("/v1/knowledgebases", json={"name": ""})
    assert resp.status_code == 422


@pytest.mark.asyncio
async def test_kb_name_too_long(client):
    """KB name exceeding 255 chars should be rejected."""
    resp = await client.post(
        "/v1/knowledgebases",
        json={"name": "x" * 256},
    )
    assert resp.status_code == 422
