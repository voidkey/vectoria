"""Mineru parser × circuit breaker integration.

Consecutive 5xx responses must open the breaker and short-circuit
subsequent calls without hitting httpx.
"""
from unittest.mock import AsyncMock, MagicMock, patch

import httpx
import pytest

from parsers.mineru_parser import MinerUParser


def _settings_mock():
    m = MagicMock()
    m.mineru_api_url = "http://mineru:8000"
    m.mineru_backend = "pipeline"
    m.mineru_language = "ch"
    m.mineru_breaker_threshold = 3
    m.mineru_breaker_reset_timeout = 60.0
    # The settings module is also touched by other callers; set enough
    # breaker fields so get_breaker doesn't KeyError.
    m.vision_breaker_threshold = 5
    m.vision_breaker_reset_timeout = 300.0
    m.embedding_breaker_threshold = 10
    m.embedding_breaker_reset_timeout = 60.0
    return m


@pytest.mark.asyncio
async def test_repeated_5xx_opens_breaker_then_short_circuits():
    """After threshold 5xx responses the parser returns empty without
    further HTTP calls — the whole point of the breaker.
    """
    call_count = 0

    def _fake_client(*_args, **_kwargs):
        mock_client = MagicMock()

        async def _post(*_a, **_k):
            nonlocal call_count
            call_count += 1
            req = httpx.Request("POST", "http://mineru:8000/file_parse")
            resp = MagicMock()
            resp.raise_for_status.side_effect = httpx.HTTPStatusError(
                "502 Bad Gateway",
                request=req,
                response=httpx.Response(502, request=req),
            )
            return resp

        mock_client.post = AsyncMock(side_effect=_post)
        cm = MagicMock()
        cm.__aenter__ = AsyncMock(return_value=mock_client)
        cm.__aexit__ = AsyncMock(return_value=False)
        return cm

    with patch("parsers.mineru_parser.httpx.AsyncClient", side_effect=_fake_client), \
         patch("parsers.mineru_parser.get_settings", return_value=_settings_mock()), \
         patch("config.get_settings", return_value=_settings_mock()):
        parser = MinerUParser(api_url="http://mineru:8000")

        # First 3 calls go through and raise (threshold=3).
        for _ in range(3):
            with pytest.raises(httpx.HTTPStatusError):
                await parser.parse(b"%PDF", filename="doc.pdf")

        assert call_count == 3

        # 4th call: breaker is now OPEN — parser returns empty without
        # invoking httpx. This is the fast-fail path operators will see
        # during a MinerU outage.
        result = await parser.parse(b"%PDF", filename="doc.pdf")
        assert result.content == ""
        assert result.title == "doc"
        assert call_count == 3, "OPEN breaker must not issue HTTP requests"


@pytest.mark.asyncio
async def test_open_circuit_makes_is_available_false_and_registry_falls_back():
    """When MinerU's circuit is OPEN, ``registry.auto_select`` for a PDF
    must skip mineru and pick the next candidate (docling). Without this,
    the breaker would only fail faster — not actually preserve the
    document-parsing functionality.
    """
    # Force breaker into OPEN via failures.
    def _fake_client(*_args, **_kwargs):
        mock_client = MagicMock()

        async def _post(*_a, **_k):
            req = httpx.Request("POST", "http://mineru:8000/file_parse")
            resp = MagicMock()
            resp.raise_for_status.side_effect = httpx.HTTPStatusError(
                "502", request=req,
                response=httpx.Response(502, request=req),
            )
            return resp

        mock_client.post = AsyncMock(side_effect=_post)
        cm = MagicMock()
        cm.__aenter__ = AsyncMock(return_value=mock_client)
        cm.__aexit__ = AsyncMock(return_value=False)
        return cm

    with patch("parsers.mineru_parser.httpx.AsyncClient", side_effect=_fake_client), \
         patch("parsers.mineru_parser.get_settings", return_value=_settings_mock()), \
         patch("config.get_settings", return_value=_settings_mock()):
        parser = MinerUParser(api_url="http://mineru:8000")
        for _ in range(3):
            with pytest.raises(httpx.HTTPStatusError):
                await parser.parse(b"%PDF", filename="doc.pdf")

        # Circuit is now OPEN. is_available should reflect that.
        from infra.circuit_breaker import State, get_breaker
        assert get_breaker("mineru").current_state() is State.OPEN
        assert MinerUParser.is_available() is False

        # And the registry should pick a different engine for a .pdf.
        from parsers.registry import registry
        # Only assert that mineru is not picked — the exact fallback
        # depends on what else is registered and locally available.
        assert registry.auto_select(filename="x.pdf") != "mineru"


@pytest.mark.asyncio
async def test_4xx_does_not_open_breaker():
    """Client errors (bad PDF) are our problem, not the service's.
    Repeated 400s should keep propagating but leave the breaker CLOSED.
    """
    def _fake_client(*_args, **_kwargs):
        mock_client = MagicMock()

        async def _post(*_a, **_k):
            req = httpx.Request("POST", "http://mineru:8000/file_parse")
            resp = MagicMock()
            resp.raise_for_status.side_effect = httpx.HTTPStatusError(
                "400 Bad Request",
                request=req,
                response=httpx.Response(400, request=req),
            )
            return resp

        mock_client.post = AsyncMock(side_effect=_post)
        cm = MagicMock()
        cm.__aenter__ = AsyncMock(return_value=mock_client)
        cm.__aexit__ = AsyncMock(return_value=False)
        return cm

    with patch("parsers.mineru_parser.httpx.AsyncClient", side_effect=_fake_client), \
         patch("parsers.mineru_parser.get_settings", return_value=_settings_mock()), \
         patch("config.get_settings", return_value=_settings_mock()):
        parser = MinerUParser(api_url="http://mineru:8000")

        # 10 > threshold (3). All should raise; breaker should still be CLOSED.
        for _ in range(10):
            with pytest.raises(httpx.HTTPStatusError):
                await parser.parse(b"%PDF", filename="doc.pdf")

        # Verify by peeking at breaker state via direct import.
        from infra.circuit_breaker import State, get_breaker
        assert get_breaker("mineru").current_state() is State.CLOSED
