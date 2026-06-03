import pytest
from unittest.mock import MagicMock

from config import get_settings
from vision.client import _describe_system_prompt, _parse_system_prompt, VisionClient


def test_describe_prompt_injects_language():
    p = _describe_system_prompt("Portuguese")
    assert "Respond in Portuguese" in p


def test_parse_prompt_injects_language_and_fixed_headers():
    p = _parse_system_prompt("Spanish")
    assert "## Description" in p
    assert "## Verbatim" in p
    assert "in Spanish" in p
    assert "not translate" in p.lower()


_PNG = b"\x89PNG\r\n\x1a\n" + b"\x00" * 16


def _client_capturing(captured):
    """A VisionClient whose LLM call records the messages it was given."""
    c = VisionClient(base_url="http://x", api_key="k", model="m")

    async def fake_create(*, model, messages, **kw):
        captured["messages"] = messages
        resp = MagicMock()
        resp.choices = [MagicMock(message=MagicMock(content="## Description\nx\n\n## Verbatim\ny"))]
        return resp

    c._client = MagicMock()
    c._client.chat.completions.create = fake_create
    return c


@pytest.mark.asyncio
async def test_describe_uses_configured_default_language(monkeypatch):
    # No per-request language anymore: the deployment default drives output.
    monkeypatch.setattr(get_settings(), "vision_default_language", "pt")
    captured = {}
    await _client_capturing(captured).describe(_PNG)
    assert "Respond in Portuguese" in captured["messages"][0]["content"]


@pytest.mark.asyncio
async def test_parse_image_uses_configured_default_language(monkeypatch):
    monkeypatch.setattr(get_settings(), "vision_default_language", "es")
    captured = {}
    await _client_capturing(captured).parse_image(_PNG)
    sys_msg = captured["messages"][0]["content"]
    assert "in Spanish" in sys_msg and "## Description" in sys_msg
