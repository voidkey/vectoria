"""VisionNativeParser — VLM-as-parser for image uploads.

The parser sits in front of ocr-native in the registry preference
chain for image extensions. ``is_available()`` is a 3-way gate (config
+ breaker + budget) so the registry can downgrade gracefully:

  configured? → breaker closed? → under budget? → vision-native
       ↓ no            ↓ open          ↓ over
       fall through to ocr-native (always available, in-process)

Tests cover the gate trio plus a happy-path parse via mocked client.
The actual VLM call is mocked — we're not validating model output
quality here, only that the parser wires inputs/outputs correctly
and the cost tracker / breaker integration is honored.
"""
from __future__ import annotations

from unittest.mock import AsyncMock, patch

import pytest

from parsers.vision_parser import VisionNativeParser


# Tiny but valid PNG so detect_mime_type() returns image/png and the
# parser doesn't reject the input as non-image.
_PNG_HEAD = b"\x89PNG\r\n\x1a\n" + b"\x00" * 16


# ---------------------------------------------------------------------------
# is_available() — 3-way gate
# ---------------------------------------------------------------------------

def test_is_available_false_when_vision_unconfigured():
    """Open-source default: vision_base_url='' → engine inactive,
    chain falls through to ocr-native."""
    from config import get_settings
    cfg = get_settings()
    saved = cfg.vision_base_url
    try:
        cfg.vision_base_url = ""
        assert VisionNativeParser.is_available() is False
    finally:
        cfg.vision_base_url = saved


def test_is_available_false_when_breaker_open(monkeypatch):
    """Provider outage tripped the vision breaker → registry skips
    vision-native at upload time, exactly the same way it skips
    mineru when its breaker opens."""
    from config import get_settings
    monkeypatch.setattr(get_settings(), "vision_base_url", "https://stub")

    from infra.circuit_breaker import State
    fake_breaker = type("FB", (), {"current_state": staticmethod(lambda: State.OPEN)})()
    monkeypatch.setattr(
        "parsers.vision_parser.get_breaker",
        lambda name: fake_breaker if name == "vision" else None,
    )
    # Budget tracker untouched — ensure it's not what's gating.
    from vision import budget as _b
    _b._reset_for_tests()

    assert VisionNativeParser.is_available() is False


def test_is_available_false_when_over_budget(monkeypatch):
    """Soft daily-budget cap: once today's spend ≥
    vision_daily_budget_usd, advertise unavailable so registry routes
    image uploads to rapidocr until UTC day flips.
    """
    from config import get_settings
    cfg = get_settings()
    monkeypatch.setattr(cfg, "vision_base_url", "https://stub")
    monkeypatch.setattr(cfg, "vision_daily_budget_usd", 0.01)
    monkeypatch.setattr(cfg, "vision_cost_per_call_usd", 0.005)

    # Force breaker CLOSED so it's not what's failing.
    from infra.circuit_breaker import State
    fake_breaker = type("FB", (), {"current_state": staticmethod(lambda: State.CLOSED)})()
    monkeypatch.setattr(
        "parsers.vision_parser.get_breaker",
        lambda name: fake_breaker if name == "vision" else None,
    )

    from vision import budget as _b
    _b._reset_for_tests()
    tracker = _b.get_cost_tracker()
    # Two calls × 0.005 = 0.01, hits the cap.
    tracker.record(purpose="parse")
    tracker.record(purpose="parse")
    assert VisionNativeParser.is_available() is False


def test_is_available_true_when_all_gates_pass(monkeypatch):
    from config import get_settings
    cfg = get_settings()
    monkeypatch.setattr(cfg, "vision_base_url", "https://stub")
    monkeypatch.setattr(cfg, "vision_daily_budget_usd", 0.0)  # uncapped

    from infra.circuit_breaker import State
    fake_breaker = type("FB", (), {"current_state": staticmethod(lambda: State.CLOSED)})()
    monkeypatch.setattr(
        "parsers.vision_parser.get_breaker",
        lambda name: fake_breaker if name == "vision" else None,
    )
    from vision import budget as _b
    _b._reset_for_tests()
    assert VisionNativeParser.is_available() is True


# ---------------------------------------------------------------------------
# parse() — happy path + error contract
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_parse_returns_markdown_with_title_header(monkeypatch):
    """Happy path: VLM returns dual-output markdown; parser wraps it
    in an h1 derived from the filename and emits a ParseResult.
    image_refs is empty — the input *is* the image."""
    from config import get_settings
    cfg = get_settings()
    monkeypatch.setattr(cfg, "vision_base_url", "https://stub")

    fake_client = AsyncMock()
    fake_client.parse_image = AsyncMock(
        return_value="## Description\nA sales comparison chart.\n\n## Verbatim\nQ1=2M Q2=3.5M",
    )
    with patch("vision.client.VisionClient", return_value=fake_client):
        result = await VisionNativeParser().parse(_PNG_HEAD, filename="chart.png")

    assert "# chart" in result.content       # title header from filename stem
    assert "## Description" in result.content
    assert "## Verbatim" in result.content
    assert result.title == "chart"
    assert result.image_refs == []           # input is the image, nothing to extract


@pytest.mark.asyncio
async def test_parse_raises_on_empty_vlm_response(monkeypatch):
    """If VLM returns empty (rate-limit cooldown returning blank,
    content-filter wipe), raise so the handler's per-attempt fallback
    routes to ocr-native rather than persisting a hollow doc.
    """
    from config import get_settings
    monkeypatch.setattr(get_settings(), "vision_base_url", "https://stub")

    fake_client = AsyncMock()
    fake_client.parse_image = AsyncMock(return_value="   \n  ")
    with patch("vision.client.VisionClient", return_value=fake_client):
        with pytest.raises(ValueError, match="empty content"):
            await VisionNativeParser().parse(_PNG_HEAD, filename="x.png")


@pytest.mark.asyncio
async def test_parse_rejects_non_image_bytes():
    """Defensive: handler always sends bytes for image uploads, but
    if a caller wires up wrong (passing a URL string), fail fast
    rather than ship garbage to the VLM and pay tokens."""
    with pytest.raises(ValueError, match="expects bytes"):
        await VisionNativeParser().parse("not bytes", filename="x.png")


# ---------------------------------------------------------------------------
# Registry wiring
# ---------------------------------------------------------------------------

def test_registry_preference_chain_is_vision_then_ocr():
    """Image extensions: vision-native first, ocr-native as fallback.
    Verifying via fallback_chain() ensures the registry actually sees
    the new entries (catches typos in _EXT_PREFERENCE)."""
    from parsers.registry import registry
    for ext in ("png", "jpg", "jpeg", "tiff", "bmp", "webp"):
        chain = registry.fallback_chain(filename=f"x.{ext}")
        assert chain == ["vision-native", "ocr-native"], (
            f"chain for .{ext} unexpected: {chain}"
        )


def test_registry_auto_select_falls_back_to_ocr_when_vision_unconfigured():
    """End-to-end: with vision_base_url='' (default open-source
    setup), uploading a PNG must auto-select ocr-native — not
    vision-native, which would fail at runtime — because
    is_available() returns False.
    """
    from config import get_settings
    saved = get_settings().vision_base_url
    try:
        get_settings().vision_base_url = ""
        from parsers.registry import registry
        assert registry.auto_select(filename="poster.png") == "ocr-native"
    finally:
        get_settings().vision_base_url = saved


@pytest.mark.asyncio
async def test_vision_parser_does_not_pass_per_request_language():
    """Output language is deployment-fixed (VISION_DEFAULT_LANGUAGE), resolved
    inside the vision client — the parser must not thread a per-request kwarg.
    """
    from unittest.mock import AsyncMock, patch
    from parsers.vision_parser import VisionNativeParser
    fake_client = AsyncMock()
    fake_client.parse_image = AsyncMock(return_value="## Description\nx\n\n## Verbatim\ny")
    # Client is constructed inline via `vision.client.VisionClient(...)` — patch the class.
    with patch("vision.client.VisionClient", return_value=fake_client):
        await VisionNativeParser().parse(b"\x89PNG\r\n\x1a\n" + b"\x00" * 16, filename="x.png")
    _, kwargs = fake_client.parse_image.call_args
    assert "language" not in kwargs
