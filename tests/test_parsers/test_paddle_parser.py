import base64
import pytest
import httpx
from unittest.mock import patch, AsyncMock, MagicMock

from parsers.paddle_parser import PaddleParser, _IMG_TAG_RE


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_payload(
    pages: list[tuple[str, dict[str, str]]], error_code: int = 0,
) -> dict:
    """Build a minimal VL response. ``pages`` is a list of (markdown_text,
    images_dict) tuples — one tuple per source PDF page, preserving order
    so the parser's per-position page indexing can be asserted.
    """
    return {
        "logId": "test-log",
        "errorCode": error_code,
        "errorMsg": "Success" if error_code == 0 else "format unsupported",
        "result": {
            "layoutParsingResults": [
                {"markdown": {"text": text, "images": images}}
                for text, images in pages
            ],
        },
    }


def _patch_settings(
    mock_settings, url: str = "http://paddle:8080", key: str = "test-key",
    concurrency: int = 3, timeout: float = 600.0,
    relay_endpoint: str = "", relay_bucket: str = "",
) -> None:
    """Wire pydantic-style settings access into a MagicMock.
    ``paddle_api_key`` is a SecretStr in real code — emulate the
    ``.get_secret_value()`` accessor with a nested mock. Relay fields
    default to empty (relay off) — a bare MagicMock attribute is truthy
    and would silently flip every test onto the relay path.
    """
    mock_settings.return_value.paddle_api_url = url
    mock_settings.return_value.paddle_api_key.get_secret_value.return_value = key
    mock_settings.return_value.paddle_timeout = timeout
    mock_settings.return_value.paddle_concurrency = concurrency
    mock_settings.return_value.paddle_relay_endpoint = relay_endpoint
    mock_settings.return_value.paddle_relay_download_endpoint = ""
    mock_settings.return_value.paddle_relay_region = ""
    mock_settings.return_value.paddle_relay_access_key = (
        "relay-ak" if relay_endpoint else ""
    )
    mock_settings.return_value.paddle_relay_secret_key.get_secret_value.return_value = (
        "relay-sk" if relay_endpoint else ""
    )
    mock_settings.return_value.paddle_relay_bucket = relay_bucket
    mock_settings.return_value.paddle_relay_addressing_style = "virtual"
    mock_settings.return_value.paddle_relay_prefix = "paddle-relay/"
    mock_settings.return_value.paddle_relay_url_expires = 3600


# ---------------------------------------------------------------------------
# Happy path
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_parse_concatenates_pages_in_order():
    payload = _make_payload([
        ("# Page 1\n\nintro", {}),
        ("# Page 2\n\nbody", {}),
        ("# Page 3\n\noutro", {}),
    ])
    with patch("parsers.paddle_parser.httpx.AsyncClient") as MockClient, \
         patch("parsers.paddle_parser.get_settings") as mock_settings:
        _patch_settings(mock_settings)
        mock_resp = MagicMock()
        mock_resp.raise_for_status = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = payload
        MockClient.return_value.__aenter__.return_value.post = AsyncMock(
            return_value=mock_resp,
        )

        parser = PaddleParser(api_url="http://paddle:8080", api_key="k")
        result = await parser.parse(b"%PDF fake", filename="doc.pdf")

    # All three page bodies present, in order, separated by blank lines.
    assert "Page 1" in result.content
    assert "Page 2" in result.content
    assert "Page 3" in result.content
    assert result.content.index("Page 1") < result.content.index("Page 2") < result.content.index("Page 3")
    assert result.page_count == 3
    assert result.title == "doc"


@pytest.mark.asyncio
async def test_parse_request_shape():
    """VL gateway requires ``X-API-Key`` header and JSON body with
    ``file`` (base64) + ``fileType=0``. Lock the wire shape so future
    refactors don't silently drift away from the API contract."""
    pdf = b"%PDF-1.4 ... bytes"
    captured = {}

    async def fake_post(url, headers=None, json=None, **_):
        captured["url"] = url
        captured["headers"] = headers
        captured["json"] = json
        mock_resp = MagicMock()
        mock_resp.raise_for_status = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = _make_payload([("hello world", {})])
        return mock_resp

    with patch("parsers.paddle_parser.httpx.AsyncClient") as MockClient, \
         patch("parsers.paddle_parser.get_settings") as mock_settings:
        _patch_settings(mock_settings)
        MockClient.return_value.__aenter__.return_value.post = AsyncMock(
            side_effect=fake_post,
        )
        parser = PaddleParser(api_url="http://paddle:8080", api_key="my-key")
        await parser.parse(pdf, filename="doc.pdf")

    assert captured["url"] == "http://paddle:8080/layout-parsing"
    assert captured["headers"]["X-API-Key"] == "my-key"
    assert captured["json"]["fileType"] == 0
    # visualize=false suppresses outputImages/inputImage in the response
    # — measured 53MB → 1.8MB on a 40-page slide deck; the parser never
    # reads those fields, and over a cross-region link they're fatal.
    assert captured["json"]["visualize"] is False
    # Round-trip the base64 to prove the wire payload decodes back to
    # the exact source bytes — guards against accidental utf-8 round-trips.
    assert base64.b64decode(captured["json"]["file"]) == pdf


@pytest.mark.asyncio
async def test_parse_yields_lazy_image_refs_with_page():
    """Image basename → ImageRef with factory; page derived from the
    layoutParsingResults array index (1-based), not from any metadata
    field. This is strictly cleaner than MinerU's content_list scheme."""
    png_bytes = b"\x89PNG\r\n\x1a\n" + b"\x00" * 32
    b64 = base64.b64encode(png_bytes).decode()
    payload = _make_payload([
        ("page 1 text", {}),
        ('<img src="imgs/img_in_image_box_108_142_279_362.jpg">',
         {"imgs/img_in_image_box_108_142_279_362.jpg": b64}),
    ])

    with patch("parsers.paddle_parser.httpx.AsyncClient") as MockClient, \
         patch("parsers.paddle_parser.get_settings") as mock_settings:
        _patch_settings(mock_settings)
        mock_resp = MagicMock()
        mock_resp.raise_for_status = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = payload
        MockClient.return_value.__aenter__.return_value.post = AsyncMock(
            return_value=mock_resp,
        )
        parser = PaddleParser(api_url="http://paddle:8080", api_key="k")
        result = await parser.parse(b"%PDF", filename="doc.pdf")

    assert len(result.image_refs) == 1
    ref = result.image_refs[0]
    # Basename only — relative path prefix ("imgs/") is stripped so the
    # markdown ![](...) reference (which the parser also rewrites to the
    # basename) matches downstream metadata-extraction by name.
    assert ref.name == "img_in_image_box_108_142_279_362.jpg"
    assert ref.mime == "image/jpeg"
    assert ref.page == 2
    assert ref.materialize() == png_bytes
    # Markdown reference was rewritten from <img> to ![]() — the
    # extension is preserved on the basename.
    assert "![](img_in_image_box_108_142_279_362.jpg)" in result.content
    assert "<img" not in result.content


def test_img_tag_rewrite_handles_attributes_and_quotes():
    """The rewrite must tolerate single/double quotes, extra attrs, and
    self-closing slashes. These all come up in real VL output."""
    cases = [
        ('<img src="a.png">', "![](a.png)"),
        ("<img src='b.jpg'>", "![](b.jpg)"),
        ('<img src="c.png" alt="x" />', "![](c.png)"),
        ('<IMG SRC="d.webp" width="100">', "![](d.webp)"),
        ('<img alt="x" src="imgs/e.png">', "![](e.png)"),
    ]
    for src, expected in cases:
        # Mirror the parser's lambda: rewrite to basename only.
        from pathlib import PurePosixPath
        out = _IMG_TAG_RE.sub(
            lambda m: f"![]({PurePosixPath(m.group(1)).name})", src,
        )
        assert out == expected, f"{src!r} → {out!r} (expected {expected!r})"


# ---------------------------------------------------------------------------
# Failure modes
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_business_error_raises_for_chain_fallback():
    """HTTP 200 + errorCode != 0 (e.g. format unsupported) must raise so
    the worker's per-attempt chain fallback can move on to mineru.
    Returning empty would short-circuit the chain in handler logic."""
    payload = _make_payload([("ignored", {})], error_code=1001)
    with patch("parsers.paddle_parser.httpx.AsyncClient") as MockClient, \
         patch("parsers.paddle_parser.get_settings") as mock_settings:
        _patch_settings(mock_settings)
        mock_resp = MagicMock()
        mock_resp.raise_for_status = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = payload
        MockClient.return_value.__aenter__.return_value.post = AsyncMock(
            return_value=mock_resp,
        )
        parser = PaddleParser(api_url="http://paddle:8080", api_key="k")
        with pytest.raises(RuntimeError, match="errorCode=1001"):
            await parser.parse(b"%PDF", filename="doc.pdf")


@pytest.mark.asyncio
async def test_http_5xx_raises_for_breaker_and_fallback():
    """5xx from the gateway must propagate as HTTPStatusError — the
    breaker's ``_http_server_error`` predicate counts it as a failure
    (toward opening), and the worker's _DEP_LEVEL_ERRORS classification
    triggers fallback to mineru."""
    with patch("parsers.paddle_parser.httpx.AsyncClient") as MockClient, \
         patch("parsers.paddle_parser.get_settings") as mock_settings:
        _patch_settings(mock_settings)
        mock_resp = MagicMock()
        mock_resp.raise_for_status.side_effect = httpx.HTTPStatusError(
            "502 Bad Gateway", request=MagicMock(), response=MagicMock(),
        )
        MockClient.return_value.__aenter__.return_value.post = AsyncMock(
            return_value=mock_resp,
        )
        parser = PaddleParser(api_url="http://paddle:8080", api_key="k")
        with pytest.raises(httpx.HTTPStatusError):
            await parser.parse(b"%PDF", filename="doc.pdf")


@pytest.mark.asyncio
async def test_empty_url_or_key_returns_empty():
    """Either URL or API key unset → empty result. The worker's "below
    min_content_chars and no images" branch then treats this exactly
    like "paddle not configured" and falls through to the next engine."""
    with patch("parsers.paddle_parser.get_settings") as mock_settings:
        _patch_settings(mock_settings, url="", key="k")
        parser = PaddleParser(api_url="", api_key="k")
        result = await parser.parse(b"data", filename="doc.pdf")
        assert result.content == ""
        assert result.title == "doc"

    with patch("parsers.paddle_parser.get_settings") as mock_settings:
        _patch_settings(mock_settings, url="http://paddle:8080", key="")
        parser = PaddleParser(api_url="http://paddle:8080", api_key="")
        result = await parser.parse(b"data", filename="doc.pdf")
        assert result.content == ""


# ---------------------------------------------------------------------------
# Relay (cross-region file hand-off via S3-compatible bucket)
# ---------------------------------------------------------------------------


def _relay_setup(mock_settings, MockStorage, presigned="https://cos/signed-url"):
    """Enable relay in settings and return the mocked storage instance
    whose put/presign_url/delete the parser should drive."""
    _patch_settings(
        mock_settings,
        relay_endpoint="https://cos.accelerate.myqcloud.com",
        relay_bucket="vk-test",
    )
    store = MagicMock()
    store.put = AsyncMock()
    store.presign_url = AsyncMock(return_value=presigned)
    store.delete = AsyncMock()
    MockStorage.return_value = store
    return store


def _gateway_capture(MockClient, payload=None, post_error=None):
    """Wire the mocked httpx client to capture the request body."""
    captured = {}

    async def fake_post(url, headers=None, json=None, **_):
        captured["json"] = json
        if post_error is not None:
            raise post_error
        mock_resp = MagicMock()
        mock_resp.raise_for_status = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = payload or _make_payload([("hi", {})])
        return mock_resp

    MockClient.return_value.__aenter__.return_value.post = AsyncMock(
        side_effect=fake_post,
    )
    return captured


@pytest.mark.asyncio
async def test_relay_sends_presigned_url_instead_of_base64():
    """With relay configured, the PDF bytes go to the bucket and the
    gateway receives the presigned URL in ``file`` — the whole point is
    that the big payload never rides the app→gateway link."""
    pdf = b"%PDF-1.4 relay me"
    with patch("parsers.paddle_parser.httpx.AsyncClient") as MockClient, \
         patch("parsers.paddle_parser.S3ObjectStorage") as MockStorage, \
         patch("parsers.paddle_parser.get_settings") as mock_settings:
        store = _relay_setup(mock_settings, MockStorage)
        captured = _gateway_capture(MockClient)

        parser = PaddleParser(api_url="http://paddle:8080", api_key="k")
        result = await parser.parse(pdf, filename="doc.pdf")

    assert captured["json"]["file"] == "https://cos/signed-url"
    assert captured["json"]["fileType"] == 0
    # Uploaded the raw bytes under the relay prefix, as a .pdf, with mime.
    store.put.assert_awaited_once()
    key, data = store.put.await_args.args[0], store.put.await_args.args[1]
    assert key.startswith("paddle-relay/")
    assert key.endswith(".pdf")
    assert data == pdf
    # Object is transient: deleted after the gateway call completes.
    store.delete.assert_awaited_once_with(key)
    assert "hi" in result.content


@pytest.mark.asyncio
async def test_relay_object_deleted_even_when_gateway_fails():
    """Gateway 5xx/network failure must still clean up the relay object —
    the bucket lifecycle rule is a backstop, not the primary GC."""
    with patch("parsers.paddle_parser.httpx.AsyncClient") as MockClient, \
         patch("parsers.paddle_parser.S3ObjectStorage") as MockStorage, \
         patch("parsers.paddle_parser.get_settings") as mock_settings:
        store = _relay_setup(mock_settings, MockStorage)
        _gateway_capture(
            MockClient, post_error=httpx.ConnectError("boom"),
        )
        parser = PaddleParser(api_url="http://paddle:8080", api_key="k")
        with pytest.raises(httpx.ConnectError):
            await parser.parse(b"%PDF", filename="doc.pdf")

    store.delete.assert_awaited_once()


@pytest.mark.asyncio
async def test_relay_upload_failure_falls_back_to_inline_base64():
    """Bucket down ≠ parse down. Upload failure logs a warning and the
    request goes out the old way (inline base64) so the document still
    parses — relay is an optimization, never a new failure mode."""
    pdf = b"%PDF-1.4 fallback"
    with patch("parsers.paddle_parser.httpx.AsyncClient") as MockClient, \
         patch("parsers.paddle_parser.S3ObjectStorage") as MockStorage, \
         patch("parsers.paddle_parser.get_settings") as mock_settings:
        store = _relay_setup(mock_settings, MockStorage)
        store.put.side_effect = RuntimeError("cos unreachable")
        captured = _gateway_capture(MockClient)

        parser = PaddleParser(api_url="http://paddle:8080", api_key="k")
        result = await parser.parse(pdf, filename="doc.pdf")

    assert base64.b64decode(captured["json"]["file"]) == pdf
    assert "hi" in result.content


@pytest.mark.asyncio
async def test_no_relay_when_not_configured():
    """Relay settings empty → no storage client is even constructed and
    the wire payload is inline base64, byte-identical to today."""
    pdf = b"%PDF-1.4 plain"
    with patch("parsers.paddle_parser.httpx.AsyncClient") as MockClient, \
         patch("parsers.paddle_parser.S3ObjectStorage") as MockStorage, \
         patch("parsers.paddle_parser.get_settings") as mock_settings:
        _patch_settings(mock_settings)
        captured = _gateway_capture(MockClient)

        parser = PaddleParser(api_url="http://paddle:8080", api_key="k")
        await parser.parse(pdf, filename="doc.pdf")

    MockStorage.assert_not_called()
    assert base64.b64decode(captured["json"]["file"]) == pdf


# ---------------------------------------------------------------------------
# Registration sanity
# ---------------------------------------------------------------------------


def test_engine_name():
    assert PaddleParser.engine_name == "paddle"


def test_is_available_requires_both_url_and_key():
    """Both URL and key must be present. Missing either short-circuits
    is_available so the registry falls straight to mineru instead of
    burning the breaker on a 401."""
    with patch("parsers.paddle_parser.get_settings") as mock_settings:
        _patch_settings(mock_settings, url="", key="k")
        assert PaddleParser.is_available() is False
    with patch("parsers.paddle_parser.get_settings") as mock_settings:
        _patch_settings(mock_settings, url="http://paddle:8080", key="")
        assert PaddleParser.is_available() is False


def test_is_available_true_when_both_set():
    """First availability check constructs the ``paddle`` breaker via
    ``get_breaker('paddle')``, which imports ``config.get_settings``
    inside the function (lazy import) — so the patch target is
    ``config.get_settings``, not ``infra.circuit_breaker.get_settings``.
    """
    from infra.circuit_breaker import _reset_breakers_for_tests
    _reset_breakers_for_tests()
    with patch("parsers.paddle_parser.get_settings") as mock_settings, \
         patch("config.get_settings") as mock_cfg:
        _patch_settings(mock_settings)
        mock_cfg.return_value.paddle_breaker_threshold = 5
        mock_cfg.return_value.paddle_breaker_reset_timeout = 300.0
        assert PaddleParser.is_available() is True
    _reset_breakers_for_tests()
