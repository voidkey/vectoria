import base64
import shutil
import subprocess

import pytest
from unittest.mock import AsyncMock, MagicMock, patch

from infra.circuit_breaker import _http_server_error_unless_client
from parsers.asr_parser import (
    AsrRequestError,
    AsrUpstreamError,
    UnreadableAudioError,
    VolcAsrParser,
    _render_markdown,
)

_needs_ffmpeg = pytest.mark.skipif(
    shutil.which("ffmpeg") is None, reason="ffmpeg not installed",
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _patch_settings(
    mock_settings, api_key: str = "new-key", app_key: str = "",
    access_key: str = "", url: str = "https://asr.test/flash",
) -> None:
    s = mock_settings.return_value
    s.volc_asr_url = url
    s.volc_asr_api_key.get_secret_value.return_value = api_key
    s.volc_asr_app_key = app_key
    s.volc_asr_access_key.get_secret_value.return_value = access_key
    s.volc_asr_resource_id = "volc.bigasr.auc_turbo"
    s.volc_asr_timeout = 30.0
    s.volc_asr_concurrency = 2
    s.max_audio_seconds = 7200


def _response(status: str, body: dict | None = None) -> MagicMock:
    resp = MagicMock()
    resp.raise_for_status = MagicMock()
    resp.headers = {
        "X-Api-Status-Code": status,
        "X-Api-Message": "msg",
        "X-Tt-Logid": "log-1",
    }
    resp.json.return_value = body or {}
    return resp


def _mock_client(MockClient, *responses: MagicMock) -> AsyncMock:
    client = AsyncMock()
    client.post.side_effect = list(responses)
    MockClient.return_value.__aenter__ = AsyncMock(return_value=client)
    MockClient.return_value.__aexit__ = AsyncMock(return_value=False)
    return client


_OK_BODY = {
    "audio_info": {"duration": 65000},
    "result": {
        "text": "你好。今天讲向量检索。",
        "utterances": [
            {"start_time": 450, "end_time": 1530, "text": "你好。"},
            {"start_time": 62000, "end_time": 65000, "text": "今天讲向量检索。"},
        ],
    },
}


@pytest.fixture
def fake_transcode():
    """Skip ffmpeg: the "transcoded" audio is the input bytes."""
    with patch(
        "parsers.asr_parser.transcode_to_mp3",
        AsyncMock(side_effect=lambda content, timeout: content),
    ) as mock:
        yield mock


def _wav_bytes() -> bytes:
    return subprocess.run(
        ["ffmpeg", "-loglevel", "error", "-f", "lavfi", "-i", "sine=f=440:d=1",
         "-ar", "16000", "-ac", "1", "-f", "wav", "pipe:1"],
        check=True, capture_output=True,
    ).stdout


# ---------------------------------------------------------------------------
# Happy path
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_sends_transcoded_audio_and_renders_timestamps(fake_transcode):
    audio = b"ID3\x03\x00fake-mp3-bytes"
    with patch("parsers.asr_parser.httpx.AsyncClient") as MockClient, \
         patch("parsers.asr_parser.get_settings") as mock_settings:
        _patch_settings(mock_settings)
        client = _mock_client(MockClient, _response("20000000", _OK_BODY))
        result = await VolcAsrParser().parse(audio, filename="lecture.mp3")

    assert result.title == "lecture"
    assert result.content == (
        "# lecture\n\n[00:00] 你好。\n\n[01:02] 今天讲向量检索。\n"
    )
    kwargs = client.post.call_args.kwargs
    assert kwargs["json"]["audio"]["data"] == base64.b64encode(audio).decode()
    assert kwargs["json"]["request"]["model_name"] == "bigmodel"
    headers = kwargs["headers"]
    assert headers["X-Api-Key"] == "new-key"
    assert headers["X-Api-Resource-Id"] == "volc.bigasr.auc_turbo"
    assert "X-Api-App-Key" not in headers


@pytest.mark.asyncio
async def test_old_console_credentials_use_app_and_access_key_headers(fake_transcode):
    with patch("parsers.asr_parser.httpx.AsyncClient") as MockClient, \
         patch("parsers.asr_parser.get_settings") as mock_settings:
        _patch_settings(mock_settings, api_key="", app_key="app", access_key="tok")
        client = _mock_client(MockClient, _response("20000000", _OK_BODY))
        await VolcAsrParser().parse(b"x", filename="a.wav")

    headers = client.post.call_args.kwargs["headers"]
    assert headers["X-Api-App-Key"] == "app"
    assert headers["X-Api-Access-Key"] == "tok"
    assert "X-Api-Key" not in headers


@pytest.mark.asyncio
@pytest.mark.parametrize("status", ["20000003", "45000002"])
async def test_silent_or_empty_audio_returns_empty_content(status, fake_transcode):
    with patch("parsers.asr_parser.httpx.AsyncClient") as MockClient, \
         patch("parsers.asr_parser.get_settings") as mock_settings:
        _patch_settings(mock_settings)
        _mock_client(MockClient, _response(status))
        result = await VolcAsrParser().parse(b"x", filename="quiet.mp3")
    assert result.content == ""


# ---------------------------------------------------------------------------
# Transcoding
# ---------------------------------------------------------------------------


@_needs_ffmpeg
@pytest.mark.asyncio
async def test_audio_is_transcoded_to_mp3():
    with patch("parsers.asr_parser.httpx.AsyncClient") as MockClient, \
         patch("parsers.asr_parser.get_settings") as mock_settings:
        _patch_settings(mock_settings)
        client = _mock_client(MockClient, _response("20000000", _OK_BODY))
        await VolcAsrParser().parse(_wav_bytes(), filename="memo.flac")

    sent = base64.b64decode(client.post.call_args.kwargs["json"]["audio"]["data"])
    # MP3: either an ID3 tag or a bare frame sync.
    assert sent[:3] == b"ID3" or sent[:2] in (b"\xff\xfb", b"\xff\xf3")


@_needs_ffmpeg
@pytest.mark.asyncio
async def test_transcodes_long_m4a_with_trailing_index(tmp_path):
    # ffmpeg's mp4 muxer writes the moov atom last by default; past the
    # demuxer's probe window that is unreadable from a pipe.
    from parsers._audio import transcode_to_mp3
    path = tmp_path / "a.m4a"
    subprocess.run(
        ["ffmpeg", "-loglevel", "error", "-f", "lavfi", "-i", "sine=f=440:d=300",
         "-b:a", "128k", str(path)],
        check=True,
    )
    out = await transcode_to_mp3(path.read_bytes(), timeout=60)
    # 300 s at 32 kbps ≈ 1.2 MB; a truncated decode yields a few bytes.
    assert len(out) > 1_000_000


@pytest.mark.asyncio
async def test_rejected_format_is_permanent(fake_transcode):
    with patch("parsers.asr_parser.httpx.AsyncClient") as MockClient, \
         patch("parsers.asr_parser.get_settings") as mock_settings:
        _patch_settings(mock_settings)
        _mock_client(MockClient, _response("45000151"))
        with pytest.raises(UnreadableAudioError):
            await VolcAsrParser().parse(b"x", filename="a.m4a")


@_needs_ffmpeg
@pytest.mark.asyncio
async def test_undecodable_input_is_permanent():
    with patch("parsers.asr_parser.get_settings") as mock_settings:
        _patch_settings(mock_settings)
        with pytest.raises(UnreadableAudioError):
            await VolcAsrParser().parse(b"not audio at all", filename="a.m4a")


# ---------------------------------------------------------------------------
# Upstream failures / availability
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_server_busy_raises_breaker_counted_error(fake_transcode):
    with patch("parsers.asr_parser.httpx.AsyncClient") as MockClient, \
         patch("parsers.asr_parser.get_settings") as mock_settings:
        _patch_settings(mock_settings)
        _mock_client(MockClient, _response("55000031"))
        with pytest.raises(AsrUpstreamError) as exc_info:
            await VolcAsrParser().parse(b"x", filename="a.mp3")
    assert _http_server_error_unless_client(exc_info.value) is True


@pytest.mark.asyncio
async def test_request_error_is_not_counted_by_breaker(fake_transcode):
    with patch("parsers.asr_parser.httpx.AsyncClient") as MockClient, \
         patch("parsers.asr_parser.get_settings") as mock_settings:
        _patch_settings(mock_settings)
        _mock_client(MockClient, _response("45000001"))
        with pytest.raises(AsrRequestError) as exc_info:
            await VolcAsrParser().parse(b"x", filename="a.mp3")
    assert _http_server_error_unless_client(exc_info.value) is False


@pytest.mark.parametrize(
    "api_key,app_key,access_key,expected",
    [
        ("k", "", "", True),
        ("", "app", "tok", True),
        ("", "app", "", False),
        ("", "", "", False),
    ],
)
def test_is_available_requires_credentials(api_key, app_key, access_key, expected):
    with patch("parsers.asr_parser.get_settings") as mock_settings:
        _patch_settings(
            mock_settings, api_key=api_key, app_key=app_key, access_key=access_key,
        )
        assert VolcAsrParser.is_available() is expected


# ---------------------------------------------------------------------------
# Rendering
# ---------------------------------------------------------------------------


def test_render_uses_hours_past_one_hour():
    body = {"result": {"utterances": [{"start_time": 3_725_000, "text": "late"}]}}
    assert "[01:02:05] late" in _render_markdown("t", body)


def test_render_falls_back_to_flat_text():
    assert _render_markdown("t", {"result": {"text": "flat"}}) == "# t\n\nflat\n"


def test_render_empty_payload():
    assert _render_markdown("t", {}) == ""


@pytest.mark.asyncio
async def test_over_max_duration_is_permanent_and_skips_request(fake_transcode):
    from parsers._audio import AudioTooLongError
    # 32 kbps → 4000 B/s; 2 h + 10 min of "encoded" audio.
    long_audio = b"\x00" * (4000 * 7800)
    with patch("parsers.asr_parser.httpx.AsyncClient") as MockClient, \
         patch("parsers.asr_parser.get_settings") as mock_settings:
        _patch_settings(mock_settings)
        client = _mock_client(MockClient)
        with pytest.raises(AudioTooLongError):
            await VolcAsrParser().parse(long_audio, filename="long.m4a")
    client.post.assert_not_called()


def test_check_duration_allows_cap_and_disabled():
    from parsers._audio import check_duration
    check_duration(4000 * 7200, 7200)       # exactly at the cap
    check_duration(4000 * 100_000, 0)       # 0 = no cap


@_needs_ffmpeg
@pytest.mark.asyncio
async def test_encoded_size_tracks_real_duration():
    # The duration check assumes our encode is 32 kbps CBR.
    from parsers._audio import transcode_to_mp3
    out = await transcode_to_mp3(
        subprocess.run(
            ["ffmpeg", "-loglevel", "error", "-f", "lavfi", "-i",
             "sine=f=440:d=120", "-f", "wav", "pipe:1"],
            check=True, capture_output=True,
        ).stdout,
        timeout=60,
    )
    assert abs(len(out) / 4000 - 120) < 1.2
