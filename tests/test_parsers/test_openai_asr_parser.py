import shutil
import subprocess
from types import SimpleNamespace

import pytest
from unittest.mock import AsyncMock, patch

from parsers._audio import UnreadableAudioError, transcode_segments
from parsers.openai_asr_parser import OpenAIAsrParser

_needs_ffmpeg = pytest.mark.skipif(
    shutil.which("ffmpeg") is None, reason="ffmpeg not installed",
)


def _patch_settings(
    mock_settings, base_url: str = "https://asr.test/v1",
    timestamps: bool = True,
) -> None:
    s = mock_settings.return_value
    s.openai_asr_base_url = base_url
    s.openai_asr_api_key.get_secret_value.return_value = "k"
    s.openai_asr_model = "whisper-1"
    s.openai_asr_timestamps = timestamps
    s.openai_asr_segment_seconds = 1200
    s.openai_asr_timeout = 30.0
    s.openai_asr_concurrency = 2


def _verbose(*segs: tuple[float, str]) -> SimpleNamespace:
    return SimpleNamespace(
        text=" ".join(t for _, t in segs),
        segments=[SimpleNamespace(start=start, text=t) for start, t in segs],
    )


def _mock_openai(MockOpenAI, *responses) -> AsyncMock:
    create = AsyncMock(side_effect=list(responses))
    MockOpenAI.return_value.audio.transcriptions.create = create
    return create


def _sine(seconds: int) -> bytes:
    return subprocess.run(
        ["ffmpeg", "-loglevel", "error", "-f", "lavfi",
         "-i", f"sine=f=440:d={seconds}", "-f", "wav", "pipe:1"],
        check=True, capture_output=True,
    ).stdout


@pytest.mark.asyncio
async def test_segments_offset_by_piece_start():
    pieces = [(0, b"p1"), (1_200_000, b"p2")]
    with patch("parsers.openai_asr_parser.AsyncOpenAI") as MockOpenAI, \
         patch("parsers.openai_asr_parser.get_settings") as mock_settings, \
         patch("parsers.openai_asr_parser.transcode_segments",
               AsyncMock(return_value=pieces)):
        _patch_settings(mock_settings)
        create = _mock_openai(
            MockOpenAI,
            _verbose((0.4, " 开场。"), (65.0, " 第一部分。")),
            _verbose((3.0, " 第二部分。")),
        )
        result = await OpenAIAsrParser().parse(b"x", filename="talk.m4a")

    assert result.content == (
        "# talk\n\n[00:00] 开场。\n\n[01:05] 第一部分。\n\n[20:03] 第二部分。\n"
    )
    assert create.await_count == 2
    kwargs = create.await_args_list[0].kwargs
    assert kwargs["model"] == "whisper-1"
    assert kwargs["response_format"] == "verbose_json"
    assert kwargs["file"][1] == b"p1"


@pytest.mark.asyncio
async def test_without_timestamps_single_piece_is_unstamped():
    with patch("parsers.openai_asr_parser.AsyncOpenAI") as MockOpenAI, \
         patch("parsers.openai_asr_parser.get_settings") as mock_settings, \
         patch("parsers.openai_asr_parser.transcode_segments",
               AsyncMock(return_value=[(0, b"p1")])):
        _patch_settings(mock_settings, timestamps=False)
        create = _mock_openai(MockOpenAI, SimpleNamespace(text="全文。"))
        result = await OpenAIAsrParser().parse(b"x", filename="memo.mp3")

    assert create.await_args.kwargs["response_format"] == "json"
    assert result.content == "# memo\n\n全文。\n"


@pytest.mark.asyncio
async def test_without_timestamps_multiple_pieces_are_stamped():
    with patch("parsers.openai_asr_parser.AsyncOpenAI") as MockOpenAI, \
         patch("parsers.openai_asr_parser.get_settings") as mock_settings, \
         patch("parsers.openai_asr_parser.transcode_segments",
               AsyncMock(return_value=[(0, b"a"), (1_200_000, b"b")])):
        _patch_settings(mock_settings, timestamps=False)
        _mock_openai(
            MockOpenAI, SimpleNamespace(text="前半。"), SimpleNamespace(text="后半。"),
        )
        result = await OpenAIAsrParser().parse(b"x", filename="m.mp3")

    assert "[00:00] 前半。" in result.content
    assert "[20:00] 后半。" in result.content


@pytest.mark.asyncio
async def test_empty_transcript_returns_empty_content():
    with patch("parsers.openai_asr_parser.AsyncOpenAI") as MockOpenAI, \
         patch("parsers.openai_asr_parser.get_settings") as mock_settings, \
         patch("parsers.openai_asr_parser.transcode_segments",
               AsyncMock(return_value=[(0, b"a")])):
        _patch_settings(mock_settings)
        _mock_openai(MockOpenAI, SimpleNamespace(text="", segments=[]))
        result = await OpenAIAsrParser().parse(b"x", filename="quiet.mp3")
    assert result.content == ""


@pytest.mark.asyncio
async def test_unconfigured_raises_instead_of_empty():
    with patch("parsers.openai_asr_parser.get_settings") as mock_settings:
        _patch_settings(mock_settings, base_url="")
        assert OpenAIAsrParser.is_available() is False
        with pytest.raises(RuntimeError, match="not configured"):
            await OpenAIAsrParser().parse(b"x", filename="a.mp3")


# ---------------------------------------------------------------------------
# Segmenting (real ffmpeg)
# ---------------------------------------------------------------------------


@_needs_ffmpeg
@pytest.mark.asyncio
async def test_transcode_segments_cuts_with_exact_starts():
    pieces = await transcode_segments(_sine(25), segment_seconds=10, timeout=60)
    # Cuts snap to MP3 frame boundaries (~26 ms at 16 kHz), so the
    # reported starts drift slightly off the nominal grid.
    starts = [start for start, _ in pieces]
    assert len(starts) == 3
    assert all(abs(s - n) < 100 for s, n in zip(starts, [0, 10_000, 20_000]))
    assert all(data for _, data in pieces)


@_needs_ffmpeg
@pytest.mark.asyncio
async def test_transcode_segments_rejects_garbage():
    with pytest.raises(UnreadableAudioError):
        await transcode_segments(b"not audio", segment_seconds=10, timeout=60)
