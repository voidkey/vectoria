"""Shared audio plumbing for the ASR parsers: ffmpeg transcoding and
transcript rendering.

ffmpeg always reads from a temp file, not a pipe: MP4-family containers
(m4a) often carry their index (moov atom) at the end, and the demuxer
cannot seek back to it on a pipe — long m4a files fail that way.

Output is 16 kHz mono MP3 at 32 kbps: speech recognition gains nothing
above 16 kHz, and the small body (~14 MB per hour) shortens uploads.
"""
from __future__ import annotations

import asyncio
import csv
import shutil
import tempfile
from pathlib import Path

from api.errors import ErrorCode
from parsers.base import PermanentParseError

_ENCODE_ARGS = ["-vn", "-ac", "1", "-ar", "16000", "-b:a", "32k"]


class UnreadableAudioError(PermanentParseError):
    """Audio that neither the ASR service nor ffmpeg can decode."""
    error_code = ErrorCode.FILE_CORRUPTED


async def _run_ffmpeg(args: list[str], timeout: float) -> bytes:
    """Run ffmpeg with ``args``; return stdout. A non-zero exit means the
    input is undecodable — the caller's own arguments are fixed."""
    if shutil.which("ffmpeg") is None:
        raise RuntimeError("ffmpeg not found; cannot transcode audio")
    proc = await asyncio.create_subprocess_exec(
        "ffmpeg", "-nostdin", "-loglevel", "error", *args,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    try:
        out, err = await asyncio.wait_for(proc.communicate(), timeout)
    except asyncio.TimeoutError:
        proc.kill()
        await proc.wait()
        raise
    if proc.returncode != 0:
        raise UnreadableAudioError(
            f"ffmpeg could not decode audio: "
            f"{err.decode('utf-8', 'replace').strip()[-300:]}",
        )
    return out


async def transcode_to_mp3(content: bytes, timeout: float) -> bytes:
    """Transcode any ffmpeg-readable audio to one 16 kHz mono MP3."""
    with tempfile.TemporaryDirectory(prefix="asr-") as tmp:
        src = Path(tmp) / "input"
        src.write_bytes(content)
        out = await _run_ffmpeg(
            ["-i", str(src), *_ENCODE_ARGS, "-f", "mp3", "pipe:1"], timeout,
        )
    if not out:
        raise UnreadableAudioError("ffmpeg produced no audio")
    return out


async def transcode_segments(
    content: bytes, segment_seconds: int, timeout: float,
) -> list[tuple[int, bytes]]:
    """Transcode to 16 kHz mono MP3 cut into ``segment_seconds`` pieces,
    for services with a per-request size cap. Returns ``(start_ms, mp3)``
    per piece, with start times read from ffmpeg's segment list so
    timestamps stay exact even where a cut lands off the nominal grid."""
    with tempfile.TemporaryDirectory(prefix="asr-") as tmp:
        tmp_path = Path(tmp)
        src = tmp_path / "input"
        src.write_bytes(content)
        seg_list = tmp_path / "segments.csv"
        await _run_ffmpeg(
            ["-i", str(src), *_ENCODE_ARGS,
             "-f", "segment", "-segment_time", str(segment_seconds),
             "-segment_list", str(seg_list), "-segment_list_type", "csv",
             str(tmp_path / "seg%04d.mp3")],
            timeout,
        )
        segments: list[tuple[int, bytes]] = []
        if seg_list.exists():
            with seg_list.open(newline="") as f:
                for name, start, _end in csv.reader(f):
                    data = (tmp_path / name).read_bytes()
                    if data:
                        segments.append((int(float(start) * 1000), data))
    if not segments:
        raise UnreadableAudioError("ffmpeg produced no audio")
    return segments


def format_ts(ms: int) -> str:
    secs = max(int(ms), 0) // 1000
    h, rem = divmod(secs, 3600)
    m, s = divmod(rem, 60)
    return f"{h:02d}:{m:02d}:{s:02d}" if h else f"{m:02d}:{s:02d}"


def render_transcript(
    title: str, segments: list[tuple[int | None, str]],
) -> str:
    """Render ``(start_ms, text)`` segments as markdown, one paragraph per
    segment, prefixed ``[mm:ss]`` when the start time is known. Returns
    ``""`` when there is no text, so the worker marks the doc
    empty_content."""
    lines: list[str] = []
    for start_ms, text in segments:
        text = (text or "").strip()
        if not text:
            continue
        lines.append(text if start_ms is None else f"[{format_ts(start_ms)}] {text}")
    if not lines:
        return ""
    body = "\n\n".join(lines)
    return f"# {title}\n\n{body}\n"
