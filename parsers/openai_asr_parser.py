"""OpenAI-compatible ASR parser for audio files.

Calls ``POST {base_url}/audio/transcriptions`` through the OpenAI SDK,
so any compatible service works: OpenAI (whisper-1 / gpt-4o-transcribe),
Groq, SiliconFlow, or a self-hosted faster-whisper server (speaches).
Second in the audio chain after volc-asr: it takes over when Volcengine
is unconfigured, its breaker is open, or a call fails transiently.

These services cap one request at ~25 MB, so the audio is always
transcoded to 16 kHz mono MP3 and cut into ``openai_asr_segment_seconds``
pieces (20 min ≈ 4.8 MB by default), transcribed in order. A word that
straddles a cut may be garbled — the price of a size cap.

Timestamps need ``response_format=verbose_json`` (segment start times),
which whisper-1 and most self-hosted servers support but gpt-4o-*
models reject; set ``OPENAI_ASR_TIMESTAMPS=false`` for those and the
transcript is emitted one paragraph per piece, stamped with the
piece's start.
"""
from __future__ import annotations

import asyncio
import logging
from pathlib import Path

from openai import AsyncOpenAI

from config import get_settings
from infra.circuit_breaker import get_breaker
from parsers._audio import render_transcript, transcode_segments
from parsers.base import BaseParser, ParseResult

logger = logging.getLogger(__name__)

_SEMAPHORE: asyncio.Semaphore | None = None
_SEM_SIZE: int = 0


def _get_semaphore(limit: int) -> asyncio.Semaphore:
    global _SEMAPHORE, _SEM_SIZE
    if _SEMAPHORE is None or _SEM_SIZE != limit:
        _SEMAPHORE = asyncio.Semaphore(limit)
        _SEM_SIZE = limit
    return _SEMAPHORE


async def _transcribe(
    client: AsyncOpenAI, model: str, audio: bytes, response_format: str,
):
    """One transcription request. Split out so the circuit breaker wraps
    exactly the network call."""
    return await client.audio.transcriptions.create(
        model=model,
        file=("audio.mp3", audio, "audio/mpeg"),
        response_format=response_format,
    )


class OpenAIAsrParser(BaseParser):
    engine_name = "openai-asr"
    supported_types = [
        ".mp3", ".wav", ".m4a", ".aac", ".flac", ".ogg", ".opus",
        ".amr", ".wma",
    ]

    @classmethod
    def is_available(cls) -> bool:
        cfg = get_settings()
        if not cfg.openai_asr_base_url:
            return False
        from infra.circuit_breaker import State
        return get_breaker("openai_asr").current_state() is not State.OPEN

    async def parse(
        self, source: bytes | str, filename: str = "", **kwargs,
    ) -> ParseResult:
        if not isinstance(source, bytes):
            raise ValueError(
                f"openai-asr expects bytes, got {type(source).__name__}",
            )
        cfg = get_settings()
        if not cfg.openai_asr_base_url:
            # Reached only as a fallback after volc-asr failed; raise so
            # the queue retries rather than reporting empty content.
            raise RuntimeError("openai-asr is not configured")
        title = Path(filename).stem if filename else "audio"

        pieces = await transcode_segments(
            source, cfg.openai_asr_segment_seconds, cfg.openai_asr_timeout,
        )
        client = AsyncOpenAI(
            base_url=cfg.openai_asr_base_url,
            api_key=cfg.openai_asr_api_key.get_secret_value() or "none",
            timeout=cfg.openai_asr_timeout,
        )
        response_format = "verbose_json" if cfg.openai_asr_timestamps else "json"
        segments: list[tuple[int | None, str]] = []
        for offset_ms, audio in pieces:
            async with _get_semaphore(cfg.openai_asr_concurrency):
                resp = await get_breaker("openai_asr").call(
                    _transcribe, client, cfg.openai_asr_model, audio,
                    response_format,
                )
            timed = getattr(resp, "segments", None) or []
            if timed:
                segments.extend(
                    (offset_ms + int(seg.start * 1000), seg.text) for seg in timed
                )
            else:
                # No segment timing (json format, or a server that ignores
                # verbose_json): stamp the piece's start — unless it is the
                # only piece, where [00:00] would carry no information.
                stamp = offset_ms if len(pieces) > 1 else None
                segments.append((stamp, getattr(resp, "text", "") or ""))

        content = render_transcript(title, segments)
        logger.info(
            "openai-asr: transcribed %s (%d piece(s)) into %d chars",
            filename or "<audio>", len(pieces), len(content),
        )
        return ParseResult(content=content, title=title)
