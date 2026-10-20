"""Volcengine ASR parser for audio files (.mp3/.wav/.m4a/...).

Sends the audio to Volcengine's big-model recording recognition
"flash" endpoint (``recognize/flash``): one synchronous request with the
audio inline as base64, no submit/query polling and no public URL
needed. The service caps a single request at 2 h / 100 MB, which our
``max_upload_bytes`` (50 MB → ~67 MB base64) stays under.

The endpoint only decodes WAV / MP3 / OGG OPUS. MP3 and WAV pass
through untouched; every other container (m4a, aac, flac, amr, wma,
ogg-vorbis, …) is transcoded with ffmpeg to 16 kHz mono MP3 first —
speech recognition gains nothing above 16 kHz and the smaller body
shortens the upload. A pass-through file the service still rejects as
an unknown format (e.g. a ``.wav`` holding ADPCM) is transcoded and
retried once.

Output is markdown with one paragraph per utterance, each prefixed
with its start timestamp, so retrieved chunks can be traced back to a
position in the recording.

Status handling (``X-Api-Status-Code`` response header):
  * 20000000            → success
  * 20000003 / 45000002 → silent / empty audio: return empty content so
                          the worker marks the doc empty_content
  * 45000151            → undecodable audio even after transcoding:
                          PermanentParseError (FILE_CORRUPTED)
  * other 45xxxxxx      → request error: not counted by the breaker
  * anything else       → service-side failure: counts toward the
                          ``volc_asr`` circuit breaker; the worker
                          falls back to openai-asr or retries the task
"""
from __future__ import annotations

import asyncio
import base64
import logging
import uuid
from pathlib import Path

import httpx

from config import get_settings
from infra.circuit_breaker import get_breaker
from parsers._audio import (
    UnreadableAudioError,
    render_transcript,
    transcode_to_mp3,
)
from parsers.base import BaseParser, ParseResult

logger = logging.getLogger(__name__)

_STATUS_OK = "20000000"
_STATUS_SILENT = "20000003"
_STATUS_EMPTY_AUDIO = "45000002"
_STATUS_BAD_FORMAT = "45000151"

# Extensions the flash endpoint decodes as-is. Ogg is deliberately not
# here: the endpoint only takes Opus inside Ogg, and telling Opus from
# Vorbis needs a probe — transcoding is cheaper than guessing.
_PASSTHROUGH_FORMATS = {".mp3", ".wav"}

_SEMAPHORE: asyncio.Semaphore | None = None
_SEM_SIZE: int = 0


def _get_semaphore(limit: int) -> asyncio.Semaphore:
    global _SEMAPHORE, _SEM_SIZE
    if _SEMAPHORE is None or _SEM_SIZE != limit:
        _SEMAPHORE = asyncio.Semaphore(limit)
        _SEM_SIZE = limit
    return _SEMAPHORE


class AsrUpstreamError(RuntimeError):
    """Service-side failure reported in the status header (55xxxxxx) on an
    HTTP 200. Counted by the circuit breaker like an HTTP 5xx."""


class AsrRequestError(RuntimeError):
    """Request / content error reported in the status header (45xxxxxx).
    ``client_error`` keeps the circuit breaker from counting it — a batch
    of bad files must not cut audio ingest off for everyone."""
    client_error = True


class AsrBadFormatError(AsrRequestError):
    """The service could not decode the audio (45000151)."""


async def _call_asr_api(
    url: str, headers: dict, body: dict, timeout: httpx.Timeout,
) -> dict:
    """POST one recognition request; return the JSON body on success,
    ``{}`` for silent / empty audio. Split out so the circuit breaker can
    wrap exactly the network call."""
    async with httpx.AsyncClient(timeout=timeout) as client:
        resp = await client.post(url, headers=headers, json=body)
    resp.raise_for_status()
    status = resp.headers.get("X-Api-Status-Code", "")
    if status == _STATUS_OK:
        return resp.json()
    if status in (_STATUS_SILENT, _STATUS_EMPTY_AUDIO):
        return {}
    detail = (
        f"volc asr status={status or '(none)'} "
        f"message={resp.headers.get('X-Api-Message', '')[:200]} "
        f"logid={resp.headers.get('X-Tt-Logid', '')}"
    )
    if status == _STATUS_BAD_FORMAT:
        raise AsrBadFormatError(detail)
    if status.startswith("45"):
        raise AsrRequestError(detail)
    raise AsrUpstreamError(detail)


def _render_markdown(title: str, payload: dict) -> str:
    result = payload.get("result") or {}
    segments = [
        (utt.get("start_time") or 0, utt.get("text") or "")
        for utt in result.get("utterances") or []
    ]
    rendered = render_transcript(title, segments)
    if rendered:
        return rendered
    # No utterance segmentation — fall back to the flat transcript.
    return render_transcript(title, [(None, result.get("text") or "")])


class VolcAsrParser(BaseParser):
    engine_name = "volc-asr"
    supported_types = [
        ".mp3", ".wav", ".m4a", ".aac", ".flac", ".ogg", ".opus",
        ".amr", ".wma",
    ]

    @classmethod
    def is_available(cls) -> bool:
        cfg = get_settings()
        if not cfg.volc_asr_url:
            return False
        # New-console API key, or the old-console app key + access token.
        has_new = bool(cfg.volc_asr_api_key.get_secret_value())
        has_old = bool(
            cfg.volc_asr_app_key and cfg.volc_asr_access_key.get_secret_value(),
        )
        if not (has_new or has_old):
            return False
        from infra.circuit_breaker import State
        return get_breaker("volc_asr").current_state() is not State.OPEN

    def _headers(self) -> dict[str, str]:
        cfg = get_settings()
        headers = {
            "X-Api-Resource-Id": cfg.volc_asr_resource_id,
            "X-Api-Request-Id": str(uuid.uuid4()),
            "X-Api-Sequence": "-1",
        }
        api_key = cfg.volc_asr_api_key.get_secret_value()
        if api_key:
            headers["X-Api-Key"] = api_key
        else:
            headers["X-Api-App-Key"] = cfg.volc_asr_app_key
            headers["X-Api-Access-Key"] = cfg.volc_asr_access_key.get_secret_value()
        return headers

    async def _recognize(self, audio: bytes) -> dict:
        cfg = get_settings()
        body = {
            "user": {"uid": cfg.volc_asr_app_key or "vectoria"},
            "audio": {"data": base64.b64encode(audio).decode("ascii")},
            "request": {
                "model_name": "bigmodel",
                "enable_itn": True,
                "enable_punc": True,
                "show_utterances": True,
            },
        }
        timeout = httpx.Timeout(cfg.volc_asr_timeout, connect=10.0)
        async with _get_semaphore(cfg.volc_asr_concurrency):
            return await get_breaker("volc_asr").call(
                _call_asr_api, cfg.volc_asr_url, self._headers(), body, timeout,
            )

    async def parse(
        self, source: bytes | str, filename: str = "", **kwargs,
    ) -> ParseResult:
        if not isinstance(source, bytes):
            raise ValueError(
                f"volc-asr expects bytes, got {type(source).__name__}",
            )
        cfg = get_settings()
        title = Path(filename).stem if filename else "audio"
        ext = Path(filename).suffix.lower()

        passthrough = ext in _PASSTHROUGH_FORMATS
        audio = (
            source if passthrough
            else await transcode_to_mp3(source, cfg.volc_asr_timeout)
        )
        # A CircuitOpenError is left to propagate: audio has no fallback
        # engine, so returning empty here would mislabel an outage as
        # empty_content instead of letting the queue retry.
        try:
            payload = await self._recognize(audio)
        except AsrBadFormatError:
            if not passthrough:
                raise UnreadableAudioError(
                    "volc asr could not decode transcoded audio",
                )
            # Right extension, unsupported codec inside — normalize and
            # give the service one more try.
            logger.info(
                "volc-asr: %s rejected as-is; retrying transcoded", filename,
            )
            audio = await transcode_to_mp3(source, cfg.volc_asr_timeout)
            try:
                payload = await self._recognize(audio)
            except AsrBadFormatError as e:
                raise UnreadableAudioError(str(e)) from e

        content = _render_markdown(title, payload)
        duration_ms = (payload.get("audio_info") or {}).get("duration")
        logger.info(
            "volc-asr: transcribed %s (%s ms audio) into %d chars",
            filename or "<audio>", duration_ms, len(content),
        )
        return ParseResult(content=content, title=title)
