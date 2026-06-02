"""Vision-native parser for image files (.png/.jpg/...).

When the upload IS an image (poster, infographic, screenshot, photo of
a chart), running plain OCR on it loses the visual semantics — you
end up with text fragments that don't capture "this is a sales chart
showing Q1 vs Q2", just the literal numbers stripped of context.

This parser sends the image to the configured Vision LLM with a
dual-output prompt (semantic description + verbatim text) so the
downstream RAG / video-script pipeline gets *both* a meaning-level
summary and exact strings. ocr-native (rapidocr, in-process, free)
remains in the chain as fallback for when:

  * vision_base_url isn't configured (open-source default)
  * vision circuit breaker is OPEN (provider outage)
  * today's spend has crossed vision_daily_budget_usd (cost guardrail)
  * a single call raises (network / rate-limit / content-filter) —
    handled by the handler's per-attempt fallback, not in this class

Like ocr-native, ``image_refs`` is empty: the input *is* the image,
there's nothing to extract beside the source bytes (handler still
holds those via storage_key).
"""
from __future__ import annotations

import logging
from pathlib import Path

from config import get_settings
from infra.circuit_breaker import State, get_breaker
from parsers.base import BaseParser, ParseResult
from vision.budget import get_cost_tracker

logger = logging.getLogger(__name__)


class VisionNativeParser(BaseParser):
    engine_name = "vision-native"
    supported_types = [".png", ".jpg", ".jpeg", ".tiff", ".bmp", ".webp", ".gif"]

    @classmethod
    def is_available(cls) -> bool:
        cfg = get_settings()
        # No endpoint → engine is unconfigured. Registry will pick
        # ocr-native (next in chain).
        if not cfg.vision_base_url:
            return False
        # Breaker OPEN → upstream is sick. Same pattern as mineru:
        # advertise unavailable so registry skips us at upload time
        # (HALF_OPEN stays available so one probe can re-close it).
        if get_breaker("vision").current_state() is State.OPEN:
            return False
        # Soft daily-budget cap. Once exhausted we let rapidocr
        # carry the day until the UTC date rolls and the tracker
        # window resets. See vision/budget.py for the trade-offs of
        # per-process accounting.
        if get_cost_tracker().over_budget():
            return False
        return True

    async def parse(
        self, source: bytes | str, filename: str = "", **kwargs,
    ) -> ParseResult:
        # ``source`` is bytes for image uploads (handler reads from S3
        # before calling). Defensive: if a caller wired up wrong, fail
        # fast rather than send garbage to the VLM.
        if not isinstance(source, bytes):
            raise ValueError(
                f"vision-native expects bytes, got {type(source).__name__}",
            )

        language = kwargs.get("language")

        # Lazy build the client so importing this module doesn't trigger
        # a config load before settings are fully wired (matches the
        # pattern used by other parsers).
        from vision.client import VisionClient
        cfg = get_settings()
        client = VisionClient(
            base_url=cfg.vision_base_url,
            api_key=cfg.vision_api_key.get_secret_value(),
            model=cfg.vision_model,
        )

        markdown = await client.parse_image(source, language=language)

        if not markdown.strip():
            # Vision returned empty — let the handler treat it as a
            # parser-level failure and try the next engine in chain.
            # Don't fabricate a fake ParseResult.
            raise ValueError("vision-native returned empty content")

        title = Path(filename).stem if filename else "image"
        # Prepend a deck-style title so downstream chunkers see a clear
        # h1 — matches the convention from pptx / docx parsers.
        content = f"# {title}\n\n{markdown}\n"
        logger.info(
            "vision-native: parsed %s into %d chars of markdown",
            filename or "<image>", len(content),
        )
        return ParseResult(content=content, title=title, image_refs=[])
