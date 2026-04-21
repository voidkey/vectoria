"""Native .pptx parser via python-pptx.

The signature capability over docling: speaker-notes text is
emitted alongside slide body content. For product-demo / lecture
decks, notes often carry the key narrative (what the presenter
actually says) and embedding them lifts retrieval quality measurably.

Output structure
----------------
Each slide becomes an ``## Slide N: Title`` section. Body text
(title + text frames + cell text from simple tables) follows. If the
slide has a notes slide, a nested ``### Notes`` block carries the
notes text — same-paragraph preservation, not merged in-line.

This gives the outline extractor ``extract_outline`` a proper
hierarchy to key off, and the splitter sees "this chunk is from
slide 5 notes, section title is 'Slide 5'" downstream.

Images are not produced here — the ``PptxImageExtractor`` plugin
(W4-c, registered at ``parsers/__init__.py``) handles them via the
W4-b override seam. Returning empty ``image_refs`` avoids
duplicate work.
"""
from __future__ import annotations

import asyncio
import io
import logging
from pathlib import Path

from config import get_settings
from parsers.base import BaseParser, ParseResult
from parsers.convert import LEGACY_FORMAT_MAP, convert_legacy_format
from parsers.isolation import run_isolated

logger = logging.getLogger(__name__)


class PptxParser(BaseParser):
    engine_name = "pptx-native"
    supported_types = [".pptx", ".ppt"]

    @classmethod
    def is_available(cls) -> bool:
        try:
            import pptx  # noqa: F401
        except ImportError:
            return False
        return True

    async def parse(
        self, source: bytes | str, filename: str = "", **kwargs,
    ) -> ParseResult:
        cfg = get_settings()
        if not cfg.parser_isolation:
            return await asyncio.get_running_loop().run_in_executor(
                None, self._parse_sync, source, filename,
            )
        return await run_isolated(
            _pptx_parse_worker, source, filename,
            timeout=cfg.parser_timeout, tier="fast",
        )

    def _parse_sync(self, source: bytes | str, filename: str) -> ParseResult:
        from pptx import Presentation

        raw = source if isinstance(source, bytes) else source.encode()

        suffix = Path(filename).suffix.lower()
        if suffix in LEGACY_FORMAT_MAP:
            import tempfile
            with tempfile.NamedTemporaryFile(suffix=suffix, delete=False) as tmp:
                tmp.write(raw)
                tmp_path = tmp.name
            try:
                converted_path = convert_legacy_format(tmp_path, suffix)
                try:
                    with open(converted_path, "rb") as f:
                        raw = f.read()
                finally:
                    Path(converted_path).unlink(missing_ok=True)
            finally:
                Path(tmp_path).unlink(missing_ok=True)

        try:
            prs = Presentation(io.BytesIO(raw))
        except Exception:
            logger.exception("python-pptx parse failed for %s", filename)
            return ParseResult(content="", title=Path(filename).stem)

        lines: list[str] = []
        deck_title = _extract_deck_title(prs) or Path(filename).stem
        lines.append(f"# {deck_title}")
        lines.append("")

        for idx, slide in enumerate(prs.slides, start=1):
            slide_title = _extract_slide_title(slide)
            header = f"## Slide {idx}"
            if slide_title:
                header = f"{header}: {slide_title}"
            lines.append(header)
            lines.append("")

            body = _extract_slide_body(slide, skip_title=slide_title)
            if body:
                lines.append(body)
                lines.append("")

            # Speaker notes — the docling-drop gap we're filling.
            if slide.has_notes_slide:
                notes_text = _extract_notes_text(slide.notes_slide)
                if notes_text:
                    lines.append("### Notes")
                    lines.append("")
                    lines.append(notes_text)
                    lines.append("")

        content = "\n".join(lines).strip() + "\n"

        return ParseResult(
            content=content,
            title=deck_title,
            # PptxImageExtractor (registered at parsers/__init__.py)
            # will override image_refs via the W4-b seam. Returning
            # empty here avoids duplicate slide-walking work.
            image_refs=[],
        )


def _extract_deck_title(prs) -> str:
    """Deck title from the core-properties metadata (Title field)."""
    try:
        title = (prs.core_properties.title or "").strip()
    except Exception:
        return ""
    return title


def _extract_slide_title(slide) -> str:
    """Title placeholder text, or empty string."""
    try:
        if slide.shapes.title is not None:
            return (slide.shapes.title.text_frame.text or "").strip()
    except Exception:
        pass
    return ""


def _extract_slide_body(slide, *, skip_title: str = "") -> str:
    """Concatenate non-title text frames and simple table cell text.

    Preserves paragraph boundaries (one per line). Skips the title
    frame (already emitted as the ``## Slide N: Title`` header) and
    empty frames. Groups recurse.
    """
    parts: list[str] = []

    def _walk_shapes(shapes):
        for shape in shapes:
            if shape.shape_type == 6:  # GROUP
                _walk_shapes(shape.shapes)
                continue
            # Title placeholder already in header — skip.
            if shape == getattr(slide.shapes, "title", None):
                continue
            if shape.has_text_frame:
                txt = _text_frame_to_str(shape.text_frame)
                if txt and txt != skip_title:
                    parts.append(txt)
            if shape.has_table:
                parts.append(_table_to_markdown(shape.table))

    _walk_shapes(slide.shapes)
    return "\n\n".join(p for p in parts if p)


def _text_frame_to_str(tf) -> str:
    """One paragraph per line, blank paragraphs dropped."""
    paras = []
    for p in tf.paragraphs:
        txt = "".join(r.text for r in p.runs).strip()
        if txt:
            paras.append(txt)
    return "\n".join(paras)


def _table_to_markdown(table) -> str:
    """Pipe-style markdown table. Simple: no cell merging awareness —
    merged cells emit the same text per physical row/column (which is
    what python-pptx gives us). RAG use doesn't care about merge
    accuracy; we care about the text content.
    """
    rows = []
    for row in table.rows:
        cells = [
            (cell.text_frame.text or "").strip().replace("\n", " ").replace("|", "\\|")
            for cell in row.cells
        ]
        rows.append("| " + " | ".join(cells) + " |")
    if len(rows) >= 1:
        header_sep = "| " + " | ".join(["---"] * len(table.rows[0].cells)) + " |"
        rows.insert(1, header_sep)
    return "\n".join(rows)


def _extract_notes_text(notes_slide) -> str:
    """Text from the notes_text_frame — skip the slide-number
    placeholder python-pptx sometimes leaves at the top of a notes
    frame when the deck hasn't been edited.
    """
    try:
        tf = notes_slide.notes_text_frame
    except Exception:
        return ""
    if tf is None:
        return ""
    return _text_frame_to_str(tf)


def _pptx_parse_worker(source: bytes | str, filename: str) -> ParseResult:
    """Subprocess entry for PptxParser."""
    return PptxParser()._parse_sync(source, filename)
