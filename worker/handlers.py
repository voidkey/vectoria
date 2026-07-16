"""Task handlers — the actual work that runs in worker processes.

Each handler receives a `payload` dict (deserialized from the task row) and
runs the heavy I/O: splitting, embedding, vector upsert, vision analysis,
image downloads, etc.

These are the same operations that previously ran inside
`asyncio.create_task()` in the API process. Extracting them here means:
  - A crash or OOM only affects the worker, not the API.
  - A failed run is retried automatically (the queue handles this).
  - Progress and timing are tracked via the tasks table.
"""

import asyncio
import logging
import traceback

from sqlalchemy import select

from config import get_settings
from db.base import get_session
from db.helpers import load_doc, update_doc
from db.models import Document, DocumentImage
from infra.metrics import (
    DOCUMENT_OUTCOMES, PARSE_EMPTY_TOTAL, PARSE_FALLBACK_TOTAL, observe_parse,
)
from api.errors import ErrorCode
from parsers.base import PermanentParseError
from parsers.image_metadata import extract_metadata_into_refs
from parsers.registry import registry
# Imported at module scope (not lazily) so the capture handler's collaborators
# are patchable at ``worker.handlers.<name>`` in tests.
from api.image_stream import stream_upload_and_store_refs
from api.url_validation import reresolve_and_check_ssrf
from parsers.url._browser import parse_session
from worker.queue import enqueue
from rag.embedder import get_embedder
from splitter.splitter import Splitter
from storage import get_storage
from vectorstore.base import ChunkData
from vectorstore.pgvector import PgVectorStore

import httpx
from infra.circuit_breaker import CircuitOpenError

# Exceptions that signify the *upstream / dependency* failed —
# the file itself is fine, just the engine couldn't reach its
# external dep (mineru HTTP, vision API, etc.) or the breaker is
# open. Triggers per-attempt engine fallback in
# ``handle_parse_document`` rather than wasting all 3 queue retries
# on the same broken upstream. File-level errors (malformed bytes,
# parser logic) intentionally do not match — falling back to a
# different engine wouldn't help.
_DEP_LEVEL_ERRORS: tuple[type[BaseException], ...] = (
    httpx.TimeoutException,   # connect / read / write / pool timeouts
    httpx.NetworkError,       # connect / read / write / close errors
    CircuitOpenError,         # this engine's breaker is OPEN
    asyncio.TimeoutError,     # asyncio-side wall-clock cuts
)


def _parse_error_code(exc: BaseException | None, *, is_url: bool) -> int:
    """Map any parse-stage exception to a frontend error code.

    A total function over the exception types the worker can see:
      * a PermanentParseError returns the code it carries (blacklist /
        anti-bot / 404 etc.), falling back to PARSE_UNRESOLVABLE;
      * a URL fetch timeout / network error → LINK_FETCH_TIMEOUT;
      * anything else → a retryable PARSE_ERROR.

    The dedicated ``except PermanentParseError`` sites read ``e.error_code``
    directly; this classifier is used at the terminal re-raise site where
    the exception type isn't statically known (``last_exc``).
    """
    if isinstance(exc, PermanentParseError):
        return getattr(exc, "error_code", ErrorCode.PARSE_UNRESOLVABLE)
    if is_url and isinstance(exc, (httpx.TimeoutException, httpx.NetworkError)):
        return ErrorCode.LINK_FETCH_TIMEOUT
    return ErrorCode.PARSE_ERROR


logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Handler registry
# ---------------------------------------------------------------------------

HANDLERS: dict[str, object] = {}  # task_type -> async handler callable


def _register(name: str):
    def decorator(fn):
        HANDLERS[name] = fn
        return fn
    return decorator


async def dispatch(task_type: str, payload: dict) -> None:
    handler = HANDLERS.get(task_type)
    if handler is None:
        raise ValueError(f"Unknown task type: {task_type}")
    await handler(payload)


# ---------------------------------------------------------------------------
# parse_document
# ---------------------------------------------------------------------------

@_register("parse_document")
async def handle_parse_document(payload: dict) -> None:
    """Fetch source, parse to markdown, upload inline images, then fan out.

    This runs everything the API ``_ingest`` used to do synchronously.
    Moving it here lets the API return a ``queued`` response in ~ms
    instead of holding the request open while MinerU/docling chew on
    a large PDF.

    Terminal outcomes are classified:
      * content below threshold (``min_content_chars``) + no images
        OR handler didn't opt in → ``failed`` / ``empty_content``
      * content below threshold + images + handler opted in via
        ``ParseResult.allow_image_only`` → ``completed`` /
        ``image_only`` (indexing skipped, image pipeline still runs)
      * content above ``max_content_chars`` → ``failed`` / ``too_large``
      * other exceptions → re-raised so the queue retries with backoff
    """
    doc_id = payload["doc_id"]
    kb_id = payload["kb_id"]
    storage_key = payload.get("storage_key")
    source = payload["source"]
    filename = payload.get("filename", "")
    selected_engine = payload["selected_engine"]

    # Guard: the doc may have been deleted, or a prior attempt may have
    # already completed this work. Don't re-parse or double-enqueue.
    async with get_session() as session:
        result = await session.execute(
            select(Document).where(Document.id == doc_id),
        )
        doc = result.scalar_one_or_none()
    if doc is None:
        logger.info("parse_document: doc %s missing, skipping", doc_id)
        return
    if doc.status in ("completed", "indexing"):
        logger.info(
            "parse_document: doc %s already past parse (%s), skipping",
            doc_id, doc.status,
        )
        return

    await update_doc(doc_id, status="parsing")

    # Fetch the source bytes (file) or URL string.
    if storage_key:
        obj_storage = await get_storage()
        raw: bytes | str = await obj_storage.get(storage_key)
    else:
        raw = source

    # Per-attempt engine fallback. selected_engine is just the upload-time
    # preference; on *any* exception we try the next engine in
    # registry.fallback_chain rather than declaring the file dead — a
    # different parser may not depend on the broken upstream (mineru
    # HTTP vs pdfium in-process) or may not have the same library
    # sharp edge (python-pptx vs markitdown). Cost: one extra attempt
    # per failure, bounded by chain length (≤ 3).
    #
    # We also fall back when a parser *succeeds but returns empty
    # content* (see ``last_was_empty`` below). Office native parsers
    # (docx_parser / pptx_parser / xlsx_parser) catch internal
    # exceptions and return ``ParseResult(content="")`` rather than
    # raising — without this branch, that gets misclassified as a
    # terminal empty_content failure even though markitdown might
    # have parsed the same file via a different code path. The
    # opportunity cost of one extra attempt is small; the win is
    # not silently losing files to library quirks.
    #
    # _DEP_LEVEL_ERRORS still has a meaning: those are *definitely*
    # transient and worth distinguishing in logs. Anything else is a
    # parser-level failure that *might* be specific to that engine —
    # we fall back optimistically. Either way the diagnostic
    # distinction stays in WARN logs so operators can tell apart
    # "network glitch" from "library bug on this file".
    #
    # The chain is computed lazily on the first failure so the happy
    # path doesn't pay for a registry call.
    cfg = get_settings()
    parse_result = used_engine = None
    last_exc: BaseException | None = None
    last_trace = ""               # captured inside except → safe to use later
    last_was_empty = False        # distinguishes "all engines returned empty" terminal
    engine_name: str | None = selected_engine
    fallback_queue: list[str] | None = None
    while engine_name is not None:
        try:
            parser = registry.get_by_engine(engine_name)
        except ValueError:
            parser = None         # not registered — fall through to next
        if parser is not None:
            try:
                async with observe_parse(engine_name):
                    candidate = await parser.parse(raw, filename=filename)
                # "Useful" = either has enough text content, or is an
                # opted-in image-only handler with images to download.
                # Anything else is treated like a parser failure for
                # fallback purposes — the next engine in the chain may
                # do better via a different code path.
                stripped_len = len(candidate.content.strip())
                useful = (
                    stripped_len >= cfg.min_content_chars
                    or (candidate.allow_image_only and bool(candidate.image_urls))
                )
                if useful:
                    parse_result = candidate
                    used_engine = engine_name
                    if engine_name != selected_engine:
                        PARSE_FALLBACK_TOTAL.labels(
                            from_engine=selected_engine, to_engine=engine_name,
                        ).inc()
                        logger.warning(
                            "parse_document: doc=%s fell back from %s to %s "
                            "after failure(s); last error: %r",
                            doc_id, selected_engine, engine_name, last_exc,
                        )
                    break
                # Empty result — not an exception, but treat as "this
                # engine couldn't extract anything useful from this
                # file" and try the chain. Office native parsers
                # swallow library exceptions and return empty; without
                # this branch they bypass the markitdown fallback.
                last_exc, last_trace = None, ""
                last_was_empty = True
                logger.warning(
                    "parse_document: %s returned empty content for doc=%s "
                    "(%d chars); trying next engine",
                    engine_name, doc_id, stripped_len,
                )
            except PermanentParseError as e:
                # Permanent — no engine in the chain can save this, and
                # queue retry would just hit the same wall. Mark failed
                # and return success to the queue (no raise) so the
                # task doesn't accumulate dead-letter alerts.
                logger.warning(
                    "parse_document: %s permanent failure on doc=%s "
                    "(%s: %s); not falling back, not retrying",
                    engine_name, doc_id, type(e).__name__, e,
                )
                DOCUMENT_OUTCOMES.labels(outcome="permanent").inc()
                await update_doc(
                    doc_id, status="failed",
                    error_msg=f"Parsing failed: {e}"[:500],
                    error_type="permanent",
                    error_trace=traceback.format_exc(),
                    error_code=getattr(e, "error_code", ErrorCode.PARSE_UNRESOLVABLE),
                )
                return
            except _DEP_LEVEL_ERRORS as e:
                last_exc, last_trace = e, traceback.format_exc()
                last_was_empty = False
                logger.warning(
                    "parse_document: %s dep-level failure on doc=%s "
                    "(%s: %s); trying next engine",
                    engine_name, doc_id, type(e).__name__, e,
                )
            except Exception as e:
                last_exc, last_trace = e, traceback.format_exc()
                last_was_empty = False
                logger.warning(
                    "parse_document: %s parser-level failure on doc=%s "
                    "(%s: %s); trying next engine",
                    engine_name, doc_id, type(e).__name__, e,
                )

        if fallback_queue is None:
            fallback_queue = list(registry.fallback_chain(
                filename=filename,
                url=("" if storage_key else source),
                after=selected_engine,
            ))
        engine_name = fallback_queue.pop(0) if fallback_queue else None

    if parse_result is None:
        # Whole chain failed. Two terminal flavors based on what the
        # *last* attempt did:
        #   - last_was_empty: every engine returned empty content
        #     (Office native libs do this on internal errors). Mark
        #     terminal empty_content — same classification a single
        #     engine would have produced before fallback existed —
        #     and don't raise (queue retry won't help; same chain).
        #   - else: last attempt raised. Mark parse_error and re-
        #     raise so the queue retries with backoff; transient
        #     issues might clear.
        if last_was_empty:
            logger.warning(
                "parse_document: every engine returned empty content "
                "for doc=%s — terminal empty_content", doc_id,
            )
            PARSE_EMPTY_TOTAL.labels(engine=selected_engine).inc()
            DOCUMENT_OUTCOMES.labels(outcome="empty_content").inc()
            await update_doc(
                doc_id, status="failed",
                error_msg="Parsing returned empty or below-threshold content",
                error_type="empty_content",
                error_code=ErrorCode.EMPTY_CONTENT,
            )
            return
        logger.error(
            "parse_document: all engines in chain failed for doc=%s: %r",
            doc_id, last_exc,
        )
        DOCUMENT_OUTCOMES.labels(outcome="parse_error").inc()
        await update_doc(
            doc_id, status="failed",
            error_msg=f"Parsing failed: {last_exc}"[:500],
            error_type="parse_error",
            error_trace=last_trace,
            error_code=_parse_error_code(last_exc, is_url=not storage_key),
        )
        raise last_exc  # type: ignore[misc]
    assert used_engine is not None  # parse_result is set ⇒ used_engine is too

    # Drop the source bytes ASAP; the parser may have materialised them
    # into structures held on parse_result but `raw` itself can go.
    raw = None  # noqa: F841

    content = parse_result.content
    stripped_len = len(content.strip())

    download_payload = {
        "kb_id": kb_id, "doc_id": doc_id,
        "source_url": source,
        "image_urls": parse_result.image_urls,
    } if parse_result.image_urls else None

    # Permanent failures: empty or oversized content won't become valid
    # on retry. Three outcomes depending on what the parser produced and
    # whether the handler is a structured source that legitimately
    # yields image-first posts:
    if stripped_len < cfg.min_content_chars:
        has_image_urls = bool(parse_result.image_urls)
        if parse_result.allow_image_only and has_image_urls:
            # Structured-source handler (xhs / x syndication API)
            # returned a post whose body is below threshold but has
            # images. Treat as image_only: completed + index skipped,
            # but still run the image download + vision pipeline so
            # figures are stored. Retrieval on these docs will match
            # by title / metadata only; image semantics are not fed
            # into the embedding index in Phase 1 (see Phase 3).
            logger.info(
                "parse_document: image_only doc=%s (body %d < %d, images=%d)",
                doc_id, stripped_len, cfg.min_content_chars,
                len(parse_result.image_urls or []),
            )
            DOCUMENT_OUTCOMES.labels(outcome="image_only").inc()
            # error_type here is a terminal-outcome label (matches
            # DOCUMENT_OUTCOMES labels), not an error; status stays completed.
            image_only_fields: dict = dict(
                title=parse_result.title or source,
                content=content,
                status="completed",
                index_status="skipped",
                parse_engine=used_engine,
                error_type="image_only",
                error_msg="",
                error_trace=None,
                error_code=None,
                image_status="pending",
            )
            if parse_result.page_count is not None:
                image_only_fields["page_count"] = parse_result.page_count
            await update_doc(doc_id, **image_only_fields)
            from worker.queue import enqueue
            await enqueue("download_and_store_images", download_payload)
            return

        logger.warning(
            "parse_document: empty content for doc %s (len=%d < %d)",
            doc_id, stripped_len, cfg.min_content_chars,
        )
        PARSE_EMPTY_TOTAL.labels(engine=used_engine).inc()
        DOCUMENT_OUTCOMES.labels(outcome="empty_content").inc()
        await update_doc(
            doc_id, status="failed",
            index_status="skipped",
            error_msg="Parsing returned empty or below-threshold content",
            error_type="empty_content",
            error_code=ErrorCode.EMPTY_CONTENT,
        )
        return

    if len(content) > cfg.max_content_chars:
        logger.warning(
            "parse_document: content too large (%d > %d) doc=%s",
            len(content), cfg.max_content_chars, doc_id,
        )
        DOCUMENT_OUTCOMES.labels(outcome="too_large").inc()
        await update_doc(
            doc_id, status="failed",
            index_status="skipped",
            error_msg=(
                f"Parsed content exceeds {cfg.max_content_chars} characters"
            ),
            error_type="too_large",
            error_code=ErrorCode.CONTENT_TOO_LARGE,
        )
        return

    has_image_urls = bool(parse_result.image_urls)
    has_inline_images = bool(parse_result.image_refs)
    image_status = "pending" if (has_image_urls or has_inline_images) else "none"
    vision_configured = bool(cfg.vision_base_url)

    do_index = cfg.enable_indexing  # content already passed the min-length gate above

    # ``page_count`` set conditionally: don't clobber an upload-time
    # value (PDF/PPTX gates) with None when the parser didn't produce
    # one (PDF parsers don't currently emit it).
    update_fields: dict = dict(
        title=parse_result.title or filename or source,
        content=content,
        status="indexing" if do_index else "completed",
        index_status="pending" if do_index else "skipped",
        parse_engine=used_engine,
        image_status=image_status,
        error_msg="", error_type=None, error_trace=None, error_code=None,
    )
    if parse_result.page_count is not None:
        update_fields["page_count"] = parse_result.page_count
    await update_doc(doc_id, **update_fields)

    if not has_image_urls and has_inline_images:
        from api.image_stream import stream_upload_and_store_refs
        enriched = extract_metadata_into_refs(content, parse_result.image_refs)
        await stream_upload_and_store_refs(
            enriched, kb_id=kb_id, doc_id=doc_id,
            vision_configured=vision_configured,
        )
        await update_doc(doc_id, image_status="completed")

    # Follow-up tasks. Do this last so a crash mid-handler doesn't leave
    # the queue with duplicate index_document tasks competing.
    from worker.queue import enqueue
    if do_index:
        await enqueue("index_document", {"doc_id": doc_id, "kb_id": kb_id})
    else:
        # Disabled path: parse completion is the doc's terminal "usable"
        # state, so count it here (index_document — which normally bumps
        # this — never runs).
        DOCUMENT_OUTCOMES.labels(outcome="completed").inc()

    if has_image_urls:
        await enqueue("download_and_store_images", download_payload)
    elif has_inline_images and vision_configured:
        await enqueue("analyze_images", {"kb_id": kb_id, "doc_id": doc_id})


# ---------------------------------------------------------------------------
# index_document
# ---------------------------------------------------------------------------

@_register("index_document")
async def handle_index_document(payload: dict) -> None:
    doc_id = payload["doc_id"]
    kb_id = payload["kb_id"]
    try:
        doc = await load_doc(doc_id)
    except ValueError:
        # Doc was deleted between enqueue and dequeue. Idempotent skip,
        # mirroring handle_parse_document's missing-doc branch.
        logger.info("index_document: doc %s missing, skipping", doc_id)
        return
    content = doc.content

    cfg = get_settings()
    splitter = Splitter(
        chunk_size=cfg.splitter_chunk_size,
        chunk_overlap=cfg.splitter_chunk_overlap,
    )
    chunks = splitter.split(content)

    embedder = get_embedder()
    texts = [c.content for c in chunks]
    try:
        embeddings = await embedder.embed_batch(texts) if texts else []
        chunk_data = [
            ChunkData(
                id=c.id, doc_id=doc_id, kb_id=kb_id,
                content=c.content, embedding=embeddings[i],
                chunk_index=c.index, parent_id=None,
            )
            for i, c in enumerate(chunks)
        ]
        async with await PgVectorStore.create() as store:
            await store.upsert(chunk_data)
    except Exception as e:
        logger.exception("index_document: indexing failed doc=%s", doc_id)
        DOCUMENT_OUTCOMES.labels(outcome="indexing_error").inc()
        # Best-effort indexing: a parse-successful doc stays usable. Mark
        # only the index dimension failed; leave status="completed" and do
        # NOT touch error_* (those describe parse failures — the index
        # error detail lives in the tasks table). Re-raise for queue retry;
        # a later successful retry flips index_status back to completed.
        await update_doc(
            doc_id, status="completed", index_status="failed",
        )
        raise  # re-raise for queue retry/backoff

    DOCUMENT_OUTCOMES.labels(outcome="completed").inc()
    await update_doc(
        doc_id, chunk_count=len(chunk_data),
        status="completed", index_status="completed",
        error_msg="", error_type=None, error_trace=None,
    )


# ---------------------------------------------------------------------------
# analyze_images
# ---------------------------------------------------------------------------

@_register("analyze_images")
async def handle_analyze_images(payload: dict) -> None:
    kb_id = payload["kb_id"]
    doc_id = payload["doc_id"]

    from vision.client import VisionClient
    from storage import get_storage

    cfg = get_settings()
    client = VisionClient(
        base_url=cfg.vision_base_url,
        api_key=cfg.vision_api_key.get_secret_value(),
        model=cfg.vision_model,
    )
    if not client.is_configured:
        return

    obj_storage = await get_storage()

    async with get_session() as session:
        result = await session.execute(
            select(DocumentImage).where(
                DocumentImage.doc_id == doc_id,
                DocumentImage.vision_status == "pending",
            )
        )
        pending = result.scalars().all()

    if not pending:
        return

    sem = asyncio.Semaphore(5)

    async def _describe_one(img: DocumentImage):
        async with sem:
            try:
                img_bytes = await obj_storage.get(img.storage_key)
                description = await client.describe(
                    img_bytes,
                    context=img.context,
                    section_title=img.section_title,
                    alt=img.alt,
                )
                status = "completed" if description else "failed"
            except Exception:
                logger.exception("Vision analysis failed for image %s", img.id)
                description = ""
                status = "failed"

            async with get_session() as sess:
                result = await sess.execute(
                    select(DocumentImage).where(DocumentImage.id == img.id)
                )
                record = result.scalar_one_or_none()
                if record:
                    record.description = description
                    record.vision_status = status
                    await sess.commit()

    await asyncio.gather(*(_describe_one(img) for img in pending))


# ---------------------------------------------------------------------------
# download_and_store_images
# ---------------------------------------------------------------------------

@_register("download_and_store_images")
async def handle_download_and_store_images(payload: dict) -> None:
    kb_id = payload["kb_id"]
    doc_id = payload["doc_id"]
    source_url = payload["source_url"]
    image_urls = payload["image_urls"]
    try:
        doc = await load_doc(doc_id)
    except ValueError:
        # Doc was deleted between enqueue and dequeue. Idempotent skip,
        # mirroring handle_parse_document's missing-doc branch.
        logger.info("download_and_store_images: doc %s missing, skipping", doc_id)
        return
    content = doc.content

    from api.image_stream import refs_from_dict, stream_upload_and_store_refs
    from parsers.image_metadata import extract_metadata_into_refs
    from parsers.url import download_images_for_url

    # ``download_images_for_url`` threads the source URL's handler:
    # platform-specific Referer/UA headers + image URL canonicalisation
    # (WeChat forces wx_fmt=jpeg, future handlers swap size variants).
    images = await download_images_for_url(source_url, image_urls)
    if not images:
        await update_doc(doc_id, image_status="completed")
        return

    # Adapt the dict into the shared streaming pipeline. Assigning the
    # returned refs then clearing the dict lets the closures hold the
    # only live references to image bytes — each upload can release its
    # bytes as soon as the put() completes.
    refs = refs_from_dict(images)
    images = None  # drop the dict so closures become sole owners

    enriched = extract_metadata_into_refs(content, refs)

    cfg = get_settings()
    vision_configured = bool(cfg.vision_base_url)

    await stream_upload_and_store_refs(
        enriched,
        kb_id=kb_id, doc_id=doc_id,
        vision_configured=vision_configured,
    )

    await update_doc(doc_id, image_status="completed")

    if vision_configured:
        from worker.queue import enqueue
        await enqueue("analyze_images", {"kb_id": kb_id, "doc_id": doc_id})


# ---------------------------------------------------------------------------
# capture_site (website capture -> SiteProfile)
# ---------------------------------------------------------------------------

def _safe_hex(css: str) -> str:
    from parsers.capture._colors import _to_hex, parse_css_color
    rgb = parse_css_color(css or "")
    return _to_hex(rgb) if rgb else ""


async def _capture_hydrate_image_ids(doc_id: str) -> dict[str, tuple[str, str]]:
    """filename -> (image_id, storage_key) for a doc's DocumentImage rows."""
    async with get_session() as session:
        rows = (await session.execute(
            select(DocumentImage.filename, DocumentImage.id, DocumentImage.storage_key)
            .where(DocumentImage.doc_id == doc_id))).all()
    return {fn: (iid, skey) for fn, iid, skey in rows}


# Only these image-asset kinds are worth a vision description — AND only when
# the bytes are a raster format the vision model can actually decode. A logo/
# favicon is frequently SVG or .ico (vector/icon), which the vision API rejects;
# those are stored but never described (avoids a guaranteed-failing, billed call).
_VISION_ASSET_KINDS = ("logo", "hero", "og_image")
_VISION_RASTER_EXT = ("png", "jpg", "jpeg", "webp", "gif")
_IMG_EXT = {"image/png": ".png", "image/jpeg": ".jpg", "image/webp": ".webp",
            "image/svg+xml": ".svg", "image/gif": ".gif",
            "image/x-icon": ".ico", "image/vnd.microsoft.icon": ".ico"}
_BIN_EXT = {"video/mp4": ".mp4", "video/webm": ".webm", "application/json": ".json"}


def _asset_gets_vision(kind: str, fname: str) -> bool:
    """True only for vision-worthy kinds whose stored format is a raster the
    vision model can decode (not svg/ico)."""
    return kind in _VISION_ASSET_KINDS and fname.rsplit(".", 1)[-1] in _VISION_RASTER_EXT


@_register("capture_site")
async def handle_capture(payload: dict) -> None:
    """Guard wrapper: the capture Document must never stay stuck in
    ``capturing``. Any failure marks it ``failed`` — permanent errors
    (blacklist/404/anti-bot) and SSRF/URL rejections are terminal (no retry,
    no dead-task alert); anything else is re-raised so the queue retries."""
    from api.errors import AppError

    doc_id = payload["doc_id"]
    await update_doc(doc_id, status="capturing")
    try:
        await _capture_core(payload)
    except PermanentParseError as e:
        await update_doc(doc_id, status="failed", error_type="permanent",
                         error_msg=str(e)[:500],
                         error_code=getattr(e, "error_code", ErrorCode.PARSE_UNRESOLVABLE))
    except AppError as e:
        # SSRF / malformed URL — the submitter's problem, not infra. Terminal
        # so we don't burn 3 retries + a dead-task alert on a private-IP URL.
        await update_doc(doc_id, status="failed", error_type="url_fetch_error",
                         error_msg=str(e)[:500],
                         error_code=e.code)
    except Exception as e:
        await update_doc(doc_id, status="failed", error_type="parse_error",
                         error_msg=str(e)[:500], error_trace=traceback.format_exc(),
                         error_code=_parse_error_code(e, is_url=True))
        raise


async def _capture_core(payload: dict) -> None:
    """Render a URL, extract a SiteProfile, store assets + screenshots, and
    persist the profile on the site_capture Document. Vision descriptions for
    image assets backfill via the existing analyze_images task."""
    from datetime import datetime, timezone

    from parsers.capture._assets import fetch_asset_bytes, image_ref_from_bytes
    from parsers.capture._colors import dominant_screenshot_hex, process_colors
    from parsers.capture._extract import run_extract
    from parsers.capture._fonts import build_font_role, cluster_spacing, section_type
    from parsers.capture._screenshots import (
        NEUTRALIZE_ANIMATION_CSS, capture_screenshots, prepare_page)
    from parsers.capture.profile import (
        AssetRef, FontFile, Fonts, MotionHints, ScreenshotRef, SectionInfo,
        SiteProfile, Spacing, TextInfo,
    )

    doc_id, kb_id, url = payload["doc_id"], payload["kb_id"], payload["url"]
    cfg = get_settings()
    await reresolve_and_check_ssrf(url)

    async with parse_session(
        block_heavy=False,
        viewport={"width": cfg.capture_viewport_width, "height": cfg.capture_viewport_height},
    ) as ctx:
        page = await ctx.new_page()
        try:
            await page.goto(url, wait_until="load",
                            timeout=int(cfg.capture_render_timeout * 1000))
        except Exception:
            logger.info("capture goto timed out/failed, using rendered DOM: %s", url)
        # Mainstream capture waits for networkidle, not just `load`. Do it as a
        # best-effort wait on top of `load`, capped so long-polling sites don't
        # burn the whole budget.
        try:
            await page.wait_for_load_state(
                "networkidle", timeout=int(cfg.capture_networkidle_timeout * 1000))
        except Exception:
            pass
        await page.wait_for_timeout(cfg.capture_settle_ms)
        # Collapse animation/transition timing so scroll-reveal effects are
        # captured at their final frame, not mid-fade.
        try:
            await page.add_style_tag(content=NEUTRALIZE_ANIMATION_CSS)
        except Exception:
            pass
        # Ready the page (walk it to fire reveals + lazy loads, wait for fonts +
        # images, hide sticky chrome) before extract + per-section screenshots.
        await prepare_page(
            page, step_frac=cfg.capture_scroll_step_frac,
            step_ms=cfg.capture_scroll_step_ms, max_steps=cfg.capture_scroll_max_steps,
            img_wait_ms=cfg.capture_img_wait_ms)
        raw = await run_extract(page)
        # Warn on likely-blocked / unhydrated pages (anti-bot challenge, empty SPA).
        _ft = ((raw.get("text") or {}).get("full_text") or "").strip()
        if len(_ft) < 100:
            logger.warning("capture: only %d chars of text for %s — page may be "
                           "blocked, challenged, or an unhydrated SPA", len(_ft), url)
        shots = await capture_screenshots(
            page, raw.get("sections", []),
            max_screenshots=cfg.capture_max_screenshots,
            max_height=cfg.capture_max_screenshot_height,
            section_settle_ms=cfg.capture_section_settle_ms,
        )

    final_url = raw.get("final_url", url)
    vision_configured = bool(cfg.vision_base_url)
    obj_storage = await get_storage()
    raw_assets = raw.get("assets", {})

    # ---- image assets (logo/hero/og/favicon) -> fetch -> ImageRef ----
    image_refs: list = []            # vision-worthy assets (logo/hero/og_image)
    novision_asset_refs: list = []   # favicon etc. — stored, never described
    filename_kind: dict[str, str] = {}
    seen_urls: set[str] = set()
    for kind in ("logo", "hero", "og_image", "favicon"):
        a_url = raw_assets.get(kind)
        if not a_url or a_url in seen_urls:
            continue
        seen_urls.add(a_url)
        got = await fetch_asset_bytes(a_url, max_bytes=cfg.capture_max_asset_bytes)
        if got is None:
            continue
        data, ctype = got
        ext = _IMG_EXT.get(ctype, ".png")
        fname = f"{kind}{ext}"
        filename_kind[fname] = kind
        ref = image_ref_from_bytes(data, filename=fname, mime=ctype or "image/png", alt=kind)
        (image_refs if _asset_gets_vision(kind, fname) else novision_asset_refs).append(ref)

    # ---- screenshots -> ImageRef ----
    shot_refs: list = []
    shot_meta: list[tuple[str, dict]] = []
    for s in shots:
        label = s["kind"] if s["section_index"] is None else f"section-{s['section_index']:02d}"
        fname = f"screenshot-{label}.png"
        shot_meta.append((fname, s))
        shot_refs.append(image_ref_from_bytes(s["bytes"], filename=fname, mime="image/png",
                                              width=s["width"], height=s["height"]))

    # Two upload calls split by whether the image should get a vision
    # description: logo/hero/og_image do; favicon + screenshots don't. The
    # post-upload re-query joins on filename, which is safe because filenames
    # are unique by construction: asset names are ``{kind}.{ext}`` for distinct
    # kinds (duplicate URLs dropped above), screenshot names are ``screenshot-
    # {label}.png`` for distinct labels — so name_picker never renames.
    novision_refs = novision_asset_refs + shot_refs
    if image_refs:
        await stream_upload_and_store_refs(image_refs, kb_id=kb_id, doc_id=doc_id,
                                           vision_configured=vision_configured)
    if novision_refs:
        await stream_upload_and_store_refs(novision_refs, kb_id=kb_id, doc_id=doc_id,
                                           vision_configured=False)

    fn_to_row = await _capture_hydrate_image_ids(doc_id)

    # ---- image assets -> profile ----
    profile_assets: list = []
    for fname, kind in filename_kind.items():
        row = fn_to_row.get(fname)
        if not row:
            continue
        img_id, skey = row
        profile_assets.append(AssetRef(
            kind=kind, image_id=img_id, storage_key=skey, format=fname.rsplit(".", 1)[-1],
            vision_status=("pending" if vision_configured and _asset_gets_vision(kind, fname) else "skipped"),
        ))

    # ---- non-image binaries: background video / lottie ----
    for kind, a_url in (("background_video", raw_assets.get("video")),
                        ("lottie", raw_assets.get("lottie"))):
        if not a_url:
            continue
        got = await fetch_asset_bytes(a_url, max_bytes=cfg.capture_max_asset_bytes)
        if got is None:
            continue
        data, ctype = got
        ext = _BIN_EXT.get(ctype, ".bin")
        key = f"captures/{kb_id}/{doc_id}/assets/{kind}{ext}"
        await obj_storage.put(key, data, content_type=ctype or "application/octet-stream")
        profile_assets.append(AssetRef(kind=kind, storage_key=key, format=ext.lstrip(".")))

    # ---- fonts (catalog match; download woff2 on miss) ----
    face_srcs = raw.get("fonts", {}).get("face_srcs", {})

    async def _font_role(info: dict):
        role = build_font_role(info, weights=[info.get("weight", 400)])
        if not role.renderable:
            srcs = face_srcs.get(role.family.lower(), [])
            if srcs:
                got = await fetch_asset_bytes(srcs[0], max_bytes=cfg.capture_max_asset_bytes)
                if got:
                    data, _ = got
                    key = (f"captures/{kb_id}/{doc_id}/fonts/"
                           f"{role.family.replace(' ', '-').lower()}.woff2")
                    await obj_storage.put(key, data, content_type="font/woff2")
                    role.files = [FontFile(url=key, weight=info.get("weight"), source="captured")]
        return role

    raw_fonts = raw.get("fonts", {})
    fonts = Fonts(display=await _font_role(raw_fonts.get("display", {})),
                  body=await _font_role(raw_fonts.get("body", {})))

    # ---- screenshots -> profile ----
    profile_shots: list = []
    for fname, s in shot_meta:
        row = fn_to_row.get(fname)
        if not row:
            continue
        img_id, _skey = row
        profile_shots.append(ScreenshotRef(kind=s["kind"], image_id=img_id,
                                           width=s["width"], height=s["height"],
                                           section_index=s["section_index"]))

    # ---- colors / spacing / sections ----
    # Pixel-sample the full-page screenshot's dominant color to cross-check the
    # computed-style background (raises confidence + catches gradient/image bgs).
    full_png = next((s["bytes"] for s in shots if s["kind"] == "full_page"), None)
    screenshot_bg = dominant_screenshot_hex(full_png) if full_png else None
    colors = process_colors(raw.get("colors", {}),
                            delta_e_threshold=cfg.capture_color_delta_e,
                            screenshot_bg_hex=screenshot_bg)
    sp = raw.get("spacing", {})
    spacing = Spacing(
        scale=cluster_spacing(sp.get("margins", []) + sp.get("paddings", [])),
        radii=cluster_spacing(sp.get("radii", []), max_val=500),
        container_max_width=sp.get("container_max_width"),
        section_gap=(round(min(sp["section_gaps"])) if sp.get("section_gaps") else None),
    )
    raw_sections = raw.get("sections", [])
    shot_by_section = {s.section_index: s.image_id
                       for s in profile_shots if s.section_index is not None}
    sections = [SectionInfo(
        index=s["index"], heading=s.get("heading", ""),
        type=section_type(s.get("heading", ""), s.get("classNames", []),
                          s["index"], len(raw_sections)),
        bg_color=_safe_hex(s.get("bg", "")),
        screenshot_image_id=shot_by_section.get(s["index"]),
    ) for s in raw_sections]

    t = raw.get("text", {})
    profile = SiteProfile(
        url=final_url, captured_at=datetime.now(timezone.utc).isoformat(),
        fetch_tier="playwright", colors=colors,
        theme_color=raw.get("colors", {}).get("theme_color"),
        fonts=fonts, spacing=spacing, sections=sections,
        text=TextInfo(headline=t.get("headline", ""), tagline=t.get("tagline", ""),
                      ctas=t.get("ctas", []), full_text=t.get("full_text", "")),
        assets=profile_assets, screenshots=profile_shots,
        motion_hints=MotionHints(**raw.get("motion", {})),
    )

    await update_doc(doc_id, status="completed", index_status="skipped",
                     image_status=("completed" if (image_refs or novision_refs) else "none"),
                     title=(t.get("headline") or final_url)[:500],
                     profile=profile.model_dump(),
                     error_msg="", error_type=None, error_trace=None)

    if vision_configured and image_refs:
        await enqueue("analyze_images", {"kb_id": kb_id, "doc_id": doc_id})
