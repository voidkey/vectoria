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
from parsers.base import PermanentParseError
from parsers.image_metadata import extract_metadata_into_refs
from parsers.registry import registry
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
        error_msg="", error_type=None, error_trace=None,
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
