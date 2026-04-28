"""Auto-retry parse_error docs by re-enqueueing parse_document.

Run hourly via ``scripts/cron-retry-dead-docs.sh``. Gives docs that
died on yesterday's bug a chance with today's code without anyone
having to handcraft SQL — particularly useful after deploys that
land new fallback chains or fix sharp edges in a specific parser.

Eligibility (intentionally narrow):
  * status = 'failed' AND error_type = 'parse_error'
    — empty_content / too_large / image_only are terminal-by-design,
      retrying them just churns
  * created_at within ``--max-age-hours`` (default 7d)
    — old failures usually represent already-discarded user intent;
      not worth waking up the worker for
  * no in-flight (pending/running) parse_document task for this doc
    — don't race the existing pipeline
  * no parse_document task created in the last ``--retry-lockout-minutes``
    (default 60m) — caps retry frequency to once per hour even if
    the cron fires more often

Each eligible doc gets:
  1. parse_engine reset via ``registry.auto_select`` so ancient docs
     stuck on a now-broken engine pick up the current preference chain
  2. error fields cleared, status → 'queued'
  3. fresh parse_document task enqueued

Defaults intentionally conservative; tune via CLI flags.

CLI:
  python -m worker.retry_dead_docs                    # apply
  python -m worker.retry_dead_docs --dry-run          # preview only
  python -m worker.retry_dead_docs --limit 10         # smaller batch
"""
from __future__ import annotations

import argparse
import asyncio
import logging
import sys
from datetime import datetime, timedelta, timezone

from sqlalchemy import and_, exists, not_, or_, select

from db.base import get_session
from db.helpers import update_doc
from db.models import Document, Task

logger = logging.getLogger(__name__)


async def find_eligible_docs(
    session,
    *,
    max_age_hours: int,
    retry_lockout_minutes: int,
    limit: int,
) -> list[Document]:
    """Failed parse_error docs eligible for re-enqueue.

    Uses ORM-level NOT EXISTS to avoid race-y two-step reads. Json
    payload->>'doc_id' lets us join through the un-foreign-keyed
    tasks.payload — the queue is intentionally agnostic of doc
    schema, but for this maintenance read we know the convention.
    """
    now = datetime.now(timezone.utc).replace(tzinfo=None)
    age_floor = now - timedelta(hours=max_age_hours)
    lockout_floor = now - timedelta(minutes=retry_lockout_minutes)

    # Subquery: any parse_document task for this doc that's either
    # in flight or recent enough we shouldn't double-retry.
    inflight_or_recent = (
        select(Task.id)
        .where(
            Task.task_type == "parse_document",
            Task.payload["doc_id"].as_string() == Document.id,
            or_(
                Task.status.in_(("pending", "running")),
                Task.created_at > lockout_floor,
            ),
        )
    )

    stmt = (
        select(Document)
        .where(
            Document.status == "failed",
            Document.error_type == "parse_error",
            Document.created_at > age_floor,
            not_(exists(inflight_or_recent)),
        )
        .order_by(Document.created_at.desc())
        .limit(limit)
    )
    result = await session.execute(stmt)
    return list(result.scalars().all())


def _build_payload(doc: Document) -> dict:
    """Reconstruct a parse_document payload from doc fields. Picks a
    fresh engine via registry so docs whose original engine has
    since changed availability (e.g. mineru breaker open) pick up
    today's preference chain instead of replaying yesterday's choice.
    """
    from parsers.registry import registry

    if doc.storage_key:
        # Files: title is the original filename (we keep it as-is on
        # ingest); registry picks the engine off that.
        filename = doc.title or ""
        engine = registry.auto_select(filename=filename)
    else:
        filename = ""
        engine = registry.auto_select(url=doc.source)

    return {
        "doc_id": doc.id,
        "kb_id": doc.kb_id,
        "storage_key": doc.storage_key,
        "source": doc.source,
        "filename": filename,
        "selected_engine": engine,
    }


async def retry_dead_docs(
    *,
    max_age_hours: int = 24 * 7,
    retry_lockout_minutes: int = 60,
    limit: int = 50,
    dry_run: bool = False,
) -> tuple[int, int]:
    """Returns (re_enqueued_count, skipped_count)."""
    from worker.queue import enqueue

    async with get_session() as session:
        docs = await find_eligible_docs(
            session,
            max_age_hours=max_age_hours,
            retry_lockout_minutes=retry_lockout_minutes,
            limit=limit,
        )

    logger.info("retry_dead_docs: %d eligible doc(s) found", len(docs))

    re_enqueued = skipped = 0
    for doc in docs:
        try:
            payload = _build_payload(doc)
        except Exception:
            logger.exception(
                "retry_dead_docs: skipping doc=%s — payload build failed", doc.id,
            )
            skipped += 1
            continue

        if dry_run:
            logger.info(
                "retry_dead_docs[dry-run]: doc=%s would re-enqueue with engine=%s "
                "(originally %s)",
                doc.id, payload["selected_engine"], doc.parse_engine,
            )
            re_enqueued += 1
            continue

        try:
            await enqueue("parse_document", payload)
            await update_doc(
                doc.id, status="queued",
                error_type=None, error_msg="", error_trace=None,
            )
            re_enqueued += 1
            logger.info(
                "retry_dead_docs: re-enqueued doc=%s engine=%s (originally %s)",
                doc.id, payload["selected_engine"], doc.parse_engine,
            )
        except Exception:
            logger.exception("retry_dead_docs: re-enqueue failed doc=%s", doc.id)
            skipped += 1

    logger.info(
        "retry_dead_docs: done — re_enqueued=%d skipped=%d",
        re_enqueued, skipped,
    )
    return re_enqueued, skipped


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    p.add_argument(
        "--max-age-hours", type=int, default=24 * 7,
        help="Skip docs older than this (default 168h = 7d)",
    )
    p.add_argument(
        "--retry-lockout-minutes", type=int, default=60,
        help="Skip docs that already have a recent parse_document task "
        "within this window (default 60m)",
    )
    p.add_argument(
        "--limit", type=int, default=50,
        help="Max docs to re-enqueue per run (default 50, prevents storm)",
    )
    p.add_argument(
        "--dry-run", action="store_true",
        help="Print what would be re-enqueued, don't actually enqueue",
    )
    return p.parse_args()


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    args = _parse_args()
    re, sk = asyncio.run(retry_dead_docs(
        max_age_hours=args.max_age_hours,
        retry_lockout_minutes=args.retry_lockout_minutes,
        limit=args.limit,
        dry_run=args.dry_run,
    ))
    # Exit 0 always: cron failure semantics shouldn't depend on
    # whether anyone failed (which is normal).  Hard errors (DB
    # unreachable) still raise out of asyncio.run.
    sys.exit(0)


if __name__ == "__main__":
    main()
