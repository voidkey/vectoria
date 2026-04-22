"""Send the bad-case ingest digest to WeCom.

Invoked by an OS cron (or any scheduler):

    python -m scripts.send_daily_digest --hours 24 --top 10

It queries the documents table for failed ingests in the last N hours,
formats a wecom-ready message, and POSTs to WECOM_WEBHOOK_URL.

Environment variables
---------------------
* ``WECOM_WEBHOOK_URL`` — required, same as alert-relay reads
* ``DIGEST_ENV_LABEL`` — optional, e.g. "test" / "prod". Prefixed into
  the message header so multi-env deployments don't mix up digests.
  Falls back to the first word of ``ENV`` or empty.

Why a standalone script instead of extending alert-relay
--------------------------------------------------------
alert-relay's container intentionally doesn't have DB credentials — its
only job is reshaping webhook payloads. Digests need DB access (read
from ``documents``), which belongs in the app/worker image. Keeping
them separate means a relay compromise can't read the database.
"""
from __future__ import annotations

import argparse
import asyncio
import logging
import os
import sys

import httpx

from monitoring.digest import build_digest, format_digest_text


logger = logging.getLogger(__name__)


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    p.add_argument("--hours", type=int, default=24, help="Window size (default 24h)")
    p.add_argument("--top", type=int, default=10, help="Sample count (default 10)")
    p.add_argument("--dry-run", action="store_true", help="Print to stdout, don't send")
    return p.parse_args()


async def _main() -> int:
    args = _parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    env_label = os.getenv("DIGEST_ENV_LABEL") or ""
    webhook = (os.getenv("WECOM_WEBHOOK_URL") or "").strip()

    digest = await build_digest(hours=args.hours, sample_limit=args.top)
    text = format_digest_text(digest, env=env_label)

    if args.dry_run:
        print(text)
        return 0

    if not webhook:
        logger.error("WECOM_WEBHOOK_URL not set — refusing to silently drop digest")
        return 2

    async with httpx.AsyncClient(timeout=10) as client:
        resp = await client.post(
            webhook,
            json={"msgtype": "text", "text": {"content": text}},
        )
    resp.raise_for_status()
    body = resp.json()
    if body.get("errcode") != 0:
        logger.error("wecom rejected digest: %s", body)
        return 3

    logger.info(
        "digest sent: window=%dh total=%d samples=%d",
        args.hours, digest["total"], len(digest["samples"]),
    )
    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(_main()))
