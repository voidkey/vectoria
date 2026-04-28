#!/usr/bin/env bash
# Hourly auto-retry for parse-failed docs.
#
# Re-enqueues parse_document for docs that died with parse_error so
# they pick up the latest parser code (new fallback chains, fixed
# sharp edges, etc.) without anyone having to handcraft SQL after
# a deploy.
#
# Eligibility filter is in worker/retry_dead_docs.py — read there
# for full criteria (in short: failed parse_error, age ≤ 7d, no
# in-flight or recent retry).
#
# Install on the host:
#   # Hourly, 5 minutes past the hour:
#   5 * * * *  /path/to/vectoria/scripts/cron-retry-dead-docs.sh \
#       >> /var/log/vectoria-retry.log 2>&1
#
# CLI knobs (forwarded as $@):
#   --dry-run                  preview only
#   --limit N                  cap per-run batch size
#   --max-age-hours N
#   --retry-lockout-minutes N
set -euo pipefail

cd "$(dirname "$0")/.."

ENV_FILE="${ENV_FILE:-.env.prod}"
if [ ! -f "$ENV_FILE" ]; then
    echo "ERROR: $ENV_FILE not found" >&2
    exit 1
fi

# ``run --rm`` mirrors cron-digest.sh: one-off container shares the
# compose network so ``db`` / ``redis`` resolve, auto-removes after.
exec docker compose \
    -f compose.yaml -f compose.prod.yaml \
    --env-file "$ENV_FILE" \
    run --rm --no-deps \
    app python -m worker.retry_dead_docs "$@"
