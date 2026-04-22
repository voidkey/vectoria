#!/usr/bin/env bash
# Daily bad-case digest cron entrypoint.
#
# Reuses the running ``app`` container to query the DB and POST to
# WeCom, avoiding a second copy of credentials. A one-shot ``run --rm``
# is chosen over ``exec`` so a long-running digest can't wedge the
# live app; and over a standalone image because the digest has the
# same dependency graph (sqlalchemy, httpx, config) as the app.
#
# Install on the host (one line per env):
#
#   # On deploy-host (test env): every day at 09:00 CST
#   0 9 * * *  /root/app/src/vectoria/scripts/cron-digest.sh >> /var/log/vectoria-digest.log 2>&1
#
# Env vars read by the digest script:
#   * WECOM_WEBHOOK_URL  (required — same value the alert-relay uses)
#   * DIGEST_ENV_LABEL   (optional — 'test' / 'prod' prefix in wecom msg)
#
# Both come from ``.env.prod`` via ``--env-file``, so there's no extra
# config to keep in sync.
set -euo pipefail

cd "$(dirname "$0")/.."

ENV_FILE="${ENV_FILE:-.env.prod}"
if [ ! -f "$ENV_FILE" ]; then
    echo "ERROR: $ENV_FILE not found" >&2
    exit 1
fi

# ``run --rm`` spins up a one-off container sharing the compose network
# (so ``db`` / ``redis`` resolve), auto-removes on exit. No volumes
# mounted beyond what compose defaults to — read-only work, nothing to
# persist.
exec docker compose \
    -f compose.yaml -f compose.prod.yaml -f monitoring/compose.monitoring.yaml \
    --env-file "$ENV_FILE" \
    run --rm --no-deps \
    -e DIGEST_ENV_LABEL="${DIGEST_ENV_LABEL:-test}" \
    app python -m scripts.send_daily_digest "$@"
