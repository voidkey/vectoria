"""Alertmanager → WeChat-group-bot webhook relay.

Why
---
Alertmanager's built-in ``wechat_configs`` is for 企业自建应用 (needs
``corp_id`` + ``api_secret`` + ``agent_id``), not for 群机器人. Group-
bot webhooks accept a specific JSON shape
(``{"msgtype":"text","text":{"content":"..."}}``) that Alertmanager
can't emit directly. This relay accepts Alertmanager's generic
webhook payload, reshapes it into the wecom group-bot contract, and
forwards.

One file, one dependency surface (httpx + fastapi — already in the
vectoria image). Runs as a sidecar in the monitoring compose, reuses
the main vectoria Docker image to avoid shipping a second artifact.

Env
---
* ``WECOM_WEBHOOK_URL`` — required, full webhook URL including ``?key=``
* ``RELAY_MAX_ALERTS`` — optional, cap alerts per message (default 10)
  so one storm doesn't overflow wecom's 2000-char text limit
"""
from __future__ import annotations

import logging
import os

import httpx
from fastapi import FastAPI, HTTPException, Request

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

app = FastAPI(title="vectoria alert relay")

_WECOM_URL = os.getenv("WECOM_WEBHOOK_URL", "").strip()
_MAX_ALERTS = int(os.getenv("RELAY_MAX_ALERTS", "10"))

_SEVERITY_EMOJI = {
    "critical": "🔴",
    "warning":  "🟡",
    "info":     "🔵",
}


def _format_alert(a: dict) -> str:
    """Render one alert as a short human-readable block."""
    labels = a.get("labels", {}) or {}
    annotations = a.get("annotations", {}) or {}
    name = labels.get("alertname", "?")
    severity = labels.get("severity", "info")
    # Label hints that operators actually want to see at a glance.
    hint_bits = []
    for k in ("task_type", "name", "api", "engine", "key", "component"):
        v = labels.get(k)
        if v:
            hint_bits.append(f"{k}={v}")
    hint = f" [{' '.join(hint_bits)}]" if hint_bits else ""

    summary = (annotations.get("summary") or "").strip()
    description = (annotations.get("description") or "").strip()
    # Wecom hard-caps total message length; keep description short.
    if len(description) > 240:
        description = description[:237] + "…"

    emoji = _SEVERITY_EMOJI.get(severity, "⚪")
    # ``{name}{hint}`` carries the machine-readable bits; summary +
    # description explain what to do.
    return f"{emoji} {name}{hint}\n{summary}\n{description}".rstrip()


def _build_content(payload: dict) -> str:
    """Turn an Alertmanager webhook payload into a wecom text body."""
    status = (payload.get("status") or "").upper()
    alerts = payload.get("alerts") or []
    if not alerts:
        return ""

    header = f"[{status}] vectoria — {len(alerts)} alert(s)"
    if len(alerts) > _MAX_ALERTS:
        header += f" (showing first {_MAX_ALERTS})"
    body_blocks = [_format_alert(a) for a in alerts[:_MAX_ALERTS]]
    return header + "\n\n" + "\n\n".join(body_blocks)


@app.post("/alert")
async def relay(request: Request) -> dict:
    if not _WECOM_URL:
        raise HTTPException(500, "WECOM_WEBHOOK_URL not configured")
    payload = await request.json()
    content = _build_content(payload)
    if not content:
        return {"ok": True, "skipped": "no alerts in payload"}

    try:
        async with httpx.AsyncClient(timeout=10) as client:
            resp = await client.post(
                _WECOM_URL,
                json={"msgtype": "text", "text": {"content": content}},
            )
        resp.raise_for_status()
        result = resp.json()
    except Exception:
        # Log with alert details so debugging doesn't need a wecom
        # replay — the original Alertmanager event is already lost.
        logger.exception("wecom relay failed for content: %s", content[:200])
        raise HTTPException(502, "wecom delivery failed")

    if result.get("errcode") != 0:
        logger.error("wecom rejected: %s", result)
        raise HTTPException(502, f"wecom errcode={result.get('errcode')}")
    return {"ok": True, "delivered": len(payload.get("alerts", []))}


@app.get("/health")
async def health() -> dict:
    return {"ok": True, "wecom_configured": bool(_WECOM_URL)}
