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
from datetime import datetime, timezone

try:
    from zoneinfo import ZoneInfo
    _LOCAL_TZ = ZoneInfo("Asia/Shanghai")
except Exception:  # pragma: no cover — alpine base without tzdata
    _LOCAL_TZ = timezone.utc

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
# Resolved alerts use a single "all-clear" marker regardless of the original
# severity — the point of the notification is "it's over", not "it was bad".
_RESOLVED_EMOJI = "✅"


def _parse_ts(s: str) -> datetime | None:
    """Parse Alertmanager's RFC3339 timestamp into local tz.

    Returns None for empty / zero-sentinel / unparseable input. Zero sentinel
    is ``0001-01-01T00:00:00Z`` (Go's time.Time{} marshalled), which
    Alertmanager sends for ``endsAt`` on still-firing alerts.
    """
    if not s:
        return None
    # Py 3.11+ accepts 'Z', but nanosecond precision (9 fractional digits)
    # still trips fromisoformat — cap to microseconds.
    s = s.replace("Z", "+00:00")
    if "." in s:
        dot = s.index(".")
        tz_start = max(s.rfind("+"), s.rfind("-"))
        if tz_start > dot:
            frac = s[dot + 1 : tz_start]
            if len(frac) > 6:
                s = s[: dot + 1] + frac[:6] + s[tz_start:]
    try:
        dt = datetime.fromisoformat(s)
    except ValueError:
        return None
    if dt.year < 1970:
        return None
    return dt.astimezone(_LOCAL_TZ)


def _format_duration(seconds: float) -> str:
    """Human-friendly Chinese duration: 47秒 / 5分37秒 / 1小时23分."""
    s = int(max(seconds, 0))
    if s < 60:
        return f"{s} 秒"
    m, s = divmod(s, 60)
    if m < 60:
        return f"{m} 分 {s} 秒" if s else f"{m} 分"
    h, m = divmod(m, 60)
    return f"{h} 小时 {m} 分" if m else f"{h} 小时"


def _format_alert(a: dict) -> str:
    """Render one alert as a short human-readable block.

    Firing and resolved get visibly different shapes so they're not confused
    at a glance: resolved uses the ✅ marker, a '已恢复' prefix, and shows
    start/end/duration instead of the runbook description (排查步骤 on a
    resolved alert is noise, not signal).
    """
    labels = a.get("labels", {}) or {}
    annotations = a.get("annotations", {}) or {}
    name = labels.get("alertname", "?")
    severity = labels.get("severity", "info")
    status = (a.get("status") or "firing").lower()

    # Label hints that operators actually want to see at a glance.
    hint_bits = []
    for k in ("task_type", "name", "api", "engine", "key", "component"):
        v = labels.get(k)
        if v:
            hint_bits.append(f"{k}={v}")
    hint = f" [{' '.join(hint_bits)}]" if hint_bits else ""

    summary = (annotations.get("summary") or "").strip()
    starts = _parse_ts(a.get("startsAt", ""))

    if status == "resolved":
        ends = _parse_ts(a.get("endsAt", ""))
        lines = [f"{_RESOLVED_EMOJI} 已恢复 {name}{hint}"]
        if summary:
            lines.append(summary)
        if starts:
            lines.append(f"开始：{starts.strftime('%m-%d %H:%M:%S')}")
        if ends:
            lines.append(f"结束：{ends.strftime('%m-%d %H:%M:%S')}")
        if starts and ends:
            lines.append(f"持续：{_format_duration((ends - starts).total_seconds())}")
        return "\n".join(lines)

    description = (annotations.get("description") or "").strip()
    # Wecom hard-caps total message length; keep description short.
    if len(description) > 240:
        description = description[:237] + "…"
    emoji = _SEVERITY_EMOJI.get(severity, "⚪")
    lines = [f"{emoji} {name}{hint}"]
    if summary:
        lines.append(summary)
    if starts:
        lines.append(f"开始：{starts.strftime('%m-%d %H:%M:%S')}")
    if description:
        lines.append(description)
    return "\n".join(lines)


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
