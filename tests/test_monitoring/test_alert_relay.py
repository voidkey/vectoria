"""Tests for the Alertmanager → WeChat group-bot relay.

Wecom will silently drop messages that don't match its contract, so
the relay's reshaping logic is the one place a bug here would result
in missing pages during an incident. Lock the contract down.
"""
import os
from unittest.mock import AsyncMock, patch

import pytest
from httpx import ASGITransport, AsyncClient


def _alertmanager_payload(n_alerts: int = 1, status: str = "firing") -> dict:
    """Shape matches Alertmanager's webhook v4 contract."""
    return {
        "version": "4",
        "status": status,
        "receiver": "wecom",
        "alerts": [
            {
                "status": status,
                "labels": {
                    "alertname": f"TestAlert{i}",
                    "severity": "warning" if i % 2 else "critical",
                    "task_type": "parse_document",
                    "component": "worker",
                },
                "annotations": {
                    "summary": f"summary {i}",
                    "description": f"description for alert {i}",
                },
                "startsAt": "2026-04-22T12:00:00Z",
                "endsAt": "0001-01-01T00:00:00Z",
            }
            for i in range(n_alerts)
        ],
    }


def _reload_relay(monkeypatch, **env):
    """Import alert_relay fresh with the given env overrides so
    module-level config (WECOM_URL) reflects the test env.
    """
    for k, v in env.items():
        monkeypatch.setenv(k, v)
    # Force reload: the module reads env at import time.
    import importlib, sys
    sys.modules.pop("monitoring.alert_relay", None)
    import monitoring.alert_relay as relay
    importlib.reload(relay)
    return relay


@pytest.mark.asyncio
async def test_relay_reshapes_to_wecom_format(monkeypatch):
    """Alertmanager payload → wecom JSON (msgtype=text, text.content)."""
    relay = _reload_relay(
        monkeypatch, WECOM_WEBHOOK_URL="https://qyapi.test/webhook?key=fake",
    )

    captured: dict = {}

    class _MockClient:
        async def __aenter__(self): return self
        async def __aexit__(self, *a): return None
        async def post(self, url, json):
            captured["url"] = url
            captured["json"] = json
            # Wecom returns {"errcode":0,"errmsg":"ok"} on success.
            class _Resp:
                def raise_for_status(self): pass
                def json(self): return {"errcode": 0, "errmsg": "ok"}
            return _Resp()

    with patch("monitoring.alert_relay.httpx.AsyncClient", return_value=_MockClient()):
        async with AsyncClient(
            transport=ASGITransport(app=relay.app),
            base_url="http://test",
        ) as c:
            resp = await c.post("/alert", json=_alertmanager_payload(2, "firing"))

    assert resp.status_code == 200
    assert resp.json()["delivered"] == 2
    assert captured["url"].startswith("https://qyapi.test/webhook")
    body = captured["json"]
    assert body["msgtype"] == "text"
    content = body["text"]["content"]
    # Summary/description must make it into the text body.
    assert "summary 0" in content
    assert "description for alert 0" in content
    assert "TestAlert0" in content
    assert "TestAlert1" in content


@pytest.mark.asyncio
async def test_relay_caps_long_descriptions(monkeypatch):
    """Wecom caps messages at ~2000 chars; a single runaway alert
    description shouldn't eat the whole budget.
    """
    relay = _reload_relay(
        monkeypatch, WECOM_WEBHOOK_URL="https://qyapi.test/webhook?key=fake",
    )
    payload = _alertmanager_payload(1, "firing")
    payload["alerts"][0]["annotations"]["description"] = "x" * 2000

    captured: dict = {}

    class _MockClient:
        async def __aenter__(self): return self
        async def __aexit__(self, *a): return None
        async def post(self, url, json):
            captured["json"] = json
            class _Resp:
                def raise_for_status(self): pass
                def json(self): return {"errcode": 0}
            return _Resp()

    with patch("monitoring.alert_relay.httpx.AsyncClient", return_value=_MockClient()):
        async with AsyncClient(
            transport=ASGITransport(app=relay.app),
            base_url="http://test",
        ) as c:
            resp = await c.post("/alert", json=payload)

    assert resp.status_code == 200
    content = captured["json"]["text"]["content"]
    # Truncated with ellipsis marker so operator knows there was more.
    assert "…" in content
    assert len(content) < 500  # single alert, reasonable total length


@pytest.mark.asyncio
async def test_relay_caps_max_alerts_per_message(monkeypatch):
    """A storm of 50 alerts should produce one message with the cap +
    a 'showing first N' header, not 50 separate messages.
    """
    relay = _reload_relay(
        monkeypatch,
        WECOM_WEBHOOK_URL="https://qyapi.test/webhook?key=fake",
        RELAY_MAX_ALERTS="5",
    )
    captured: dict = {}

    class _MockClient:
        async def __aenter__(self): return self
        async def __aexit__(self, *a): return None
        async def post(self, url, json):
            captured["json"] = json
            class _Resp:
                def raise_for_status(self): pass
                def json(self): return {"errcode": 0}
            return _Resp()

    with patch("monitoring.alert_relay.httpx.AsyncClient", return_value=_MockClient()):
        async with AsyncClient(
            transport=ASGITransport(app=relay.app),
            base_url="http://test",
        ) as c:
            await c.post("/alert", json=_alertmanager_payload(50, "firing"))

    content = captured["json"]["text"]["content"]
    assert "50 alert(s)" in content
    assert "showing first 5" in content
    # Alerts beyond the cap must not be in the text.
    assert "TestAlert5" not in content
    assert "TestAlert49" not in content


@pytest.mark.asyncio
async def test_relay_returns_502_on_wecom_error_code(monkeypatch):
    """Wecom returns errcode != 0 for bad content / bad key; surface
    as 502 so Alertmanager retries later.
    """
    relay = _reload_relay(
        monkeypatch, WECOM_WEBHOOK_URL="https://qyapi.test/webhook?key=fake",
    )

    class _MockClient:
        async def __aenter__(self): return self
        async def __aexit__(self, *a): return None
        async def post(self, url, json):
            class _Resp:
                def raise_for_status(self): pass
                def json(self): return {"errcode": 93000, "errmsg": "invalid key"}
            return _Resp()

    with patch("monitoring.alert_relay.httpx.AsyncClient", return_value=_MockClient()):
        async with AsyncClient(
            transport=ASGITransport(app=relay.app),
            base_url="http://test",
        ) as c:
            resp = await c.post("/alert", json=_alertmanager_payload(1, "firing"))

    assert resp.status_code == 502


@pytest.mark.asyncio
async def test_relay_refuses_without_webhook_url(monkeypatch):
    """Misconfig: if WECOM_WEBHOOK_URL is empty, return 500 rather
    than silently dropping alerts on the floor.
    """
    relay = _reload_relay(monkeypatch, WECOM_WEBHOOK_URL="")

    async with AsyncClient(
        transport=ASGITransport(app=relay.app),
        base_url="http://test",
    ) as c:
        resp = await c.post("/alert", json=_alertmanager_payload(1, "firing"))

    assert resp.status_code == 500
    assert "WECOM_WEBHOOK_URL" in resp.json().get("detail", "")


@pytest.mark.asyncio
async def test_relay_formats_resolved_distinctly_from_firing(monkeypatch):
    """Resolved alerts must render visibly different from firing ones —
    same emoji + same description would make operators think the alert
    is still active. Checks the '已恢复' prefix, ✅ marker, duration
    computation, and that the (now-irrelevant) description is dropped.
    """
    relay = _reload_relay(
        monkeypatch, WECOM_WEBHOOK_URL="https://qyapi.test/webhook?key=fake",
    )
    payload = _alertmanager_payload(1, "resolved")
    # endsAt 5m37s after startsAt
    payload["alerts"][0]["startsAt"] = "2026-04-22T15:29:43Z"
    payload["alerts"][0]["endsAt"] = "2026-04-22T15:35:20Z"

    captured: dict = {}

    class _MockClient:
        async def __aenter__(self): return self
        async def __aexit__(self, *a): return None
        async def post(self, url, json):
            captured["json"] = json
            class _Resp:
                def raise_for_status(self): pass
                def json(self): return {"errcode": 0}
            return _Resp()

    with patch("monitoring.alert_relay.httpx.AsyncClient", return_value=_MockClient()):
        async with AsyncClient(
            transport=ASGITransport(app=relay.app),
            base_url="http://test",
        ) as c:
            await c.post("/alert", json=payload)

    content = captured["json"]["text"]["content"]
    assert "[RESOLVED]" in content           # header carries the status
    assert "✅" in content                   # all-clear emoji, not 🔴/🟡/🔵
    assert "已恢复" in content
    assert "TestAlert0" in content
    assert "结束：" in content
    assert "持续：5 分 37 秒" in content      # duration math, CST rendering
    # Resolved alerts should NOT carry the firing-time runbook.
    assert "description for alert 0" not in content


@pytest.mark.asyncio
async def test_relay_firing_includes_start_time(monkeypatch):
    """Firing alerts carry startsAt so operators know when it started
    (the wecom timestamp is delivery time, not incident time).
    """
    relay = _reload_relay(
        monkeypatch, WECOM_WEBHOOK_URL="https://qyapi.test/webhook?key=fake",
    )
    captured: dict = {}

    class _MockClient:
        async def __aenter__(self): return self
        async def __aexit__(self, *a): return None
        async def post(self, url, json):
            captured["json"] = json
            class _Resp:
                def raise_for_status(self): pass
                def json(self): return {"errcode": 0}
            return _Resp()

    with patch("monitoring.alert_relay.httpx.AsyncClient", return_value=_MockClient()):
        async with AsyncClient(
            transport=ASGITransport(app=relay.app),
            base_url="http://test",
        ) as c:
            await c.post("/alert", json=_alertmanager_payload(1, "firing"))

    content = captured["json"]["text"]["content"]
    assert "开始：" in content
    # 12:00 UTC = 20:00 CST; renderer must convert to local.
    assert "20:00:00" in content


@pytest.mark.asyncio
async def test_relay_health_reports_config_status(monkeypatch):
    relay = _reload_relay(monkeypatch, WECOM_WEBHOOK_URL="https://test")
    async with AsyncClient(
        transport=ASGITransport(app=relay.app),
        base_url="http://test",
    ) as c:
        resp = await c.get("/health")
    assert resp.status_code == 200
    body = resp.json()
    assert body["ok"] is True
    assert body["wecom_configured"] is True
