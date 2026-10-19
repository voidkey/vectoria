"""SSRF egress guard: redirects into internal addresses must be refused on
every fetch tier (httpx hook, egress proxy for Chromium / curl_cffi)."""
import asyncio
import socket
from unittest.mock import AsyncMock, MagicMock, patch

import httpx
import pytest

from parsers.url import _browser, _egress
from parsers.url._egress import BlockedAddressError
from parsers.url._http import make_async_client


def _fake_dns(mapping):
    def getaddrinfo(host, *a, **kw):
        ip = mapping.get(host, host)
        return [(socket.AF_INET, socket.SOCK_STREAM, 6, "", (ip, 0))]
    return getaddrinfo


@pytest.mark.real_egress_guard
async def test_httpx_redirect_into_private_address_is_blocked():
    seen = []

    def handler(request):
        seen.append(str(request.url))
        return httpx.Response(302, headers={"location": "http://169.254.169.254/latest/"})

    dns = _fake_dns({"pub.test": "93.184.216.34"})
    with patch("api.url_validation.socket.getaddrinfo", side_effect=dns):
        async with make_async_client(transport=httpx.MockTransport(handler)) as c:
            with pytest.raises(BlockedAddressError):
                await c.get("https://pub.test/redirect")
    assert seen == ["https://pub.test/redirect"]  # the internal hop never went out


@pytest.mark.real_egress_guard
async def test_httpx_public_redirect_still_followed():
    def handler(request):
        if request.url.host == "a.test":
            return httpx.Response(302, headers={"location": "https://b.test/"})
        return httpx.Response(200, text="ok")

    dns = _fake_dns({"a.test": "93.184.216.34", "b.test": "93.184.216.35"})
    with patch("api.url_validation.socket.getaddrinfo", side_effect=dns):
        async with make_async_client(transport=httpx.MockTransport(handler)) as c:
            r = await c.get("https://a.test/")
    assert r.status_code == 200


async def test_caller_event_hooks_do_not_replace_guard():
    extra = AsyncMock()
    c = make_async_client(event_hooks={"request": [extra]})
    assert c.event_hooks["request"] == [_egress.guard_httpx_request, extra]
    await c.aclose()


async def _proxy_request(proxy, raw: bytes) -> bytes:
    r, w = await asyncio.open_connection("127.0.0.1", proxy.port)
    w.write(raw)
    await w.drain()
    data = await asyncio.wait_for(r.read(), timeout=5)
    w.close()
    return data


@pytest.mark.parametrize("target", [
    "127.0.0.1:443", "169.254.169.254:443", "100.96.0.96:443", "[::1]:443",
    "[::ffff:127.0.0.1]:443", "93.184.216.34:22",
])
async def test_proxy_refuses_internal_or_odd_port_connect(target):
    proxy = await _egress.start_proxy()
    try:
        data = await _proxy_request(proxy, f"CONNECT {target} HTTP/1.1\r\n\r\n".encode())
    finally:
        await _egress.stop_proxy(proxy)
    assert data == b""  # closed without "200 Connection Established"
    assert proxy.blocked


async def test_proxy_refuses_plain_http_to_internal():
    proxy = await _egress.start_proxy()
    try:
        data = await _proxy_request(
            proxy, b"GET http://127.0.0.1/ HTTP/1.1\r\nHost: 127.0.0.1\r\n\r\n")
    finally:
        await _egress.stop_proxy(proxy)
    assert data == b""
    assert proxy.blocked


async def test_proxy_tunnels_allowed_target(monkeypatch):
    async def upstream(reader, writer):
        writer.write(b"pong:" + await reader.read(4))
        await writer.drain()
        writer.close()

    srv = await asyncio.start_server(upstream, "127.0.0.1", 0)
    port = srv.sockets[0].getsockname()[1]
    monkeypatch.setattr(_egress, "_ALLOWED_PORTS", {port})
    monkeypatch.setattr(_egress, "is_blocked_ip", lambda ip: False)
    proxy = await _egress.start_proxy()
    try:
        r, w = await asyncio.open_connection("127.0.0.1", proxy.port)
        w.write(f"CONNECT 127.0.0.1:{port} HTTP/1.1\r\n\r\n".encode())
        await w.drain()
        assert (await r.readuntil(b"\r\n\r\n")).startswith(b"HTTP/1.1 200")
        w.write(b"ping")
        await w.drain()
        assert await asyncio.wait_for(r.read(), timeout=5) == b"pong:ping"
        w.close()
    finally:
        await _egress.stop_proxy(proxy)
        srv.close()
    assert not proxy.blocked


def test_rewrite_plain_http_forces_close_and_origin_form():
    host, port, out = _egress._rewrite_plain_http(
        b"GET http://pub.test:80/a?b=1 HTTP/1.1\r\nHost: pub.test\r\n"
        b"Proxy-Connection: keep-alive\r\n\r\n")
    assert (host, port) == ("pub.test", 80)
    assert out.startswith(b"GET /a?b=1 HTTP/1.1\r\n")
    assert b"Proxy-Connection" not in out
    assert out.endswith(b"Connection: close\r\n\r\n")


async def test_parse_session_turns_failure_after_block_into_permanent():
    proxy = MagicMock(blocked=["127.0.0.1 resolves to internal address"],
                      server_url="http://127.0.0.1:1")
    ctx = MagicMock(close=AsyncMock(), route=AsyncMock())
    browser = MagicMock(new_context=AsyncMock(return_value=ctx))
    with patch.object(_browser, "get_browser", AsyncMock(return_value=browser)), \
         patch.object(_browser, "start_proxy", AsyncMock(return_value=proxy)), \
         patch.object(_browser, "stop_proxy", AsyncMock()) as stop:
        with pytest.raises(BlockedAddressError):
            async with _browser.parse_session():
                raise RuntimeError("net::ERR_TUNNEL_CONNECTION_FAILED")
    kw = browser.new_context.call_args.kwargs
    assert kw["proxy"]["server"] == "http://127.0.0.1:1"
    assert kw["service_workers"] == "block"
    stop.assert_awaited_once_with(proxy)


@pytest.mark.parametrize("addr", [
    "64:ff9b::7f00:1",                          # NAT64 → 127.0.0.1
    "64:ff9b::a9fe:a9fe",                       # NAT64 → 169.254.169.254
    "2002:7f00:1::",                            # 6to4 → 127.0.0.1
    "2001:0:4136:e378:8000:63bf:80ff:fffe",     # Teredo → 127.0.0.1
    "::ffff:10.0.0.1",                          # IPv4-mapped
    "198.18.0.1",                               # benchmarking, not in the explicit list
    "192.0.2.1",                                # documentation net
])
def test_is_blocked_ip_unwraps_embedded_and_special_ranges(addr):
    import ipaddress
    from api.url_validation import is_blocked_ip
    assert is_blocked_ip(ipaddress.ip_address(addr))


@pytest.mark.parametrize("addr", ["8.8.8.8", "2606:4700::1111", "64:ff9b::808:808"])
def test_is_blocked_ip_allows_public(addr):
    import ipaddress
    from api.url_validation import is_blocked_ip
    assert not is_blocked_ip(ipaddress.ip_address(addr))


def test_resolve_handles_scoped_ipv6():
    from api.errors import AppError
    from api.url_validation import _resolve_and_check_ssrf
    infos = [(socket.AF_INET6, socket.SOCK_STREAM, 6, "", ("fe80::1%eth0", 0, 0, 2))]
    with patch("api.url_validation.socket.getaddrinfo", return_value=infos):
        with pytest.raises(AppError) as ei:
            _resolve_and_check_ssrf("h.test", "http://h.test/")
    assert ei.value.status_code == 403


async def test_httpx_client_routes_through_shared_proxy():
    c = make_async_client()
    try:
        url = _egress.shared_proxy_url()
        assert url.startswith("http://127.0.0.1:")
        assert url == _egress.shared_proxy_url()  # one per process
        assert c._mounts  # proxy mount installed
    finally:
        await c.aclose()
