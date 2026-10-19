"""Egress guard for the worker's outbound URL fetches.

The API validates the submitted URL, but every fetcher follows redirects:
``https://public.example/redirect?to=http://169.254.169.254/`` passes the
up-front check and then lands on an internal address. Two guards close that:

* **httpx** — :func:`guard_httpx_request` is installed as a request event
  hook by ``make_async_client``. httpx fires request hooks for every
  redirect hop, so each hop is re-validated before it is sent.
* **Chromium** — Playwright's ``route`` handler only sees the first URL of
  a redirect chain, so it can't police hops. Instead each browser context
  gets its own :class:`EgressProxy` listener: all browser traffic (every
  redirect hop, subresource and WebSocket) goes through it, the proxy
  resolves the host, rejects internal addresses and connects to the IP it
  checked, which also pins the connection against DNS rebinding. HTTPS
  stays an end-to-end CONNECT tunnel, so Chromium's own TLS stack (and its
  fingerprint, which anti-bot checks look at) is untouched.

The proxy runs on a dedicated thread with its own event loop, so a worker
loop stalled by CPU-heavy parsing can't stall page loads with it.
"""
from __future__ import annotations

import asyncio
import ipaddress
import logging
import socket
import threading
from collections import deque
from urllib.parse import urlsplit

import httpx

from api.errors import AppError, ErrorCode
from api.url_validation import is_blocked_ip, reresolve_and_check_ssrf
from parsers.base import PermanentParseError

logger = logging.getLogger(__name__)

_ALLOWED_PORTS = {80, 443}
_MAX_HEAD_BYTES = 64 * 1024
_CONNECT_TIMEOUT = 15
_PIPE_CHUNK = 64 * 1024


class BlockedAddressError(PermanentParseError):
    """A fetch (usually a redirect hop) targeted an internal address.

    Permanent: the submitter's URL leads somewhere we never fetch, so
    retrying only re-runs the probe and raises dead-task alerts."""
    error_code = ErrorCode.BLOCKED_ADDRESS


async def guard_httpx_request(request: httpx.Request) -> None:
    """httpx request hook: re-validate every outgoing request, including
    each redirect hop the client follows on its own."""
    try:
        await reresolve_and_check_ssrf(str(request.url))
    except AppError as e:
        raise BlockedAddressError(f"blocked fetch of {request.url}: {e.detail}") from e


class _Blocked(Exception):
    pass


async def _resolve_public(host: str, port: int) -> str:
    """Resolve ``host`` and return one address to connect to, refusing if
    the port isn't web-default or *any* resolved address is internal."""
    if port not in _ALLOWED_PORTS:
        raise _Blocked(f"port {port} not allowed")
    try:
        infos = await asyncio.get_running_loop().getaddrinfo(
            host, port, type=socket.SOCK_STREAM)
    except socket.gaierror as e:
        raise _Blocked(f"cannot resolve {host}: {e}") from e
    addrs = [info[4][0] for info in infos]
    if not addrs:
        raise _Blocked(f"cannot resolve {host}")
    for a in addrs:
        if is_blocked_ip(ipaddress.ip_address(a.split("%", 1)[0])):
            raise _Blocked(f"{host} resolves to internal address {a}")
    return addrs[0]


def _split_host_port(authority: str, default_port: int) -> tuple[str, int]:
    parts = urlsplit(f"//{authority}")
    return parts.hostname or "", parts.port or default_port


async def _pipe(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
    try:
        while data := await reader.read(_PIPE_CHUNK):
            writer.write(data)
            await writer.drain()
    except (ConnectionError, asyncio.CancelledError):
        pass
    finally:
        try:
            writer.close()
        except Exception:
            pass


def _rewrite_plain_http(head: bytes) -> tuple[str, int, bytes]:
    """Turn an absolute-form proxy request into an origin-form one and
    force ``Connection: close`` so a reused proxy connection can't carry a
    second request to a different host through the same upstream."""
    lines = head.split(b"\r\n")
    method, target, version = lines[0].decode("latin-1").split(" ", 2)
    parts = urlsplit(target)
    if parts.scheme != "http" or not parts.hostname:
        raise _Blocked(f"unsupported proxy target {target!r}")
    path = parts.path or "/"
    if parts.query:
        path += "?" + parts.query
    headers = [
        ln for ln in lines[1:]
        if ln and not ln.lower().startswith((b"connection:", b"proxy-connection:",
                                             b"proxy-authorization:", b"keep-alive:"))
    ]
    out = [f"{method} {path} {version}".encode("latin-1"), *headers,
           b"Connection: close", b"", b""]
    return parts.hostname, parts.port or 80, b"\r\n".join(out)


class EgressProxy:
    """One listener per browser context, so a block is attributable to the
    parse that hit it (:attr:`blocked`)."""

    def __init__(self) -> None:
        # Bounded: the shared httpx proxy lives for the whole process.
        self.blocked: deque[str] = deque(maxlen=50)
        self._server: asyncio.base_events.Server | None = None
        self.port: int = 0

    @property
    def server_url(self) -> str:
        return f"http://127.0.0.1:{self.port}"

    async def _start(self) -> None:
        self._server = await asyncio.start_server(self._handle, "127.0.0.1", 0)
        self.port = self._server.sockets[0].getsockname()[1]

    async def _stop(self) -> None:
        if self._server is not None:
            self._server.close()
            # Open tunnels are owned by their handler tasks; closing the
            # listener is enough, they end when either side hangs up.

    async def _handle(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        try:
            head = await asyncio.wait_for(
                reader.readuntil(b"\r\n\r\n"), timeout=_CONNECT_TIMEOUT)
            if len(head) > _MAX_HEAD_BYTES:
                raise _Blocked("request head too large")
            method = head.split(b" ", 1)[0].upper()
            if method == b"CONNECT":
                target = head.split(b" ", 2)[1].decode("latin-1")
                host, port = _split_host_port(target, 443)
                ip = await _resolve_public(host, port)
                up_r, up_w = await asyncio.wait_for(
                    asyncio.open_connection(ip, port), timeout=_CONNECT_TIMEOUT)
                writer.write(b"HTTP/1.1 200 Connection Established\r\n\r\n")
                await writer.drain()
            else:
                host, port, rewritten = _rewrite_plain_http(head)
                ip = await _resolve_public(host, port)
                up_r, up_w = await asyncio.wait_for(
                    asyncio.open_connection(ip, port), timeout=_CONNECT_TIMEOUT)
                up_w.write(rewritten)
                await up_w.drain()
        except _Blocked as e:
            self.blocked.append(str(e))
            logger.warning("egress proxy blocked: %s", e)
            writer.close()
            return
        except Exception:
            # Upstream unreachable, client hung up mid-head, etc. Closing
            # surfaces as a normal network error in the browser.
            writer.close()
            return
        await asyncio.gather(_pipe(reader, up_w), _pipe(up_r, writer))


_loop: asyncio.AbstractEventLoop | None = None
_loop_lock = threading.RLock()  # re-entered by shared_proxy_url via _proxy_loop


def _proxy_loop() -> asyncio.AbstractEventLoop:
    global _loop  # noqa: PLW0603
    with _loop_lock:
        if _loop is None:
            loop = asyncio.new_event_loop()
            threading.Thread(target=loop.run_forever, name="egress-proxy",
                             daemon=True).start()
            _loop = loop
        return _loop


async def start_proxy() -> EgressProxy:
    proxy = EgressProxy()
    fut = asyncio.run_coroutine_threadsafe(proxy._start(), _proxy_loop())
    await asyncio.wrap_future(fut)
    return proxy


async def stop_proxy(proxy: EgressProxy) -> None:
    fut = asyncio.run_coroutine_threadsafe(proxy._stop(), _proxy_loop())
    await asyncio.wrap_future(fut)


_shared: EgressProxy | None = None


def shared_proxy_url() -> str:
    """Process-wide proxy for httpx clients. The request hook already
    attributes and reports blocks; routing through the proxy additionally
    pins each connection to the address that was checked, closing the
    re-resolve-then-connect DNS-rebinding window. Sync because
    ``make_async_client`` is; binding a listener takes milliseconds."""
    global _shared  # noqa: PLW0603
    with _loop_lock:
        proxy = _shared
    if proxy is None:
        proxy = EgressProxy()
        asyncio.run_coroutine_threadsafe(proxy._start(), _proxy_loop()).result(timeout=5)
        with _loop_lock:
            if _shared is None:
                _shared = proxy
            else:  # lost a race; keep the first one
                asyncio.run_coroutine_threadsafe(proxy._stop(), _proxy_loop())
                proxy = _shared
    return proxy.server_url
