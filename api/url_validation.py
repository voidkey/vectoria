import asyncio
import ipaddress
import socket
import logging
from urllib.parse import urlparse

from api.errors import AppError, ErrorCode

logger = logging.getLogger(__name__)

_MAX_URL_LENGTH = 2048

_ALLOWED_SCHEMES = {"http", "https"}

# Destination ports the ingest URL is allowed to reach. Anything else
# lets an attacker probe internal services that happen to listen on a
# public IP — e.g. ``http://some-host:22`` for an SSH handshake — and
# nothing legitimate on the web uses non-default HTTP/HTTPS ports.
# ``None`` covers the default-port case (no explicit port in the URL).
_ALLOWED_PORTS = {80, 443, None}

# File extensions that won't yield useful text content
_BLOCKED_EXTENSIONS = {
    # Video
    ".mp4", ".avi", ".mov", ".wmv", ".flv", ".mkv", ".webm", ".m4v",
    # Audio
    ".mp3", ".wav", ".flac", ".aac", ".ogg", ".wma", ".m4a",
    # Archives
    ".zip", ".rar", ".7z", ".tar", ".gz", ".bz2", ".xz",
    # Executables / installers
    ".exe", ".msi", ".dmg", ".pkg", ".deb", ".rpm", ".apk", ".ipa",
    # Disk images
    ".iso", ".img",
    # Fonts
    ".ttf", ".otf", ".woff", ".woff2",
}

# IP ranges we must never dispatch a request to. The DNS resolution
# step normalises any IPv4-encoding tricks (hex 0x7f.0.0.1, decimal
# 2130706433, etc.) into a canonical ``ipaddress.IPv4Address`` before
# the membership test, so those attack surfaces are also covered.
#
# DNS rebinding is mitigated by ``reresolve_and_check_ssrf``: the URL
# parser re-runs this same check at fetch time (worker side), so an
# attacker flipping DNS between validate (API) and fetch (worker) is
# caught before the HTTP call leaves the box. The sub-second race
# between the worker's re-resolve and the fetch's own resolve is
# bounded by single-digit ms and considered acceptable residual risk.
_SSRF_BLOCKED_NETWORKS = (
    ipaddress.ip_network("0.0.0.0/8"),       # "this" network / 0.0.0.0 trick
    ipaddress.ip_network("10.0.0.0/8"),      # RFC1918 private
    ipaddress.ip_network("100.64.0.0/10"),   # CGNAT (covers Alibaba metadata 100.100.100.200)
    ipaddress.ip_network("127.0.0.0/8"),     # loopback
    ipaddress.ip_network("169.254.0.0/16"),  # link-local (covers AWS/Azure/GCP metadata 169.254.169.254)
    ipaddress.ip_network("172.16.0.0/12"),   # RFC1918 private
    ipaddress.ip_network("192.168.0.0/16"),  # RFC1918 private
    ipaddress.ip_network("224.0.0.0/4"),     # IPv4 multicast
    ipaddress.ip_network("240.0.0.0/4"),     # reserved (covers 255.255.255.255 broadcast)
    ipaddress.ip_network("::/128"),          # unspecified IPv6
    ipaddress.ip_network("::1/128"),         # loopback IPv6
    ipaddress.ip_network("fc00::/7"),        # unique local IPv6
    ipaddress.ip_network("fe80::/10"),       # link-local IPv6
    ipaddress.ip_network("ff00::/8"),        # IPv6 multicast
)


def _check_url_format(url: str) -> str:
    """Validate URL format. Returns hostname if valid, raises AppError otherwise."""
    if not url or len(url) > _MAX_URL_LENGTH:
        raise AppError(422, ErrorCode.INVALID_URL,
                        f"URL must be between 1 and {_MAX_URL_LENGTH} characters")

    try:
        parsed = urlparse(url)
    except Exception:
        raise AppError(422, ErrorCode.INVALID_URL, "Invalid URL format")

    if parsed.scheme not in _ALLOWED_SCHEMES:
        raise AppError(422, ErrorCode.INVALID_URL,
                        f"Only HTTP/HTTPS URLs are supported, got '{parsed.scheme}'")

    # Embedded credentials (``http://user:pass@host/``) leak in logs and
    # redirects and offer no legitimate use case for content ingestion.
    # ``urlparse`` reports them via .username/.password — presence of
    # either means the URL string carried a credential section.
    if parsed.username or parsed.password:
        raise AppError(422, ErrorCode.INVALID_URL,
                        "URLs with embedded credentials are not allowed")

    # Explicit non-default port → must be 80 or 443. Blocks
    # ``http://target:22`` (SSH), ``:3306`` (MySQL), ``:6379`` (Redis),
    # etc. — ports that have no business being crawled as web content.
    try:
        port = parsed.port
    except ValueError:
        # urllib raises on malformed ports like ``:99999``
        raise AppError(422, ErrorCode.INVALID_URL, "Invalid port in URL")
    if port not in _ALLOWED_PORTS:
        raise AppError(422, ErrorCode.INVALID_URL,
                        f"Port {port} is not allowed; only 80 and 443 are permitted")

    hostname = parsed.hostname
    if not hostname:
        raise AppError(422, ErrorCode.INVALID_URL, "URL must contain a valid hostname")

    path_lower = parsed.path.lower()
    for ext in _BLOCKED_EXTENSIONS:
        if path_lower.endswith(ext):
            raise AppError(422, ErrorCode.UNSUPPORTED_FILE_TYPE,
                            f"URL points to a file type ({ext}) that cannot be parsed as text content")

    return hostname


def _resolve_and_check_ssrf(hostname: str, url: str) -> None:
    """Blocking DNS resolve + private IP check. Run in executor."""
    try:
        addr_infos = socket.getaddrinfo(hostname, None, socket.AF_UNSPEC, socket.SOCK_STREAM)
    except socket.gaierror:
        raise AppError(422, ErrorCode.DNS_RESOLVE_FAILED,
                        f"Cannot resolve hostname: {hostname}")

    for _, _, _, _, sockaddr in addr_infos:
        ip = ipaddress.ip_address(sockaddr[0])
        if any(ip in net for net in _SSRF_BLOCKED_NETWORKS):
            logger.warning("SSRF blocked: %s resolves to private IP %s", url, ip)
            raise AppError(403, ErrorCode.BLOCKED_ADDRESS,
                            "URL points to a private/internal network address")


async def validate_url(url: str) -> None:
    """Validate a URL for ingestion. Raises AppError on failure."""
    hostname = _check_url_format(url)

    await asyncio.get_running_loop().run_in_executor(
        None, _resolve_and_check_ssrf, hostname, url,
    )


async def reresolve_and_check_ssrf(url: str) -> None:
    """Worker-side DNS-rebinding guard.

    The API validates at enqueue time; the worker can run this much
    later (queue backlog + parse time). An attacker controlling DNS
    with a low TTL can flip the record to a private IP between those
    two moments. This function is called *immediately before* the
    worker's actual HTTP fetch so the window for rebinding shrinks
    from seconds-to-minutes down to single-digit milliseconds (the
    gap between this re-resolve and the fetch's own resolve inside
    httpx / playwright).

    URL format is re-checked too, because a handler could have
    followed a redirect the API never saw.

    Raises ``AppError(403, BLOCKED_ADDRESS)`` if the URL now resolves
    to a private / link-local / metadata network, or
    ``AppError(422, DNS_RESOLVE_FAILED)`` if DNS can't resolve it at
    all (compared to the API's earlier successful resolve).
    """
    hostname = _check_url_format(url)
    await asyncio.get_running_loop().run_in_executor(
        None, _resolve_and_check_ssrf, hostname, url,
    )
