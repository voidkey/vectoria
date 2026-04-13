import ipaddress
import socket
import logging
from urllib.parse import urlparse

logger = logging.getLogger(__name__)

_MAX_URL_LENGTH = 2048

_ALLOWED_SCHEMES = {"http", "https"}

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

# RFC 1918 + loopback + link-local networks
_PRIVATE_NETWORKS = [
    ipaddress.ip_network("127.0.0.0/8"),
    ipaddress.ip_network("10.0.0.0/8"),
    ipaddress.ip_network("172.16.0.0/12"),
    ipaddress.ip_network("192.168.0.0/16"),
    ipaddress.ip_network("169.254.0.0/16"),
    ipaddress.ip_network("::1/128"),
    ipaddress.ip_network("fc00::/7"),
    ipaddress.ip_network("fe80::/10"),
]


def validate_url(url: str) -> str | None:
    """Validate a URL for ingestion. Returns an error message, or None if valid."""

    if not url or len(url) > _MAX_URL_LENGTH:
        return f"URL must be between 1 and {_MAX_URL_LENGTH} characters"

    try:
        parsed = urlparse(url)
    except Exception:
        return "Invalid URL format"

    # Scheme check
    if parsed.scheme not in _ALLOWED_SCHEMES:
        return f"Only HTTP/HTTPS URLs are supported, got '{parsed.scheme}'"

    # Hostname must exist
    hostname = parsed.hostname
    if not hostname:
        return "URL must contain a valid hostname"

    # Block file extension
    path_lower = parsed.path.lower()
    for ext in _BLOCKED_EXTENSIONS:
        if path_lower.endswith(ext):
            return f"URL points to a file type ({ext}) that cannot be parsed as text content"

    # SSRF protection: resolve hostname and check against private networks
    try:
        addr_infos = socket.getaddrinfo(hostname, None, socket.AF_UNSPEC, socket.SOCK_STREAM)
    except socket.gaierror:
        return f"Cannot resolve hostname: {hostname}"

    for family, _, _, _, sockaddr in addr_infos:
        ip = ipaddress.ip_address(sockaddr[0])
        for network in _PRIVATE_NETWORKS:
            if ip in network:
                logger.warning("SSRF blocked: %s resolves to private IP %s", url, ip)
                return "URL points to a private/internal network address"

    return None
