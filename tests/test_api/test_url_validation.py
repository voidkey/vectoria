"""SSRF hardening for the ingest_url code path.

Each test names the attack it defends against. If a guard regresses, the
test name tells ops exactly what opened up — "SSRF now allows AWS
metadata" is a louder failure than "url_validation test 7 broke".

The resolver is mocked in the IP-class tests so we never actually hit
DNS: the tests are about what the classifier decides, not about whether
your resolver is up.
"""
from unittest.mock import patch

import pytest

from api.errors import AppError
from api.url_validation import validate_url


def _mock_resolve(ip_str: str):
    """Return a side_effect that makes ``socket.getaddrinfo`` resolve to
    a single given IP, so the IP-class checks can be tested in isolation
    from real DNS.
    """
    family = 2 if ":" not in ip_str else 10  # AF_INET / AF_INET6
    return [(family, 1, 6, "", (ip_str, 0))]


# ---------------------------------------------------------------------------
# Scheme / format
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
@pytest.mark.parametrize("bad_url", [
    "file:///etc/passwd",
    "data:text/html,<h1>xss</h1>",
    "javascript:alert(1)",
    "gopher://attacker.com/x",
    "ftp://example.com/file",
])
async def test_rejects_non_http_schemes(bad_url):
    with pytest.raises(AppError) as e:
        await validate_url(bad_url)
    assert e.value.status_code == 422


@pytest.mark.asyncio
async def test_rejects_empty_and_oversized_url():
    with pytest.raises(AppError):
        await validate_url("")
    with pytest.raises(AppError):
        await validate_url("http://example.com/" + "a" * 3000)


@pytest.mark.asyncio
async def test_rejects_url_with_no_hostname():
    with pytest.raises(AppError):
        await validate_url("http:///path")


# ---------------------------------------------------------------------------
# Credentials in URL (info-leak + SSRF adjacent)
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
@pytest.mark.parametrize("bad_url", [
    "http://user:pass@example.com/",
    "https://admin:secret@example.com/page",
    "http://user@example.com/",  # username only
])
async def test_rejects_urls_with_embedded_credentials(bad_url):
    with pytest.raises(AppError) as e:
        await validate_url(bad_url)
    assert "credentials" in e.value.detail.lower()


# ---------------------------------------------------------------------------
# Port whitelist — nothing but 80/443/default
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
@pytest.mark.parametrize("bad_url", [
    "http://example.com:22/",      # SSH
    "http://example.com:3306/",    # MySQL
    "http://example.com:6379/",    # Redis
    "http://example.com:8080/",    # common dev port
    "https://example.com:9200/",   # Elasticsearch
])
async def test_rejects_non_standard_ports(bad_url):
    with pytest.raises(AppError) as e:
        await validate_url(bad_url)
    assert "port" in e.value.detail.lower()


@pytest.mark.asyncio
@pytest.mark.parametrize("good_url", [
    "http://example.com/",
    "http://example.com:80/",
    "https://example.com:443/",
])
async def test_allows_standard_ports(good_url):
    with patch("api.url_validation.socket.getaddrinfo",
               return_value=_mock_resolve("93.184.216.34")):
        await validate_url(good_url)  # public IP (example.com), no raise


# ---------------------------------------------------------------------------
# Private IPs via DNS resolution
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
@pytest.mark.parametrize("private_ip, label", [
    ("127.0.0.1",       "loopback"),
    ("10.0.0.5",        "RFC1918 /8"),
    ("172.16.0.1",      "RFC1918 /12"),
    ("192.168.1.100",   "RFC1918 /16"),
    ("169.254.169.254", "AWS/Azure/GCP metadata"),
    ("100.100.100.200", "Alibaba metadata (CGNAT range)"),
    ("0.0.0.0",         "this network"),
    ("224.0.0.1",       "IPv4 multicast"),
    ("255.255.255.255", "broadcast (covered by 240/4)"),
    ("::1",             "IPv6 loopback"),
    ("fe80::1",         "IPv6 link-local"),
    ("fc00::1",         "IPv6 ULA"),
    ("ff02::1",         "IPv6 multicast"),
])
async def test_blocks_private_and_reserved_ips(private_ip, label):
    with patch("api.url_validation.socket.getaddrinfo",
               return_value=_mock_resolve(private_ip)):
        with pytest.raises(AppError) as e:
            await validate_url("http://attacker-controlled.example.com/")
    assert e.value.status_code == 403, f"{label} ({private_ip}) must return 403"


@pytest.mark.asyncio
async def test_blocks_direct_ip_literal_to_private():
    """``http://127.0.0.1/`` — the IP is parsed by ``getaddrinfo`` and
    returned as the literal itself, hitting the same classifier.
    """
    with patch("api.url_validation.socket.getaddrinfo",
               return_value=_mock_resolve("127.0.0.1")):
        with pytest.raises(AppError):
            await validate_url("http://127.0.0.1/")


@pytest.mark.asyncio
async def test_blocks_ipv4_encoding_tricks():
    """``http://0x7f000001/`` (hex) or ``http://2130706433/`` (decimal)
    both resolve to 127.0.0.1 via libc's getaddrinfo, so the IP check
    normalises them before the range membership test.
    """
    # Simulate what getaddrinfo would return for both encodings.
    with patch("api.url_validation.socket.getaddrinfo",
               return_value=_mock_resolve("127.0.0.1")):
        for trick in ("http://0x7f000001/", "http://2130706433/"):
            with pytest.raises(AppError) as e:
                await validate_url(trick)
            assert e.value.status_code == 403


# ---------------------------------------------------------------------------
# DNS failure
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_dns_failure_surfaces_as_422():
    import socket
    with patch("api.url_validation.socket.getaddrinfo",
               side_effect=socket.gaierror("nope")):
        with pytest.raises(AppError) as e:
            await validate_url("http://definitely-does-not-resolve.invalid/")
        assert e.value.status_code == 422


# ---------------------------------------------------------------------------
# File-extension blocks (existing behaviour — guard regression)
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
@pytest.mark.parametrize("url", [
    "http://example.com/movie.mp4",
    "https://example.com/archive.zip",
    "http://example.com/installer.exe",
])
async def test_blocks_binary_extensions(url):
    with pytest.raises(AppError) as e:
        await validate_url(url)
    # These check before DNS, so no mock needed.
    assert e.value.status_code == 422


# ---------------------------------------------------------------------------
# Happy path — legitimate public URL
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_happy_path_public_https_url():
    with patch("api.url_validation.socket.getaddrinfo",
               return_value=_mock_resolve("93.184.216.34")):
        await validate_url("https://example.com/some-article")  # no raise
