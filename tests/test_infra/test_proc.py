"""Tests for the /proc/self/status VmRSS reader.

We don't touch real /proc — we monkeypatch the Path the helper reads from
so the same test runs identically on macOS developer laptops and Linux CI.
"""
from pathlib import Path

from infra import proc


def _fake_status(tmp_path: Path, body: str) -> Path:
    p = tmp_path / "status"
    p.write_text(body)
    return p


def test_rss_bytes_parses_kb_and_converts_to_bytes(monkeypatch, tmp_path):
    path = _fake_status(
        tmp_path,
        "Name:\tpython\n"
        "VmPeak:\t  200000 kB\n"
        "VmSize:\t  150000 kB\n"
        "VmRSS:\t   12345 kB\n"
        "VmSwap:\t       0 kB\n",
    )
    monkeypatch.setattr(proc, "_STATUS_PATH", path)
    assert proc.rss_bytes() == 12345 * 1024


def test_rss_bytes_returns_zero_when_file_missing(monkeypatch, tmp_path):
    monkeypatch.setattr(proc, "_STATUS_PATH", tmp_path / "does-not-exist")
    assert proc.rss_bytes() == 0


def test_rss_bytes_returns_zero_when_no_vmrss_line(monkeypatch, tmp_path):
    # Valid procfs format but VmRSS absent — treat as unavailable.
    path = _fake_status(tmp_path, "Name:\tpython\nVmSize:\t 1024 kB\n")
    monkeypatch.setattr(proc, "_STATUS_PATH", path)
    assert proc.rss_bytes() == 0


def test_rss_bytes_returns_zero_on_malformed_line(monkeypatch, tmp_path):
    # Someone broke kernel format (extremely unlikely). We should fail
    # closed, not raise out of a hot worker loop.
    path = _fake_status(tmp_path, "VmRSS: not-a-number\n")
    monkeypatch.setattr(proc, "_STATUS_PATH", path)
    assert proc.rss_bytes() == 0
