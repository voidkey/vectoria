from config import Settings


def test_enable_indexing_defaults_true():
    assert Settings().enable_indexing is True


def test_enable_indexing_reads_env(monkeypatch):
    monkeypatch.setenv("ENABLE_INDEXING", "false")
    assert Settings().enable_indexing is False
