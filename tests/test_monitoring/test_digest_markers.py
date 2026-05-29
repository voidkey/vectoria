def test_permanent_has_marker():
    from monitoring.digest import _TYPE_MARKER
    assert "permanent" in _TYPE_MARKER
