import json

from parsers.capture.catalog import match_font, _normalize, _load_catalog


def test_normalize():
    assert _normalize("  Inter Display Bold ") == "inter"
    assert _normalize('"Space Grotesk", sans-serif') == "space grotesk"


def test_no_catalog_is_miss(monkeypatch):
    monkeypatch.setattr("parsers.capture.catalog._settings_path", lambda: "")
    _load_catalog.cache_clear()
    assert match_font("Inter").matched is False


def test_catalog_hit(tmp_path, monkeypatch):
    cat = tmp_path / "fonts.json"
    cat.write_text(json.dumps([
        {"family": "Space Grotesk", "slug": "space-grotesk",
         "css_url": "https://cdn.example/fonts/space-grotesk/index.css",
         "weights": [400, 700]},
    ]))
    monkeypatch.setattr("parsers.capture.catalog._settings_path", lambda: str(cat))
    _load_catalog.cache_clear()
    m = match_font('"Space Grotesk", system-ui')
    assert m.matched is True
    assert m.slug == "space-grotesk"
    assert m.css_url.endswith("index.css")
    assert m.source == "catalog"
    assert match_font("Helvetica").matched is False
    _load_catalog.cache_clear()
