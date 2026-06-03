import pytest

from vision.language import resolve_language
from config import get_settings


@pytest.mark.parametrize("configured,expected", [
    ("en", "English"),
    ("pt", "Portuguese"),
    ("pt-BR", "Portuguese"),
    ("es-MX", "Spanish"),
    ("fr", "French"),
    ("zh", "Chinese"),
    ("ZH", "Chinese"),
])
def test_configured_locale_maps_to_language_name(monkeypatch, configured, expected):
    monkeypatch.setattr(get_settings(), "vision_default_language", configured)
    assert resolve_language() == expected


def test_unknown_but_valid_locale_passes_through(monkeypatch):
    monkeypatch.setattr(get_settings(), "vision_default_language", "nl-NL")
    assert resolve_language() == "nl-NL"


def test_misconfigured_default_does_not_leak_to_prompt(monkeypatch):
    # A non-locale operator value must never reach the prompt verbatim.
    monkeypatch.setattr(get_settings(), "vision_default_language", "garbage value")
    assert resolve_language() == "English"


def test_trailing_newline_in_config_is_rejected(monkeypatch):
    # \Z (not $) anchors the locale regex so a stray newline can't slip
    # an injected second line into the prompt.
    monkeypatch.setattr(get_settings(), "vision_default_language", "en\ninjection")
    assert resolve_language() == "English"
