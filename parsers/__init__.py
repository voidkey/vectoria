"""Parsers package.

Also hosts the default BaseImageExtractor registrations so any code
that imports from ``parsers`` automatically benefits from plugin
extractors. Plugin modules stay lightweight (they only import heavy
dependencies inside ``extract()``), so this doesn't inflate
worker-startup RSS — the whole point of the lazy-import refactor
(W4-a) is preserved.
"""
from parsers.image_extractor import register_image_extractor
from parsers._pptx_images import PptxImageExtractor


def _register_default_image_extractors() -> None:
    """Register the bundled BaseImageExtractor plugins.

    Exposed as a function so tests that clear the registry can
    restore the defaults in teardown — otherwise one test file's
    ``_clear_for_tests`` leaks "no plugins registered" state into
    subsequent test files.
    """
    register_image_extractor(PptxImageExtractor())


# Register on import. Order = priority (reverse-chronological lookup):
# register specific-first so runtime overrides naturally win.
_register_default_image_extractors()
