"""phash computation + storage on image upload (W3-f).

Guards that:
  * ``_compute_phash`` returns a 16-hex-char string for a valid image,
    and None for garbage bytes (no exceptions escape).
  * ``stream_upload_and_store_refs`` stashes the phash on the
    DocumentImage row so future dedup queries can key off it.
  * A compute failure on one image doesn't break the batch — the
    DocumentImage is still stored with ``phash=None``.
"""
import io
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from PIL import Image

from api.image_stream import _compute_phash, stream_upload_and_store_refs
from parsers.image_ref import ImageRef


def _make_png(width: int = 64, height: int = 64, color=(200, 50, 100)) -> bytes:
    buf = io.BytesIO()
    Image.new("RGB", (width, height), color).save(buf, format="PNG")
    return buf.getvalue()


def _make_ref(payload: bytes, name: str = "img.png") -> ImageRef:
    return ImageRef(
        name=name, mime="image/png",
        width=64, height=64,
        _factory=lambda d=payload: d,
    )


# ---------------------------------------------------------------------------
# _compute_phash
# ---------------------------------------------------------------------------

def test_compute_phash_returns_16_hex_chars_for_valid_image():
    h = _compute_phash(_make_png())
    assert h is not None
    assert len(h) == 16
    int(h, 16)  # must be valid hex


def test_compute_phash_similar_images_produce_close_hashes():
    """Perceptual hash: near-duplicates should be close in Hamming
    distance. If this breaks, the phash implementation regressed (or
    ``imagehash`` was swapped for something non-perceptual).
    """
    a = _compute_phash(_make_png(color=(100, 100, 100)))
    b = _compute_phash(_make_png(color=(101, 101, 101)))  # 1 pixel diff
    assert a is not None and b is not None
    # Hamming distance: count differing hex-decoded bits.
    diff = bin(int(a, 16) ^ int(b, 16)).count("1")
    assert diff <= 5, f"close images should have hamming distance ≤5; got {diff}"


def _noise_png(seed: int) -> bytes:
    """Draw a 256×256 image full of deterministic noise so pHash sees
    enough high-frequency DCT coefficients to produce a non-trivial
    hash. Tiny solid-color or low-detail images collapse to the same
    ``8000000000000000`` hash regardless of tint.
    """
    import random
    from PIL import ImageDraw
    rng = random.Random(seed)
    img = Image.new("RGB", (256, 256), (255, 255, 255))
    draw = ImageDraw.Draw(img)
    for _ in range(200):
        x, y = rng.randint(0, 255), rng.randint(0, 255)
        draw.rectangle([x, y, x + 8, y + 8], fill=(0, 0, 0))
    buf = io.BytesIO()
    img.save(buf, format="PNG")
    return buf.getvalue()


def test_compute_phash_different_images_produce_distant_hashes():
    """Structurally-different random-noise patterns must produce
    different perceptual hashes.
    """
    a = _compute_phash(_noise_png(seed=42))
    b = _compute_phash(_noise_png(seed=99))
    assert a is not None and b is not None
    assert a != b, f"different noise patterns should differ; got {a} == {b}"


def test_compute_phash_returns_none_for_garbage_bytes():
    """Must not raise — returns None so the upload pipeline still stores
    the row (with phash=None) and moves on.
    """
    assert _compute_phash(b"not an image at all") is None
    assert _compute_phash(b"") is None


# ---------------------------------------------------------------------------
# DocumentImage row carries phash
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_upload_stores_phash_on_document_image():
    png = _make_png()
    ref = _make_ref(png)

    captured_rows = []

    class _FakeSession:
        async def __aenter__(self):
            return self
        async def __aexit__(self, *a):
            return False
        def add(self, obj):
            captured_rows.append(obj)
        async def commit(self):
            pass

    mock_storage = AsyncMock()

    with (
        patch("api.image_stream.get_storage", return_value=mock_storage),
        patch("db.base.get_session", return_value=_FakeSession()),
    ):
        await stream_upload_and_store_refs(
            [ref], kb_id="kb", doc_id="doc", vision_configured=False,
        )

    assert len(captured_rows) == 1
    row = captured_rows[0]
    assert row.phash is not None, (
        "phash should be populated for valid images; dedup queries depend on it"
    )
    assert len(row.phash) == 16


@pytest.mark.asyncio
async def test_upload_stores_null_phash_for_non_image_bytes():
    """Malformed/non-image bytes mustn't break the batch — the row is
    still stored with ``phash=None`` so downstream vision/indexing
    still happens and nothing is silently dropped.
    """
    garbage_ref = _make_ref(b"absolutely not an image", name="garbage.jpg")

    captured = []

    class _FakeSession:
        async def __aenter__(self): return self
        async def __aexit__(self, *a): return False
        def add(self, obj): captured.append(obj)
        async def commit(self): pass

    mock_storage = AsyncMock()

    with (
        patch("api.image_stream.get_storage", return_value=mock_storage),
        patch("db.base.get_session", return_value=_FakeSession()),
    ):
        await stream_upload_and_store_refs(
            [garbage_ref], kb_id="kb", doc_id="doc", vision_configured=False,
        )

    assert len(captured) == 1
    assert captured[0].phash is None
