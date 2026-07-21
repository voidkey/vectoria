"""Unit tests for the pure-Pillow contact-sheet builder (no browser)."""
import io

from PIL import Image

from parsers.capture._contact_sheet import build_contact_sheet


def _png(w=40, h=30, color=(200, 100, 50)) -> bytes:
    buf = io.BytesIO()
    Image.new("RGB", (w, h), color).save(buf, format="PNG")
    return buf.getvalue()


def _jpeg_dims(data: bytes) -> tuple[int, int]:
    return Image.open(io.BytesIO(data)).size


def test_build_contact_sheet_empty_returns_empty():
    assert build_contact_sheet([], cols=3, per_page=9, thumb_w=600) == []


def test_build_contact_sheet_two_pngs_single_page():
    items = [(_png(), "one"), (_png(color=(10, 20, 30)), "two")]
    pages = build_contact_sheet(items, cols=3, per_page=9, thumb_w=600)
    assert len(pages) == 1
    # Decodable JPEG; width covers 2 cells laid across a 3-col grid.
    w, h = _jpeg_dims(pages[0])
    im = Image.open(io.BytesIO(pages[0]))
    assert im.format == "JPEG"
    # thumb_w=600, 3 cols -> total_w = 3*600 + 4*4 = 1816
    assert w == 3 * 600 + 4 * 4


def test_build_contact_sheet_paginates_at_per_page():
    items = [(_png(), f"n{i}") for i in range(10)]
    pages = build_contact_sheet(items, cols=3, per_page=9, thumb_w=600)
    assert len(pages) == 2   # 9 + 1


def test_build_contact_sheet_asset_grid_four_cols():
    items = [(_png(), f"asset-{i}") for i in range(13)]
    pages = build_contact_sheet(items, cols=4, per_page=12, thumb_w=480)
    assert len(pages) == 2   # 12 + 1
    w, _ = _jpeg_dims(pages[0])
    assert w == 4 * 480 + 5 * 4


def test_build_contact_sheet_skips_page_when_all_undecodable():
    # A page whose images all fail to decode yields no page (logged), not a crash.
    pages = build_contact_sheet([(b"not-an-image", "bad")], cols=3, per_page=9, thumb_w=600)
    assert pages == []


def test_build_contact_sheet_mixed_decodable_and_not():
    items = [(b"garbage", "bad"), (_png(), "good")]
    pages = build_contact_sheet(items, cols=3, per_page=9, thumb_w=600)
    # The good image still renders; the bad one is dropped from the page.
    assert len(pages) == 1
    assert Image.open(io.BytesIO(pages[0])).format == "JPEG"
