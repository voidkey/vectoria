import pytest
from parsers.image_metadata import extract_image_metadata, ImageMeta


def test_extracts_basic_metadata():
    md = "# Intro\n\nSome text before.\n\n![arch diagram](img-001.png)\n\nSome text after.\n"
    images = {"img-001.png": b"\x89PNG" + b"\x00" * 100}
    result = extract_image_metadata(md, images)
    assert len(result) == 1
    meta = result[0]
    assert meta.filename == "img-001.png"
    assert meta.index == 0
    assert meta.alt == "arch diagram"
    assert "Some text before" in meta.context
    assert "Some text after" in meta.context


def test_extracts_section_title():
    md = "# Chapter 1\n\n## Architecture\n\nText.\n\n![](img.png)\n\nMore text.\n"
    images = {"img.png": b"\x89PNG" + b"\x00" * 100}
    result = extract_image_metadata(md, images)
    assert result[0].section_title == "Architecture"


def test_no_heading_returns_empty_section_title():
    md = "Text before.\n\n![](img.png)\n\nText after.\n"
    images = {"img.png": b"\x89PNG" + b"\x00" * 100}
    result = extract_image_metadata(md, images)
    assert result[0].section_title == ""


def test_multiple_images_ordered():
    md = "![](a.png)\n\nMiddle.\n\n![](b.png)\n"
    images = {"a.png": b"\x89PNG" + b"\x00" * 100, "b.png": b"\x89PNG" + b"\x00" * 100}
    result = extract_image_metadata(md, images)
    assert len(result) == 2
    assert result[0].filename == "a.png"
    assert result[0].index == 0
    assert result[1].filename == "b.png"
    assert result[1].index == 1


def test_filters_small_images():
    """Images with width or height < 100px should be excluded."""
    import struct, zlib
    signature = b"\x89PNG\r\n\x1a\n"
    ihdr_data = struct.pack(">IIBBBBB", 50, 50, 8, 2, 0, 0, 0)
    ihdr_crc = zlib.crc32(b"IHDR" + ihdr_data) & 0xFFFFFFFF
    ihdr_chunk = struct.pack(">I", 13) + b"IHDR" + ihdr_data + struct.pack(">I", ihdr_crc)
    small_png = signature + ihdr_chunk

    md = "![](small.png)\n"
    images = {"small.png": small_png}
    result = extract_image_metadata(md, images)
    assert len(result) == 0


def test_dimensions_from_valid_image():
    """Should extract width/height from image bytes."""
    import struct, zlib
    signature = b"\x89PNG\r\n\x1a\n"
    ihdr_data = struct.pack(">IIBBBBB", 800, 600, 8, 2, 0, 0, 0)
    ihdr_crc = zlib.crc32(b"IHDR" + ihdr_data) & 0xFFFFFFFF
    ihdr_chunk = struct.pack(">I", 13) + b"IHDR" + ihdr_data + struct.pack(">I", ihdr_crc)
    png_bytes = signature + ihdr_chunk

    md = "![](photo.png)\n"
    images = {"photo.png": png_bytes}
    result = extract_image_metadata(md, images)
    assert len(result) == 1
    assert result[0].width == 800
    assert result[0].height == 600
