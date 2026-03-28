import pytest
from parsers.outline import extract_outline


def test_extracts_headings_with_levels():
    md = "# Title\n\nSome text\n\n## Section A\n\nMore text\n\n### Sub A1\n\n## Section B\n"
    result = extract_outline(md)
    assert result == [
        {"level": 1, "title": "Title"},
        {"level": 2, "title": "Section A"},
        {"level": 3, "title": "Sub A1"},
        {"level": 2, "title": "Section B"},
    ]


def test_empty_markdown_returns_empty():
    assert extract_outline("") == []


def test_no_headings_returns_empty():
    assert extract_outline("Just plain text.\n\nAnother paragraph.") == []


def test_heading_with_inline_formatting():
    md = "## **Bold** and *italic* heading\n"
    result = extract_outline(md)
    assert result == [{"level": 2, "title": "**Bold** and *italic* heading"}]


def test_ignores_code_block_headings():
    md = "# Real Heading\n\n```\n# Not a heading\n```\n\n## Another Real\n"
    result = extract_outline(md)
    assert result == [
        {"level": 1, "title": "Real Heading"},
        {"level": 2, "title": "Another Real"},
    ]
