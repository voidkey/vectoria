import pytest
from splitter.splitter import Splitter, Chunk


def test_fixed_split_basic():
    text = "word " * 200  # 1000 chars
    splitter = Splitter(chunk_size=200, chunk_overlap=20)
    chunks = splitter.split(text)
    assert len(chunks) > 1
    assert all(isinstance(c, Chunk) for c in chunks)


def test_chunk_has_index():
    splitter = Splitter(chunk_size=100, chunk_overlap=10)
    chunks = splitter.split("hello world " * 50)
    for i, c in enumerate(chunks):
        assert c.index == i


def test_parent_child_split():
    text = "# Section 1\n\n" + "word " * 100 + "\n\n# Section 2\n\n" + "word " * 100
    splitter = Splitter(chunk_size=200, chunk_overlap=20, parent_chunk_size=800)
    chunks = splitter.split(text)
    child_chunks = [c for c in chunks if c.parent_id is not None]
    parent_chunks = [c for c in chunks if c.parent_id is None]
    assert len(parent_chunks) > 0
    assert len(child_chunks) > 0


def test_overlap_content():
    text = "a b c d e f g h i j k l m n o p"
    splitter = Splitter(chunk_size=10, chunk_overlap=5)
    chunks = splitter.split(text)
    assert len(chunks) >= 2


def test_empty_text():
    splitter = Splitter(chunk_size=100, chunk_overlap=10)
    chunks = splitter.split("")
    assert chunks == []


def test_no_separator_does_not_explode_into_char_list():
    """Text with no separators must not produce a piece list of per-char strings.

    Why: a 45MB file with no usable separator previously hit `pieces = list(text)`
    and allocated tens of millions of 1-char str objects (~50 bytes each) —
    multi-GB RSS and OOM. The fallback must slice by chunk_size instead.
    """
    text = "a" * 10_000
    splitter = Splitter(chunk_size=100, chunk_overlap=0)

    original = splitter._recursive_split
    max_intermediate = 0

    def spy(text_arg, seps, chunk_size):
        nonlocal max_intermediate
        result = original(text_arg, seps, chunk_size)
        max_intermediate = max(max_intermediate, len(result))
        return result

    splitter._recursive_split = spy
    chunks = splitter.split(text)

    assert max_intermediate < len(text) / 10, (
        f"splitter produced {max_intermediate} pieces for a {len(text)}-char "
        f"no-separator text; expected bounded by chunk_size"
    )
    assert sum(len(c.content) for c in chunks) >= len(text) * 0.9
