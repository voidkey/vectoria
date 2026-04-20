"""Streaming upload pipeline tests.

Key invariants to guard:
  * Each ref is released after its upload completes — closures can be
    freed as the pipeline progresses rather than at end of document.
  * Concurrency is capped at the configured bound (peak memory scales
    with bound × avg_size, not doc_size × avg_size).
  * materialize() runs off the event loop (via to_thread) so CPU-bound
    PIL / base64 work doesn't block other async tasks.
"""
import asyncio
from unittest.mock import AsyncMock, patch

import pytest

from api.image_stream import (
    refs_from_dict,
    stream_upload_and_store_refs,
    stream_upload_refs,
)
from parsers.image_ref import ImageRef


def _ref(name: str, payload: bytes = b"\x89PNG\r\n\x1a\n" + b"\x00" * 24,
         width=300, height=300) -> ImageRef:
    return ImageRef(
        name=name, mime="image/png",
        width=width, height=height,
        _factory=lambda d=payload: d,
    )


@pytest.mark.asyncio
async def test_each_ref_released_after_upload():
    """Release-as-you-go is the whole point of the streaming pipeline."""
    refs = [_ref(f"img_{i}.png") for i in range(5)]

    mock_storage = AsyncMock()
    mock_storage.put = AsyncMock()

    with patch("api.image_stream.get_storage", return_value=mock_storage):
        count = await stream_upload_refs(refs)

    assert count and len(count) == 5
    assert all(r.consumed for r in refs), (
        "every ref must be released once its upload completes"
    )


@pytest.mark.asyncio
async def test_concurrency_is_bounded():
    """At most N materialize() calls are active concurrently — N being
    the configured semaphore bound.
    """
    inflight = 0
    peak = 0
    lock = asyncio.Lock()

    async def blocking_put(key, data, *, content_type=""):
        nonlocal inflight, peak
        async with lock:
            inflight += 1
            peak = max(peak, inflight)
        try:
            await asyncio.sleep(0.01)  # simulate network
        finally:
            async with lock:
                inflight -= 1

    mock_storage = AsyncMock()
    mock_storage.put = blocking_put
    mock_storage.presign_url = AsyncMock(return_value="https://x")

    refs = [_ref(f"img_{i}.png") for i in range(20)]

    with patch("api.image_stream.get_storage", return_value=mock_storage):
        await stream_upload_refs(refs, concurrency=3)

    assert peak <= 3, f"concurrency bound violated: peak={peak}"


@pytest.mark.asyncio
async def test_materialize_runs_off_the_event_loop():
    """Factory is CPU-bound (PIL encode, base64 decode). If it runs
    inline on the event loop it blocks concurrent uploads — make sure
    the pipeline dispatches via to_thread.
    """
    calls: list[str] = []
    loop_thread_id = None

    def blocking_factory():
        import threading
        calls.append(threading.current_thread().name)
        return b"data"

    loop_thread_id = __import__("threading").current_thread().name
    ref = ImageRef(
        name="img.png", mime="image/png",
        width=200, height=200, _factory=blocking_factory,
    )

    mock_storage = AsyncMock()
    with patch("api.image_stream.get_storage", return_value=mock_storage):
        await stream_upload_refs([ref])

    assert len(calls) == 1
    assert calls[0] != loop_thread_id, (
        "factory must run off the main event-loop thread"
    )


@pytest.mark.asyncio
async def test_refs_from_dict_adapts_legacy_callers():
    """The URL download handler still produces dict[str, bytes]. The
    adapter must wrap each entry as a ref whose factory returns the
    captured bytes unchanged.
    """
    d = {"a.png": b"aaa", "b.png": b"bbb"}
    refs = refs_from_dict(d)

    assert [r.name for r in refs] == ["a.png", "b.png"]
    assert refs[0].materialize() == b"aaa"
    assert refs[1].materialize() == b"bbb"


@pytest.mark.asyncio
async def test_upload_failure_isolated_and_still_releases_ref():
    """Partial failure semantics: a single S3 put error must NOT abort
    the batch, the failed ref must still be released, and the caller
    sees a reduced result (not an exception). Protects against a
    single-image hiccup cancelling the other concurrent uploads and
    forcing the worker to re-parse the entire document.
    """
    good = _ref("good.png")
    bad = _ref("bad.png")

    mock_storage = AsyncMock()
    put_calls = []

    async def _put(key, data, *, content_type=""):
        put_calls.append(key)
        if "bad" in key:
            raise RuntimeError("s3 down")

    mock_storage.put = _put
    mock_storage.presign_url = AsyncMock(return_value="https://x")

    with patch("api.image_stream.get_storage", return_value=mock_storage):
        # Must NOT raise.
        result = await stream_upload_refs([good, bad])

    # Good one survives, bad one is dropped.
    assert len(result) == 1
    assert result[0]["id"].startswith("good")
    # Both refs released regardless — no memory leak either way.
    assert good.consumed and bad.consumed


@pytest.mark.asyncio
async def test_store_refs_skips_below_vision_dim():
    """Refs below the vision-size gate are stored but their DB row gets
    vision_status='skipped' — spares the vision worker LLM calls on
    favicons/decorations.
    """
    refs = [
        _ref("tiny.png", width=150, height=150),  # < 200 → skipped
        _ref("big.png", width=400, height=400),   # ≥ 200 → pending
    ]

    mock_storage = AsyncMock()
    session_writes = []

    class _FakeSession:
        async def __aenter__(self):
            return self
        async def __aexit__(self, *a):
            return False
        def add(self, obj):
            session_writes.append(obj)
        async def commit(self):
            pass

    with patch("api.image_stream.get_storage", return_value=mock_storage), \
         patch("db.base.get_session", return_value=_FakeSession()):
        count = await stream_upload_and_store_refs(
            refs, kb_id="kb", doc_id="doc", vision_configured=True,
        )

    assert count == 2
    statuses = {r.filename.split(".")[0]: r.vision_status for r in session_writes}
    assert statuses == {"tiny": "skipped", "big": "pending"}
