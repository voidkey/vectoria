"""ImageRef contract tests.

The whole streaming pipeline rests on two guarantees:
  * ``materialize()`` calls the factory each time it's invoked.
  * ``release()`` drops the factory reference so closures can be GC'd,
    and further ``materialize()`` calls fail loudly (not silently).

If either of these breaks, memory-bounded upload semantics are gone.
"""
import gc
import weakref

import pytest

from parsers.image_ref import ImageRef


def test_materialize_calls_factory_each_time():
    calls = []

    def factory():
        calls.append(1)
        return b"payload"

    ref = ImageRef(name="x", mime="image/png", _factory=factory)
    assert ref.materialize() == b"payload"
    assert ref.materialize() == b"payload"
    assert len(calls) == 2, "factory must be invoked on each materialize"


def test_release_drops_factory_and_marks_consumed():
    ref = ImageRef(name="x", mime="image/png", _factory=lambda: b"x")
    assert not ref.consumed
    ref.release()
    assert ref.consumed
    with pytest.raises(RuntimeError, match="materialize.*release"):
        ref.materialize()


def test_release_is_idempotent():
    ref = ImageRef(name="x", mime="image/png", _factory=lambda: b"x")
    ref.release()
    ref.release()  # must not raise
    assert ref.consumed


def test_release_frees_closure_captures():
    """The point of release(): if a factory captures a large object, the
    object must be GC-able once the ref is released. Uses weakref to
    verify the capture is actually dropped.
    """
    class Big:
        pass

    big = Big()
    big_ref = weakref.ref(big)

    def factory(captured=big):
        return b"produced from " + id(captured).to_bytes(8, "big")

    ref = ImageRef(name="x", mime="image/png", _factory=factory)
    del big  # drop our local reference; only factory's default arg holds it
    gc.collect()
    # factory still holds it via default arg, so the weakref is alive.
    assert big_ref() is not None

    ref.release()
    factory = None  # drop local factory ref too; release() cleared ref's
    gc.collect()
    assert big_ref() is None, (
        "release() must allow GC of factory-captured objects"
    )
