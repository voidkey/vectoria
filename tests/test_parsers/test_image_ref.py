"""ImageRef contract tests.

The whole streaming pipeline rests on two guarantees:
  * ``materialize()`` calls the factory each time it's invoked.
  * ``release()`` drops the factory reference so closures can be GC'd,
    and further ``materialize()`` calls fail loudly (not silently).

If either of these breaks, memory-bounded upload semantics are gone.
"""
import base64
import gc
import pickle
import weakref

import pytest

from parsers.base import ParseResult
from parsers.image_ref import Base64Factory, BytesFactory, ImageRef


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


def test_parse_result_with_image_refs_survives_pickle_round_trip():
    """Regression: parsers run under ``parser_isolation`` return
    ``ParseResult`` across a ProcessPoolExecutor boundary. A nested
    ``def _factory`` breaks this with ``Can't get local object
    ...<locals>._factory`` on unpickle in the parent.
    """
    payload = b"payload-bytes"
    b64 = base64.b64encode(payload).decode()
    pr = ParseResult(
        content="md",
        title="t",
        image_refs=[
            ImageRef(name="a.png", mime="image/png", _factory=BytesFactory(payload)),
            ImageRef(name="b.png", mime="image/png", _factory=Base64Factory(b64)),
        ],
    )

    roundtripped = pickle.loads(pickle.dumps(pr))

    assert roundtripped.content == "md"
    assert [r.name for r in roundtripped.image_refs] == ["a.png", "b.png"]
    assert roundtripped.image_refs[0].materialize() == payload
    assert roundtripped.image_refs[1].materialize() == payload
