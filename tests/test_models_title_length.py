"""``Document.title`` is varchar(500); Postgres rejects anything longer
with StringDataRightTruncationError, which surfaced as a 500 on
POST /documents/url for WeChat share links (576+ chars of tracking
query string used verbatim as the placeholder title).

The cap is enforced on the model rather than at each call site because
the call sites are the thing that keeps drifting: capture already had
``[:500]``, the two ingest paths didn't.
"""
from db.models import Document, TITLE_MAX_LEN


def test_title_max_len_matches_column_width():
    assert Document.__table__.c.title.type.length == TITLE_MAX_LEN


def test_constructor_clamps_overlong_title():
    doc = Document(title="x" * (TITLE_MAX_LEN + 100))
    assert len(doc.title) == TITLE_MAX_LEN


def test_assignment_clamps_overlong_title():
    """``db.helpers.update_doc`` writes via setattr, so the worker's
    parse/capture updates only stay safe if assignment clamps too."""
    doc = Document(title="ok")
    doc.title = "y" * (TITLE_MAX_LEN + 1)
    assert len(doc.title) == TITLE_MAX_LEN


def test_title_within_limit_is_untouched():
    doc = Document(title="A Normal Article Title")
    assert doc.title == "A Normal Article Title"


def test_none_title_is_preserved():
    """Clamping must not coerce None into a string — callers that pass
    None rely on the column default rather than an empty title."""
    doc = Document(title=None)
    assert doc.title is None


def test_capture_title_clamp_is_the_models_job():
    """``handle_capture_site`` used to carry its own ``[:500]``. Dropping
    that literal is only safe because assignment through ``update_doc``
    clamps — this pins the behaviour the handler now relies on."""
    doc = Document()
    doc.title = "长标题" * 400
    assert len(doc.title) == TITLE_MAX_LEN
