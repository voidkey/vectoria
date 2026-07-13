"""`_run_upload_gates` is the single validation used by both the multipart
`/file` path and the presigned `complete` path — this guards its behavior
directly so drift between the two paths is caught here."""
import pytest

from api.errors import AppError, ErrorCode


def test_gates_reject_empty():
    from api.routes.documents import _run_upload_gates
    with pytest.raises(AppError) as ei:
        _run_upload_gates("kb-x", "note.txt", b"")
    assert ei.value.code == ErrorCode.EMPTY_UPLOAD


def test_gates_reject_oversize():
    from config import get_settings
    from api.routes.documents import _run_upload_gates
    big = b"a" * (get_settings().max_upload_bytes + 1)
    with pytest.raises(AppError) as ei:
        _run_upload_gates("kb-x", "note.txt", big)
    assert ei.value.code == ErrorCode.UPLOAD_TOO_LARGE


def test_gates_pass_returns_page_count_none_for_txt():
    from api.routes.documents import _run_upload_gates
    assert _run_upload_gates("kb-x", "note.txt", b"hello world") is None
