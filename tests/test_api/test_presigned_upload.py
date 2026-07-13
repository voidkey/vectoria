"""Presigned direct-upload path: mint URL (`uploads`) + `complete`."""
import base64
import pytest
from unittest.mock import patch, AsyncMock


def test_upload_not_found_error_code_exists():
    from api.errors import ErrorCode
    assert ErrorCode.UPLOAD_NOT_FOUND == 1211


def test_presign_upload_expires_default():
    from config import get_settings
    assert get_settings().s3_presign_upload_expires == 600


def test_upload_id_roundtrip():
    from api.routes.documents import _encode_upload_id, _decode_upload_id
    key = "upload_staging/kb-x/doc-1/report file.pdf"
    token = _encode_upload_id(key)
    assert "/" not in token  # single URL path segment
    assert _decode_upload_id(token) == key


def test_decode_bad_token_raises():
    from api.routes.documents import _decode_upload_id
    with pytest.raises(Exception):
        _decode_upload_id("!!!not-base64!!!")
