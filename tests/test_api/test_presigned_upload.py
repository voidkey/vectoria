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
