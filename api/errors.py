from enum import Enum

from fastapi import HTTPException


class Action(str, Enum):
    """Frontend-facing next step. Backend owns the vocabulary; frontend
    owns the localized copy/button for each value."""
    UPLOAD_SOURCE = "upload_source"          # 直接上传源文件
    CHECK_LINK = "check_link"                # 检查链接是否有效
    USE_OTHER_NETWORK = "use_other_network"  # 换网络 / 区域不可达
    RETRY_LATER = "retry_later"              # 稍后重试
    REMOVE_PASSWORD = "remove_password"      # 去密码后重传（预留）
    REDUCE_SIZE = "reduce_size"              # 精简 / 拆分
    REPLACE_FILE = "replace_file"            # 换文件
    CONTACT_SUPPORT = "contact_support"      # 兜底
    NONE = "none"


class ErrorCode:
    # Auth (1001-1099)
    UNAUTHORIZED = 1001
    RATE_LIMITED = 1002
    FORBIDDEN = 1003

    # URL validation (1101-1199)
    INVALID_URL = 1101
    UNSUPPORTED_FILE_TYPE = 1102
    BLOCKED_ADDRESS = 1103
    DNS_RESOLVE_FAILED = 1104

    # Parsing (1201-1299)
    PARSE_ERROR = 1201
    EMPTY_CONTENT = 1202
    CONTENT_TOO_LARGE = 1203
    UPLOAD_TOO_LARGE = 1204
    PARSE_TIMEOUT = 1205
    INGEST_BUSY = 1206
    MIME_MISMATCH = 1207
    PDF_TOO_MANY_PAGES = 1208
    PPTX_TOO_MANY_SLIDES = 1209
    EMPTY_UPLOAD = 1210
    UPLOAD_NOT_FOUND = 1211
    UPLOAD_NOT_SUPPORTED = 1212
    CAPTURE_NOT_FOUND = 1213
    FILE_ENCRYPTED = 1214           # reserved: detection deferred
    FILE_CORRUPTED = 1215           # reserved: detection deferred
    SCANNED_NEEDS_OCR = 1216        # reserved: detection deferred
    PARSE_UNRESOLVABLE = 1299       # permanent fallback (no specific code)

    # Resource not found (1301-1399)
    NOT_FOUND = 1301

    # Query (1401-1499)
    QUERY_ERROR = 1401
    INDEXING_DISABLED = 1402

    # Link retrieval (1501-1599) — async URL-fetch failures
    LINK_VIDEO_UNSUPPORTED = 1501   # 短视频/播放器链接
    LINK_LOGIN_REQUIRED = 1502      # 需要登录 (generic detection reserved)
    LINK_ANTIBOT_BLOCKED = 1503     # 风控/人机验证
    LINK_REGION_BLOCKED = 1504      # 区域不可达/需翻墙
    LINK_PAGE_GONE = 1505           # 页面已删除 404/410
    LINK_FORBIDDEN = 1506           # 站点拒绝 403 (reserved: no raise site yet)
    LINK_FETCH_TIMEOUT = 1507       # 连接超时/网络错误

    # Generic (9001-9099)
    VALIDATION_ERROR = 9001
    INTERNAL_ERROR = 9999


class ErrorMeta:
    """Derived, frontend-facing attributes for a code (retryable + action)."""
    __slots__ = ("retryable", "action")

    def __init__(self, retryable: bool, action: Action):
        self.retryable = retryable
        self.action = action


_R = ErrorMeta  # local alias to keep the table compact

# Single source of truth. EVERY ErrorCode must appear here (guarded by
# tests/test_api/test_error_registry.py::test_every_code_has_meta).
ERROR_META: dict[int, ErrorMeta] = {
    # Auth
    ErrorCode.UNAUTHORIZED:          _R(False, Action.NONE),
    ErrorCode.RATE_LIMITED:          _R(True,  Action.RETRY_LATER),
    ErrorCode.FORBIDDEN:             _R(False, Action.NONE),
    # URL validation
    ErrorCode.INVALID_URL:           _R(False, Action.CHECK_LINK),
    ErrorCode.UNSUPPORTED_FILE_TYPE: _R(False, Action.REPLACE_FILE),
    ErrorCode.BLOCKED_ADDRESS:       _R(False, Action.CHECK_LINK),
    ErrorCode.DNS_RESOLVE_FAILED:    _R(False, Action.CHECK_LINK),
    # Parsing / content
    ErrorCode.PARSE_ERROR:           _R(True,  Action.RETRY_LATER),
    ErrorCode.EMPTY_CONTENT:         _R(False, Action.CHECK_LINK),
    ErrorCode.CONTENT_TOO_LARGE:     _R(False, Action.REDUCE_SIZE),
    ErrorCode.UPLOAD_TOO_LARGE:      _R(False, Action.REDUCE_SIZE),
    ErrorCode.PARSE_TIMEOUT:         _R(True,  Action.RETRY_LATER),
    ErrorCode.INGEST_BUSY:           _R(True,  Action.RETRY_LATER),
    ErrorCode.MIME_MISMATCH:         _R(False, Action.REPLACE_FILE),
    ErrorCode.PDF_TOO_MANY_PAGES:    _R(False, Action.REDUCE_SIZE),
    ErrorCode.PPTX_TOO_MANY_SLIDES:  _R(False, Action.REDUCE_SIZE),
    ErrorCode.EMPTY_UPLOAD:          _R(False, Action.REPLACE_FILE),
    ErrorCode.UPLOAD_NOT_FOUND:      _R(False, Action.REPLACE_FILE),
    ErrorCode.UPLOAD_NOT_SUPPORTED:  _R(False, Action.REPLACE_FILE),
    ErrorCode.CAPTURE_NOT_FOUND:     _R(False, Action.NONE),
    ErrorCode.FILE_ENCRYPTED:        _R(False, Action.REMOVE_PASSWORD),
    ErrorCode.FILE_CORRUPTED:        _R(False, Action.REPLACE_FILE),
    ErrorCode.SCANNED_NEEDS_OCR:     _R(False, Action.NONE),  # TODO: pick a real action when OCR detection lands
    ErrorCode.PARSE_UNRESOLVABLE:    _R(False, Action.CONTACT_SUPPORT),
    # Not found
    ErrorCode.NOT_FOUND:             _R(False, Action.NONE),
    # Query
    ErrorCode.QUERY_ERROR:           _R(True,  Action.RETRY_LATER),
    ErrorCode.INDEXING_DISABLED:     _R(False, Action.NONE),
    # Link retrieval
    ErrorCode.LINK_VIDEO_UNSUPPORTED: _R(False, Action.UPLOAD_SOURCE),
    ErrorCode.LINK_LOGIN_REQUIRED:    _R(False, Action.UPLOAD_SOURCE),
    ErrorCode.LINK_ANTIBOT_BLOCKED:   _R(False, Action.UPLOAD_SOURCE),
    ErrorCode.LINK_REGION_BLOCKED:    _R(False, Action.USE_OTHER_NETWORK),
    ErrorCode.LINK_PAGE_GONE:         _R(False, Action.CHECK_LINK),
    ErrorCode.LINK_FORBIDDEN:         _R(False, Action.CHECK_LINK),
    ErrorCode.LINK_FETCH_TIMEOUT:     _R(True,  Action.RETRY_LATER),
    # Generic
    ErrorCode.VALIDATION_ERROR:      _R(False, Action.NONE),
    ErrorCode.INTERNAL_ERROR:        _R(True,  Action.RETRY_LATER),
}


def error_meta(code: int | None) -> ErrorMeta | None:
    """Look up the registry entry for a code. None for unknown/None."""
    if code is None:
        return None
    return ERROR_META.get(code)


def error_fields(code: int | None) -> dict:
    """The three public error fields for a given code, for spreading into
    a response model. All None when the code is None or unknown so a
    successful / legacy doc carries no misleading retryable/action."""
    meta = error_meta(code)
    return {
        "error_code": code,
        "retryable": meta.retryable if meta else None,
        "suggested_action": meta.action.value if meta else None,
    }


class AppError(HTTPException):
    """Structured application error with machine-readable code.

    Optional ``headers`` are passed through to the JSONResponse by
    ``app_error_handler``. Used by the rate limiter to ship
    ``Retry-After`` and ``X-RateLimit-*`` on 429 (and reserved for
    401 ``WWW-Authenticate`` / 503 maintenance challenges).
    """

    def __init__(
        self,
        status_code: int,
        code: int,
        detail: str,
        headers: dict[str, str] | None = None,
    ):
        super().__init__(status_code=status_code, detail=detail, headers=headers)
        self.code = code
