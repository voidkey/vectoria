from fastapi import HTTPException


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

    # Resource not found (1301-1399)
    NOT_FOUND = 1301

    # Query (1401-1499)
    QUERY_ERROR = 1401
    INDEXING_DISABLED = 1402

    # Generic (9001-9099)
    VALIDATION_ERROR = 9001
    INTERNAL_ERROR = 9999


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
