from fastapi import HTTPException


class ErrorCode:
    # Auth (1001-1099)
    UNAUTHORIZED = 1001

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

    # Resource not found (1301-1399)
    NOT_FOUND = 1301

    # Query (1401-1499)
    QUERY_ERROR = 1401

    # Generic (9001-9099)
    VALIDATION_ERROR = 9001
    INTERNAL_ERROR = 9999


class AppError(HTTPException):
    """Structured application error with machine-readable code."""

    def __init__(self, status_code: int, code: int, detail: str):
        super().__init__(status_code=status_code, detail=detail)
        self.code = code
