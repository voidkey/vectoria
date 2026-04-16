import hmac

from fastapi import Depends, Security
from fastapi.security import APIKeyHeader
from api.errors import AppError, ErrorCode
from config import get_settings

_api_key_header = APIKeyHeader(name="X-API-Key", auto_error=False)


async def verify_api_key(
    key: str | None = Security(_api_key_header),
) -> str | None:
    """Validate the API key when one is configured.

    If API_KEY is not set (empty), all requests are allowed so that local
    development works without extra setup.  Once API_KEY is set, every
    protected request must carry a matching ``X-API-Key`` header.

    Uses constant-time comparison to prevent timing attacks.
    """
    expected = get_settings().api_key.get_secret_value()
    if not expected:
        return None
    if not key or not hmac.compare_digest(key, expected):
        raise AppError(401, ErrorCode.UNAUTHORIZED, "Invalid or missing API key")
    return key
