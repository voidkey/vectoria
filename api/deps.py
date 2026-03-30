from fastapi import Depends, HTTPException, Security
from fastapi.security import APIKeyHeader
from config import get_settings

_api_key_header = APIKeyHeader(name="X-API-Key", auto_error=False)


async def verify_api_key(
    key: str | None = Security(_api_key_header),
) -> str | None:
    """Validate the API key when one is configured.

    If API_KEY is not set (empty), all requests are allowed so that local
    development works without extra setup.  Once API_KEY is set, every
    protected request must carry a matching ``X-API-Key`` header.
    """
    expected = get_settings().api_key.get_secret_value()
    if not expected:
        return None
    if not key or key != expected:
        raise HTTPException(status_code=401, detail="Invalid or missing API key")
    return key
