"""Request-scoped middleware: assigns a unique request_id and injects it into logs."""

import logging
import uuid
from contextvars import ContextVar
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import Response

# Context variable accessible throughout the request lifecycle.
request_id_var: ContextVar[str] = ContextVar("request_id", default="")


class RequestIdMiddleware(BaseHTTPMiddleware):
    """Attach a unique X-Request-Id to every request/response and inject it
    into the logging context so all log lines for a request can be correlated.
    """

    async def dispatch(self, request: Request, call_next):
        rid = request.headers.get("X-Request-Id") or str(uuid.uuid4())[:8]
        request_id_var.set(rid)
        response: Response = await call_next(request)
        response.headers["X-Request-Id"] = rid
        return response


class RequestIdFilter(logging.Filter):
    """Logging filter that injects the current request_id into log records."""

    def filter(self, record: logging.LogRecord) -> bool:
        record.request_id = request_id_var.get("")  # type: ignore[attr-defined]
        return True
