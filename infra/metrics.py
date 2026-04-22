"""Process-wide Prometheus metric definitions.

Metric *definitions* live here; *observations* (``.inc()`` / ``.observe()``)
happen at their call sites: worker loop, parsers, circuit breakers, etc.
Defining them centrally prevents duplicate-registration errors when both
the API and worker import a shared helper module that touches metrics.

Buckets are tuned for this workload:
  - task/parse durations span a few seconds (embedding batch) to minutes
    (large PDF + MinerU), so we cover 0.1 s .. 10 min.
  - external-API latency is usually sub-second but can block for a full
    read-timeout on hung MinerU, so the upper bucket reaches 120 s.
"""
import time
from contextlib import asynccontextmanager

from prometheus_client import Counter, Gauge, Histogram, start_http_server

# ---------------------------------------------------------------------------
# USE — worker process state
# ---------------------------------------------------------------------------

WORKER_RSS_BYTES = Gauge(
    "vectoria_worker_rss_bytes",
    "Worker process resident set size in bytes (sampled per task loop).",
)

WORKER_RSS_KILLS = Counter(
    "vectoria_worker_rss_kills_total",
    "Worker self-exits triggered by RSS exceeding the configured limit.",
)

WORKER_RSS_LIMIT_BYTES = Gauge(
    "vectoria_worker_rss_limit_bytes",
    "Configured RSS self-kill threshold (bytes). 0 when disabled. "
    "Exported at worker startup so the 'near-limit' alert has a real "
    "threshold to compare vectoria_worker_rss_bytes against.",
)

WORKER_TASKS_INFLIGHT = Gauge(
    "vectoria_worker_tasks_inflight",
    "Tasks currently being processed by this worker (0 or 1 under current serial model).",
)

# ---------------------------------------------------------------------------
# Task lifecycle (business)
# ---------------------------------------------------------------------------

_TASK_BUCKETS = (0.1, 0.5, 1, 5, 10, 30, 60, 120, 300, 600)

TASK_DURATION_SECONDS = Histogram(
    "vectoria_task_duration_seconds",
    "Wall-clock time spent in worker handler per task.",
    labelnames=("task_type", "status"),
    buckets=_TASK_BUCKETS,
)

TASK_TOTAL = Counter(
    "vectoria_tasks_total",
    "Tasks processed by the worker, labelled by outcome.",
    labelnames=("task_type", "status"),  # status ∈ {completed, failed, dead}
)

QUEUE_DEPTH = Gauge(
    "vectoria_queue_depth",
    "Pending tasks in the queue (sampled periodically).",
    labelnames=("task_type",),
)

QUEUE_OLDEST_AGE_SECONDS = Gauge(
    "vectoria_queue_oldest_age_seconds",
    "Age in seconds of the oldest pending task, per task type.",
    labelnames=("task_type",),
)

QUEUE_DEAD_TASKS = Gauge(
    "vectoria_queue_dead_tasks",
    "Tasks in terminal failure state per task type "
    "(status='dead', i.e. max_attempts exhausted). "
    "A non-zero here deserves an alert — retries already gave up.",
    labelnames=("task_type",),
)

# ---------------------------------------------------------------------------
# External dependencies (reliability)
# ---------------------------------------------------------------------------

_EXT_BUCKETS = (0.05, 0.1, 0.5, 1, 2, 5, 10, 30, 60, 120)

EXTERNAL_API_CALLS = Counter(
    "vectoria_external_api_calls_total",
    "Calls to external services (MinerU, Vision LLM, embedding API, ...).",
    labelnames=("api", "status"),  # api ∈ {mineru, vision, embedding, ...}
)

EXTERNAL_API_DURATION_SECONDS = Histogram(
    "vectoria_external_api_duration_seconds",
    "Latency of external service calls.",
    labelnames=("api",),
    buckets=_EXT_BUCKETS,
)

CIRCUIT_STATE = Gauge(
    "vectoria_circuit_state",
    "Circuit breaker state: 0=closed, 1=half_open, 2=open.",
    labelnames=("name",),
)

CIRCUIT_TRANSITIONS = Counter(
    "vectoria_circuit_transitions_total",
    "Circuit breaker state transitions.",
    labelnames=("name", "to_state"),
)

# ---------------------------------------------------------------------------
# Parser internals
# ---------------------------------------------------------------------------

PARSE_DURATION_SECONDS = Histogram(
    "vectoria_parse_duration_seconds",
    "Wall-clock time spent in parser.parse().",
    labelnames=("engine", "status"),  # status ∈ {ok, error, timeout, circuit_open}
    buckets=_TASK_BUCKETS,
)

PARSE_EMPTY_TOTAL = Counter(
    "vectoria_parse_empty_total",
    "Parses that returned empty / whitespace-only content despite no "
    "exception. Different from ``status=error`` in parse_duration — "
    "these completed cleanly but produced nothing useful (scanned "
    "PDFs without OCR, corrupt Office files, CAPTCHA-blocked URLs).",
    labelnames=("engine",),
)

# ---------------------------------------------------------------------------
# Document lifecycle outcomes
# ---------------------------------------------------------------------------
# Counts documents by the state they settle in after the full ingest
# pipeline. Complements the task-level metrics: a task can complete
# successfully while its document ends up ``failed`` (empty content,
# oversized, parse exception). ``completed`` is the happy-path counter.

DOCUMENT_OUTCOMES = Counter(
    "vectoria_documents_total",
    "Documents by terminal outcome after the ingest pipeline.",
    labelnames=("outcome",),
    # outcome ∈ {
    #   completed        — indexed successfully
    #   empty_content    — parse returned nothing
    #   too_large        — content exceeded max_content_chars
    #   parse_error      — parser raised an exception
    #   indexing_error   — embedding / vector insert failed
    # }
)

# ---------------------------------------------------------------------------
# Rate limiting (infra/ratelimit.py)
# ---------------------------------------------------------------------------

RATELIMIT_CHECKS_TOTAL = Counter(
    "vectoria_ratelimit_checks_total",
    "Distributed rate-limit decisions labelled by outcome. "
    "allowed/blocked = shared Redis bucket decision; "
    "local_fallback = Redis down, degraded to per-process bucket; "
    "error = local fallback also failed (request was allowed to avoid "
    "deadlocking ingestion).",
    labelnames=("key", "result"),
    # result ∈ {allowed, blocked, local_fallback, error}
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_metrics_server_started = False


def start_metrics_server(port: int) -> None:
    """Start the prometheus_client stdlib HTTP server on the given port.

    Call this from worker entry points (workers have no FastAPI app to
    hang ``/metrics`` off of). Idempotent: only the first call binds;
    subsequent calls are no-ops.

    The API process does NOT use this — it exposes ``/metrics`` via the
    ``prometheus-fastapi-instrumentator`` on the main uvicorn port.
    """
    global _metrics_server_started  # noqa: PLW0603
    if _metrics_server_started:
        return
    start_http_server(port)
    _metrics_server_started = True


@asynccontextmanager
async def observe_parse(engine: str):
    """Record ``PARSE_DURATION_SECONDS`` around a ``parser.parse()`` call.

    Use as::

        async with observe_parse("mineru"):
            result = await parser.parse(...)

    Classifies the outcome automatically:
      * ``ok`` — clean exit
      * ``circuit_open`` — ``CircuitOpenError`` (downstream circuit tripped,
        distinct from parser/engine failure)
      * ``timeout`` — ``TimeoutError`` (isolation-pool wall-clock timeout;
        the subprocess is being recycled but the call is user-visible failure)
      * ``error`` — any other exception

    All statuses re-raise; this block only records and re-throws.
    """
    start = time.monotonic()
    status = "ok"
    try:
        yield
    except BaseException as exc:  # noqa: BLE001 — re-raised below
        # Lazy import: circuit_breaker imports this module at top-level
        # so we must avoid a circular import here.
        from infra.circuit_breaker import CircuitOpenError
        if isinstance(exc, CircuitOpenError):
            status = "circuit_open"
        elif isinstance(exc, TimeoutError):
            # Py 3.11+ unifies asyncio.TimeoutError and
            # concurrent.futures.TimeoutError under builtin TimeoutError,
            # so this single isinstance covers both isolation.py paths.
            status = "timeout"
        else:
            status = "error"
        raise
    finally:
        PARSE_DURATION_SECONDS.labels(engine=engine, status=status).observe(
            time.monotonic() - start
        )
