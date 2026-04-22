# vectoria / monitoring

Operator-facing artifacts for the metrics surface `infra/metrics.py`
exposes. Nothing here ships into the runtime image — these are
configuration files for your Prometheus server and Grafana instance.

## Files

| File | Purpose |
|------|---------|
| `prometheus-rules.yaml` | 10 alert rules covering queue, worker, parser, external APIs, and rate-limit degradation. Add to Prometheus via `rule_files:`. |
| `grafana-dashboard.json` | Baseline dashboard: queue depth/age/DLQ, worker RSS + task rate, parser latency + status, circuit breakers + external API health, rate-limit decisions. Import via Grafana UI. |

## Layering

The alerts are designed to fire on _sustained_ conditions (`for: 2m`
or longer) so transient blips during redeploys / GC pauses don't
page. Severity floor is `warning`; only `VectoriaCircuitOpen` is
`critical` because a fully open breaker means downstream functionality
is unavailable.

## Metric catalogue

| Metric | Type | Labels | Meaning |
|---|---|---|---|
| `vectoria_worker_rss_bytes` | gauge | — | Current worker resident memory |
| `vectoria_worker_rss_kills_total` | counter | — | Worker self-exits on RSS cap |
| `vectoria_tasks_total` | counter | task_type, status | Task outcomes (completed/failed/dead) |
| `vectoria_task_duration_seconds` | histogram | task_type, status | Task wall-clock duration |
| `vectoria_queue_depth` | gauge | task_type | Pending queue size per type |
| `vectoria_queue_oldest_age_seconds` | gauge | task_type | Age of oldest pending task |
| `vectoria_queue_dead_tasks` | gauge | task_type | DLQ size per type (W5-6) |
| `vectoria_parse_duration_seconds` | histogram | engine, status | Parser latency (status ∈ ok/error/timeout/empty) |
| `vectoria_external_api_calls_total` | counter | api, status | Mineru / vision / embedding call outcomes |
| `vectoria_external_api_duration_seconds` | histogram | api | External API latency |
| `vectoria_circuit_state` | gauge | name | 0=closed, 1=half_open, 2=open |
| `vectoria_circuit_transitions_total` | counter | name, to_state | Breaker state changes |
| `vectoria_ratelimit_checks_total` | counter | key, result | Rate-limit decisions (allowed/blocked/local_fallback/error) |

## Tuning

- **Queue depth threshold (50)**: pick a value higher than your steady-state queue under normal ingest traffic. Tune after observing a week of baseline.
- **RSS near-limit (80%)**: static threshold; not configurable here. If your cluster has heterogeneous worker memory caps, split the expression per deployment with `on(deployment)`.
- **Parse error rate (20%)**: parsers legitimately fail on malformed inputs (CAPTCHAs, password-protected PDFs). 20% is permissive; lower if your traffic is mostly well-formed.
- **Rate-limit blocked (0.1 ops)**: informational severity. Not a page; just a signal to revisit the per-domain caps in `parsers/url/_handlers.py:_DOMAIN_RATES`.

## Deployment

Prometheus operator / standalone:

```yaml
# prometheus.yml
rule_files:
  - /etc/prometheus/vectoria-rules.yaml

scrape_configs:
  - job_name: vectoria
    static_configs:
      - targets: ['vectoria-app:8000', 'vectoria-worker:9001']
```

Grafana import:

1. **Dashboards** → **New** → **Import**
2. Upload `grafana-dashboard.json`
3. Select your Prometheus datasource (default name `Prometheus` — if yours differs, edit the `datasource` fields in the JSON first).

Reload Prometheus after editing rules:

```bash
curl -X POST http://prometheus:9090/-/reload
```
