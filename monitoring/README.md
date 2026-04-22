# vectoria / monitoring

Operator-facing artifacts for the metrics surface `infra/metrics.py`
exposes. Nothing here ships into the runtime image — these are
configuration files for your Prometheus server and Grafana instance.

## Quickstart (recommended)

Run Prometheus + Grafana in the **same compose project** as vectoria
so they can scrape `app:8000` and `worker:9001` by service name:

```bash
cd /path/to/vectoria
docker compose \
  -f compose.yaml -f compose.prod.yaml \
  -f monitoring/compose.monitoring.yaml \
  --env-file .env.prod \
  up -d prometheus grafana
```

That brings up:
- **Prometheus** on `http://<host>:9090` (loopback by default — see
  `PROM_BIND` env var if you need remote access; it has no auth, so
  front with an auth proxy before exposing)
- **Grafana** on `http://<host>:3000` (admin/admin; change immediately
  or set `GF_ADMIN_PASSWORD` in the env). The Vectoria dashboard
  auto-provisions into the "Vectoria" folder.

Both dashboard + rules hot-reload from disk. Edit
`monitoring/prometheus-rules.yaml` and:

```bash
curl -X POST http://127.0.0.1:9090/-/reload
```

**Gotcha**: when rule files are updated via `git pull`, Git replaces
the file atomically (rename-over-write). Bind-mounts hold the old
inode — Prometheus keeps serving the previous rule set even after
`-/reload`. Workaround: restart the container after a git-pull-driven
rule edit (`docker restart vectoria-prometheus-1`). In-place edits
(e.g. `vi prometheus-rules.yaml`) reload cleanly and don't need this.

## Files

| File | Purpose |
|------|---------|
| `compose.monitoring.yaml` | Compose overlay that brings up Prometheus + Grafana wired into the vectoria stack. |
| `prometheus.yaml` | Prometheus config: scrape `app:8000` + `worker:9001`, load `rules.yaml`. |
| `prometheus-rules.yaml` | 11 alert rules covering queue, worker, parser, external APIs, and rate-limit degradation. |
| `grafana/provisioning/datasources/prom.yaml` | Auto-configures the Prometheus datasource in Grafana. |
| `grafana/provisioning/dashboards/provider.yaml` | Tells Grafana to pick up dashboards from `/var/lib/grafana/dashboards` inside the container. |
| `grafana/dashboards/vectoria.json` | The Vectoria dashboard (auto-loaded). |
| `grafana-dashboard.json` | Same dashboard as a standalone reference — import manually into an existing Grafana via UI when you don't want to run our stack. |

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
