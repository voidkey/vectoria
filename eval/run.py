"""Retrieval evaluation runner — vectoria W6-7.

Hits a running vectoria instance with each query in ``dataset.yaml``
under multiple config combinations (rerank on/off, query_rewrite
on/off) and reports hit@k + MRR per config.

Use this to:
  * establish a baseline before tuning retrieval (chunk size, fusion
    weights, reranker, etc.)
  * detect regressions — any retrieval change should re-run the eval
    and show equal-or-better numbers
  * quantify "does rerank help" / "does query_rewrite help" on real
    content rather than guessing

Usage
-----
    # Against a local / SSH-tunneled API:
    VECTORIA_URL=http://localhost:8000 \\
    VECTORIA_API_KEY=xxx \\
    python -m eval.run

    # Restrict to a tier (direct / topical / conceptual):
    python -m eval.run --tier conceptual

    # Write a JSON report alongside stdout summary:
    python -m eval.run --out eval/report.json

Output interpretation
---------------------
hit@k = fraction of queries where at least one of the expected
        phrases appeared in any of the top-k returned chunks.
MRR   = mean reciprocal rank of the FIRST relevant chunk. Higher =
        relevant content ranked earlier.

Perfect retrieval = hit@1 == hit@5 == 1.0, MRR == 1.0.
If hit@5 >> hit@1, the right chunks are found but mis-ranked →
reranker helps.
If hit@5 << 1.0, retrieval misses the content entirely → check
keyword + vector paths, fusion weights, embedding model.
"""
from __future__ import annotations

import argparse
import asyncio
import json
import os
import statistics
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import httpx
import yaml


@dataclass
class Query:
    id: str
    query: str
    must_contain_any: list[str]
    tier: str


@dataclass
class QueryOutcome:
    query_id: str
    query: str
    tier: str
    config: str
    hit_rank: int | None                 # 1-indexed; None if no hit
    reciprocal_rank: float               # 0 when no hit
    latency_ms: float
    top_chunks: list[dict[str, Any]]     # {chunk_id, snippet, score}


@dataclass
class ConfigOutcome:
    name: str
    hits_at_1: int = 0
    hits_at_3: int = 0
    hits_at_5: int = 0
    reciprocal_ranks: list[float] = field(default_factory=list)
    latencies_ms: list[float] = field(default_factory=list)
    per_query: list[QueryOutcome] = field(default_factory=list)

    def summary(self, total: int) -> dict[str, Any]:
        return {
            "config": self.name,
            "n_queries": total,
            "hit_at_1": round(self.hits_at_1 / total, 3),
            "hit_at_3": round(self.hits_at_3 / total, 3),
            "hit_at_5": round(self.hits_at_5 / total, 3),
            "mrr": round(statistics.mean(self.reciprocal_ranks), 3) if self.reciprocal_ranks else 0.0,
            "latency_ms_p50": round(statistics.median(self.latencies_ms), 1) if self.latencies_ms else 0.0,
            "latency_ms_p95": round(_percentile(self.latencies_ms, 0.95), 1) if self.latencies_ms else 0.0,
        }


def _percentile(values: list[float], p: float) -> float:
    if not values:
        return 0.0
    s = sorted(values)
    k = max(0, min(len(s) - 1, int(round(p * (len(s) - 1)))))
    return s[k]


def _find_hit_rank(query: Query, top_chunks: list[dict[str, Any]]) -> int | None:
    """Return the 1-indexed rank of the first chunk whose content
    contains any of ``query.must_contain_any``, or None if no hit.
    """
    for rank, chunk in enumerate(top_chunks, start=1):
        content = chunk.get("content") or ""
        for phrase in query.must_contain_any:
            if phrase and phrase in content:
                return rank
    return None


async def _run_query(
    client: httpx.AsyncClient,
    url: str,
    api_key: str,
    kb_id: str,
    q: Query,
    top_k: int,
    *,
    rerank: bool,
    query_rewrite: bool,
) -> QueryOutcome:
    endpoint = f"{url}/v1/knowledgebases/{kb_id}/query"
    body = {
        "query": q.query,
        "top_k": top_k,
        "rerank": rerank,
        "query_rewrite": query_rewrite,
        # Eval only scores retrieval — skip the ~8 s LLM answer
        # generation so a full run drops from ~11 min to under a
        # minute. ``sources`` is still populated.
        "retrieve_only": True,
    }
    headers = {"X-API-Key": api_key}
    t0 = time.monotonic()
    resp = await client.post(endpoint, json=body, headers=headers, timeout=60.0)
    latency_ms = (time.monotonic() - t0) * 1000
    resp.raise_for_status()
    data = resp.json()
    sources = data.get("sources") or []

    top = [
        {
            "chunk_id": s.get("chunk_id"),
            "score":    s.get("score"),
            "content":  s.get("content") or "",
        }
        for s in sources
    ]
    rank = _find_hit_rank(q, top)
    rr = 1.0 / rank if rank else 0.0
    # Trim chunk content in the kept record so --out report isn't huge.
    for t in top:
        t["content_snippet"] = (t.pop("content") or "")[:160]
    return QueryOutcome(
        query_id=q.id, query=q.query, tier=q.tier,
        config=f"rerank={rerank},rewrite={query_rewrite}",
        hit_rank=rank, reciprocal_rank=rr,
        latency_ms=latency_ms, top_chunks=top,
    )


CONFIGS = [
    ("baseline (hybrid only)",     dict(rerank=False, query_rewrite=False)),
    ("+ query_rewrite",            dict(rerank=False, query_rewrite=True)),
    ("+ rerank",                   dict(rerank=True,  query_rewrite=False)),
    ("+ rerank + query_rewrite",   dict(rerank=True,  query_rewrite=True)),
]


async def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--dataset",
        default=str(Path(__file__).parent / "dataset.yaml"),
        help="Path to dataset.yaml",
    )
    parser.add_argument(
        "--tier",
        choices=("direct", "topical", "conceptual", "all"),
        default="all",
        help="Restrict to a specific tier",
    )
    parser.add_argument(
        "--configs",
        default="",
        help=(
            "Comma-separated indices into CONFIGS (e.g. '0,2' for "
            "baseline + rerank). Default: all four configs."
        ),
    )
    parser.add_argument(
        "--out",
        default="",
        help="Optional path to write the JSON report",
    )
    parser.add_argument(
        "--verbose", "-v", action="store_true",
        help="Print per-query result, not just aggregates",
    )
    args = parser.parse_args()

    url = os.environ.get("VECTORIA_URL", "").rstrip("/")
    api_key = os.environ.get("VECTORIA_API_KEY", "")
    if not url or not api_key:
        print(
            "error: set VECTORIA_URL and VECTORIA_API_KEY in environment",
            file=sys.stderr,
        )
        return 2

    with open(args.dataset) as f:
        data = yaml.safe_load(f)
    kb_id = data["kb_id"]
    top_k = int(data.get("top_k", 5))
    queries = [Query(**q) for q in data["queries"]]
    if args.tier != "all":
        queries = [q for q in queries if q.tier == args.tier]
    if not queries:
        print(f"no queries match tier={args.tier}", file=sys.stderr)
        return 2

    configs = CONFIGS
    if args.configs:
        idx = [int(i) for i in args.configs.split(",")]
        configs = [CONFIGS[i] for i in idx]

    print(f"kb_id={kb_id}")
    print(f"queries={len(queries)} (top_k={top_k})")
    print(f"configs={[c[0] for c in configs]}")
    print()

    all_outcomes: list[ConfigOutcome] = []
    async with httpx.AsyncClient() as client:
        for cfg_name, cfg_kwargs in configs:
            outcome = ConfigOutcome(name=cfg_name)
            for q in queries:
                r = await _run_query(
                    client, url, api_key, kb_id, q, top_k, **cfg_kwargs,
                )
                outcome.per_query.append(r)
                outcome.reciprocal_ranks.append(r.reciprocal_rank)
                outcome.latencies_ms.append(r.latency_ms)
                if r.hit_rank is not None:
                    if r.hit_rank <= 1: outcome.hits_at_1 += 1
                    if r.hit_rank <= 3: outcome.hits_at_3 += 1
                    if r.hit_rank <= 5: outcome.hits_at_5 += 1
                if args.verbose:
                    hit = f"rank={r.hit_rank}" if r.hit_rank else "MISS"
                    print(f"  [{cfg_name}] {q.id} {q.tier:10s} {hit:8s} {r.latency_ms:.0f}ms  {q.query}")
            all_outcomes.append(outcome)

    # Aggregate printout: one row per config.
    print("=" * 110)
    print(f"{'config':<30} {'hit@1':>8} {'hit@3':>8} {'hit@5':>8} {'mrr':>8} {'p50(ms)':>10} {'p95(ms)':>10}")
    print("-" * 110)
    for o in all_outcomes:
        s = o.summary(len(queries))
        print(
            f"{s['config']:<30} "
            f"{s['hit_at_1']:>8.3f} {s['hit_at_3']:>8.3f} {s['hit_at_5']:>8.3f} "
            f"{s['mrr']:>8.3f} {s['latency_ms_p50']:>10.1f} {s['latency_ms_p95']:>10.1f}"
        )

    # Per-tier breakdown on the FIRST config — lets you see where the
    # baseline struggles without cluttering the aggregate table.
    print()
    baseline = all_outcomes[0]
    tiers = sorted({q.tier for q in queries})
    print(f"Per-tier breakdown ({baseline.name}):")
    for t in tiers:
        in_tier = [o for o in baseline.per_query if o.tier == t]
        if not in_tier:
            continue
        hits5 = sum(1 for o in in_tier if o.hit_rank and o.hit_rank <= 5)
        mrr = statistics.mean(o.reciprocal_rank for o in in_tier)
        print(f"  {t:<12s} n={len(in_tier):<3d} hit@5={hits5/len(in_tier):.2f}  mrr={mrr:.2f}")

    if args.out:
        report = {
            "kb_id": kb_id, "top_k": top_k,
            "n_queries": len(queries),
            "configs": [
                {
                    **o.summary(len(queries)),
                    "per_query": [
                        {
                            "id": r.query_id, "tier": r.tier, "query": r.query,
                            "hit_rank": r.hit_rank,
                            "reciprocal_rank": round(r.reciprocal_rank, 3),
                            "latency_ms": round(r.latency_ms, 1),
                            "top_chunks": r.top_chunks,
                        }
                        for r in o.per_query
                    ],
                }
                for o in all_outcomes
            ],
        }
        Path(args.out).write_text(json.dumps(report, ensure_ascii=False, indent=2))
        print(f"\nreport written to {args.out}")

    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
