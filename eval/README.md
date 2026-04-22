# Retrieval evaluation — vectoria

A small, curated query set + runner that answers **"how good is
our retrieval?"** with numbers instead of feelings.

## Why this exists

W1–W6 shipped a pile of retrieval changes (CJK trigram keyword
search, HNSW vector index, rerank input cap, fusion tweaks) without
a quantitative baseline. This harness lets any future change be
compared against a fixed reference.

Example decisions you can now make with data instead of guessing:

- Does turning the reranker on help? By how much? Is it worth the
  per-request cost?
- Does query rewrite actually improve recall, or just burn LLM tokens?
- If we change chunk_size from 1024 to 512, does hit@5 go up or down?
- If someone swaps the embedding model, did anything regress?

## How it works

1. `dataset.yaml` holds ~20 Chinese queries against a specific KB,
   each with "expected phrases" that must appear in at least one
   returned chunk for the query to count as a hit.
2. `run.py` POSTs each query to `/v1/knowledgebases/{kb_id}/query`
   under multiple config combinations, computes hit@k + MRR per
   config, and prints a summary table.
3. Labels use **content substrings**, not chunk IDs, so the eval
   survives re-ingestion and chunk-boundary reshuffles.

## Usage

Against deploy-host through an SSH tunnel:

```bash
# In one terminal — tunnel the API:
ssh -L 8000:localhost:8000 deploy-host

# In another:
export VECTORIA_URL=http://localhost:8000
export VECTORIA_API_KEY=<your-key>
uv run python -m eval.run
```

Options:

```bash
# Restrict to one tier (direct / topical / conceptual)
python -m eval.run --tier conceptual

# Only run the baseline config (no rerank, no query_rewrite)
python -m eval.run --configs 0

# Per-query verbose output
python -m eval.run -v

# Dump JSON report (diff against previous runs)
python -m eval.run --out eval/reports/baseline-$(date +%F).json
```

## Reading the numbers

```
config                          hit@1    hit@3    hit@5     mrr   p50(ms)   p95(ms)
baseline (hybrid only)          0.500    0.750    0.900    0.650     320.0     680.0
+ query_rewrite                 0.550    0.780    0.900    0.680     820.0    1200.0
+ rerank                        0.700    0.850    0.900    0.780     680.0    1100.0
+ rerank + query_rewrite        0.720    0.870    0.900    0.790    1180.0    1800.0
```

Read this as:
- **baseline hit@5 = 0.90** → retrieval gets relevant content in
  the top-5 for 90% of queries. Good.
- **baseline hit@1 = 0.50** → but ranks it first only half the
  time. Reranker candidate.
- **+ rerank** bumps hit@1 to 0.70 → reranker earns its keep. At
  ~360ms extra latency per query — operator decides if it's worth
  it for the user experience.
- **+ query_rewrite** moves the needle only slightly while adding
  ~500ms. Probably not worth the cost in this dataset; might matter
  more for short/ambiguous queries.
- **p95 (ms)** — total API latency (including embedding, LLM
  answer generation, etc.). Useful for SLO gating.

## Tiers

- **direct** — query phrase appears almost verbatim in the KB.
  Trigram keyword search alone should nail these. Regression here
  means something is broken in the basic path.
- **topical** — query is a paraphrase / different wording of the
  anchor text. Vector search is the primary contributor.
- **conceptual** — query requires some semantic bridging; anchor
  phrases are loose (single word / topic). Hardest tier; gap between
  baseline and +rerank will likely be biggest here.

## Extending the set

- Add a query block to `dataset.yaml`.
- Keep `must_contain_any` phrases short (5–15 chars) so they're
  robust against chunk-boundary placement.
- When a query genuinely has multiple correct answer chunks, list
  an anchor phrase from each — the eval counts a hit if ANY matches.
- Re-run and save the report as a new baseline.

## Limits of this harness

- Hit@k measures "did we retrieve ANY relevant chunk". It doesn't
  measure answer quality (the LLM generation step isn't scored).
  For answer quality, sample outputs by hand or wire an LLM-as-judge.
- Content-substring labeling is forgiving — it says "this is
  relevant" when the phrase appears, even if the chunk is about
  something else that mentions the phrase in passing. For very
  loose phrases this can inflate hit rates.
- The dataset is small (20 queries) and monolingual (zh). Multiply
  and translate as your real traffic shape demands.
