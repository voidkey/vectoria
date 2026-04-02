import asyncio
import logging
import random

from openai import AsyncOpenAI, APIConnectionError, APIStatusError, APITimeoutError

from config import get_settings

logger = logging.getLogger(__name__)

_MAX_CONCURRENCY = 5
_MAX_RETRIES = 3


class Embedder:
    def __init__(self):
        cfg = get_settings()
        self._client = AsyncOpenAI(
            base_url=cfg.effective_embedding_base_url,
            api_key=cfg.effective_embedding_api_key,
        )
        self._model = cfg.embedding_model
        self._batch_size = cfg.embedding_batch_size

    async def embed(self, text: str) -> list[float]:
        result = await self._embed_with_retry([text])
        return result[0]

    async def embed_batch(self, texts: list[str]) -> list[list[float]]:
        bs = self._batch_size
        batches = [texts[i : i + bs] for i in range(0, len(texts), bs)]
        sem = asyncio.Semaphore(_MAX_CONCURRENCY)

        async def _embed_one_batch(batch: list[str]) -> list[list[float]]:
            async with sem:
                return await self._embed_with_retry(batch)

        batch_results = await asyncio.gather(
            *(_embed_one_batch(b) for b in batches)
        )
        return [vec for batch_result in batch_results for vec in batch_result]

    async def _embed_with_retry(self, texts: list[str]) -> list[list[float]]:
        delay = 1.0
        last_exc: Exception | None = None
        for attempt in range(_MAX_RETRIES):
            try:
                resp = await self._client.embeddings.create(
                    input=texts, model=self._model,
                )
                return [d.embedding for d in sorted(resp.data, key=lambda d: d.index)]
            except APIStatusError as e:
                if e.status_code < 500 and e.status_code != 429:
                    raise
                last_exc = e
            except (APIConnectionError, APITimeoutError, OSError) as e:
                last_exc = e
            if attempt < _MAX_RETRIES - 1:
                jitter = random.uniform(0, delay * 0.5)
                logger.warning(
                    "Embedding attempt %d/%d failed, retrying in %.1fs",
                    attempt + 1, _MAX_RETRIES, delay + jitter,
                )
                await asyncio.sleep(delay + jitter)
                delay = min(delay * 2, 10.0)
        raise last_exc  # type: ignore[misc]


_embedder: Embedder | None = None


def get_embedder() -> Embedder:
    global _embedder  # noqa: PLW0603
    if _embedder is None:
        _embedder = Embedder()
    return _embedder
