from openai import AsyncOpenAI
from config import get_settings


class Embedder:
    def __init__(self):
        cfg = get_settings()
        self._client = AsyncOpenAI(
            base_url=cfg.effective_embedding_base_url,
            api_key=cfg.effective_embedding_api_key,
        )
        self._model = cfg.embedding_model

    async def embed(self, text: str) -> list[float]:
        resp = await self._client.embeddings.create(input=text, model=self._model)
        return resp.data[0].embedding

    async def embed_batch(self, texts: list[str]) -> list[list[float]]:
        resp = await self._client.embeddings.create(input=texts, model=self._model)
        return [d.embedding for d in resp.data]
