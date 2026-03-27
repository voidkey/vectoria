from openai import AsyncOpenAI
from rag.steps.base import PipelineStep, PipelineContext
from config import get_settings

_SYSTEM = "You are a search query optimizer. Rewrite the user's question to improve document retrieval. Return only the rewritten query, nothing else."


class QueryRewriteStep(PipelineStep):
    def __init__(self, llm_client: AsyncOpenAI, enabled: bool = True):
        self.enabled = enabled
        self._client = llm_client
        self._model = get_settings().llm_model

    async def run(self, ctx: PipelineContext) -> PipelineContext:
        if not self.enabled:
            ctx.rewritten_query = ctx.query
            return ctx
        try:
            resp = await self._client.chat.completions.create(
                model=self._model,
                messages=[
                    {"role": "system", "content": _SYSTEM},
                    {"role": "user", "content": ctx.query},
                ],
                max_tokens=200,
                temperature=0,
            )
            ctx.rewritten_query = resp.choices[0].message.content.strip()
        except Exception:
            ctx.rewritten_query = ctx.query
        return ctx
