from openai import AsyncOpenAI
from rag.steps.base import PipelineStep, PipelineContext
from config import get_settings

_SYSTEM = """You are a helpful assistant. Answer the user's question based on the provided context.
Cite your sources using [1], [2], etc. at the end of sentences. Only use information from the context."""


class GenerateStep(PipelineStep):
    def __init__(self, llm_client: AsyncOpenAI):
        super().__init__()
        self._client = llm_client
        self._model = get_settings().llm_model

    async def run(self, ctx: PipelineContext) -> PipelineContext:
        if not ctx.final_results:
            ctx.answer = "No relevant information found."
            ctx.sources = []
            return ctx

        context_block = "\n\n".join(
            f"[{i+1}] {r.content}" for i, r in enumerate(ctx.final_results)
        )
        resp = await self._client.chat.completions.create(
            model=self._model,
            messages=[
                {"role": "system", "content": _SYSTEM},
                {"role": "user", "content": f"Context:\n{context_block}\n\nQuestion: {ctx.query}"},
            ],
            temperature=0.3,
        )
        ctx.answer = resp.choices[0].message.content.strip()
        ctx.sources = [
            {"chunk_id": r.chunk_id, "content": r.content, "score": r.score, "doc_id": r.doc_id}
            for r in ctx.final_results
        ]
        return ctx
