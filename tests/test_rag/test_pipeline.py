import pytest
from unittest.mock import AsyncMock, MagicMock, patch
from rag.pipeline import Pipeline, build_default_pipeline
from rag.steps.base import PipelineContext, PipelineStep
from vectorstore.base import SearchResult


class EchoStep(PipelineStep):
    def __init__(self):
        super().__init__()

    async def run(self, ctx: PipelineContext) -> PipelineContext:
        ctx.answer = f"echo:{ctx.query}"
        return ctx


class DisabledStep(PipelineStep):
    def __init__(self):
        super().__init__(enabled=False)

    async def run(self, ctx: PipelineContext) -> PipelineContext:
        ctx.answer = "should not run"
        return ctx


@pytest.mark.asyncio
async def test_pipeline_runs_enabled_steps():
    pipeline = Pipeline(steps=[EchoStep()])
    ctx = await pipeline.run("hello", kb_id="kb1")
    assert ctx.answer == "echo:hello"


@pytest.mark.asyncio
async def test_pipeline_skips_disabled_steps():
    pipeline = Pipeline(steps=[EchoStep(), DisabledStep()])
    ctx = await pipeline.run("hello", kb_id="kb1")
    assert ctx.answer == "echo:hello"
