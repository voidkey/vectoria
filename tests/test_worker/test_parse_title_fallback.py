"""When a parser reports no title of its own, the document falls back
to a title derived from its source. For URL-sourced documents that
source is the raw URL, which is unreadable for mobile share links — the
same input that used to overflow the ``title`` column.
"""
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from parsers.base import ParseResult

WECHAT_SHARE_URL = (
    "https://mp.weixin.qq.com/s/P2HDXN-FK89R5iQP7vrkwA"
    "?xtrack=1&scene=90&subscene=93&sessionid=1785720292"
    "&devicetype=android-36&version=28004750&nettype=WIFI&lang=zh_CN"
)


def _build_session(doc):
    session = AsyncMock()
    result = MagicMock()
    result.scalar_one_or_none.return_value = doc
    session.execute = AsyncMock(return_value=result)
    return session


async def _run_url_parse(parse_result: ParseResult) -> list[dict]:
    fake_parser = MagicMock()
    fake_parser.parse = AsyncMock(return_value=parse_result)

    doc = MagicMock()
    doc.status = "queued"
    update_calls: list[dict] = []

    async def _update(_doc_id, **fields):
        update_calls.append(fields)

    with (
        patch("worker.handlers.get_session") as mock_sess,
        patch("worker.handlers.registry") as mock_reg,
        patch("worker.handlers.update_doc", new=_update),
        patch("worker.queue.enqueue", new=AsyncMock()),
    ):
        mock_sess.return_value.__aenter__.return_value = _build_session(doc)
        mock_reg.get_by_engine.return_value = fake_parser

        from worker.handlers import handle_parse_document
        await handle_parse_document({
            "doc_id": "d1", "kb_id": "k1",
            "storage_key": None,
            "source": WECHAT_SHARE_URL, "filename": "",
            "selected_engine": "url",
        })
    return update_calls


@pytest.mark.asyncio
async def test_untitled_url_doc_falls_back_to_host_and_path():
    """A parser that extracts body text but no title must not label the
    doc with the full tracking-parameter URL."""
    updates = await _run_url_parse(
        ParseResult(content="Body text with enough chars to pass. " * 10, title=""),
    )
    titles = [u["title"] for u in updates if "title" in u]
    assert titles == ["mp.weixin.qq.com/s/P2HDXN-FK89R5iQP7vrkwA"]


@pytest.mark.asyncio
async def test_parser_supplied_title_still_wins():
    updates = await _run_url_parse(
        ParseResult(content="Body text with enough chars to pass. " * 10,
                    title="全球首个胆道闭锁大型III期临床药物试验有结果了！"),
    )
    titles = [u["title"] for u in updates if "title" in u]
    assert titles == ["全球首个胆道闭锁大型III期临床药物试验有结果了！"]
