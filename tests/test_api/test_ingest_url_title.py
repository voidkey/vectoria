"""URL ingest seeds a placeholder title until the parser reports the
real one. Using the raw URL made that placeholder unreadable — WeChat
share links carry ~500 chars of tracking parameters — so the placeholder
is built from host + path only.
"""
from datetime import datetime
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from api.doc_title import title_from_url
from db.models import TITLE_MAX_LEN

# The link from the incident: 576 chars, all but 49 of them tracking
# parameters appended by the WeChat Android client.
WECHAT_SHARE_URL = (
    "https://mp.weixin.qq.com/s/P2HDXN-FK89R5iQP7vrkwA"
    "?search_click_id=18384194309335244928-1785720376010-3272815196"
    "&xtrack=1&scene=90&subscene=93&sessionid=1785720292&flutter_pos=0"
    "&clicktime=1785720294&enterid=1785720294&finder_biz_enter_id=4"
    "&jumppath=20020_1785720186408,50094_1785720290643,1001_1785720291433"
    ",50094_1785720293113&jumppathdepth=4&ascene=56&fasttmpl_type=0"
    "&fasttmpl_fullversion=8370000-zh_CN-zip&fasttmpl_flag=0"
    "&realreporttime=1785720294941&devicetype=android-36&version=28004750"
    "&nettype=WIFI&lang=zh_CN&session_us=gh_055e989364df&countrycode=EH"
    "&color_scheme=light"
)


def test_drops_scheme_and_query_string():
    assert title_from_url(WECHAT_SHARE_URL) == "mp.weixin.qq.com/s/P2HDXN-FK89R5iQP7vrkwA"


def test_bare_host_url_keeps_the_host():
    assert title_from_url("https://example.com") == "example.com"


def test_long_path_is_capped_to_the_column_width():
    long_url = "https://example.com/" + "a" * (TITLE_MAX_LEN * 2)
    assert len(title_from_url(long_url)) == TITLE_MAX_LEN


def test_unparseable_input_falls_back_to_the_raw_string():
    """Never return empty — an empty title would leave the document
    unidentifiable in the UI if parsing later fails."""
    assert title_from_url("not-a-url") == "not-a-url"


def _configure_session(session: AsyncMock, add_captures: list):
    def _execute(_stmt):
        r = MagicMock()
        r.scalar_one_or_none.return_value = None
        return r

    session.execute = AsyncMock(side_effect=_execute)
    session.add = MagicMock(side_effect=lambda d: add_captures.append(d))
    session.commit = AsyncMock()

    def _refresh(obj):
        obj.created_at = datetime(2026, 8, 5)

    session.refresh = AsyncMock(side_effect=_refresh)
    return session


@pytest.mark.asyncio
async def test_url_ingest_stores_readable_title_and_full_source(client):
    """The 576-char link used to blow up the INSERT (title is
    varchar(500)). Title is now the readable host+path; ``source`` still
    carries the full URL so the fetch and the URL dedup hash are
    unchanged.
    """
    captures: list = []

    with (
        patch("api.routes.documents.validate_url", new=AsyncMock()),
        patch("api.routes.documents._validate_kb", new=AsyncMock()),
        patch("api.routes.documents.get_session") as mock_sess,
        patch("worker.queue.enqueue_in_session", new=MagicMock()),
    ):
        session = AsyncMock()
        _configure_session(session, captures)
        mock_sess.return_value.__aenter__.return_value = session

        resp = await client.post(
            "/v1/knowledgebases/kb-x/documents/url",
            json={"url": WECHAT_SHARE_URL},
        )

    assert resp.status_code == 201, resp.text
    body = resp.json()
    assert body["title"] == "mp.weixin.qq.com/s/P2HDXN-FK89R5iQP7vrkwA"
    assert body["source"] == WECHAT_SHARE_URL
    assert captures[0].title == "mp.weixin.qq.com/s/P2HDXN-FK89R5iQP7vrkwA"
    assert captures[0].source == WECHAT_SHARE_URL


def test_trailing_slash_is_not_kept():
    assert title_from_url("https://example.com/blog/") == "example.com/blog"
