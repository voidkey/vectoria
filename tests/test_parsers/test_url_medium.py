import pytest
from lxml import etree

import parsers.url._medium as m
from parsers.base import AntiBotBlockedError, ParseResult
from parsers.url._medium import MediumHandler, feed_and_post

_FEED = b"""<?xml version="1.0" encoding="UTF-8"?>
<rss xmlns:content="http://purl.org/rss/1.0/modules/content/" version="2.0"><channel>
<title>Pub</title>
<item>
  <title><![CDATA[Other post]]></title>
  <link>https://medium.com/pub/other-post-111111111111?source=rss</link>
  <guid isPermaLink="false">https://medium.com/p/111111111111</guid>
  <content:encoded><![CDATA[<p>Not this one.</p>]]></content:encoded>
</item>
<item>
  <title><![CDATA[Target post]]></title>
  <link>https://medium.com/pub/target-post-abcdef012345?source=rss</link>
  <guid isPermaLink="false">https://medium.com/p/abcdef012345</guid>
  <content:encoded><![CDATA[<h4>Subtitle</h4><figure><img src="https://cdn-images-1.medium.com/max/1024/1*x.png" /></figure>
<p>The first paragraph of the target post carries enough words to be extracted as real body text.</p>
<p>A second paragraph keeps going so the extractor treats this as an article rather than boilerplate.</p>]]></content:encoded>
</item>
</channel></rss>"""


def _feed_root():
    return etree.fromstring(_FEED, m._XML_PARSER)


def test_match_medium_hosts():
    h = MediumHandler()
    assert h.match("https://medium.com/blog/x-002977326982")
    assert h.match("https://www.medium.com/@someone/x-abcdef012345")
    assert h.match("https://someone.medium.com/x-abcdef012345")
    assert not h.match("https://miro.medium.com/v2/1*x.png")
    assert not h.match("https://cdn-images-1.medium.com/max/1024/1*x.png")
    assert not h.match("https://notmedium.com/x")


def test_registration_order_medium_before_generic():
    from parsers.url import find_handler
    assert isinstance(find_handler("https://medium.com/blog/x-002977326982"), MediumHandler)


def test_medium_exempt_from_domain_cooldown():
    """A blocked generic fallback must not cooldown the domain — that would
    skip the unaffected feed path for every other Medium URL."""
    assert MediumHandler().trips_cooldown is False


@pytest.mark.parametrize("url,feed,post", [
    ("https://medium.com/blog/dark-mode-002977326982",
     "https://medium.com/feed/blog", "002977326982"),
    ("https://medium.com/@someone/a-title-abcdef012345?source=x",
     "https://medium.com/feed/@someone", "abcdef012345"),
    ("https://someone.medium.com/a-title-abcdef012345",
     "https://someone.medium.com/feed", "abcdef012345"),
    ("https://medium.com/p/abcdef012345", None, "abcdef012345"),
    ("https://medium.com/blog", "https://medium.com/feed/blog", None),
    ("https://medium.com/", None, None),
])
def test_feed_and_post(url, feed, post):
    assert feed_and_post(url) == (feed, post)


async def test_article_resolved_from_feed(monkeypatch):
    seen = []

    async def fake_feed(url):
        seen.append(url)
        return _feed_root()

    monkeypatch.setattr(m, "_fetch_feed", fake_feed)
    r = await MediumHandler().parse("https://medium.com/pub/target-post-abcdef012345")
    assert seen == ["https://medium.com/feed/pub"]
    assert r.title == "Target post"
    assert "first paragraph of the target post" in r.content
    assert "Not this one" not in r.content
    assert r.image_urls == ["https://cdn-images-1.medium.com/max/1024/1*x.png"]


async def test_post_missing_from_feed_falls_back_to_generic(monkeypatch):
    async def fake_feed(url):
        return _feed_root()

    async def fake_generic(self, url):
        return ParseResult(content="via generic", title="g")

    monkeypatch.setattr(m, "_fetch_feed", fake_feed)
    monkeypatch.setattr(m.GenericHandler, "parse", fake_generic)
    r = await MediumHandler().parse("https://medium.com/pub/old-post-999999999999")
    assert r.content == "via generic"


async def test_generic_block_reports_feed_limit(monkeypatch):
    async def no_feed(url):
        return None

    async def blocked(self, url):
        raise AntiBotBlockedError(f"JS challenge page (just a moment) at {url}")

    monkeypatch.setattr(m, "_fetch_feed", no_feed)
    monkeypatch.setattr(m.GenericHandler, "parse", blocked)
    with pytest.raises(AntiBotBlockedError, match="RSS"):
        await MediumHandler().parse("https://medium.com/pub/old-post-999999999999")
