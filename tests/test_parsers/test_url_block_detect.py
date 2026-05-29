from parsers.base import AntiBotBlockedError, PermanentParseError


def test_antibot_is_permanent_subclass():
    """AntiBotBlockedError 必须是 PermanentParseError 子类,
    才能复用 worker 的"不 fallback / 不重试 / 不死信"流转。"""
    assert issubclass(AntiBotBlockedError, PermanentParseError)
    err = AntiBotBlockedError("blocked: captcha")
    assert isinstance(err, PermanentParseError)
    assert "captcha" in str(err)


from parsers.url._handlers import detect_block_reason, DEFAULT_BROWSER_UA


def test_detect_block_by_title():
    assert detect_block_reason("<html><body>x</body></html>", "百度安全验证") is not None
    assert detect_block_reason("<html></html>", "百度百科-验证") is not None


def test_detect_block_by_short_body_marker():
    html = "<html><body>请完成下方验证后继续操作 正在验证...</body></html>"
    assert detect_block_reason(html, "加载中") is not None


def test_detect_block_js_challenge():
    html = "<html><body>Please enable JavaScript to continue</body></html>"
    assert detect_block_reason(html, "Just a moment") is not None


def test_no_false_positive_on_long_article_mentioning_captcha():
    body = "本文讨论 captcha 与人机验证的安全机制。" + ("内容正文。" * 200)
    html = f"<html><body>{body}</body></html>"
    assert detect_block_reason(html, "验证码技术综述") is None


def test_no_false_positive_on_normal_short_page():
    html = "<html><body>这是一篇正常的短笔记,记录今天的天气和心情。</body></html>"
    assert detect_block_reason(html, "今日随笔") is None


def test_default_ua_is_browser_like():
    assert "Mozilla/5.0" in DEFAULT_BROWSER_UA
    assert "python-httpx" not in DEFAULT_BROWSER_UA.lower()
