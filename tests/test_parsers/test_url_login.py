"""Login / permission-wall classification (error_code 1502).

A URL that a real person could open only after logging in or being granted
access must be reported as "需要登录 / 无权限" (LINK_LOGIN_REQUIRED = 1502),
not as "无可读文字" (EMPTY_CONTENT = 1202) or "风控/人机验证"
(LINK_ANTIBOT_BLOCKED = 1503). Mis-classifying it sends the user the wrong
next step ("turn your images into a text PDF" instead of "log in / upload
the file"). See vectoria-parse-error-contract.md §1.
"""
from api.errors import ErrorCode
from parsers.base import LoginRequiredError, PermanentParseError
from parsers.url._handlers import detect_login_wall


def test_login_required_is_permanent_with_1502():
    """Inherits the permanent short-circuit (no fallback / no retry / no
    dead-task alert — login won't appear between attempts) and carries the
    frontend-facing 1502 the worker persists."""
    assert issubclass(LoginRequiredError, PermanentParseError)
    err = LoginRequiredError("needs login")
    assert isinstance(err, PermanentParseError)
    assert err.error_code == ErrorCode.LINK_LOGIN_REQUIRED == 1502


def test_detect_login_wall_by_chinese_permission_marker():
    html = "<html><body>抱歉，你暂时无权限访问该文档，请申请访问权限。</body></html>"
    assert detect_login_wall(html, "飞书文档") is not None


def test_detect_login_wall_by_login_prompt():
    html = "<html><body>登录后查看完整内容</body></html>"
    assert detect_login_wall(html, "请登录") is not None


def test_detect_login_wall_by_english_marker():
    html = "<html><body>You don't have access to this document. Request access.</body></html>"
    assert detect_login_wall(html, "Sign in") is not None


def test_no_false_positive_on_long_article_mentioning_login():
    """A real article that merely discusses login/permission systems must
    not be judged a wall — same short-body gate detect_block_reason uses."""
    body = "本文讲解如何设计登录后查看与申请访问权限的流程。" + ("正文内容。" * 200)
    html = f"<html><body>{body}</body></html>"
    assert detect_login_wall(html, "权限系统设计") is None


def test_no_false_positive_on_normal_short_page():
    html = "<html><body>这是一篇正常的短笔记，记录今天的安排。</body></html>"
    assert detect_login_wall(html, "今日随笔") is None


def test_no_false_positive_on_long_article_with_marker_in_title():
    """A real, parseable article whose TITLE happens to contain a marker
    phrase must not be judged a wall — otherwise a legit doc gets
    permanently failed as 1502 instead of ingested. The short-body gate
    must cover the title match too, not just the body."""
    body = ("本文系统梳理常见故障的定位方法与处理流程，覆盖网络、权限与配置多个方面。" * 40)
    html = f"<html><body>{body}</body></html>"
    # Titles that embed the least-specific markers (访问受限 / 申请权限 /
    # access denied / 无权限访问) on an otherwise long, real article.
    assert detect_login_wall(html, "访问受限的排查与解决") is None
    assert detect_login_wall(html, "如何申请权限：企业审批流程详解") is None
    assert detect_login_wall(html, "Access Denied 错误码大全") is None
    assert detect_login_wall(html, "无权限访问问题定位手册") is None
