from parsers.base import AntiBotBlockedError, PermanentParseError


def test_antibot_is_permanent_subclass():
    """AntiBotBlockedError 必须是 PermanentParseError 子类,
    才能复用 worker 的"不 fallback / 不重试 / 不死信"流转。"""
    assert issubclass(AntiBotBlockedError, PermanentParseError)
    err = AntiBotBlockedError("blocked: captcha")
    assert isinstance(err, PermanentParseError)
    assert "captcha" in str(err)
