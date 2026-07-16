from api.errors import Action, ErrorCode, ERROR_META, error_meta, error_fields


def _all_codes() -> list[int]:
    return [v for k, v in vars(ErrorCode).items()
            if not k.startswith("_") and isinstance(v, int)]


def test_every_code_has_meta():
    """Registry completeness: no code may lack retryable/suggested_action."""
    missing = [c for c in _all_codes() if c not in ERROR_META]
    assert missing == [], f"ErrorCode(s) with no ERROR_META entry: {missing}"


def test_meta_values_are_typed():
    for code, meta in ERROR_META.items():
        assert isinstance(meta.retryable, bool)
        assert isinstance(meta.action, Action)


def test_new_link_codes_exist():
    # Numeric values are part of the public API contract — pinned on purpose.
    assert ErrorCode.LINK_VIDEO_UNSUPPORTED == 1501
    assert ErrorCode.LINK_LOGIN_REQUIRED == 1502
    assert ErrorCode.LINK_ANTIBOT_BLOCKED == 1503
    assert ErrorCode.LINK_REGION_BLOCKED == 1504
    assert ErrorCode.LINK_PAGE_GONE == 1505
    assert ErrorCode.LINK_FORBIDDEN == 1506
    assert ErrorCode.LINK_FETCH_TIMEOUT == 1507
    assert ErrorCode.PARSE_UNRESOLVABLE == 1299


def test_error_fields_known_code():
    f = error_fields(ErrorCode.LINK_FETCH_TIMEOUT)
    assert f == {"error_code": 1507, "retryable": True,
                 "suggested_action": "retry_later"}


def test_error_fields_none_code():
    """NULL error_code (success / legacy row) → all three null, no default."""
    assert error_fields(None) == {
        "error_code": None, "retryable": None, "suggested_action": None}


def test_error_meta_unknown_code():
    assert error_meta(424242) is None
