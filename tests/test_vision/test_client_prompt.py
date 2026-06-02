from vision.client import _describe_system_prompt, _parse_system_prompt


def test_describe_prompt_injects_language():
    p = _describe_system_prompt("Portuguese")
    assert "Respond in Portuguese" in p


def test_parse_prompt_injects_language_and_fixed_headers():
    p = _parse_system_prompt("Spanish")
    assert "## Description" in p
    assert "## Verbatim" in p
    assert "in Spanish" in p
    assert "not translate" in p.lower()
