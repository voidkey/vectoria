def test_siteprofile_roundtrip():
    from parsers.capture.profile import (
        SiteProfile, Fonts, FontRole, CatalogMatch, Spacing, TextInfo, MotionHints,
    )
    p = SiteProfile(
        url="https://x", captured_at="2026-07-14T00:00:00Z", fetch_tier="playwright",
        fonts=Fonts(
            display=FontRole(family="Inter", stack="Inter, sans-serif",
                             sample_selector="h1", catalog_match=CatalogMatch(matched=False),
                             renderable=False),
            body=FontRole(family="Inter", stack="Inter, sans-serif",
                          sample_selector="p", catalog_match=CatalogMatch(matched=False),
                          renderable=False),
        ),
        spacing=Spacing(), text=TextInfo(headline="Hi"), motion_hints=MotionHints(),
    )
    d = p.model_dump()
    assert d["url"] == "https://x"
    assert d["fonts"]["display"]["family"] == "Inter"
    p2 = SiteProfile.model_validate(d)
    assert p2.text.headline == "Hi"
