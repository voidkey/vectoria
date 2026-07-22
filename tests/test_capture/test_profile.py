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
    # Phase 6: animation catalog + shaders default (backward-compat).
    assert d["animation_catalog"] is None
    assert d["shaders"] == []
    assert p2.animation_catalog is None
    assert p2.shaders == []
    # Phase 7: video_manifest default (backward-compat).
    assert d["video_manifest"] is None
    assert p2.video_manifest is None


def test_siteprofile_carries_animation_catalog_and_shaders():
    from parsers.capture.profile import (
        SiteProfile, Fonts, FontRole, CatalogMatch, Spacing, TextInfo, MotionHints,
    )
    cat = {"webAnimations": [], "cssDeclarations": [], "scrollTargets": [],
           "cdpAnimations": [], "summary": {"webAnimations": 0, "cssDeclarations": 0,
           "scrollTargets": 0, "cdpAnimations": 0, "canvases": 1}}
    p = SiteProfile(
        url="https://x", captured_at="2026-07-14T00:00:00Z",
        fonts=Fonts(
            display=FontRole(family="Inter", stack="Inter", sample_selector="h1",
                             catalog_match=CatalogMatch(matched=False), renderable=False),
            body=FontRole(family="Inter", stack="Inter", sample_selector="p",
                          catalog_match=CatalogMatch(matched=False), renderable=False)),
        spacing=Spacing(), text=TextInfo(headline="Hi"), motion_hints=MotionHints(),
        animation_catalog=cat, shaders=[{"type": "vertex", "source": "void main(){}"}])
    d = p.model_dump()
    assert d["animation_catalog"]["summary"]["canvases"] == 1
    assert d["shaders"][0]["source"] == "void main(){}"
    # Old profile dict lacking the new keys still validates (backward-compat).
    old = {k: v for k, v in d.items() if k not in ("animation_catalog", "shaders")}
    p3 = SiteProfile.model_validate(old)
    assert p3.animation_catalog is None
    assert p3.shaders == []


def test_siteprofile_carries_video_manifest():
    from parsers.capture.profile import (
        SiteProfile, Fonts, FontRole, CatalogMatch, Spacing, TextInfo, MotionHints,
    )
    vm = [{"index": 0, "url": "https://x/hero.mp4", "filename": "hero.mp4",
           "width": 1280, "height": 720, "sourceWidth": 1920, "sourceHeight": 1080,
           "heading": "", "caption": "", "ariaLabel": "",
           "localPath": "assets/videos/video-0.mp4"}]
    p = SiteProfile(
        url="https://x", captured_at="2026-07-14T00:00:00Z",
        fonts=Fonts(
            display=FontRole(family="Inter", stack="Inter", sample_selector="h1",
                             catalog_match=CatalogMatch(matched=False), renderable=False),
            body=FontRole(family="Inter", stack="Inter", sample_selector="p",
                          catalog_match=CatalogMatch(matched=False), renderable=False)),
        spacing=Spacing(), text=TextInfo(headline="Hi"), motion_hints=MotionHints(),
        video_manifest=vm)
    d = p.model_dump()
    assert d["video_manifest"][0]["url"] == "https://x/hero.mp4"
    # Old profile dict lacking the key still validates (backward-compat).
    old = {k: v for k, v in d.items() if k != "video_manifest"}
    p2 = SiteProfile.model_validate(old)
    assert p2.video_manifest is None
