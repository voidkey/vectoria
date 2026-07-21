"""Phase 1: new SiteProfile fields for tokens.json fidelity (headings/svgs/
page/rich sections)."""


def _base_profile(**extra):
    from parsers.capture.profile import (
        SiteProfile, Fonts, FontRole, CatalogMatch, Spacing, TextInfo, MotionHints,
    )
    return SiteProfile(
        url="https://x", captured_at="2026-07-21T00:00:00Z", fetch_tier="playwright",
        fonts=Fonts(
            display=FontRole(family="Inter", stack="Inter, sans-serif",
                             sample_selector="h1", catalog_match=CatalogMatch(matched=False),
                             renderable=False),
            body=FontRole(family="Inter", stack="Inter, sans-serif",
                          sample_selector="p", catalog_match=CatalogMatch(matched=False),
                          renderable=False),
        ),
        spacing=Spacing(), text=TextInfo(headline="Hi"), motion_hints=MotionHints(),
        **extra,
    )


def test_new_token_fields_roundtrip():
    from parsers.capture.profile import Heading, SvgInfo, PageGeom, SectionInfo

    p = _base_profile(
        css_variables={"--brand": "#f00", "--radius": "8px"},
        headings=[Heading(level=1, text="Hero", font_size="48px",
                          font_weight="700", color="#111")],
        svgs=[SvgInfo(label="logo", view_box="0 0 24 24", width=24, height=24,
                      is_logo=True)],
        page=PageGeom(width=1440, height=5000, viewport_width=1280,
                      viewport_height=800),
        sections=[SectionInfo(index=0, heading="Hero", type="hero",
                              bg_color="#0b0b0f", layout="split",
                              background_image="https://x/bg.png",
                              cta_texts=["Start", "Learn"],
                              asset_urls=["https://x/a.png"],
                              text="hero body text")],
    )
    d = p.model_dump()

    assert d["css_variables"] == {"--brand": "#f00", "--radius": "8px"}
    h = d["headings"][0]
    assert h["level"] == 1 and h["text"] == "Hero"
    assert h["font_size"] == "48px" and h["font_weight"] == "700" and h["color"] == "#111"
    s = d["svgs"][0]
    assert s == {"label": "logo", "view_box": "0 0 24 24", "width": 24,
                 "height": 24, "is_logo": True}
    # DB-bloat guard: SvgInfo must never carry raw markup.
    assert "outerHTML" not in s and "outer_html" not in s
    assert d["page"] == {"width": 1440, "height": 5000,
                         "viewport_width": 1280, "viewport_height": 800}
    sec = d["sections"][0]
    assert sec["layout"] == "split"
    assert sec["background_image"] == "https://x/bg.png"
    assert sec["cta_texts"] == ["Start", "Learn"]
    assert sec["asset_urls"] == ["https://x/a.png"]
    assert sec["text"] == "hero body text"


def test_colors_ranked_and_color_stats_roundtrip():
    """Phase 2: SiteProfile carries the reference-shaped colors_ranked (top-20
    hex strings) and color_stats (top-48 raw stat dicts) alongside role tokens."""
    stats = [{"hex": "#0B0B0F", "count": 42, "bgCount": 30, "interactiveBg": 3,
              "areaBg": 5, "textCount": 2, "maxArea": 900000}]
    p = _base_profile(colors_ranked=["#0B0B0F", "#FFFFFF", "#FF3366"],
                      color_stats=stats)
    d = p.model_dump()
    assert d["colors_ranked"] == ["#0B0B0F", "#FFFFFF", "#FF3366"]
    assert d["color_stats"] == stats
    assert d["color_stats"][0]["interactiveBg"] == 3  # verbatim hyperframes field name


def test_colors_ranked_and_color_stats_default():
    """Both new fields default to [] and legacy dicts (without them) validate."""
    p = _base_profile()
    d = p.model_dump()
    assert d["colors_ranked"] == []
    assert d["color_stats"] == []

    from parsers.capture.profile import SiteProfile
    legacy = {k: v for k, v in d.items()
              if k not in ("colors_ranked", "color_stats")}
    rt = SiteProfile.model_validate(legacy)
    assert rt.colors_ranked == [] and rt.color_stats == []


def test_new_token_fields_default_and_backward_compat():
    """Old profile dicts (without the new keys) still validate; new fields default."""
    p = _base_profile()
    d = p.model_dump()
    assert d["css_variables"] == {}
    assert d["headings"] == []
    assert d["svgs"] == []
    assert d["page"] is None

    # A legacy section dict without the new keys still validates.
    from parsers.capture.profile import SectionInfo
    sec = SectionInfo(index=0, heading="H")
    sd = sec.model_dump()
    assert sd["layout"] == "" and sd["background_image"] == ""
    assert sd["cta_texts"] == [] and sd["asset_urls"] == [] and sd["text"] == ""

    # Round-trip a legacy-shaped SiteProfile dict (no Phase-1 keys).
    from parsers.capture.profile import SiteProfile
    legacy = {k: v for k, v in d.items()
              if k not in ("css_variables", "headings", "svgs", "page")}
    SiteProfile.model_validate(legacy)
