from parsers.capture._fonts import build_font_role, cluster_spacing, section_type


def test_section_type_heuristics():
    assert section_type("Pricing", ["price-table"], 3, 5) == "pricing"
    assert section_type("", [], 0, 5) == "hero"
    assert section_type("", ["site-footer"], 4, 5) == "footer"
    assert section_type("Our Features", ["features"], 1, 5) == "features"
    assert section_type("Get started today", ["cta"], 2, 5) == "cta"
    assert section_type("Random", [], 2, 5) == "generic"


def test_section_type_chinese():
    assert section_type("选个方案，开始创作", [], 2, 10) == "pricing"
    assert section_type("看看用户怎么说", [], 3, 10) == "testimonial"
    assert section_type("更快更强的功能", [], 4, 10) == "features"
    assert section_type("免费开始创作", [], 5, 10) == "cta"


def test_cluster_spacing():
    scale = cluster_spacing([8, 9, 16, 15, 48, 47, 96])
    # near-duplicates (8/9, 15/16, 47/48) collapse to one representative each
    assert scale == [8, 15, 47, 96]
    assert scale == sorted(scale)
    # consecutive buckets are more than tol apart
    assert all(b - a > 3 for a, b in zip(scale, scale[1:]))
    assert cluster_spacing([-5, 0, 8, 8]) == [8]  # non-positive dropped, dedup


def test_cluster_spacing_drops_outliers():
    # a 33554400px pill/circle radius is not a real design token
    assert cluster_spacing([6, 10, 50, 33554400], max_val=500) == [6, 10, 50]


def test_build_font_role_miss(monkeypatch):
    from parsers.capture.profile import CatalogMatch
    monkeypatch.setattr("parsers.capture._fonts.match_font",
                        lambda fam: CatalogMatch(matched=False))
    role = build_font_role({"family": "Inter, sans-serif", "weight": 400,
                            "selector": "p"}, weights=[400, 700])
    assert role.family == "Inter"
    assert role.renderable is False
    assert role.catalog_match.matched is False


def test_build_font_role_hit(monkeypatch):
    from parsers.capture.profile import CatalogMatch
    monkeypatch.setattr("parsers.capture._fonts.match_font",
                        lambda fam: CatalogMatch(matched=True, slug="inter",
                                                 css_url="https://c/inter.css", source="catalog"))
    role = build_font_role({"family": "Inter", "weight": 400, "selector": "p"}, weights=[400])
    assert role.renderable is True
    assert role.catalog_match.css_url.endswith("inter.css")
    assert role.files == []
