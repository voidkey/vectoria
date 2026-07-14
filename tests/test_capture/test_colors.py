from parsers.capture._colors import (
    parse_css_color, rgb_to_lab, rgb_to_oklch, delta_e, process_colors,
)


def test_parse_hex_rgb_hsl():
    assert parse_css_color("#ffffff") == (255, 255, 255)
    assert parse_css_color("#fff") == (255, 255, 255)
    assert parse_css_color("rgb(11, 11, 15)") == (11, 11, 15)
    assert parse_css_color("rgba(255,0,0,0.5)") == (255, 0, 0)
    assert parse_css_color("hsl(0,100%,50%)") == (255, 0, 0)
    assert parse_css_color("white") == (255, 255, 255)
    assert parse_css_color("transparent") is None
    assert parse_css_color("var(--x)") is None
    assert parse_css_color("linear-gradient(#fff,#000)") is None


def test_delta_e_zero_for_identical():
    lab = rgb_to_lab((100, 150, 200))
    assert delta_e(lab, lab) == 0.0


def test_oklch_format():
    assert rgb_to_oklch((11, 11, 15)).startswith("oklch(")


def test_process_assigns_roles_and_clusters():
    raw = {
        "samples": [
            {"color": "rgb(11,11,15)", "area": 400000, "text": False},
            {"color": "rgb(12,12,16)", "area": 100000, "text": False},  # near-dup of bg
            {"color": "#f5f5f7", "area": 250000, "text": True},
            {"color": "#6c5ce7", "area": 30000, "text": False},
        ],
        "css_vars": {"--brand": "#6c5ce7"},
        "theme_color": "#0b0b0f",
    }
    colors = process_colors(raw, delta_e_threshold=10.0)
    roles = {c.role for c in colors}
    assert "background" in roles
    assert "text" in roles
    primary = [c for c in colors if c.role == "primary"]
    assert primary and any("css-var:--brand" in c.sources for c in primary)
    assert len(colors) <= 5
    for c in colors:
        assert c.hex.startswith("#") and c.oklch.startswith("oklch(")
        assert 0.0 <= c.confidence <= 1.0
