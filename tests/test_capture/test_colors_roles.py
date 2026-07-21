from parsers.capture._colors import process_colors


def _roles(out):
    return {t.hex: t.role for t in out}


def test_theme_color_and_primary_var_win_over_saturated_accent():
    """vibeknow regression: the muted green (`--vk-primary` + `<meta theme-color>`)
    is the brand primary; a tiny high-saturation orange (`--vk-accent-orange`,
    coverage ~0) must NOT be labeled primary. The old ranking sorted by saturation
    after a binary brand flag, so pure orange beat muted green."""
    raw = {
        "samples": [
            {"color": "rgb(248,248,247)", "area": 900000, "text": False, "interactive": False},  # bg
            {"color": "rgb(31,33,25)",    "area": 80000,  "text": True},                          # text
            {"color": "rgb(46,103,84)",   "area": 4000,   "text": False, "interactive": True},    # green button fill
            {"color": "rgb(255,136,0)",   "area": 100,    "text": False, "interactive": False},   # orange speck
            {"color": "rgb(0,0,0)",       "area": 1300,   "text": False, "interactive": False},   # black
        ],
        "css_vars": {"--vk-primary": "#2e6754", "--vk-accent-orange": "#ff8800", "--vk-bg": "#f8f8f7"},
        "theme_color": "#2E6754",
    }
    r = _roles(process_colors(raw, delta_e_threshold=10.0))
    assert r.get("#2e6754") == "primary", r
    assert r.get("#ff8800") == "accent", r


def test_usage_picks_primary_without_named_vars_or_theme():
    """No theme-color and no helpfully-named vars: primary = the chromatic color
    most used as an INTERACTIVE fill (hyperframes-style), not the most saturated
    speck. Blue buttons should win over a hot-pink one-off."""
    raw = {
        "samples": [
            {"color": "rgb(255,255,255)", "area": 900000, "text": False, "interactive": False},  # bg
            {"color": "rgb(20,20,20)",    "area": 80000,  "text": True},                          # text
            {"color": "rgb(30,90,200)",   "area": 6000,   "text": False, "interactive": True},    # blue buttons (brand)
            {"color": "rgb(255,0,200)",   "area": 80,     "text": False, "interactive": False},   # hot-pink speck
        ],
        "css_vars": {},
        "theme_color": None,
    }
    r = _roles(process_colors(raw, delta_e_threshold=10.0))
    assert r.get("#1e5ac8") == "primary", r  # blue
    assert r.get("#ff00c8") == "accent", r   # pink demoted to accent


def test_background_and_text_still_assigned():
    """Guard: the bg (largest area) and text (largest text area) roles are unchanged."""
    raw = {
        "samples": [
            {"color": "rgb(255,255,255)", "area": 900000, "text": False, "interactive": False},
            {"color": "rgb(20,20,20)",    "area": 80000,  "text": True},
            {"color": "rgb(30,90,200)",   "area": 6000,   "text": False, "interactive": True},
        ],
        "css_vars": {}, "theme_color": None,
    }
    r = _roles(process_colors(raw, delta_e_threshold=10.0))
    assert r.get("#ffffff") == "background"
    assert r.get("#141414") == "text"
