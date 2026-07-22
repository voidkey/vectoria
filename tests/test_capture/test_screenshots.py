import pytest
from unittest.mock import AsyncMock

from parsers.capture._screenshots import (
    DISMISS_CONSENT_JS, _PREPARE_JS, capture_screenshots, prepare_page)


def _screenshot_page(scroll_h=4000, vh=800, vw=1280):
    """Fake page whose geometry probes return real numbers so capture_screenshots
    computes scroll positions (screenshot returns fixed bytes)."""
    page = AsyncMock()
    page.screenshot = AsyncMock(return_value=b"PNG")
    page.viewport_size = {"width": vw, "height": vh}
    page.wait_for_timeout = AsyncMock()

    async def _eval(script, *a, **k):
        if "Math.max(document.body.scrollHeight" in script:
            return scroll_h
        if script == "window.innerHeight":
            return vh
        if "documentElement.scrollHeight" in script:
            return scroll_h
        return None
    page.evaluate = _eval
    return page


@pytest.mark.asyncio
async def test_capture_screenshots_scroll_positions_pct_and_cap():
    """Reference scroll model: strips labelled by scroll percentage, capped, always
    top (0%) → bottom (100%); plus one internal full_page shot (not exported)."""
    page = _screenshot_page(scroll_h=4000, vh=800)
    shots = await capture_screenshots(page, max_screenshots=5, max_height=20000, settle_ms=0)
    scroll = [s for s in shots if s["kind"] == "scroll"]
    full = [s for s in shots if s["kind"] == "full_page"]
    assert len(scroll) == 5                       # capped at max_screenshots
    assert len(full) == 1                         # internal color-cross-check shot
    assert scroll[0]["pct"] == 0                  # top
    assert scroll[-1]["pct"] == 100               # bottom
    pcts = [s["pct"] for s in scroll]
    assert pcts == sorted(pcts)                   # monotonic
    assert all(s["bytes"] == b"PNG" for s in scroll)
    assert all(s["section_index"] is None for s in scroll)


@pytest.mark.asyncio
async def test_capture_screenshots_scrolls_through_positions_to_bottom():
    """Each computed position is scrolled to before its shot; the bottom is reached."""
    page = _screenshot_page(scroll_h=4000, vh=800)
    calls = []
    orig = page.evaluate

    async def _eval(script, *a, **k):
        calls.append(script)
        return await orig(script, *a, **k)
    page.evaluate = _eval
    await capture_screenshots(page, max_screenshots=20, max_height=20000, settle_ms=0)
    # bottom position = scroll_h - viewport = 4000 - 800 = 3200
    assert any(c == "window.scrollTo(0, 3200)" for c in calls)
    assert any(c == "window.scrollTo(0, 0)" for c in calls)   # starts at top


@pytest.mark.asyncio
async def test_capture_screenshots_short_page_single_shot():
    """A page no taller than the viewport yields exactly one scroll shot at 0%."""
    page = _screenshot_page(scroll_h=800, vh=800)
    shots = await capture_screenshots(page, max_screenshots=10, max_height=20000, settle_ms=0)
    scroll = [s for s in shots if s["kind"] == "scroll"]
    assert len(scroll) == 1 and scroll[0]["pct"] == 0


@pytest.mark.asyncio
async def test_prepare_page_runs_walk_when_enabled():
    page = AsyncMock()
    page.viewport_size = {"width": 1280, "height": 800}
    await prepare_page(page, step_frac=0.8, step_ms=0, max_steps=60, img_wait_ms=0)
    # find the walk call (consent dismissal also runs an evaluate now)
    walk_call = next(c for c in page.evaluate.await_args_list
                     if c.args and "document.fonts" in c.args[0])
    js, arg = walk_call.args
    assert "scrollTo" in js and "document.fonts" in js   # walk + font wait in one pass
    assert arg["maxSteps"] == 60


@pytest.mark.asyncio
async def test_prepare_page_disabled_when_max_steps_zero():
    page = AsyncMock()
    await prepare_page(page, step_frac=0.8, step_ms=0, max_steps=0, img_wait_ms=0)
    page.evaluate.assert_not_awaited()


@pytest.mark.asyncio
async def test_prepare_page_swallows_errors():
    page = AsyncMock()
    page.evaluate = AsyncMock(side_effect=RuntimeError("navigated away"))
    await prepare_page(page, step_frac=0.8, step_ms=0, max_steps=5, img_wait_ms=0)  # no raise


# --- Task 1: cookie/consent dismissal -------------------------------------

def test_dismiss_consent_js_targets_consent_containers():
    """Dismissal is scoped to cookie/consent/gdpr containers (not the whole page)."""
    js = DISMISS_CONSENT_JS
    assert "cookie" in js
    assert "consent" in js
    assert "gdpr" in js


def test_dismiss_consent_js_matches_accept_text():
    """Accept-button text set = reference English terms + documented zh extension."""
    js = DISMISS_CONSENT_JS
    for term in ("accept", "agree", "got it", "allow", "consent"):
        assert term in js
    for zh in ("同意", "接受", "同意并继续"):
        assert zh in js


def test_dismiss_consent_js_is_best_effort_guarded():
    """The whole pass is wrapped so a JS error can never abort the capture."""
    js = DISMISS_CONSENT_JS
    assert "try" in js and "catch" in js


def test_dismiss_consent_js_does_not_click_reject_or_manage():
    """Reject/decline/manage/settings/preference terms appear ONLY in a guard
    that excludes them — never as clickable accept targets."""
    js = DISMISS_CONSENT_JS
    # the reject guard regex is present and excludes these
    assert "rejectRe" in js
    assert "reject" in js and "decline" in js and "manage" in js
    assert "settings" in js and "preference" in js
    # the accept regex must NOT contain any reject/manage term
    accept_line = next(ln for ln in js.splitlines() if "acceptRe" in ln and "=" in ln)
    for bad in ("reject", "decline", "manage", "settings", "preference"):
        assert bad not in accept_line


@pytest.mark.asyncio
async def test_prepare_page_dismisses_consent_before_walk():
    """prepare_page runs the consent dismissal, then the scroll-walk."""
    page = AsyncMock()
    page.viewport_size = {"width": 1280, "height": 800}
    await prepare_page(page, step_frac=0.8, step_ms=0, max_steps=60, img_wait_ms=0)
    scripts = [c.args[0] for c in page.evaluate.await_args_list if c.args]
    # consent dismissal ran, and it ran before the walk script
    assert any(s is DISMISS_CONSENT_JS for s in scripts)
    consent_i = next(i for i, s in enumerate(scripts) if s is DISMISS_CONSENT_JS)
    walk_i = next(i for i, s in enumerate(scripts) if "document.fonts" in s)
    assert consent_i < walk_i


@pytest.mark.asyncio
async def test_prepare_page_consent_failure_does_not_abort_walk():
    """A consent-dismissal error is swallowed; the walk still runs."""
    calls = []

    async def _eval(script, *args, **kwargs):
        calls.append(script)
        if script is DISMISS_CONSENT_JS:
            raise RuntimeError("boom")
        return None

    page = AsyncMock()
    page.viewport_size = {"width": 1280, "height": 800}
    page.evaluate = _eval
    await prepare_page(page, step_frac=0.8, step_ms=0, max_steps=5, img_wait_ms=0)
    assert any("document.fonts" in s for s in calls)  # walk still ran


# --- Task 2: overlay-hiding aligned to reference (z>100, bounded scan) -----

def test_overlay_hiding_uses_bounded_treewalker():
    """Reference heuristic: bounded TreeWalker scan (≤5000), not querySelectorAll('*')."""
    js = _PREPARE_JS
    assert "createTreeWalker" in js
    assert "5000" in js


def test_overlay_hiding_skips_header_and_nav():
    js = _PREPARE_JS
    assert "HEADER" in js and "NAV" in js
    assert "closest('header')" in js and "closest('nav')" in js


def test_overlay_hiding_only_zindex_over_100():
    """Only fixed/sticky with z-index>100 are hidden (no blanket position:fixed)."""
    js = _PREPARE_JS
    assert "> 100" in js
    assert "zIndex !== 'auto'" in js  # reference guard: skip auto z-index
    # the old blanket "hide any fixed regardless of z-index" clause is gone:
    # position:fixed only qualifies as part of the (fixed || sticky) && z>100 test
    assert "|| cs.position === 'fixed'" not in js
