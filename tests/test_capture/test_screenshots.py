import pytest
from unittest.mock import AsyncMock

from parsers.capture._screenshots import (
    DISMISS_CONSENT_JS, _PREPARE_JS, capture_screenshots, prepare_page)


@pytest.mark.asyncio
async def test_capture_screenshots_caps_and_labels():
    page = AsyncMock()
    page.screenshot = AsyncMock(return_value=b"PNG")
    page.viewport_size = {"width": 1280, "height": 800}
    sections = [{"index": i, "rect": {"y": i * 500, "height": 480}} for i in range(20)]
    shots = await capture_screenshots(page, sections, max_screenshots=5, max_height=20000)
    assert len(shots) == 5                       # above_fold + full_page + 3 sections
    assert shots[0]["kind"] == "above_fold"
    assert shots[1]["kind"] == "full_page"
    assert shots[2]["kind"] == "section"
    assert all(s["bytes"] == b"PNG" for s in shots)


@pytest.mark.asyncio
async def test_capture_sections_scroll_into_view_before_shot():
    """Each section is scrolled to its top before being screenshotted."""
    page = AsyncMock()
    page.screenshot = AsyncMock(return_value=b"PNG")
    page.viewport_size = {"width": 1280, "height": 800}
    sections = [{"index": 0, "rect": {"y": 1500, "height": 600}}]
    await capture_screenshots(page, sections, max_screenshots=10, max_height=20000,
                              section_settle_ms=0)
    # the section's y (1500) was passed to a scrollTo evaluate call
    scroll_ys = [c.args[1] for c in page.evaluate.await_args_list if len(c.args) > 1]
    assert 1500 in scroll_ys


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
