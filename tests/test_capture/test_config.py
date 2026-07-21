def test_capture_settings_defaults():
    from config import get_settings
    s = get_settings()
    assert s.capture_render_timeout == 30.0
    assert s.capture_settle_ms == 1500
    assert s.capture_max_screenshots == 10
    assert s.capture_viewport_width == 1280
    assert s.capture_viewport_height == 800
    assert s.capture_max_asset_bytes == 25 * 1024 * 1024
    assert s.capture_max_screenshot_height == 20000
    assert s.capture_color_delta_e == 10.0
    assert s.font_catalog_path == ""
    # Phase 7: video manifest download bounds.
    assert s.capture_max_videos == 6
    assert s.capture_max_video_downloads == 3
    assert s.capture_max_video_bytes == 75 * 1024 * 1024
    assert s.capture_video_download_budget_s == 180.0


def test_capture_not_found_code():
    from api.errors import ErrorCode
    assert ErrorCode.CAPTURE_NOT_FOUND == 1213
