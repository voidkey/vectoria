import pytest
from unittest.mock import AsyncMock, MagicMock, patch


@pytest.mark.asyncio
async def test_create_capture_enqueues(client):
    fake_doc = MagicMock(id="cap-1", status="queued", image_status="none")
    fake_doc.created_at.isoformat.return_value = "2026-07-14T00:00:00"
    sess = MagicMock()
    sess.add = MagicMock()
    sess.commit = AsyncMock()
    sess.refresh = AsyncMock()
    cm = MagicMock()
    cm.__aenter__ = AsyncMock(return_value=sess)
    cm.__aexit__ = AsyncMock(return_value=False)
    with (
        patch("api.routes.captures._validate_kb", new=AsyncMock()),
        patch("api.routes.captures.validate_url", new=AsyncMock()),
        patch("api.routes.captures.enqueue_in_session", return_value="t1"),
        patch("api.routes.captures.get_session", return_value=cm),
        patch("api.routes.captures.Document", return_value=fake_doc),
    ):
        resp = await client.post("/v1/knowledgebases/kb-x/captures",
                                 json={"url": "https://example.com"})
    assert resp.status_code == 202, resp.text
    body = resp.json()
    assert body["status"] == "queued"
    assert body["id"] == "cap-1"


@pytest.mark.asyncio
async def test_create_capture_bad_url(client):
    from api.errors import AppError, ErrorCode
    with (
        patch("api.routes.captures._validate_kb", new=AsyncMock()),
        patch("api.routes.captures.validate_url",
              new=AsyncMock(side_effect=AppError(400, ErrorCode.INVALID_URL, "bad"))),
    ):
        resp = await client.post("/v1/knowledgebases/kb-x/captures",
                                 json={"url": "http://127.0.0.1"})
    assert resp.status_code == 400


@pytest.mark.asyncio
async def test_get_capture_not_found(client):
    with patch("api.routes.captures._load_capture", new=AsyncMock(return_value=None)):
        resp = await client.get("/v1/knowledgebases/kb-x/captures/nope")
    assert resp.status_code == 404
    assert resp.json()["code"] == 1213


@pytest.mark.asyncio
async def test_get_capture_hydrates_profile(client):
    doc = MagicMock(id="c1", status="completed", image_status="completed", error_msg="")
    doc.created_at.isoformat.return_value = "2026-07-14T00:00:00"
    doc.profile = {
        "assets": [{"kind": "logo", "image_id": "i1", "storage_key": "captures/kb/c1/logo.svg"}],
        "screenshots": [{"kind": "above_fold", "image_id": "s1"}],
        "fonts": {"display": {"files": [{"url": "captures/kb/c1/fonts/inter.woff2"}]},
                  "body": {"files": []}},
    }
    img_logo = MagicMock(id="i1", description="brand logo", vision_status="completed",
                         storage_key="captures/kb/c1/logo.svg")
    img_shot = MagicMock(id="s1", storage_key="images/kb/c1/a.png")
    storage = AsyncMock()
    storage.presign_url = AsyncMock(side_effect=lambda k: f"https://signed/{k}")
    scalars = MagicMock()
    scalars.scalars.return_value.all.return_value = [img_logo, img_shot]
    sess = MagicMock()
    sess.execute = AsyncMock(return_value=scalars)
    cm = MagicMock(); cm.__aenter__ = AsyncMock(return_value=sess); cm.__aexit__ = AsyncMock(return_value=False)
    with (
        patch("api.routes.captures._load_capture", new=AsyncMock(return_value=doc)),
        patch("api.routes.captures.get_storage", new=AsyncMock(return_value=storage)),
        patch("api.routes.captures.get_session", return_value=cm),
    ):
        resp = await client.get("/v1/knowledgebases/kb/captures/c1")
    assert resp.status_code == 200, resp.text
    prof = resp.json()["profile"]
    assert prof["assets"][0]["url"] == "https://signed/captures/kb/c1/logo.svg"
    assert prof["assets"][0]["description"] == "brand logo"
    assert prof["screenshots"][0]["url"] == "https://signed/images/kb/c1/a.png"
    assert prof["fonts"]["display"]["files"][0]["url"].startswith("https://signed/")


@pytest.mark.asyncio
async def test_export_unknown_format(client):
    resp = await client.get("/v1/knowledgebases/kb/captures/c1/export?format=zip")
    assert resp.status_code == 422


@pytest.mark.asyncio
async def test_export_not_completed_409(client):
    doc = MagicMock(status="queued", profile=None)
    with patch("api.routes.captures._load_capture", new=AsyncMock(return_value=doc)):
        resp = await client.get("/v1/knowledgebases/kb/captures/c1/export?format=hyperframes")
    assert resp.status_code == 409
