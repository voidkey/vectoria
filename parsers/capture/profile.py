"""Pydantic models for the website-capture SiteProfile."""
from pydantic import BaseModel, Field


class ColorToken(BaseModel):
    hex: str
    oklch: str
    lab: list[float]
    role: str
    coverage: float
    confidence: float
    sources: list[str] = Field(default_factory=list)


class FontFile(BaseModel):
    url: str = ""
    weight: int | None = None
    style: str = "normal"
    format: str = "woff2"
    source: str = "captured"


class CatalogMatch(BaseModel):
    matched: bool
    slug: str | None = None
    css_url: str | None = None
    source: str | None = None


class FontRole(BaseModel):
    family: str
    stack: str
    weights: list[int] = Field(default_factory=list)
    sample_selector: str
    catalog_match: CatalogMatch
    renderable: bool
    files: list[FontFile] = Field(default_factory=list)


class Fonts(BaseModel):
    display: FontRole
    body: FontRole


class Spacing(BaseModel):
    scale: list[int] = Field(default_factory=list)
    radii: list[int] = Field(default_factory=list)
    container_max_width: int | None = None
    section_gap: int | None = None


class SectionInfo(BaseModel):
    index: int
    heading: str = ""
    type: str = "generic"
    bg_color: str = ""
    screenshot_image_id: str | None = None


class TextInfo(BaseModel):
    headline: str = ""
    tagline: str = ""
    ctas: list[str] = Field(default_factory=list)
    full_text: str = ""


class AssetRef(BaseModel):
    kind: str
    image_id: str | None = None
    storage_key: str = ""
    url: str = ""
    format: str = ""
    width: int | None = None
    height: int | None = None
    description: str = ""
    vision_status: str = "none"


class ScreenshotRef(BaseModel):
    kind: str
    image_id: str = ""
    url: str = ""
    width: int = 0
    height: int = 0
    section_index: int | None = None


class MotionHints(BaseModel):
    libraries: list[str] = Field(default_factory=list)
    has_video_background: bool = False
    has_canvas: bool = False


class SiteProfile(BaseModel):
    url: str
    captured_at: str
    fetch_tier: str = "playwright"
    # capture_quality gates high-fidelity artifacts + tells downstream how far to
    # trust the capture: "full" (rich real content — page.html/design-styles emitted),
    # "partial" (thin/likely-blocked body but head-level brand salvaged — brand only,
    # NO structural rebuild), "blocked" (anti-bot challenge / near-empty).
    capture_quality: str = "full"
    blocked_reason: str | None = None
    # page_html_key: S3 key of the self-contained page recreation (extracted/page.html);
    # only set when capture_quality == "full". export.py fetches it into the zip.
    page_html_key: str | None = None
    # design_styles: computed design-system summary (typography/buttons/cards/shadows);
    # only set when capture_quality == "full". Small enough to inline in the profile.
    design_styles: dict | None = None
    colors: list[ColorToken] = Field(default_factory=list)
    theme_color: str | None = None
    fonts: Fonts
    spacing: Spacing
    sections: list[SectionInfo] = Field(default_factory=list)
    text: TextInfo
    assets: list[AssetRef] = Field(default_factory=list)
    screenshots: list[ScreenshotRef] = Field(default_factory=list)
    motion_hints: MotionHints
    # asset_catalog / videos: URL-only site media (images/videos/backgrounds/icons/
    # fonts) discovered on the page, aligned with hyperframes' asset cataloger.
    # Nothing is downloaded — downstream decides which URLs to fetch. Kept as raw
    # hyperframes-shaped dicts (CatalogedAsset / VideoDescriptor) for output parity.
    asset_catalog: list[dict] = Field(default_factory=list)
    videos: list[dict] = Field(default_factory=list)
