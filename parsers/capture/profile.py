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


class Heading(BaseModel):
    level: int
    text: str = ""
    font_size: str = ""
    font_weight: str = ""
    color: str = ""


class SvgInfo(BaseModel):
    # DB-bloat guard: the extractor's raw svg dict carries `outerHTML` (later
    # phases download it), but it MUST NOT be persisted here — metadata only.
    label: str = ""
    view_box: str = ""
    width: int = 0
    height: int = 0
    is_logo: bool = False


class PageGeom(BaseModel):
    width: int = 0
    height: int = 0
    viewport_width: int = 0
    viewport_height: int = 0


class SectionInfo(BaseModel):
    index: int
    heading: str = ""
    type: str = "generic"
    bg_color: str = ""
    screenshot_image_id: str | None = None
    # Phase 1: rich section content for faithful page-card recreation downstream.
    layout: str = ""
    background_image: str = ""
    cta_texts: list[str] = Field(default_factory=list)
    asset_urls: list[str] = Field(default_factory=list)
    text: str = ""


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
    # Phase 2 — reference-shaped color parity (kept alongside the role-tagged
    # `colors`): `colors_ranked` is the top-20 usage-ranked hex strings and
    # `color_stats` the top-48 raw per-hex stat dicts (hyperframes DesignTokens
    # `colorStats`: {hex,count,bgCount,interactiveBg,areaBg,textCount,maxArea}).
    # Stored as raw dicts (like asset_catalog/videos) for verbatim output parity;
    # both default to [] so older stored profiles still validate.
    colors_ranked: list[str] = Field(default_factory=list)
    color_stats: list[dict] = Field(default_factory=list)
    theme_color: str | None = None
    # Phase 1: hyperframes DesignTokens parity — :root custom properties,
    # heading typography, SVG (logo) metadata, and full-page/viewport geometry.
    css_variables: dict[str, str] = Field(default_factory=dict)
    headings: list[Heading] = Field(default_factory=list)
    svgs: list[SvgInfo] = Field(default_factory=list)
    page: PageGeom | None = None
    fonts: Fonts
    # Phase 4: raw FontFileMetadata dicts (from fonttools) for every captured face
    # — role fonts + the bounded site face set — each carrying its storage_key. Feeds
    # the real fonts-manifest.json + full fonts.css at export. Defaults to [] so older
    # stored profiles (no bounded face set) still validate and fall back at export.
    font_files: list[dict] = Field(default_factory=list)
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
    # Phase 6: animation catalog + captured WebGL shaders (only collected when
    # capture_quality == "full"). animation_catalog is the raw AnimationCatalog
    # dict (types.ts verbatim: webAnimations/cssDeclarations/scrollTargets/
    # cdpAnimations/summary); shaders is the deduped [{type, source}] list. Both
    # default (None / []) so older stored profiles still validate + export.
    animation_catalog: dict | None = None
    shaders: list[dict] = Field(default_factory=list)
    # Phase 7: two-layer (network + DOM) video manifest. Downloaded bodies + preview
    # frames land as AssetRefs in `assets` (kind="video"/"video_preview"). The manifest
    # is the hyperframes reference BARE ARRAY: [{index, url, filename, width, height,
    # sourceWidth, sourceHeight, heading, caption, ariaLabel, preview?, localPath?}] —
    # entries with no usable artifact are dropped. Kept alongside the URL-only `videos`
    # descriptor catalog. Defaults to None so older stored profiles still validate +
    # export (file omitted).
    video_manifest: list | None = None
    # Phase 8: lottie manifest — {lotties:[{file, url, name, width, height, duration,
    # frameRate, layers, preview?}], meta:{discovered, previews}}. The animation JSON
    # (dotLottie unzipped) lands as AssetRefs (kind="lottie_json") + best-effort
    # mid-frame previews (kind="lottie_preview"); this carries the discovery record.
    # Defaults to None so older stored profiles still validate + export (file omitted).
    lottie_manifest: dict | None = None
