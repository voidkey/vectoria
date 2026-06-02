import base64
import logging

from openai import AsyncOpenAI

from infra.circuit_breaker import CircuitOpenError, get_breaker
from parsers.image_metadata import detect_mime_type
from vision.budget import get_cost_tracker
from vision.language import resolve_language

logger = logging.getLogger(__name__)


def _describe_system_prompt(language: str) -> str:
    return (
        "You are an assistant for understanding images embedded in an "
        "article. "
        f"Respond in {language}, in 1-2 sentences: "
        "(1) what the image is (e.g. a book cover, a product photo, a data "
        "chart); (2) its role in the article (illustration, explanation, "
        "decoration). Do not describe visual details (color, layout, fonts) "
        "— only the image's semantic identity and purpose."
    )


# Vision-native parser prompt. Dual output: semantic description for
# downstream RAG / video-script use, plus verbatim text so callers
# that need exact strings (slogans, numbers, names) don't lose them
# to VLM paraphrasing. Markdown structure is intentional — handler
# stores it as-is and the splitter / chunker treat headings as
# section breaks.
def _parse_system_prompt(language: str) -> str:
    return (
        "You are a document-image parser. Given an image, output markdown:"
        "\n\n## Description\n"
        f"<1-3 sentences in {language} describing the image's core content, "
        "type and purpose, e.g. 'A bar chart comparing Q1 and Q2 revenue.'>"
        "\n\n## Verbatim\n"
        "<Transcribe ALL readable text in the image in its ORIGINAL language "
        "(do NOT translate), in visual order; preserve tables as markdown "
        "tables; if there is no text, write '(no text)'.>"
        "\n\nRules:"
        "\n- Do not invent content not present in the image; if unsure, write "
        "'uncertain'."
        "\n- Do not explain your reasoning."
        "\n- Start directly with '## Description', with no preamble."
    )


def _build_user_text(
    context: str = "", section_title: str = "", alt: str = "",
) -> str:
    """Build contextual user prompt from available metadata."""
    parts: list[str] = []
    if section_title:
        parts.append(f"The image is from the article section \"{section_title}\".")
    if alt:
        parts.append(f"Original image caption: {alt}.")
    if context:
        trimmed = context[:500] if len(context) > 500 else context
        parts.append(f"Surrounding text: {trimmed}")
    return "\n".join(parts) if parts else "Judge based on the image content."


class VisionClient:
    def __init__(self, base_url: str, api_key: str, model: str):
        self._model = model
        self.is_configured = bool(base_url)
        if self.is_configured:
            self._client = AsyncOpenAI(base_url=base_url, api_key=api_key)
        else:
            self._client = None

    async def describe(
        self,
        image_bytes: bytes,
        context: str = "",
        section_title: str = "",
        alt: str = "",
        language: str | None = None,
    ) -> str:
        """Describe an image using the vision LLM.

        Returns a short description string, or empty string on failure.
        """
        if not self._client:
            return ""
        mime = detect_mime_type(image_bytes)
        if not mime.startswith("image/"):
            logger.warning("Skipping non-image data (detected %s)", mime)
            return ""
        lang = resolve_language(language)
        b64 = base64.b64encode(image_bytes).decode()
        user_text = _build_user_text(context, section_title, alt)
        messages = [
            {"role": "system", "content": _describe_system_prompt(lang)},
            {
                "role": "user",
                "content": [
                    {"type": "text", "text": user_text},
                    {
                        "type": "image_url",
                        "image_url": {"url": f"data:{mime};base64,{b64}"},
                    },
                ],
            },
        ]
        try:
            resp = await get_breaker("vision").call(
                self._client.chat.completions.create,
                model=self._model, messages=messages, max_tokens=200,
            )
            get_cost_tracker().record(purpose="describe")
            return resp.choices[0].message.content.strip()
        except CircuitOpenError:
            # Vision is non-critical (image descriptions are metadata).
            # Fail soft so the overall document still completes.
            logger.warning("Vision circuit open; skipping describe")
            return ""
        except Exception:
            logger.exception("Vision LLM call failed")
            return ""

    async def parse_image(
        self,
        image_bytes: bytes,
        *,
        max_tokens: int = 1500,
        language: str | None = None,
    ) -> str:
        """Whole-image markdown extraction for vision-native parser.

        Returns markdown with ``## Description`` + ``## Verbatim`` sections,
        or empty string when the VLM itself returns no content. Caller is
        responsible for treating empty as "vision didn't help, registry
        should fall back".

        Failures (breaker open / API error) are *raised* (not returned as
        empty) so the worker
        handler's per-attempt fallback chain can route to the next
        engine — different from ``describe()`` which fails soft for
        post-parse enrichment.
        """
        if not self._client:
            raise RuntimeError("VisionClient not configured")
        mime = detect_mime_type(image_bytes)
        if not mime.startswith("image/"):
            raise ValueError(f"not an image: detected mime {mime!r}")
        lang = resolve_language(language)
        b64 = base64.b64encode(image_bytes).decode()
        messages = [
            {"role": "system", "content": _parse_system_prompt(lang)},
            {
                "role": "user",
                "content": [
                    {"type": "text", "text": "Parse this image as instructed."},
                    {
                        "type": "image_url",
                        "image_url": {"url": f"data:{mime};base64,{b64}"},
                    },
                ],
            },
        ]
        # Don't suppress here — let the parser's caller (handler with
        # per-attempt fallback) see real exceptions and route around.
        resp = await get_breaker("vision").call(
            self._client.chat.completions.create,
            model=self._model, messages=messages, max_tokens=max_tokens,
        )
        get_cost_tracker().record(purpose="parse")
        return (resp.choices[0].message.content or "").strip()
