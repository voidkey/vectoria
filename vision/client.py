import base64
import logging

from openai import AsyncOpenAI

from infra.circuit_breaker import CircuitOpenError, get_breaker
from parsers.image_metadata import detect_mime_type
from vision.budget import get_cost_tracker

logger = logging.getLogger(__name__)

_SYSTEM_PROMPT = (
    "你是一个视频配图助手。用户正在将一篇文章制作成视频，需要你帮助理解图片的用途。\n"
    "请用中文回答，1-2句话：\n"
    "1. 这张图片是什么（如：某本书的封面、某个产品的照片、数据图表等）\n"
    "2. 它在文章中起什么作用（配图、说明、装饰等）\n"
    "不要详细描述图片中的视觉细节（颜色、排版、字体等），只关注它的语义身份和用途。"
)

# Vision-native parser prompt. Dual output: semantic description for
# downstream RAG / video-script use, plus verbatim text so callers
# that need exact strings (slogans, numbers, names) don't lose them
# to VLM paraphrasing. Markdown structure is intentional — handler
# stores it as-is and the splitter / chunker treat headings as
# section breaks.
_PARSE_SYSTEM_PROMPT = (
    "你是一个文档图片解析器。给定一张图片，输出 markdown 格式："
    "\n\n## 描述\n<用 1-3 句中文说明这张图片的核心内容、类型和用途，"
    "如「这是一张销售数据柱状图，对比了 Q1 与 Q2 的营收」>"
    "\n\n## 逐字内容\n<逐字转录图中可读的所有文字，按视觉顺序排列；"
    "若是表格保留行列结构（用 markdown 表格）；图中无文字写「（无文字）」>"
    "\n\n规则："
    "\n- 不要复述图片不存在的内容；不确定就写「不确定」"
    "\n- 不要解释你的推理过程"
    "\n- 直接以「## 描述」开头，不要其它前言"
)


def _build_user_text(
    context: str = "", section_title: str = "", alt: str = "",
) -> str:
    """Build contextual user prompt from available metadata."""
    parts: list[str] = []
    if section_title:
        parts.append(f"图片来自文章章节「{section_title}」。")
    if alt:
        parts.append(f"图片原始标注：{alt}。")
    if context:
        trimmed = context[:500] if len(context) > 500 else context
        parts.append(f"图片周围的文字：{trimmed}")
    return "\n".join(parts) if parts else "请根据图片内容判断。"


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
        b64 = base64.b64encode(image_bytes).decode()
        user_text = _build_user_text(context, section_title, alt)
        messages = [
            {"role": "system", "content": _SYSTEM_PROMPT},
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
    ) -> str:
        """Whole-image markdown extraction for vision-native parser.

        Returns markdown with ``## 描述`` + ``## 逐字内容`` sections,
        empty string on failure. Caller is responsible for treating
        empty as "vision didn't help, registry should fall back".

        Failures (breaker open / API error) are *raised* so the worker
        handler's per-attempt fallback chain can route to the next
        engine — different from ``describe()`` which fails soft for
        post-parse enrichment.
        """
        if not self._client:
            raise RuntimeError("VisionClient not configured")
        mime = detect_mime_type(image_bytes)
        if not mime.startswith("image/"):
            raise ValueError(f"not an image: detected mime {mime!r}")
        b64 = base64.b64encode(image_bytes).decode()
        messages = [
            {"role": "system", "content": _PARSE_SYSTEM_PROMPT},
            {
                "role": "user",
                "content": [
                    {"type": "text", "text": "请按要求解析这张图片。"},
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
