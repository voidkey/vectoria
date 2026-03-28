import base64
import logging

from openai import AsyncOpenAI

logger = logging.getLogger(__name__)

_SYSTEM_PROMPT = (
    "请用中文描述这张图片，1-2句话。包括：图片展示了什么内容，"
    "图片类型（照片、图表、示意图、截图、插画等），"
    "以及它可能在什么场景下有用。请简洁。"
)


class VisionClient:
    def __init__(self, base_url: str, api_key: str, model: str):
        self._model = model
        self.is_configured = bool(base_url)
        if self.is_configured:
            self._client = AsyncOpenAI(base_url=base_url, api_key=api_key)
        else:
            self._client = None

    async def describe(self, image_bytes: bytes) -> str:
        """Describe an image using the vision LLM.

        Returns a short description string, or empty string on failure.
        """
        if not self._client:
            return ""
        try:
            b64 = base64.b64encode(image_bytes).decode()
            resp = await self._client.chat.completions.create(
                model=self._model,
                messages=[
                    {
                        "role": "user",
                        "content": [
                            {"type": "text", "text": _SYSTEM_PROMPT},
                            {
                                "type": "image_url",
                                "image_url": {"url": f"data:image/png;base64,{b64}"},
                            },
                        ],
                    }
                ],
                max_tokens=200,
            )
            return resp.choices[0].message.content.strip()
        except Exception:
            logger.exception("Vision LLM call failed")
            return ""
