import json
import logging
import re
from typing import Any, Dict, Optional

from google import genai
from google.genai import types

from ..zmongo_toolbag.data_processor import DataProcessor
from ..zmongo_toolbag.safe_result import SafeResult

logger = logging.getLogger(__name__)


def _safe_json(value: Any) -> str:
    try:
        if isinstance(value, SafeResult):
            return value.to_json(indent=2)
        return DataProcessor.to_json(value, indent=2)
    except Exception as exc:
        return json.dumps({"error": f"Serialization failed: {exc}"}, indent=2)


class ZMongoGeminiNode:
    """
    Gemini generation node for ComfyUI/ZMongo workflows.

    Design:
    - Accepts GEMINI_API_KEY as a string input or wired string node.
    - Uses the modern google-genai SDK.
    - Supports plain text mode and JSON mode.
    - Returns prompt passthrough data for larger workflows.
    """

    CATEGORY = "ZMongo/LLM"
    FUNCTION = "generate"

    RETURN_TYPES = ("STRING", "STRING", "BOOLEAN", "STRING")
    RETURN_NAMES = ("output_text", "output_json", "success", "status_json")

    RESPONSE_FORMATS = ["text", "json"]

    @classmethod
    def INPUT_TYPES(cls):
        return {
            "required": {
                "gemini_api_key": ("STRING", {"default": "", "forceInput": True}),
                "prompt": ("STRING", {"default": "Describe a cat.", "multiline": True}),
                "model_name": ("STRING", {"default": "gemini-3.1-pro-preview"}),
                "response_format": (cls.RESPONSE_FORMATS, {"default": "text"}),
            },
            "optional": {
                "system_prompt": ("STRING", {"default": "", "multiline": True}),
                "temperature": ("FLOAT", {"default": 0.7, "min": 0.0, "max": 2.0, "step": 0.05}),
                "max_output_tokens": ("INT", {"default": 2048, "min": 1, "max": 65536, "step": 1}),
            },
        }

    @classmethod
    def IS_CHANGED(
        cls,
        gemini_api_key: str,
        prompt: str,
        model_name: str,
        response_format: str,
        system_prompt: str = "",
        temperature: float = 0.7,
        max_output_tokens: int = 2048,
    ):
        masked_key = f"len:{len(str(gemini_api_key or ''))}"
        return (
            f"{masked_key}|{prompt}|{model_name}|{response_format}|"
            f"{system_prompt}|{temperature}|{max_output_tokens}"
        )

    @staticmethod
    def _parse_json_safely(raw: str) -> Optional[Any]:
        if not raw:
            return None

        txt = str(raw).strip()
        txt = re.sub(r"^```json", "", txt, flags=re.IGNORECASE).strip()
        txt = re.sub(r"^```", "", txt).strip()
        txt = re.sub(r"```$", "", txt).strip()

        try:
            return json.loads(txt)
        except json.JSONDecodeError:
            match = re.search(r"(\{.*\}|\[.*\])", txt, re.DOTALL)
            if match:
                try:
                    return json.loads(match.group(1))
                except Exception:
                    return None
            return None

    @staticmethod
    def _build_contents(system_prompt: str, prompt: str) -> str:
        system_prompt = str(system_prompt or "").strip()
        prompt = str(prompt or "").strip()

        if system_prompt:
            return f"System instructions:\n{system_prompt}\n\nUser request:\n{prompt}"
        return prompt

    def generate(
        self,
        gemini_api_key: str,
        prompt: str,
        model_name: str,
        response_format: str,
        system_prompt: str = "",
        temperature: float = 0.7,
        max_output_tokens: int = 2048,
    ):
        gemini_api_key = str(gemini_api_key or "").strip()
        prompt = str(prompt or "").strip()
        model_name = str(model_name or "").strip()
        response_format = str(response_format or "text").strip().lower()

        if not gemini_api_key:
            failure = SafeResult.fail("gemini_api_key is required")
            return ("", "{}", False, failure.to_json(indent=2))

        if not prompt:
            failure = SafeResult.fail("prompt is required")
            return ("", "{}", False, failure.to_json(indent=2))

        if not model_name:
            failure = SafeResult.fail("model_name is required")
            return ("", "{}", False, failure.to_json(indent=2))

        try:
            client = genai.Client(api_key=gemini_api_key)
            contents = self._build_contents(system_prompt=system_prompt, prompt=prompt)

            config_kwargs: Dict[str, Any] = {
                "temperature": float(temperature),
                "max_output_tokens": int(max_output_tokens),
            }

            if response_format == "json":
                config_kwargs["response_mime_type"] = "application/json"

            response = client.models.generate_content(
                model=model_name,
                contents=contents,
                config=types.GenerateContentConfig(**config_kwargs),
            )

            output_text = str(getattr(response, "text", "") or "").strip()

            if response_format == "json":
                parsed = self._parse_json_safely(output_text)
                if parsed is None:
                    status_payload = {
                        "success": False,
                        "message": "Gemini returned text that could not be parsed as JSON.",
                        "model_name": model_name,
                        "response_format": response_format,
                        "raw_text_preview": output_text[:500],
                    }
                    return (output_text, "{}", False, _safe_json(status_payload))

                status_payload = {
                    "success": True,
                    "message": "Gemini JSON generation succeeded.",
                    "model_name": model_name,
                    "response_format": response_format,
                }
                return (output_text, _safe_json(parsed), True, _safe_json(status_payload))

            status_payload = {
                "success": True,
                "message": "Gemini text generation succeeded.",
                "model_name": model_name,
                "response_format": response_format,
            }
            return (output_text, "{}", True, _safe_json(status_payload))

        except Exception as exc:
            logger.exception("ZMongoGeminiNode failure")
            failure = SafeResult.from_exception(exc, operation="gemini_generate")
            return ("", "{}", False, failure.to_json(indent=2))


NODE_CLASS_MAPPINGS = {
    "ZMongoGeminiNode": ZMongoGeminiNode,
}

NODE_DISPLAY_NAME_MAPPINGS = {
    "ZMongoGeminiNode": "ZMongo Gemini",
}