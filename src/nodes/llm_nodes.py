import json
import logging
import re
from pathlib import Path
from typing import Any, Dict, Optional, Union

from flask.cli import load_dotenv
from google import genai
from google.genai import types

logger = logging.getLogger(__name__)

## load_dotenv(Path.home() / ".resources" / ".secrets")


class ZGeminiPromptNode:
    """
    ComfyUI node that sends a prompt to Gemini and returns:
      - raw text
      - parsed json text
      - success flag
      - status message
    """

    CATEGORY = "ZMongo/LLM"
    FUNCTION = "run"

    RETURN_TYPES = ("STRING", "STRING", "BOOLEAN", "STRING")
    RETURN_NAMES = ("response_text", "response_json", "success", "status")

    @classmethod
    def INPUT_TYPES(cls):
        return {
            "required": {
                "GEMINI_API_KEY": ("STRING", {
                    "default": "",
                    "multiline": False
                }),
                "user_prompt": ("STRING", {
                    "default": "",
                    "multiline": True
                }),
            },
            "optional": {
                "model_name": ("STRING", {
                    "default": "gemini-3.1-pro-preview",
                    "multiline": False
                }),
                "json_mode": ("BOOLEAN", {
                    "default": False
                }),
            }
        }

    def run(
        self,
        GEMINI_API_KEY: str,
        user_prompt: str,
        model_name: str = "gemini-3.1-pro-preview",
        json_mode: bool = False,
    ):
        try:
            api_key = (GEMINI_API_KEY or "").strip()
            prompt = (user_prompt or "").strip()

            if not api_key:
                return ("", "", False, "GEMINI_API_KEY is required.")

            if not prompt:
                return ("", "", False, "user_prompt is required.")

            client = genai.Client(api_key=api_key)

            config = None
            if json_mode:
                config = types.GenerateContentConfig(
                    response_mime_type="application/json"
                )

            response = client.models.generate_content(
                model=model_name,
                contents=prompt,
                config=config,
            )

            response_text = getattr(response, "text", "") or ""
            parsed = self._parse_json_safely(response_text) if json_mode else None
            response_json = (
                json.dumps(parsed, indent=2, ensure_ascii=False)
                if parsed is not None else ""
            )

            return (
                response_text,
                response_json,
                True,
                "OK",
            )

        except Exception as exc:
            logger.exception("Gemini node failed")
            return ("", "", False, f"Gemini error: {exc}")

    @staticmethod
    def _parse_json_safely(raw: str) -> Optional[Union[Dict[str, Any], list]]:
        if not raw:
            return None

        txt = raw.strip()
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


NODE_CLASS_MAPPINGS = {
    "ZGeminiPromptNode": ZGeminiPromptNode,
}

NODE_DISPLAY_NAME_MAPPINGS = {
    "ZGeminiPromptNode": "Z Gemini Prompt",
}