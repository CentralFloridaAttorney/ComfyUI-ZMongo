from __future__ import annotations
import asyncio
import requests
from typing import Any
from .generic_helpers import AlwaysDirtyMixin

# 1. Import ComfyUI's PromptServer
from server import PromptServer

class ZMongoMotdDisplayNode(AlwaysDirtyMixin):
    CATEGORY = "ZMongo"
    FUNCTION = "display_motd"

    @classmethod
    def INPUT_TYPES(cls):
        return {
            "required": {
                "api_key": ("STRING", {"description": "Your ZMongo API key"})
            }
        }

    RETURN_TYPES = ("STRING", "STRING")  # message, expires_at
    RETURN_NAMES = ("motd_message", "expires_at")
    DISPLAY_ONLY = True

    @staticmethod
    async def display_motd(api_key: str):
        BASE_URL = "https://businessprocessapplications.com/comfy-zmongo"
        clean_key = (api_key or "").strip()
        HEADERS = {
            "Content-Type": "application/json",
            "ZAI_API_KEY": clean_key,
            "X-API-Key": clean_key,
            "X-ZAI-API-Key": clean_key,
            "X-ZMongo-API-Key": clean_key,
        }

        try:
            resp = requests.get(f"{BASE_URL}/api/motd", headers=HEADERS, timeout=10)
            data = resp.json()

            if data.get("success") and data.get("data") and data["data"].get("message"):
                motd_doc = data["data"]
                motd_message = f"🌟 {motd_doc.get('message', '')}"
                expires_at = motd_doc.get("expires_at", "")

                # 2. Broadcast the live data to your JavaScript panel!
                PromptServer.instance.send_sync("zmongo.motd_update", {
                    "message": motd_message,
                    "expires_at": expires_at
                })

                return motd_message, expires_at

        except Exception as e:
            print("Error fetching MOTD:", e)

        # 3. Fallback broadcast in case of failure
        error_msg = "🌟 Unable to load Message of the Day. Sign in to see updates."
        PromptServer.instance.send_sync("zmongo.motd_update", {
            "message": error_msg,
            "expires_at": ""
        })

        return error_msg, ""

    @classmethod
    def execute(cls, api_key: str):
        return asyncio.run(cls.display_motd(api_key))

# ComfyUI registration
NODE_CLASS_MAPPINGS = {"ZMongoMotdDisplayNode": ZMongoMotdDisplayNode}
NODE_DISPLAY_NAME_MAPPINGS = {"ZMongoMotdDisplayNode": "ZMongo: Message of the Day"}