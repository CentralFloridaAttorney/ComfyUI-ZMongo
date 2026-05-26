from __future__ import annotations

import json
import os
import time
import uuid
import urllib.parse
from pathlib import Path
from typing import Any, Optional

import requests
from dotenv import load_dotenv
from .generic_helpers import AlwaysDirtyMixin, DEFAULT_BASE_URL, DEFAULT_COMFY_ZMONGO_PREFIX, DEFAULT_FLEET_PREFIX, \
    DEFAULT_COMFY_ZMONGO_FLEET_PREFIX, DEFAULT_TIMEOUT, _normalize_base_url, _clean_prefix, _json_text, _error_payload, \
    _success_payload, _indexed_list_text, _extract_collections, _as_comfy_list, _dirty_token, _parse_json_object, \
    _parse_json_list, _extract_doc_ids, _extract_count, _parse_any_json, _extract_document_from_payload, \
    ZMongoLocalFileStoreSessionNode, safe_get_by_path, _ensure_payload_dict
# -----------------------------------------------------------------------------
# 99 Helper nodes
# -----------------------------------------------------------------------------

class ZMongoApiSelectNthItemNode(AlwaysDirtyMixin):
    @classmethod
    def INPUT_TYPES(cls):
        return {
            "required": {
                "items_list": ("*",),
                "index": ("INT", {"default": 0, "min": 0, "max": 1000000}),
                "fallback": ("STRING", {"default": ""}),
            }
        }

    RETURN_TYPES = ("STRING", "STRING")
    RETURN_NAMES = ("item", "status")
    FUNCTION = "select_nth_item"
    CATEGORY = "ZMongo/99 Helpers"
    INPUT_IS_LIST = True

    @staticmethod
    def _unwrap_scalar(value: Any, default: Any = None) -> Any:
        if isinstance(value, list):
            if not value:
                return default
            return value[0]
        return value if value is not None else default

    @staticmethod
    def _normalize_items(value: Any) -> list[Any]:
        if value is None:
            return []
        if isinstance(value, tuple):
            value = list(value)
        if isinstance(value, list):
            if len(value) == 1 and isinstance(value[0], (list, tuple)):
                return list(value[0])
            return value
        return [value]

    def select_nth_item(self, items_list, index, fallback):
        raw_items = self._normalize_items(items_list)
        fallback_value = str(self._unwrap_scalar(fallback, "") or "")
        index_value = self._unwrap_scalar(index, 0)

        try:
            safe_index = int(index_value or 0)
        except Exception:
            safe_index = 0

        cleaned = [str(item).strip() for item in raw_items if str(item).strip()]
        if not cleaned:
            return (fallback_value, "Input list was empty.")

        selected_index = max(0, min(safe_index, len(cleaned) - 1))
        selected = cleaned[selected_index]
        return (selected, f"Selected {selected_index + 1}/{len(cleaned)}: {selected}")


class ZMongoApiJsonPickNode(AlwaysDirtyMixin):
    @classmethod
    def INPUT_TYPES(cls):
        return {
            "required": {
                "json_text": ("STRING", {"default": "{}", "multiline": True}),
                "path": ("STRING", {"default": "data"}),
                "fallback": ("STRING", {"default": ""}),
            }
        }

    RETURN_TYPES = ("STRING",)
    RETURN_NAMES = ("value",)
    FUNCTION = "json_pick"
    CATEGORY = "ZMongo/99 Helpers"

    def json_pick(self, json_text: str, path: str, fallback: str):
        try:
            data = json.loads(json_text or "{}")
            current: Any = data
            for part in (path or "").split("."):
                if not part:
                    continue
                if isinstance(current, dict):
                    current = current[part]
                elif isinstance(current, list):
                    current = current[int(part)]
                else:
                    return (fallback or "",)
            if isinstance(current, (dict, list)):
                return (_json_text(current),)
            return ("" if current is None else str(current),)
        except Exception:
            return (fallback or "",)


# -----------------------------------------------------------------------------
# ComfyUI mappings
# -----------------------------------------------------------------------------

NODE_CLASS_MAPPINGS = {
    # 99 Helpers
    "ZMongoApiSelectNthItemNode": ZMongoApiSelectNthItemNode,
    "ZMongoApiJsonPickNode": ZMongoApiJsonPickNode,
}

NODE_DISPLAY_NAME_MAPPINGS = {
    # 99 Helpers
    "ZMongoApiSelectNthItemNode": "99 Select Nth Item",
    "ZMongoApiJsonPickNode": "99 JSON Pick",
}


__all__ = [
    "NODE_CLASS_MAPPINGS",
    "NODE_DISPLAY_NAME_MAPPINGS",
]