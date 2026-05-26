from __future__ import annotations
import json
import time

from .generic_helpers import (
    AlwaysDirtyMixin,
    _dirty_token,
    _json_text,
    _error_payload,
    _success_payload,
    _parse_json_object,
    safe_get_by_path,
    _clean_scalar
)


class ZMongoSaveNodePreset(AlwaysDirtyMixin):
    """
    Captures a set of node values and saves them as a reusable preset in ZMongo.
    """

    @classmethod
    def INPUT_TYPES(cls):
        return {
            "required": {
                "session": ("ZMONGO_API_SESSION",),
                "collection_name": ("STRING", {"default": "node_presets"}),
                "node_class": ("STRING", {"default": "KSampler"}),
                "preset_name": ("STRING", {"default": "SD3.5 Cinematic Portrait"}),
                "values_json": ("STRING", {"default": "{}", "multiline": True}),
            },
            "optional": {
                "tags_json": ("STRING", {"default": "[]"}),
            }
        }

    RETURN_TYPES = ("STRING", "STRING", "BOOLEAN")
    RETURN_NAMES = ("json", "refresh", "success")
    FUNCTION = "save_preset"
    CATEGORY = "ZMongo/08 Presets"

    def save_preset(self, session, collection_name, node_class, preset_name, values_json, tags_json="[]"):
        cleaned_collection = _clean_scalar(collection_name) or "node_presets"
        cleaned_class = _clean_scalar(node_class)
        cleaned_preset = _clean_scalar(preset_name)
        refresh = _dirty_token("save_preset", cleaned_collection, cleaned_class, cleaned_preset)

        if session is None:
            return (_json_text(_error_payload("No API session provided.")), refresh, False)

        try:
            values = _parse_json_object(values_json, "values_json")
            tags = json.loads(_clean_scalar(tags_json) or "[]")
            if not isinstance(tags, list):
                tags = []

            # Standardized Preset Schema
            document = {
                "type": "comfy_node_preset",
                "node_class": cleaned_class,
                "preset_name": cleaned_preset,
                "values": values,
                "schema_version": "1.0",
                "tags": tags,
                "updated_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
            }

            # Upsert based on identity keys
            query = {
                "type": "comfy_node_preset",
                "node_class": cleaned_class,
                "preset_name": cleaned_preset
            }

            payload = session.update_doc(
                collection=cleaned_collection,
                query=query,
                update={"$set": document},
                upsert=True
            )

            success = bool(payload.get("success")) if isinstance(payload, dict) else False
            return (_json_text(payload), refresh, success)

        except Exception as exc:
            payload = _error_payload(f"Failed to save preset: {exc}")
            return (_json_text(payload), refresh, False)


class ZMongoLoadNodePreset(AlwaysDirtyMixin):
    """
    Loads a saved configuration from ZMongo to apply to any compatible ComfyUI node.
    """

    @classmethod
    def INPUT_TYPES(cls):
        return {
            "required": {
                "session": ("ZMONGO_API_SESSION",),
                "collection_name": ("STRING", {"default": "node_presets"}),
                "node_class": ("STRING", {"default": "KSampler"}),
                "preset_name": ("STRING", {"default": "SD3.5 Cinematic Portrait"}),
            }
        }

    RETURN_TYPES = ("STRING", "STRING", "BOOLEAN")
    RETURN_NAMES = ("values_json", "status", "success")
    FUNCTION = "load_preset"
    CATEGORY = "ZMongo/08 Presets"

    def load_preset(self, session, collection_name, node_class, preset_name):
        cleaned_collection = _clean_scalar(collection_name) or "node_presets"
        cleaned_class = _clean_scalar(node_class)
        cleaned_preset = _clean_scalar(preset_name)

        if session is None:
            return ("{}", "No API session provided.", False)

        try:
            query = {
                "type": "comfy_node_preset",
                "node_class": cleaned_class,
                "preset_name": cleaned_preset
            }

            # Use query_docs to find the exact preset
            payload = session.query_docs(
                collection=cleaned_collection,
                query=query,
                many=False,
                limit=1
            )

            data = payload.get("data", {}) if isinstance(payload, dict) else {}
            docs = data.get("documents", [])

            if not docs:
                return ("{}", f"Preset '{cleaned_preset}' for '{cleaned_class}' not found.", False)

            document = docs[0]
            values = document.get("values", {})

            status = f"Loaded preset: {cleaned_preset}"
            return (_json_text(values), status, True)

        except Exception as exc:
            return ("{}", f"Failed to load preset: {exc}", False)


# --- Typed Extractors (Phase 4) ---

class ZMongoJsonExtractBase(AlwaysDirtyMixin):
    """Base class for extracting typed values from the values_json string."""

    @classmethod
    def INPUT_TYPES(cls):
        return {
            "required": {
                "values_json": ("STRING", {"default": "{}"}),
                "key_path": ("STRING", {"default": "seed"}),
            }
        }

    CATEGORY = "ZMongo/08 Presets/Extractors"


class ZMongoJsonGetString(ZMongoJsonExtractBase):
    RETURN_TYPES = ("STRING",)
    RETURN_NAMES = ("string_value",)
    FUNCTION = "extract"

    def extract(self, values_json, key_path):
        try:
            data = _parse_json_object(values_json, "values_json")
            val = safe_get_by_path(data, key_path)
            return (str(val) if val is not None else "",)
        except Exception:
            return ("",)


class ZMongoJsonGetInt(ZMongoJsonExtractBase):
    RETURN_TYPES = ("INT",)
    RETURN_NAMES = ("int_value",)
    FUNCTION = "extract"

    def extract(self, values_json, key_path):
        try:
            data = _parse_json_object(values_json, "values_json")
            val = safe_get_by_path(data, key_path)
            return (int(val) if val is not None else 0,)
        except Exception:
            return (0,)


class ZMongoJsonGetFloat(ZMongoJsonExtractBase):
    RETURN_TYPES = ("FLOAT",)
    RETURN_NAMES = ("float_value",)
    FUNCTION = "extract"

    def extract(self, values_json, key_path):
        try:
            data = _parse_json_object(values_json, "values_json")
            val = safe_get_by_path(data, key_path)
            return (float(val) if val is not None else 0.0,)
        except Exception:
            return (0.0,)


class ZMongoJsonGetBool(ZMongoJsonExtractBase):
    RETURN_TYPES = ("BOOLEAN",)
    RETURN_NAMES = ("bool_value",)
    FUNCTION = "extract"

    def extract(self, values_json, key_path):
        try:
            data = _parse_json_object(values_json, "values_json")
            val = safe_get_by_path(data, key_path)
            return (bool(val) if val is not None else False,)
        except Exception:
            return (False,)


NODE_CLASS_MAPPINGS = {
    "ZMongoSaveNodePreset": ZMongoSaveNodePreset,
    "ZMongoLoadNodePreset": ZMongoLoadNodePreset,
    "ZMongoJsonGetString": ZMongoJsonGetString,
    "ZMongoJsonGetInt": ZMongoJsonGetInt,
    "ZMongoJsonGetFloat": ZMongoJsonGetFloat,
    "ZMongoJsonGetBool": ZMongoJsonGetBool,
}

NODE_DISPLAY_NAME_MAPPINGS = {
    "ZMongoSaveNodePreset": "08 Save Node Preset",
    "ZMongoLoadNodePreset": "08 Load Node Preset",
    "ZMongoJsonGetString": "08 JSON to String",
    "ZMongoJsonGetInt": "08 JSON to Int",
    "ZMongoJsonGetFloat": "08 JSON to Float",
    "ZMongoJsonGetBool": "08 JSON to Bool",
}