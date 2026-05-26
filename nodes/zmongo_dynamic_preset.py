from __future__ import annotations

import json
from typing import Any
from .generic_helpers import AlwaysDirtyMixin, _clean_scalar, _json_text


class ZMongoDynamicNodePreset(AlwaysDirtyMixin):
    """
    Frontend-assisted dynamic output node.
    The JS frontend adds outputs based on the schema and passes 'output_keys' to Python.
    """

    @classmethod
    def INPUT_TYPES(cls):
        return {
            "required": {
                "session": ("ZMONGO_API_SESSION",),
                "collection_name": ("STRING", {"default": "node_presets"}),
                "node_class": ("STRING", {"default": "KSampler"}),
                "preset_name": ("STRING", {"default": "SD3.5 Cinematic Portrait"}),
            },
            "hidden": {
                # The JS frontend populates this with a comma-separated list of the current output names
                "output_keys": ("STRING", {"default": ""}),
            }
        }

    # ComfyUI allows dynamic returns if we leave these empty; the JS handles the visual sockets
    RETURN_TYPES = ()
    RETURN_NAMES = ()
    FUNCTION = "load_dynamic_preset"
    CATEGORY = "ZMongo/08 Presets"

    def load_dynamic_preset(self, session, collection_name: str, node_class: str, preset_name: str,
                            output_keys: str = ""):
        cleaned_collection = _clean_scalar(collection_name) or "node_presets"
        cleaned_class = _clean_scalar(node_class)
        cleaned_preset = _clean_scalar(preset_name)

        # 1. Determine which keys the frontend expects us to return
        keys_to_return = [k.strip() for k in output_keys.split(",") if k.strip()]

        if not keys_to_return:
            return ()  # Nothing to output

        # 2. Fetch the preset from ZMongo
        values = {}
        if session is not None:
            query = {
                "type": "comfy_node_preset",
                "node_class": cleaned_class,
                "preset_name": cleaned_preset
            }
            try:
                # Query the node_presets collection for the exact configuration
                payload = session.query_docs(collection=cleaned_collection, query=query, many=False, limit=1)
                docs = payload.get("data", {}).get("documents", [])
                if docs:
                    values = docs[0].get("values", {})
            except Exception as e:
                print(f"[ZMongo] Dynamic preset fetch failed: {e}")

        # 3. Build the ordered tuple of results matching the JS sockets
        # Data Integrity: The backend array must be built in the precise, sequential order
        # that ComfyUI's C++ execution engine requires based on the UI layout.
        results = []
        for key in keys_to_return:
            results.append(values.get(key, None))

        return tuple(results)


NODE_CLASS_MAPPINGS = {
    "ZMongoDynamicNodePreset": ZMongoDynamicNodePreset
}

NODE_DISPLAY_NAME_MAPPINGS = {
    "ZMongoDynamicNodePreset": "08 Dynamic Preset Loader"
}