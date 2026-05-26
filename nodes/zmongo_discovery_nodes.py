from __future__ import annotations
import json
import nodes  # ComfyUI core registry
from typing import Any

from .generic_helpers import (
    AlwaysDirtyMixin,
    _json_text,
    _error_payload,
    _success_payload,
    safe_get_by_path,
    _clean_scalar
)


# -----------------------------------------------------------------------------
# Phase 1: Node Discovery & Schema Generation
# -----------------------------------------------------------------------------

class ZMongoDiscoverNodeSchema(AlwaysDirtyMixin):
    """
    Inspects installed ComfyUI nodes and determines their expected inputs,
    types, defaults, and allowed values. Saves the schema to ZMongo.
    """

    @classmethod
    def INPUT_TYPES(cls):
        return {
            "required": {
                "session": ("ZMONGO_API_SESSION",),
                "node_class": ("STRING", {"default": "KSampler"}),
                "save_schema_to_zmongo": ("BOOLEAN", {"default": True}),
            }
        }

    RETURN_TYPES = ("STRING", "STRING", "STRING", "STRING")
    RETURN_NAMES = ("schema_json", "saveable_fields", "socket_fields", "status")
    FUNCTION = "discover_schema"
    CATEGORY = "ZMongo/09 Discovery"

    def discover_schema(self, session, node_class: str, save_schema_to_zmongo: bool):
        cleaned_class = _clean_scalar(node_class)

        if cleaned_class not in nodes.NODE_CLASS_MAPPINGS:
            err = f"Node class '{cleaned_class}' not found in ComfyUI registry."
            return ("{}", "[]", "[]", err)

        cls_obj = nodes.NODE_CLASS_MAPPINGS[cleaned_class]

        if not hasattr(cls_obj, "INPUT_TYPES"):
            err = f"Node class '{cleaned_class}' has no INPUT_TYPES method."
            return ("{}", "[]", "[]", err)

        # Extract ComfyUI node inputs
        try:
            inputs = cls_obj.INPUT_TYPES()
        except Exception as e:
            return ("{}", "[]", "[]", f"Failed to parse INPUT_TYPES: {e}")

        schema = {
            "type": "comfy_node_schema",
            "node_class": cleaned_class,
            "schema_version": "1.0",
            "inputs": {},
            "saveable_fields": [],
            "reference_fields": []
        }

        all_inputs = {}
        if "required" in inputs:
            all_inputs.update(inputs["required"])
        if "optional" in inputs:
            all_inputs.update(inputs["optional"])

        # Determine Saveable vs Reference Types
        SAVEABLE_TYPES = {"INT", "FLOAT", "STRING", "BOOLEAN"}

        for field_name, field_def in all_inputs.items():
            field_type = field_def[0]

            # Combo lists
            if isinstance(field_type, list):
                schema["inputs"][field_name] = {
                    "kind": "widget",
                    "type": "COMBO",
                    "values": field_type,
                    "saveable": True
                }
                schema["saveable_fields"].append(field_name)

            # Simple primitives
            elif field_type in SAVEABLE_TYPES:
                field_meta = {
                    "kind": "widget",
                    "type": field_type,
                    "saveable": True
                }
                # Attach min/max/default if provided in the tuple's dict (index 1)
                if len(field_def) > 1 and isinstance(field_def[1], dict):
                    field_meta.update(field_def[1])

                schema["inputs"][field_name] = field_meta
                schema["saveable_fields"].append(field_name)

            # Objects / Sockets (MODEL, LATENT, CONDITIONING, IMAGE)
            else:
                schema["inputs"][field_name] = {
                    "kind": "socket",
                    "type": field_type,
                    "saveable": False,
                    "reference_only": True
                }
                schema["reference_fields"].append(field_name)

        status = f"Discovered: {len(schema['saveable_fields'])} saveable, {len(schema['reference_fields'])} sockets."

        # Save to database
        if save_schema_to_zmongo and session is not None:
            try:
                session.save_value(
                    collection="comfy_node_schemas",
                    query={"node_class": cleaned_class},
                    document_id="",
                    field_path="",
                    value=schema,
                    upsert_if_missing=True,
                    parse_json_strings=False,
                    normalize_for_storage=False
                )
                status += " (Schema synced to ZMongo)."
            except Exception as e:
                status += f" (ZMongo Sync Failed: {e})"

        return (
            _json_text(schema),
            _json_text(schema["saveable_fields"]),
            _json_text(schema["reference_fields"]),
            status
        )


# -----------------------------------------------------------------------------
# Phase 2: Node-Specific Convenience Loaders
# -----------------------------------------------------------------------------

class ZMongoKSamplerPreset(AlwaysDirtyMixin):
    """
    Friendly convenience node for KSampler presets.
    Outputs highly-typed values ready for direct wiring into a KSampler node.
    """

    @classmethod
    def INPUT_TYPES(cls):
        return {
            "required": {
                "session": ("ZMONGO_API_SESSION",),
                "preset_name": ("STRING", {"default": "SD3.5 Cinematic Portrait"}),
            }
        }

    RETURN_TYPES = ("INT", "INT", "FLOAT", "STRING", "STRING", "FLOAT", "STRING", "STRING")
    RETURN_NAMES = ("seed", "steps", "cfg", "sampler_name", "scheduler", "denoise", "values_json", "status")
    FUNCTION = "load_ksampler_preset"
    CATEGORY = "ZMongo/08 Presets/Convenience"

    def load_ksampler_preset(self, session, preset_name):
        cleaned_preset = _clean_scalar(preset_name)

        # Safe Defaults
        seed, steps, cfg = 0, 20, 8.0
        sampler_name, scheduler = "euler", "normal"
        denoise = 1.0
        values_json = "{}"

        if session is None:
            return (seed, steps, cfg, sampler_name, scheduler, denoise, values_json, "No API session provided.")

        try:
            query = {
                "type": "comfy_node_preset",
                "node_class": "KSampler",
                "preset_name": cleaned_preset
            }

            payload = session.query_docs(collection="node_presets", query=query, many=False, limit=1)
            data = payload.get("data", {}) if isinstance(payload, dict) else {}
            docs = data.get("documents", [])

            if not docs:
                return (seed, steps, cfg, sampler_name, scheduler, denoise, values_json,
                        f"Preset '{cleaned_preset}' not found.")

            document = docs[0]
            values = document.get("values", {})
            values_json = _json_text(values)

            # Hydrate values with fallback to safe defaults
            seed = int(values.get("seed", seed))
            steps = int(values.get("steps", steps))
            cfg = float(values.get("cfg", cfg))
            sampler_name = str(values.get("sampler_name", sampler_name))
            scheduler = str(values.get("scheduler", scheduler))
            denoise = float(values.get("denoise", denoise))

            return (seed, steps, cfg, sampler_name, scheduler, denoise, values_json,
                    f"Successfully loaded '{cleaned_preset}'")

        except Exception as exc:
            return (seed, steps, cfg, sampler_name, scheduler, denoise, values_json, f"Failed to load preset: {exc}")


NODE_CLASS_MAPPINGS = {
    "ZMongoDiscoverNodeSchema": ZMongoDiscoverNodeSchema,
    "ZMongoKSamplerPreset": ZMongoKSamplerPreset,
}

NODE_DISPLAY_NAME_MAPPINGS = {
    "ZMongoDiscoverNodeSchema": "09 Discover Node Schema",
    "ZMongoKSamplerPreset": "09 KSampler Preset",
}