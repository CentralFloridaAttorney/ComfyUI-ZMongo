from __future__ import annotations

import json
import time
from typing import Any

try:
    import nodes
except Exception:
    nodes = None

from .generic_helpers import (
    AlwaysDirtyMixin, _json_text, _error_payload, _success_payload,
    _clean_scalar, safe_get_by_path,
)

DEFAULT_SCHEMA_COLLECTION = "comfy_node_schemas"
DEFAULT_PRESET_COLLECTION = "node_presets"


def _utc_now() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def _as_int_text(value: Any) -> str:
    text = _clean_scalar(value)
    try:
        return str(int(text))
    except Exception:
        return text


def _safe_json_loads(value: Any, default: Any) -> Any:
    if isinstance(value, (dict, list)):
        return value
    text = _clean_scalar(value)
    if not text:
        return default
    try:
        return json.loads(text)
    except Exception:
        return default


def _extract_documents_from_payload(payload: Any) -> list[dict[str, Any]]:
    if not isinstance(payload, dict):
        return []
    data = payload.get("data")
    if isinstance(data, dict):
        for key in ("documents", "results"):
            value = data.get(key)
            if isinstance(value, list):
                return [item for item in value if isinstance(item, dict)]
        value = data.get("document")
        if isinstance(value, dict):
            return [value]
        if any(key in data for key in ("_id", "document_id", "node_class")):
            return [data]
    if isinstance(data, list):
        return [item for item in data if isinstance(item, dict)]
    return []


def _session_upsert_doc(session: Any, collection: str, query: dict[str, Any], document: dict[str, Any]) -> dict[str, Any]:
    update_doc = getattr(session, "update_doc", None)
    if callable(update_doc):
        try:
            return update_doc(collection=collection, query=query, update={"$set": document}, upsert=True)
        except TypeError:
            pass

    save_value = getattr(session, "save_value", None)
    if callable(save_value):
        try:
            return save_value(
                collection=collection, query=query, document_id="", field_path="schema_document",
                value=document, upsert_if_missing=True, parse_json_strings=False, normalize_for_storage=False,
            )
        except TypeError:
            pass

    create_doc = getattr(session, "create_doc", None)
    if callable(create_doc):
        return create_doc(collection=collection, document=document)

    return _error_payload("Session does not expose update_doc, save_value, or create_doc.")


def _query_one(session: Any, collection: str, query: dict[str, Any]) -> dict[str, Any]:
    query_docs = getattr(session, "query_docs", None)
    if callable(query_docs):
        try:
            payload = query_docs(collection=collection, query=query, many=False, limit=1)
            docs = _extract_documents_from_payload(payload)
            return docs[0] if docs else {}
        except TypeError:
            pass
    list_docs = getattr(session, "list_docs", None)
    if callable(list_docs):
        try:
            payload = list_docs(collection=collection, query=query, limit=1, skip=0)
            docs = _extract_documents_from_payload(payload)
            return docs[0] if docs else {}
        except TypeError:
            pass
    return {}


def _get_prompt_node(prompt: Any, target_node_id: Any) -> dict[str, Any]:
    if not isinstance(prompt, dict):
        return {}
    target = _as_int_text(target_node_id)
    for key, value in prompt.items():
        if _as_int_text(key) == target and isinstance(value, dict):
            return value
    return {}


def _get_workflow(extra_pnginfo: Any) -> dict[str, Any]:
    if not isinstance(extra_pnginfo, dict):
        return {}
    workflow = extra_pnginfo.get("workflow")
    if isinstance(workflow, str):
        workflow = _safe_json_loads(workflow, {})
    return workflow if isinstance(workflow, dict) else {}


def _get_workflow_node(extra_pnginfo: Any, target_node_id: Any) -> dict[str, Any]:
    workflow = _get_workflow(extra_pnginfo)
    workflow_nodes = workflow.get("nodes")
    if not isinstance(workflow_nodes, list):
        return {}
    target = _as_int_text(target_node_id)
    for node in workflow_nodes:
        if isinstance(node, dict) and _as_int_text(node.get("id")) == target:
            return node
    return {}


def _widget_names_from_workflow_node(workflow_node: dict[str, Any]) -> list[str]:
    names: list[str] = []
    inputs = workflow_node.get("inputs")
    if isinstance(inputs, list):
        for item in inputs:
            if not isinstance(item, dict):
                continue
            widget = item.get("widget")
            if isinstance(widget, dict) and widget.get("name"):
                names.append(str(widget.get("name")))
    return names


def _workflow_node_widget_map(workflow_node: dict[str, Any]) -> dict[str, Any]:
    values = workflow_node.get("widgets_values")
    if not isinstance(values, list):
        values = []
    names = _widget_names_from_workflow_node(workflow_node)
    return {names[i] if i < len(names) else f"widget_{i}": value for i, value in enumerate(values)}


def _input_schema_from_comfy_class(node_class: str) -> tuple[dict[str, Any], str]:
    cleaned_class = _clean_scalar(node_class)
    if nodes is None:
        return {}, "ComfyUI nodes registry is not available outside the ComfyUI runtime."
    registry = getattr(nodes, "NODE_CLASS_MAPPINGS", {})
    if cleaned_class not in registry:
        return {}, f"Node class '{cleaned_class}' not found in ComfyUI registry."

    cls_obj = registry[cleaned_class]
    if not hasattr(cls_obj, "INPUT_TYPES"):
        return {}, f"Node class '{cleaned_class}' has no INPUT_TYPES method."

    try:
        inputs = cls_obj.INPUT_TYPES()
    except Exception as exc:
        return {}, f"Failed to parse INPUT_TYPES: {exc}"

    schema = {
        "type": "comfy_node_schema",
        "node_class": cleaned_class,
        "schema_version": "2.0",
        "inputs": {},
        "saveable_fields": [],
        "reference_fields": [],
        "updated_at": _utc_now(),
    }

    all_inputs: dict[str, Any] = {}
    for group_name in ("required", "optional", "hidden"):
        group = inputs.get(group_name) if isinstance(inputs, dict) else None
        if isinstance(group, dict):
            for key, value in group.items():
                all_inputs[key] = (group_name, value)

    saveable_types = {"INT", "FLOAT", "STRING", "BOOLEAN"}
    for field_name, grouped_def in all_inputs.items():
        group_name, field_def = grouped_def
        if isinstance(field_def, str):
            field_type = field_def
            meta: dict[str, Any] = {}
        elif isinstance(field_def, tuple) and field_def:
            field_type = field_def[0]
            meta = dict(field_def[1]) if len(field_def) > 1 and isinstance(field_def[1], dict) else {}
        else:
            field_type = str(field_def)
            meta = {}

        if isinstance(field_type, list):
            schema["inputs"][field_name] = {"group": group_name, "kind": "widget", "type": "COMBO", "values": field_type, "saveable": True, **meta}
            schema["saveable_fields"].append(field_name)
        elif field_type in saveable_types:
            schema["inputs"][field_name] = {"group": group_name, "kind": "widget", "type": field_type, "saveable": True, **meta}
            schema["saveable_fields"].append(field_name)
        else:
            schema["inputs"][field_name] = {"group": group_name, "kind": "socket", "type": field_type, "saveable": False, "reference_only": True, **meta}
            schema["reference_fields"].append(field_name)

    return schema, f"Discovered {len(schema['saveable_fields'])} saveable field(s) and {len(schema['reference_fields'])} socket/reference field(s)."


class ZMongoDiscoverWorkflowNodeSettings(AlwaysDirtyMixin):
    @classmethod
    def INPUT_TYPES(cls):
        return {
            "required": {
                "target_node_id": ("STRING", {"default": "3"}),
            },
            "hidden": {
                "prompt": "PROMPT",
                "extra_pnginfo": "EXTRA_PNGINFO",
                "unique_id": "UNIQUE_ID",
            },
        }

    RETURN_TYPES = ("STRING", "STRING", "STRING", "STRING", "STRING", "BOOLEAN")
    RETURN_NAMES = ("settings_json", "widget_values_json", "prompt_inputs_json", "node_class", "status", "success")
    FUNCTION = "discover_workflow_node_settings"
    CATEGORY = "ZMongo/09 Discovery"
    OUTPUT_NODE = True

    def discover_workflow_node_settings(self, target_node_id: str, prompt: Any = None, extra_pnginfo: Any = None, unique_id: Any = None):
        clean_node_id = _as_int_text(target_node_id)
        prompt_node = _get_prompt_node(prompt, clean_node_id)
        workflow_node = _get_workflow_node(extra_pnginfo, clean_node_id)

        node_class = _clean_scalar(prompt_node.get("class_type")) or _clean_scalar(workflow_node.get("type"))
        widget_map = _workflow_node_widget_map(workflow_node)
        prompt_inputs = prompt_node.get("inputs") if isinstance(prompt_node.get("inputs"), dict) else {}

        settings = dict(widget_map)
        prompt_dict = prompt if isinstance(prompt, dict) else {}
        for key, value in prompt_inputs.items():
            if isinstance(value, list) and len(value) == 2:
                source_id = str(value[0])
                source_node = prompt_dict.get(source_id)
                if isinstance(source_node, dict) and source_node.get("class_type") == "PrimitiveNode":
                    prim_inputs = source_node.get("inputs", {})
                    if "value" in prim_inputs:
                        settings.setdefault(str(key), prim_inputs["value"])
                continue
            settings.setdefault(str(key), value)

        payload = _success_payload(
            f"Discovered settings for node #{clean_node_id}.",
            data={
                "target_node_id": clean_node_id,
                "node_class": node_class,
                "settings": settings,
                "widget_values": widget_map,
                "prompt_inputs": prompt_inputs,
                "workflow_node": {
                    "id": workflow_node.get("id"),
                    "type": workflow_node.get("type"),
                    "title": workflow_node.get("title"),
                } if workflow_node else {},
                "has_prompt_node": bool(prompt_node),
                "has_workflow_node": bool(workflow_node),
            },
        )
        success = bool(settings)
        if not success:
            payload = _error_payload(
                "No settings were discovered for that node id.",
                data={"target_node_id": clean_node_id, "has_prompt_node": bool(prompt_node), "has_workflow_node": bool(workflow_node)},
                status_code=404,
            )
        return (_json_text(settings), _json_text(widget_map), _json_text(prompt_inputs), node_class, payload.get("message", ""), success)


class ZMongoDiscoverNodeSchema(AlwaysDirtyMixin):
    @classmethod
    def INPUT_TYPES(cls):
        return {
            "required": {
                "session": ("ZMONGO_API_SESSION",),
                "node_class": ("STRING", {"default": "KSampler"}),
                "save_schema_to_zmongo": ("BOOLEAN", {"default": True}),
            },
            "optional": {
                "collection_name": ("STRING", {"default": DEFAULT_SCHEMA_COLLECTION}),
            },
        }

    RETURN_TYPES = ("STRING", "STRING", "STRING", "STRING")
    RETURN_NAMES = ("schema_json", "saveable_fields", "socket_fields", "status")
    FUNCTION = "discover_schema"
    CATEGORY = "ZMongo/09 Discovery"

    def discover_schema(self, session, node_class: str, save_schema_to_zmongo: bool, collection_name: str = DEFAULT_SCHEMA_COLLECTION):
        cleaned_class = _clean_scalar(node_class)
        cleaned_collection = _clean_scalar(collection_name) or DEFAULT_SCHEMA_COLLECTION
        schema, status = _input_schema_from_comfy_class(cleaned_class)
        if not schema:
            return ("{}", "[]", "[]", status)

        if save_schema_to_zmongo and session is not None:
            try:
                payload = _session_upsert_doc(
                    session,
                    cleaned_collection,
                    {"type": "comfy_node_schema", "node_class": cleaned_class},
                    schema,
                )
                status += " Schema synced to ZMongo." if payload.get("success") else f" ZMongo sync failed: {payload.get('message')}"
            except Exception as exc:
                status += f" ZMongo sync failed: {exc}"

        return (_json_text(schema), _json_text(schema["saveable_fields"]), _json_text(schema["reference_fields"]), status)


class ZMongoKSamplerPreset(AlwaysDirtyMixin):
    @classmethod
    def INPUT_TYPES(cls):
        return {
            "required": {
                "session": ("ZMONGO_API_SESSION",),
                "preset_name": ("STRING", {"default": "my_ksampler_preset"}),
            },
            "optional": {
                "collection_name": ("STRING", {"default": DEFAULT_PRESET_COLLECTION}),
            },
        }

    RETURN_TYPES = ("INT", "INT", "FLOAT", "*", "*", "FLOAT", "STRING", "STRING")
    RETURN_NAMES = ("seed", "steps", "cfg", "sampler_name", "scheduler", "denoise", "values_json", "status")
    FUNCTION = "load_ksampler_preset"
    CATEGORY = "ZMongo/08 Presets/Convenience"

    def load_ksampler_preset(self, session, preset_name, collection_name=DEFAULT_PRESET_COLLECTION):
        seed, steps, cfg = 0, 20, 8.0
        sampler_name, scheduler = "euler", "simple"
        denoise = 1.0
        values_json = "{}"
        clean_preset = _clean_scalar(preset_name)
        clean_collection = _clean_scalar(collection_name) or DEFAULT_PRESET_COLLECTION

        if session is None:
            return (seed, steps, cfg, sampler_name, scheduler, denoise, values_json, "No API session provided.")

        try:
            document = _query_one(session, clean_collection, {"type": "comfy_node_preset", "preset_name": clean_preset, "node_class": "KSampler"})
            if not document:
                document = _query_one(session, clean_collection, {"type": "comfy_node_preset", "preset_name": clean_preset})
            if not document:
                return (seed, steps, cfg, sampler_name, scheduler, denoise, values_json, f"Preset '{clean_preset}' not found.")

            values = document.get("values", {}) if isinstance(document.get("values"), dict) else {}
            if not values and isinstance(document.get("preset_document"), dict):
                wrapped = document.get("preset_document")
                values = wrapped.get("values", {}) if isinstance(wrapped.get("values"), dict) else {}
            values_json = _json_text(values)
            seed = int(safe_get_by_path(values, "seed", seed))
            steps = int(safe_get_by_path(values, "steps", steps))
            cfg = float(safe_get_by_path(values, "cfg", cfg))
            sampler_name = str(safe_get_by_path(values, "sampler_name", sampler_name))
            scheduler = str(safe_get_by_path(values, "scheduler", scheduler))
            denoise = float(safe_get_by_path(values, "denoise", denoise))
            return (seed, steps, cfg, sampler_name, scheduler, denoise, values_json, f"Successfully loaded '{clean_preset}'.")
        except Exception as exc:
            return (seed, steps, cfg, sampler_name, scheduler, denoise, values_json, f"Failed to load preset: {exc}")


NODE_CLASS_MAPPINGS = {
    "ZMongoDiscoverWorkflowNodeSettings": ZMongoDiscoverWorkflowNodeSettings,
    "ZMongoDiscoverNodeSchema": ZMongoDiscoverNodeSchema,
    "ZMongoKSamplerPreset": ZMongoKSamplerPreset,
}

NODE_DISPLAY_NAME_MAPPINGS = {
    "ZMongoDiscoverWorkflowNodeSettings": "09 Discover Workflow Node Settings",
    "ZMongoDiscoverNodeSchema": "09 Discover Node Schema",
    "ZMongoKSamplerPreset": "09 KSampler Preset",
}

__all__ = ["NODE_CLASS_MAPPINGS", "NODE_DISPLAY_NAME_MAPPINGS"]