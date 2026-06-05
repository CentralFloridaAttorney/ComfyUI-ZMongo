from __future__ import annotations

# pylint: disable=too-many-lines,too-many-locals,too-many-branches,broad-exception-caught,line-too-long,unused-argument

"""
ComfyUI-ZMongo Static Content Pack Workflow Nodes
================================================

Purpose
-------
Take a ZMongo Content Pack V3 object, flatten every field/value with its stored
Comfy datatype, and export a self-contained workflow.json that opens with one
static node exposing one output per content-pack value.

Install target:
    custom_nodes/ComfyUI-ZMongo/zmongo_content_pack_static_workflow_nodes.py

Also install the companion JS in:
    custom_nodes/ComfyUI-ZMongo/web/zmongo_content_pack_static_workflow.js

Then register this module from __init__.py by merging NODE_CLASS_MAPPINGS and
NODE_DISPLAY_NAME_MAPPINGS.
"""

import base64
import hashlib
import io
import json
import re
import time
from copy import deepcopy
from datetime import datetime, timezone
from typing import Any, Optional

try:
    import torch
except Exception:  # pragma: no cover
    torch = None  # type: ignore[assignment]

try:
    import numpy as np
except Exception:  # pragma: no cover
    np = None  # type: ignore[assignment]

try:
    from PIL import Image, ImageOps
except Exception:  # pragma: no cover
    Image = None  # type: ignore[assignment]
    ImageOps = None  # type: ignore[assignment]

try:
    from .generic_helpers import AlwaysDirtyMixin, _as_comfy_list, _indexed_list_text
except Exception:  # pragma: no cover
    class AlwaysDirtyMixin:
        @classmethod
        def IS_CHANGED(cls, *args: Any, **kwargs: Any) -> float:
            return time.time()

    def _as_comfy_list(values: Any) -> list[Any]:
        if values is None:
            return []
        if isinstance(values, list):
            return values
        if isinstance(values, tuple):
            return list(values)
        return [values]

    def _indexed_list_text(values: list[Any]) -> str:
        return json.dumps([f"{index}: {value}" for index, value in enumerate(values)], indent=2, ensure_ascii=False)

try:
    from .zmongo_content_pack_v3_nodes import (  # type: ignore
        CONTENT_PACK_TYPE,
        CONTENT_PACK_SCHEMA_KIND,
        PORTABLE_CONTENT_PACK_SCHEMA_KIND,
        CONTENT_PACK_SCHEMA_VERSION,
        _as_content_pack,
        _coerce_field_value,
        _decode_image_bytes_from_value,
        _field_list,
        _json_dumps,
        _manifest_only,
        _normalize_type,
        _preview_markdown,
        _safe_str,
        _stable_hash,
        _title_from_alias,
        _value_summary,
    )
except Exception:  # pragma: no cover
    CONTENT_PACK_TYPE = "ZMONGO_CONTENT_PACK"
    CONTENT_PACK_SCHEMA_KIND = "zmongo_content_pack"
    PORTABLE_CONTENT_PACK_SCHEMA_KIND = "zmongo_portable_content_pack"
    CONTENT_PACK_SCHEMA_VERSION = "3.0.0"

    def _safe_str(value: Any, default: str = "") -> str:
        if value is None:
            return default
        if isinstance(value, (list, tuple)):
            value = value[0] if value else default
        return str(value).strip()

    def _json_dumps(value: Any, *, pretty: bool = True) -> str:
        if pretty:
            return json.dumps(value, indent=2, ensure_ascii=False, default=str)
        return json.dumps(value, ensure_ascii=False, separators=(",", ":"), default=str)

    def _normalize_type(value: Any, fallback: str = "ANY") -> str:
        text = _safe_str(value).upper()
        aliases = {"STR": "STRING", "TEXT": "STRING", "INTEGER": "INT", "BOOL": "BOOLEAN", "IMAGE_ASSET": "IMAGE", "OBJECT": "JSON", "DICT": "JSON", "LIST": "JSON"}
        text = aliases.get(text, text)
        return text if text in {"STRING", "INT", "FLOAT", "BOOLEAN", "IMAGE", "JSON", "ANY"} else fallback

    def _parse_json(value: Any, default: Any = None) -> Any:
        if default is None:
            default = {}
        if isinstance(value, (dict, list)):
            return value
        try:
            return json.loads(_safe_str(value))
        except Exception:
            return default

    def _as_content_pack(value: Any) -> dict[str, Any]:
        parsed = _parse_json(value, default={})
        if isinstance(parsed, dict) and parsed.get("schema_kind") == PORTABLE_CONTENT_PACK_SCHEMA_KIND:
            parsed = deepcopy(parsed)
            parsed["schema_kind"] = CONTENT_PACK_SCHEMA_KIND
        return parsed if isinstance(parsed, dict) else {}

    def _field_list(content_pack: Any) -> list[dict[str, Any]]:
        pack = _as_content_pack(content_pack)
        fields = pack.get("fields")
        return [field for field in fields if isinstance(field, dict)] if isinstance(fields, list) else []

    def _coerce_field_value(field: dict[str, Any]) -> Any:
        if "value" in field:
            return field.get("value")
        return field.get("asset_ref") or field.get("json_ref") or None

    def _decode_image_bytes_from_value(value: Any) -> bytes:
        if isinstance(value, str):
            text = value.strip()
            if text.startswith("data:") and "," in text:
                text = text.split(",", 1)[1]
            return base64.b64decode(text, validate=False)
        if isinstance(value, bytes):
            return value
        raise ValueError(f"Unsupported image payload: {type(value).__name__}")

    def _stable_hash(value: Any) -> str:
        return hashlib.sha256(_json_dumps(value, pretty=False).encode("utf-8", errors="ignore")).hexdigest()

    def _title_from_alias(alias: str) -> str:
        return " ".join(part[:1].upper() + part[1:] for part in re.split(r"[_\s]+", _safe_str(alias)) if part)

    def _value_summary(value: Any, limit: int = 160) -> dict[str, Any]:
        text = _json_dumps(value, pretty=False) if isinstance(value, (dict, list)) else str(value)
        return {"display": text[:limit], "length": len(text), "truncated": len(text) > limit}

    def _manifest_only(content_pack: dict[str, Any]) -> dict[str, Any]:
        fields = _field_list(content_pack)
        return {"schema_kind": "zmongo_content_pack_manifest", "schema_version": CONTENT_PACK_SCHEMA_VERSION, "content_pack_name": content_pack.get("content_pack_name", ""), "project_name": content_pack.get("project_name", ""), "field_count": len(fields), "fields": [{k: f.get(k) for k in ("index", "alias", "label", "source_path", "comfy_type", "json_type", "storage", "summary")} for f in fields]}

    def _preview_markdown(content_pack: dict[str, Any]) -> str:
        fields = _field_list(content_pack)
        lines = [f"# Content Pack V3: {content_pack.get('content_pack_name', '')}", "", "## Fields", ""]
        for field in fields:
            lines.append(f"{field.get('index', 0)}. `{field.get('alias', '')}` — **{field.get('comfy_type', 'ANY')}**")
        return "\n".join(lines)


class AnyType(str):
    """Comfy wildcard type that validates against every datatype."""

    def __ne__(self, other: object) -> bool:  # Comfy checks type inequality.
        return False


ANY_TYPE = AnyType("*")
MAX_STATIC_OUTPUTS = 64
SUPPORTED_OUTPUT_TYPES = {"STRING", "INT", "FLOAT", "BOOLEAN", "IMAGE", "JSON", "ANY"}


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _safe_filename_stem(value: Any, fallback: str = "content_pack_static_workflow") -> str:
    text = _safe_str(value) or fallback
    text = re.sub(r"[^A-Za-z0-9_.-]+", "_", text).strip("._-")
    return text or fallback


def _empty_image(width: int = 1, height: int = 1) -> Any:
    if torch is None:
        return None
    return torch.zeros((1, max(1, height), max(1, width), 3), dtype=torch.float32)


def _pil_to_comfy_image(image: Any) -> Any:
    if torch is None or np is None or ImageOps is None:
        return _empty_image()
    image = ImageOps.exif_transpose(image).convert("RGB")
    array = np.asarray(image).astype(np.float32) / 255.0
    return torch.from_numpy(array)[None,]


def _image_bytes_to_tensor(image_bytes: bytes) -> Any:
    if Image is None:
        raise RuntimeError("Pillow is required to decode content pack images.")
    with Image.open(io.BytesIO(image_bytes)) as image:
        return _pil_to_comfy_image(image)


def _image_sequence_to_batch_tensor(values: Any) -> Any:
    """Decode one image value or a list of image values into a Comfy IMAGE batch [N,H,W,C]."""
    if isinstance(values, list):
        tensors: list[Any] = []
        for item in values:
            try:
                tensors.append(_image_bytes_to_tensor(_decode_image_bytes_from_value(item)))
            except Exception:
                continue
        if not tensors:
            return _empty_image()
        if torch is None:
            return tensors[0]
        try:
            return torch.cat(tensors, dim=0)
        except Exception:
            # If frames differ in size, resize all later frames to the first frame.
            try:
                first = tensors[0]
                target_h = int(first.shape[1])
                target_w = int(first.shape[2])
                resized: list[Any] = [first]
                for tensor in tensors[1:]:
                    if int(tensor.shape[1]) == target_h and int(tensor.shape[2]) == target_w:
                        resized.append(tensor)
                        continue
                    if Image is None or np is None:
                        continue
                    arr = tensor[0].detach().cpu().numpy() if hasattr(tensor, "detach") else np.asarray(tensor[0])
                    arr = np.clip(arr * 255.0, 0, 255).astype(np.uint8)
                    pil = Image.fromarray(arr[..., :3], mode="RGB").resize((target_w, target_h), Image.LANCZOS)
                    resized.append(_pil_to_comfy_image(pil))
                return torch.cat(resized, dim=0)
            except Exception:
                return tensors[0]
    return _image_bytes_to_tensor(_decode_image_bytes_from_value(values))


def _safe_default_for_type(comfy_type: Any) -> Any:
    kind = _normalize_type(comfy_type, "ANY")
    if kind == "IMAGE":
        return _empty_image()
    if kind == "INT":
        return 0
    if kind == "FLOAT":
        return 0.0
    if kind == "BOOLEAN":
        return False
    if kind == "JSON":
        return "{}"
    return ""


def _typed_fallback_values_from_content_pack_json(content_pack_json: Any) -> list[Any]:
    parsed = _parse_json_local(content_pack_json, default={})
    raw_fields = parsed.get("fields") if isinstance(parsed, dict) and isinstance(parsed.get("fields"), list) else []
    values: list[Any] = []
    for field in raw_fields[:MAX_STATIC_OUTPUTS]:
        if isinstance(field, dict):
            values.append(_safe_default_for_type(field.get("comfy_type")))
    while len(values) < MAX_STATIC_OUTPUTS:
        values.append("")
    return values[:MAX_STATIC_OUTPUTS]


def _field_alias(field: dict[str, Any], index: int) -> str:
    raw = _safe_str(field.get("alias") or field.get("source_path") or f"field_{index}")
    clean = re.sub(r"[^A-Za-z0-9_]+", "_", raw).strip("_") or f"field_{index}"
    if clean[0].isdigit():
        clean = f"field_{clean}"
    return clean


def _flatten_content_pack_for_static_outputs(content_pack: Any) -> dict[str, Any]:
    """Return a compact, workflow-embeddable pack with fields in output order.

    Accepts:
    - zmongo_content_pack
    - zmongo_portable_content_pack
    - zmongo_static_content_pack_workflow

    The exported static node intentionally exposes only restored values. No
    alias/index/manifest metadata outputs are added.
    """
    parsed = _parse_json_local(content_pack, default={})

    if isinstance(parsed, dict) and parsed.get("schema_kind") == STATIC_SCHEMA_KIND:
        pack = deepcopy(parsed)
    else:
        pack = deepcopy(_as_content_pack(content_pack))

    if not pack:
        raise ValueError("content_pack is empty or invalid.")

    raw_fields = pack.get("fields") if isinstance(pack.get("fields"), list) else _field_list(pack)
    out_fields: list[dict[str, Any]] = []
    used_aliases: set[str] = set()

    for index, field in enumerate([item for item in raw_fields if isinstance(item, dict)][:MAX_STATIC_OUTPUTS]):
        alias = _field_alias(field, index)
        if alias in used_aliases:
            base = alias
            suffix = 2
            while f"{base}_{suffix}" in used_aliases:
                suffix += 1
            alias = f"{base}_{suffix}"
        used_aliases.add(alias)

        comfy_type = _normalize_type(field.get("comfy_type"), "ANY")
        if comfy_type not in SUPPORTED_OUTPUT_TYPES:
            comfy_type = "ANY"

        item = {
            "index": len(out_fields),
            "alias": alias,
            "label": _safe_str(field.get("label")) or _title_from_alias(alias),
            "source_path": _safe_str(field.get("source_path")) or alias,
            "comfy_type": comfy_type,
            "json_type": _safe_str(field.get("json_type")) or comfy_type.lower(),
            "storage": _safe_str(field.get("storage")) or "inline",
            "summary": field.get("summary") if isinstance(field.get("summary"), dict) else _value_summary(field.get("value")),
        }

        for key in ("value", "asset_ref", "json_ref", "content_type", "filename", "sequence"):
            if key in field:
                item[key] = deepcopy(field.get(key))
        out_fields.append(item)

    flat = {
        "schema_kind": STATIC_SCHEMA_KIND,
        "schema_version": CONTENT_PACK_SCHEMA_VERSION,
        "created_at": pack.get("created_at") or _utc_now_iso(),
        "updated_at": _utc_now_iso(),
        "content_pack_name": pack.get("content_pack_name") or "content_pack",
        "project_name": pack.get("project_name") or "default",
        "source_schema_kind": pack.get("source_schema_kind") or pack.get("schema_kind") or CONTENT_PACK_SCHEMA_KIND,
        "source_manifest_hash": pack.get("source_manifest_hash") or pack.get("manifest_hash", ""),
        "field_count": len(out_fields),
        "fields": out_fields,
        "content": {
            "summary": f"Static workflow pack '{pack.get('content_pack_name') or 'content_pack'}' with {len(out_fields)} restored value output(s).",
            "markdown": _preview_markdown({**pack, "schema_kind": CONTENT_PACK_SCHEMA_KIND, "fields": out_fields}),
        },
    }
    flat["manifest_hash"] = _stable_hash(_manifest_only({**flat, "schema_kind": CONTENT_PACK_SCHEMA_KIND}))
    return flat


def _extract_static_value(field: dict[str, Any], session: Any = None, master_key_hex: str = "") -> Any:
    comfy_type = _normalize_type(field.get("comfy_type"), "ANY")
    value = _coerce_field_value(field)

    if comfy_type == "STRING":
        if isinstance(value, str):
            return value
        if value is None:
            return ""
        return _json_dumps(value, pretty=False) if isinstance(value, (dict, list)) else str(value)

    if comfy_type == "INT":
        try:
            return int(float(value))
        except Exception:
            return 0

    if comfy_type == "FLOAT":
        try:
            return float(value)
        except Exception:
            return 0.0

    if comfy_type == "BOOLEAN":
        if isinstance(value, bool):
            return value
        return _safe_str(value).lower() in {"1", "true", "yes", "on"}

    if comfy_type == "IMAGE":
        try:
            storage = _safe_str(field.get("storage"))
            asset_ref = field.get("asset_ref") if isinstance(field.get("asset_ref"), dict) else {}
            image_bytes: Optional[bytes] = None

            if storage in {"inline_base64_sequence", "inline_image_sequence"} or isinstance(value, list):
                return _image_sequence_to_batch_tensor(value)
            if storage == "inline_base64" or (isinstance(value, str) and value.strip().startswith("data:image/")):
                image_bytes = _decode_image_bytes_from_value(value)
            elif isinstance(value, (str, bytes, bytearray, dict)) and not asset_ref:
                image_bytes = _decode_image_bytes_from_value(value)
            elif asset_ref and session is not None and hasattr(session, "fetch_image_field"):
                collection = _safe_str(asset_ref.get("collection") or asset_ref.get("asset_collection") or ((asset_ref.get("original") or {}).get("asset_collection") if isinstance(asset_ref.get("original"), dict) else ""))
                document_id = _safe_str(asset_ref.get("document_id") or asset_ref.get("file_id") or asset_ref.get("_id") or ((asset_ref.get("original") or {}).get("file_id") if isinstance(asset_ref.get("original"), dict) else ""))
                field_path = _safe_str(asset_ref.get("field_path") or field.get("source_path") or "image_data")
                image_bytes, _source = session.fetch_image_field(collection=collection, document_id=document_id, field_path=field_path, master_key_hex=master_key_hex)
            else:
                image_bytes = _decode_image_bytes_from_value(value)
            return _image_bytes_to_tensor(image_bytes or b"")
        except Exception:
            return _empty_image()

    if comfy_type == "JSON":
        return _json_dumps(value)

    return value


def _static_node_output_defs(flat_pack: dict[str, Any]) -> list[dict[str, Any]]:
    """Return only restored value outputs, one output per content-pack field."""
    outputs: list[dict[str, Any]] = []
    for field in flat_pack.get("fields", []):
        if not isinstance(field, dict):
            continue
        outputs.append({
            "localized_name": field.get("alias"),
            "name": field.get("alias"),
            "type": _normalize_type(field.get("comfy_type"), "ANY"),
            "links": None,
        })
    return outputs


def _make_static_content_pack_workflow(flat_pack: dict[str, Any], *, node_title: str = "", workflow_name: str = "") -> dict[str, Any]:
    title = _safe_str(node_title) or f"📦 {flat_pack.get('content_pack_name', 'Static Content Pack')}"
    workflow_id = hashlib.sha256(_json_dumps(flat_pack, pretty=False).encode("utf-8", errors="ignore")).hexdigest()[:12]
    return {
        "id": f"zmongo-static-content-pack-{workflow_id}",
        "revision": 0,
        "last_node_id": 1,
        "last_link_id": 0,
        "nodes": [
            {
                "id": 1,
                "type": "ZMongoContentPackStaticOutputsV3",
                "pos": [640, 360],
                "size": [520, max(260, 120 + 28 * min(len(flat_pack.get("fields", [])), MAX_STATIC_OUTPUTS))],
                "flags": {},
                "order": 0,
                "mode": 0,
                "inputs": [
                    {"localized_name": "content_pack_json", "name": "content_pack_json", "type": "STRING", "widget": {"name": "content_pack_json"}, "link": None},
                    {"localized_name": "refresh_token", "name": "refresh_token", "shape": 7, "type": "STRING", "widget": {"name": "refresh_token"}, "link": None},
                    {"localized_name": "session", "name": "session", "shape": 7, "type": "ZMONGO_API_SESSION", "link": None},
                    {"localized_name": "master_key_hex", "name": "master_key_hex", "shape": 7, "type": "STRING", "widget": {"name": "master_key_hex"}, "link": None},
                ],
                "outputs": _static_node_output_defs(flat_pack),
                "title": title,
                "properties": {"Node name for S&R": "ZMongoContentPackStaticOutputsV3"},
                "widgets_values": [_json_dumps(flat_pack), "", ""],
            }
        ],
        "links": [],
        "groups": [],
        "config": {},
        "extra": {
            "ds": {"scale": 1.0, "offset": [0, 0]},
            "zmongo_static_content_pack": {
                "workflow_name": workflow_name or flat_pack.get("content_pack_name") or "static_content_pack",
                "content_pack_name": flat_pack.get("content_pack_name"),
                "field_count": flat_pack.get("field_count"),
                "manifest_hash": flat_pack.get("manifest_hash"),
            },
        },
        "version": 0.4,
    }


class ZMongoContentPackStaticOutputsV3(AlwaysDirtyMixin):
    """Static workflow node that exposes one output per embedded content-pack field.

    The companion JS reads content_pack_json and relabels/hides the fixed backend
    outputs so the workflow opens with appropriately typed output sockets.
    """

    CATEGORY = "ZMongo/Content Packs/V3/Static Workflow"
    FUNCTION = "static_outputs"
    RETURN_TYPES = tuple([ANY_TYPE] * MAX_STATIC_OUTPUTS)
    RETURN_NAMES = tuple([f"out_{index:02d}" for index in range(MAX_STATIC_OUTPUTS)])
    OUTPUT_IS_LIST = tuple([False] * MAX_STATIC_OUTPUTS)

    @classmethod
    def INPUT_TYPES(cls):
        return {
            "required": {
                "content_pack_json": ("STRING", {"default": "{}", "multiline": True}),
            },
            "optional": {
                "refresh_token": ("STRING", {"default": ""}),
                "session": ("ZMONGO_API_SESSION",),
                "master_key_hex": ("STRING", {"default": ""}),
            },
        }

    def static_outputs(self, content_pack_json: str = "{}", refresh_token: str = "", session: Any = None, master_key_hex: str = ""):
        try:
            flat_pack = _flatten_content_pack_for_static_outputs(content_pack_json)
            fields = [field for field in flat_pack.get("fields", []) if isinstance(field, dict)]

            values: list[Any] = []
            for index in range(MAX_STATIC_OUTPUTS):
                if index < len(fields):
                    values.append(_extract_static_value(fields[index], session=session, master_key_hex=master_key_hex))
                else:
                    values.append("")

            return tuple(values[:MAX_STATIC_OUTPUTS])
        except Exception:
            return tuple(_typed_fallback_values_from_content_pack_json(content_pack_json))


class ZMongoContentPackExportStaticWorkflowV3(AlwaysDirtyMixin):
    """Export a content pack as a self-contained workflow.json with static outputs."""

    CATEGORY = "ZMongo/Content Packs/V3/Static Workflow"
    FUNCTION = "export_static_workflow"
    RETURN_TYPES = ("STRING", "STRING", "STRING", "BOOLEAN", "STRING")
    RETURN_NAMES = ("workflow_json", "flat_pack_json", "filename", "success", "refresh")
    OUTPUT_NODE = True

    @classmethod
    def INPUT_TYPES(cls):
        return {
            "required": {
                "content_pack": (CONTENT_PACK_TYPE,),
                "workflow_filename_prefix": ("STRING", {"default": "content_pack_static_workflow"}),
                "node_title": ("STRING", {"default": "📦 Static Content Pack Outputs"}),
                "pretty_json": ("BOOLEAN", {"default": True}),
                "browser_download": ("BOOLEAN", {"default": True}),
            },
            "optional": {
                "refresh_token": ("STRING", {"default": ""}),
            },
        }

    def export_static_workflow(
        self,
        content_pack: Any,
        workflow_filename_prefix: str = "content_pack_static_workflow",
        node_title: str = "📦 Static Content Pack Outputs",
        pretty_json: bool = True,
        browser_download: bool = True,
        refresh_token: str = "",
    ):
        refresh = hashlib.sha256(f"{time.time()}::{refresh_token}".encode("utf-8", errors="ignore")).hexdigest()[:16]
        try:
            flat_pack = _flatten_content_pack_for_static_outputs(content_pack)
            workflow = _make_static_content_pack_workflow(
                flat_pack,
                node_title=node_title,
                workflow_name=workflow_filename_prefix or flat_pack.get("content_pack_name") or "content_pack_static_workflow",
            )
            workflow_json = _json_dumps(workflow, pretty=bool(pretty_json))
            flat_json = _json_dumps(flat_pack, pretty=bool(pretty_json))
            filename = f"{_safe_filename_stem(workflow_filename_prefix or flat_pack.get('content_pack_name'))}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.workflow.json"
            success = True
            ui = {
                "workflow_json": [workflow_json],
                "flat_pack_json": [flat_json],
                "filename": [filename],
                "message": [f"Static content-pack workflow exported with {flat_pack.get('field_count', 0)} typed output(s)."],
                "browser_download": [bool(browser_download)],
            }
            return {"ui": ui, "result": (workflow_json, flat_json, filename, success, refresh)}
        except Exception as exc:
            payload = {"success": False, "message": f"Static workflow export failed: {exc}", "error": {"type": exc.__class__.__name__, "msg": str(exc)}}
            return (_json_dumps(payload), "{}", "", False, refresh)


NODE_CLASS_MAPPINGS = {
    "ZMongoContentPackStaticOutputsV3": ZMongoContentPackStaticOutputsV3,
    "ZMongoContentPackExportStaticWorkflowV3": ZMongoContentPackExportStaticWorkflowV3,
}

NODE_DISPLAY_NAME_MAPPINGS = {
    "ZMongoContentPackStaticOutputsV3": "ZMongo Content Pack Static Outputs V3",
    "ZMongoContentPackExportStaticWorkflowV3": "ZMongo Content Pack Export Static Workflow V3",
}

__all__ = ["NODE_CLASS_MAPPINGS", "NODE_DISPLAY_NAME_MAPPINGS"]
