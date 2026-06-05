from __future__ import annotations

# pylint: disable=too-many-lines,too-many-locals,too-many-branches,too-many-statements,broad-exception-caught,unused-argument,line-too-long,missing-class-docstring,missing-function-docstring

"""
ComfyUI-ZMongo Content Pack V3 Nodes
====================================

V3 architecture goal:
- A content pack is a normalized typed manifest, not a dynamic Python node schema.
- Original source dot paths are metadata.
- User-facing outputs use stable aliases.
- Backend output types remain fixed and Comfy-compatible.
- Images are rehydrated into real Comfy IMAGE tensors by typed extractor nodes.

This script is intentionally self-contained and can live beside the legacy
zmongo_content_pack_nodes.py while the V3 architecture is introduced.

Install target:
    custom_nodes/ComfyUI-ZMongo/zmongo_content_pack_v3_nodes.py

Then register it from __init__.py by importing this module and merging its
NODE_CLASS_MAPPINGS / NODE_DISPLAY_NAME_MAPPINGS.
"""

import base64
import hashlib
import io
import json
import re
import time
import uuid
import os
from pathlib import Path
from copy import deepcopy
from datetime import datetime, timezone
from typing import Any, Iterable, Optional

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


CONTENT_PACK_SCHEMA_KIND = "zmongo_content_pack"
PORTABLE_CONTENT_PACK_SCHEMA_KIND = "zmongo_portable_content_pack"
CONTENT_PACK_SCHEMA_VERSION = "3.0.0"
CONTENT_PACK_TYPE = "ZMONGO_CONTENT_PACK"
CONTENT_PACK_REF_TYPE = "ZMONGO_CONTENT_PACK_REF"
DEFAULT_CONTENT_PACK_COLLECTION = "text_agent_context_packs"

SUPPORTED_COMFY_TYPES = {"STRING", "INT", "FLOAT", "BOOLEAN", "IMAGE", "JSON", "ANY"}


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _safe_str(value: Any, default: str = "") -> str:
    if value is None:
        return default
    if isinstance(value, (list, tuple)):
        value = value[0] if value else default
    return str(value).strip()


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except Exception:
        return int(default)


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except Exception:
        return float(default)


def _json_dumps(value: Any, *, pretty: bool = True) -> str:
    try:
        if pretty:
            return json.dumps(value, indent=2, ensure_ascii=False, sort_keys=False, default=str)
        return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), default=str)
    except Exception:
        return json.dumps({"error": "json serialization failed", "repr": repr(value)}, indent=2)


def _parse_json(value: Any, default: Any = None) -> Any:
    if default is None:
        default = {}
    if isinstance(value, (dict, list)):
        return value
    text = _safe_str(value)
    if not text:
        return default
    try:
        return json.loads(text)
    except Exception:
        return default


def _csv_list(value: Any) -> list[str]:
    if isinstance(value, list):
        return [_safe_str(item) for item in value if _safe_str(item)]
    text = _safe_str(value)
    if not text:
        return []
    parsed = _parse_json(text, default=None)
    if isinstance(parsed, list):
        return [_safe_str(item) for item in parsed if _safe_str(item)]
    return [part.strip() for part in re.split(r"[,\n\r\t]+", text) if part.strip()]


def _slug_alias(value: str, fallback: str = "field") -> str:
    text = _safe_str(value).strip(".")
    if not text:
        text = fallback
    text = text.replace("[]", "")
    text = re.sub(r"[^A-Za-z0-9_]+", "_", text)
    text = re.sub(r"_+", "_", text).strip("_")
    if not text:
        text = fallback
    if text[0].isdigit():
        text = f"field_{text}"
    return text.lower()


def _unique_alias(base: str, used: set[str]) -> str:
    candidate = _slug_alias(base)
    if candidate not in used:
        used.add(candidate)
        return candidate
    index = 2
    while f"{candidate}_{index}" in used:
        index += 1
    final = f"{candidate}_{index}"
    used.add(final)
    return final


def _title_from_alias(alias: str) -> str:
    words = re.split(r"[_\s]+", _safe_str(alias))
    return " ".join(word[:1].upper() + word[1:] for word in words if word)


def _get_dot_path(value: Any, path: str, default: Any = None) -> Any:
    if not path:
        return value
    cur = value
    for part in str(path).split("."):
        if isinstance(cur, dict):
            if part not in cur:
                return default
            cur = cur[part]
        elif isinstance(cur, list):
            try:
                cur = cur[int(part)]
            except Exception:
                return default
        else:
            return default
    return cur


def _flatten_json(value: Any, prefix: str = "") -> list[tuple[str, Any]]:
    rows: list[tuple[str, Any]] = []

    if isinstance(value, dict):
        if not value:
            rows.append((prefix, value))
        for key, item in value.items():
            child = f"{prefix}.{key}" if prefix else str(key)
            rows.extend(_flatten_json(item, child))
        return rows

    if isinstance(value, list):
        if not value:
            rows.append((prefix, value))
        for index, item in enumerate(value):
            child = f"{prefix}.{index}" if prefix else str(index)
            rows.extend(_flatten_json(item, child))
        return rows

    rows.append((prefix, value))
    return rows


def _path_matches(path: str, patterns: Iterable[str]) -> bool:
    clean = _safe_str(path)
    for raw in patterns:
        pattern = _safe_str(raw).strip(".")
        if not pattern:
            continue
        if clean == pattern or clean.startswith(pattern + "."):
            return True
    return False


def _looks_like_image_path(path: str) -> bool:
    lowered = _safe_str(path).lower()
    tokens = ("image", "img", "picture", "photo", "thumbnail", "preview", "mask")
    return any(token in lowered for token in tokens)


def _looks_like_image_value(value: Any, path: str = "") -> bool:
    if isinstance(value, dict):
        content_type = _safe_str(value.get("content_type") or value.get("mime_type")).lower()
        asset_type = _safe_str(value.get("asset_type") or value.get("type")).lower()
        if content_type.startswith("image/") or asset_type in {"image", "image_asset", "preview_image"}:
            return True
        image_keys = {
            "image_data", "image", "images", "base64", "b64", "bytes", "file_id",
            "asset_ref", "original", "variants", "preview", "thumbnail", "local_path",
        }
        if _looks_like_image_path(path) and any(key in value for key in image_keys):
            return True
        if any(key in value for key in ("asset_collection", "document_id", "field_path")) and _looks_like_image_path(path):
            return True
    if isinstance(value, str):
        text = value.strip()
        if text.startswith("data:image/"):
            return True
        if _looks_like_image_path(path) and len(text) > 100 and re.match(r"^[A-Za-z0-9+/=\s]+$", text[:200]):
            return True
    return False


def _classify_value(value: Any, path: str = "") -> tuple[str, str]:
    if _looks_like_image_value(value, path):
        return "IMAGE", "image_asset"
    if isinstance(value, bool):
        return "BOOLEAN", "boolean"
    if isinstance(value, int) and not isinstance(value, bool):
        return "INT", "integer"
    if isinstance(value, float):
        return "FLOAT", "float"
    if isinstance(value, str):
        return "STRING", "string"
    if isinstance(value, (dict, list)):
        return "JSON", "json"
    if value is None:
        return "ANY", "null"
    return "STRING", type(value).__name__


def _normalize_type(value: str, fallback: str = "ANY") -> str:
    text = _safe_str(value).upper()
    aliases = {
        "STR": "STRING",
        "TEXT": "STRING",
        "BOOL": "BOOLEAN",
        "INTEGER": "INT",
        "NUMBER": "FLOAT",
        "DOUBLE": "FLOAT",
        "IMAGE_ASSET": "IMAGE",
        "OBJECT": "JSON",
        "DICT": "JSON",
        "LIST": "JSON",
    }
    text = aliases.get(text, text)
    return text if text in SUPPORTED_COMFY_TYPES else fallback


def _value_summary(value: Any, limit: int = 160) -> dict[str, Any]:
    if isinstance(value, str):
        text = value.replace("\n", " ").replace("\r", " ").strip()
        return {"display": text[:limit], "length": len(value), "truncated": len(text) > limit}
    if isinstance(value, (int, float, bool)) or value is None:
        return {"display": str(value), "truncated": False}
    if isinstance(value, (dict, list)):
        text = _json_dumps(value, pretty=False)
        return {"display": text[:limit], "length": len(text), "truncated": len(text) > limit}
    return {"display": str(value)[:limit], "truncated": len(str(value)) > limit}


def _stable_hash(value: Any) -> str:
    return hashlib.sha256(_json_dumps(value, pretty=False).encode("utf-8", errors="ignore")).hexdigest()


def _as_content_pack(value: Any) -> dict[str, Any]:
    parsed = _parse_json(value, default={})
    if isinstance(parsed, dict) and parsed.get("schema_kind") == CONTENT_PACK_SCHEMA_KIND:
        return parsed
    if isinstance(parsed, dict) and parsed.get("schema_kind") == PORTABLE_CONTENT_PACK_SCHEMA_KIND:
        pack = deepcopy(parsed)
        pack["schema_kind"] = CONTENT_PACK_SCHEMA_KIND
        pack.setdefault("schema_version", CONTENT_PACK_SCHEMA_VERSION)
        pack.setdefault("source", {"source_mode": "portable_json"})
        pack.setdefault("outputs", _build_field_outputs(pack.get("fields") if isinstance(pack.get("fields"), list) else []))
        pack.setdefault("field_count", len(pack.get("fields") if isinstance(pack.get("fields"), list) else []))
        return pack
    if isinstance(value, dict) and value.get("schema_kind") == CONTENT_PACK_SCHEMA_KIND:
        return value
    if isinstance(value, dict) and value.get("schema_kind") == PORTABLE_CONTENT_PACK_SCHEMA_KIND:
        pack = deepcopy(value)
        pack["schema_kind"] = CONTENT_PACK_SCHEMA_KIND
        pack.setdefault("schema_version", CONTENT_PACK_SCHEMA_VERSION)
        pack.setdefault("source", {"source_mode": "portable_json"})
        pack.setdefault("outputs", _build_field_outputs(pack.get("fields") if isinstance(pack.get("fields"), list) else []))
        pack.setdefault("field_count", len(pack.get("fields") if isinstance(pack.get("fields"), list) else []))
        return pack
    return {}


def _field_list(content_pack: Any) -> list[dict[str, Any]]:
    pack = _as_content_pack(content_pack)
    fields = pack.get("fields")
    if isinstance(fields, list):
        return [field for field in fields if isinstance(field, dict)]

    # Backwards tolerance for legacy field_index docs.
    legacy = pack.get("field_index")
    if isinstance(legacy, list):
        converted = []
        used: set[str] = set()
        for index, item in enumerate(legacy):
            if not isinstance(item, dict):
                continue
            path = _safe_str(item.get("path") or item.get("name") or f"field_{index}")
            comfy_type, json_type = _classify_value(item.get("value"), path)
            kind = _safe_str(item.get("kind"))
            if kind == "image_asset":
                comfy_type, json_type = "IMAGE", "image_asset"
            elif kind == "boolean":
                comfy_type, json_type = "BOOLEAN", "boolean"
            elif kind == "number":
                comfy_type = "INT" if item.get("value_type") == "int" else "FLOAT"
                json_type = item.get("value_type") or "number"
            elif kind in {"text", "metadata"}:
                comfy_type, json_type = "STRING", "string"
            alias = _unique_alias(path.split(".")[-1] or path, used)
            converted.append({
                "index": len(converted),
                "alias": alias,
                "label": _title_from_alias(alias),
                "source_path": path,
                "comfy_type": comfy_type,
                "json_type": json_type,
                "storage": item.get("storage") or "inline",
                "value": item.get("value"),
                "asset_ref": item.get("ref") if isinstance(item.get("ref"), dict) else item.get("asset_ref", {}),
                "summary": item.get("summary") or _value_summary(item.get("value")),
            })
        return converted

    return []


def _find_field(content_pack: Any, field_alias: str, expected_type: str = "") -> Optional[dict[str, Any]]:
    alias = _safe_str(field_alias)
    expected = _normalize_type(expected_type, "") if expected_type else ""
    fields = _field_list(content_pack)

    # Exact alias first.
    for field in fields:
        if _safe_str(field.get("alias")) == alias:
            if not expected or _normalize_type(field.get("comfy_type"), "ANY") == expected:
                return field
            return field

    # Allow source path or label as fallback.
    for field in fields:
        if alias in {_safe_str(field.get("source_path")), _safe_str(field.get("label"))}:
            return field

    # Allow numeric index.
    try:
        wanted = int(alias)
        for field in fields:
            if int(field.get("index", -1)) == wanted:
                return field
    except Exception:
        pass

    return None


def _coerce_field_value(field: dict[str, Any]) -> Any:
    if not isinstance(field, dict):
        return None
    if field.get("storage") == "inline":
        return field.get("value")
    if "value" in field and field.get("value") is not None:
        return field.get("value")
    return field.get("asset_ref") or field.get("json_ref") or None


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


def _decode_base64_image_text(text: str) -> bytes:
    clean = _safe_str(text)
    if clean.startswith("data:") and "," in clean:
        clean = clean.split(",", 1)[1]
    return base64.b64decode(clean, validate=False)


def _decode_image_bytes_from_value(value: Any) -> bytes:
    if value is None:
        raise ValueError("Image value is empty.")
    if isinstance(value, bytes):
        return value
    if isinstance(value, bytearray):
        return bytes(value)
    if isinstance(value, str):
        return _decode_base64_image_text(value)
    if isinstance(value, list):
        if not value:
            raise ValueError("Image list is empty.")
        return _decode_image_bytes_from_value(value[0])
    if isinstance(value, dict):
        if value.get("__type__") == "bytes" and value.get("encoding") == "base64":
            return _decode_base64_image_text(str(value.get("data") or ""))
        for key in ("data", "image_data", "base64", "b64", "bytes", "image", "content"):
            if key in value and value.get(key) is not None:
                try:
                    return _decode_image_bytes_from_value(value.get(key))
                except Exception:
                    pass
    raise ValueError(f"Unsupported image payload type: {type(value).__name__}")


def _image_bytes_to_tensor(image_bytes: bytes) -> Any:
    if Image is None:
        raise RuntimeError("Pillow is required to decode content pack images.")
    with Image.open(io.BytesIO(image_bytes)) as image:
        return _pil_to_comfy_image(image)


def _extract_documents_from_payload(payload: Any) -> list[dict[str, Any]]:
    if isinstance(payload, list):
        return [item for item in payload if isinstance(item, dict)]
    if not isinstance(payload, dict):
        return []

    docs: list[dict[str, Any]] = []

    def add(value: Any) -> None:
        if isinstance(value, dict):
            docs.append(value)
        elif isinstance(value, list):
            docs.extend(item for item in value if isinstance(item, dict))

    add(payload.get("document"))
    add(payload.get("doc"))
    add(payload.get("item"))
    add(payload.get("record"))
    add(payload.get("documents"))
    add(payload.get("docs"))
    add(payload.get("items"))
    add(payload.get("records"))

    data = payload.get("data")
    if isinstance(data, dict):
        add(data.get("document"))
        add(data.get("doc"))
        add(data.get("item"))
        add(data.get("record"))
        add(data.get("documents"))
        add(data.get("docs"))
        add(data.get("items"))
        add(data.get("records"))
        if data.get("_id") and data.get("schema_kind"):
            add(data)

    if payload.get("_id") and payload.get("schema_kind"):
        add(payload)

    # Deduplicate by _id if possible.
    seen: set[str] = set()
    unique: list[dict[str, Any]] = []
    for doc in docs:
        key = _safe_str(doc.get("_id") or id(doc))
        if key in seen:
            continue
        seen.add(key)
        unique.append(doc)
    return unique


def _content_pack_doc_score(doc: dict[str, Any]) -> tuple[int, int, float, str]:
    if not isinstance(doc, dict):
        return (-1, -1, 0.0, "")
    schema_bonus = 1000 if doc.get("schema_kind") == CONTENT_PACK_SCHEMA_KIND else 0
    fields = doc.get("fields")
    legacy = doc.get("field_index")
    count = 0
    if isinstance(fields, list):
        count = max(count, len(fields))
    if isinstance(legacy, list):
        count = max(count, len(legacy))
    usable_bonus = 500 if count > 0 else 0
    timestamp = 0.0
    for key in ("updated_at_unix", "created_at_unix"):
        try:
            timestamp = max(timestamp, float(doc.get(key) or 0.0))
        except Exception:
            pass
    return (schema_bonus + usable_bonus, count, timestamp, _safe_str(doc.get("updated_at") or doc.get("created_at")))


def _select_best_content_pack_doc(docs: list[dict[str, Any]]) -> dict[str, Any]:
    candidates = [doc for doc in docs if isinstance(doc, dict) and doc.get("schema_kind") == CONTENT_PACK_SCHEMA_KIND]
    if not candidates:
        candidates = [doc for doc in docs if isinstance(doc, dict)]
    if not candidates:
        return {}
    return max(candidates, key=_content_pack_doc_score)


def _content_pack_alias_type_pairs(content_pack: Any, *, include_types: str = "") -> list[tuple[str, str]]:
    """Return (alias, comfy_type) pairs in field order.

    The alias list and data type list are intended to be selected with the
    same index, matching the List Collections/List Docs selectable + indexed
    pattern used elsewhere in ComfyUI-ZMongo.
    """
    allowed = {_normalize_type(item, "") for item in _csv_list(include_types)}
    allowed.discard("")
    pairs: list[tuple[str, str]] = []
    for field in _field_list(content_pack):
        alias = _safe_str(field.get("alias"))
        if not alias:
            continue
        comfy_type = _normalize_type(field.get("comfy_type"), "ANY")
        if allowed and comfy_type not in allowed:
            continue
        pairs.append((alias, comfy_type))
    return pairs


def _content_pack_alias_items(content_pack: Any, *, include_types: str = "") -> list[str]:
    """Return plain alias names from the normalized V3 field manifest."""
    return [alias for alias, _comfy_type in _content_pack_alias_type_pairs(content_pack, include_types=include_types)]


def _content_pack_type_items(content_pack: Any, *, include_types: str = "") -> list[str]:
    """Return Comfy datatypes in the same order as _content_pack_alias_items()."""
    return [comfy_type for _alias, comfy_type in _content_pack_alias_type_pairs(content_pack, include_types=include_types)]


def _content_pack_selectable_aliases(content_pack: Any) -> list[dict[str, Any]]:
    """Return machine-readable selectable alias records for UI/dropdown helpers."""
    rows: list[dict[str, Any]] = []
    for index, field in enumerate(_field_list(content_pack)):
        alias = _safe_str(field.get("alias"))
        if not alias:
            continue
        comfy_type = _normalize_type(field.get("comfy_type"), "ANY")
        label = _safe_str(field.get("label")) or _title_from_alias(alias)
        source_path = _safe_str(field.get("source_path"))
        rows.append({
            "index": index,
            "alias": alias,
            "data_type": comfy_type,
            "comfy_type": comfy_type,
            "label": label,
            "json_type": _safe_str(field.get("json_type")),
            "source_path": source_path,
            "storage": _safe_str(field.get("storage")),
            "display": f"{index}: {alias} [{comfy_type}]",
            "dropdown_label": f"{alias}  •  {comfy_type}",
        })
    return rows


def _content_pack_indexed_aliases_text(content_pack: Any) -> str:
    """Return indexed aliases with datatype displayed next to each alias."""
    return _indexed_list_text([f"{alias} [{comfy_type}]" for alias, comfy_type in _content_pack_alias_type_pairs(content_pack)])


def _content_pack_aliases_csv(content_pack: Any, *, include_types: str = "") -> str:
    return ",".join(_content_pack_alias_items(content_pack, include_types=include_types))


def _content_pack_data_types_csv(content_pack: Any, *, include_types: str = "") -> str:
    return ",".join(_content_pack_type_items(content_pack, include_types=include_types))

def _manifest_only(content_pack: dict[str, Any]) -> dict[str, Any]:
    pack = _as_content_pack(content_pack)
    return {
        "schema_kind": "zmongo_content_pack_manifest",
        "schema_version": pack.get("schema_version") or CONTENT_PACK_SCHEMA_VERSION,
        "content_pack_name": pack.get("content_pack_name", ""),
        "project_name": pack.get("project_name", ""),
        "manifest_hash": pack.get("manifest_hash", ""),
        "field_count": len(_field_list(pack)),
        "fields": [
            {
                "index": field.get("index"),
                "alias": field.get("alias"),
                "label": field.get("label"),
                "source_path": field.get("source_path"),
                "comfy_type": field.get("comfy_type"),
                "json_type": field.get("json_type"),
                "storage": field.get("storage"),
                "summary": field.get("summary"),
            }
            for field in _field_list(pack)
        ],
    }


def _preview_markdown(content_pack: dict[str, Any]) -> str:
    pack = _as_content_pack(content_pack)
    fields = _field_list(pack)
    lines = [
        f"# Content Pack V3: {pack.get('content_pack_name', '')}",
        "",
        f"- Project: `{pack.get('project_name', '')}`",
        f"- Schema: `{pack.get('schema_version', '')}`",
        f"- Fields: `{len(fields)}`",
        f"- Manifest hash: `{pack.get('manifest_hash', '')}`",
        "",
        "## Fields",
        "",
    ]
    if not fields:
        lines.append("No fields.")
    for field in fields:
        alias = field.get("alias", "")
        source_path = field.get("source_path", "")
        comfy_type = field.get("comfy_type", "ANY")
        summary = field.get("summary") if isinstance(field.get("summary"), dict) else {}
        display = summary.get("display", "")
        lines.append(f"{field.get('index', 0)}. `{alias}` — **{comfy_type}**")
        lines.append(f"   - Source: `{source_path}`")
        if display:
            lines.append(f"   - Value: {display}")
    return "\n".join(lines)


def _build_field_outputs(fields: list[dict[str, Any]]) -> dict[str, Any]:
    groups = {"strings": [], "ints": [], "floats": [], "booleans": [], "images": [], "json": [], "any": []}
    for field in fields:
        item = {
            "index": field.get("index"),
            "alias": field.get("alias"),
            "label": field.get("label"),
            "source_path": field.get("source_path"),
            "comfy_type": field.get("comfy_type"),
            "summary": field.get("summary"),
        }
        comfy_type = _normalize_type(field.get("comfy_type"), "ANY")
        if comfy_type == "STRING":
            groups["strings"].append(item)
        elif comfy_type == "INT":
            groups["ints"].append(item)
        elif comfy_type == "FLOAT":
            groups["floats"].append(item)
        elif comfy_type == "BOOLEAN":
            groups["booleans"].append(item)
        elif comfy_type == "IMAGE":
            groups["images"].append(item)
        elif comfy_type == "JSON":
            groups["json"].append(item)
        else:
            groups["any"].append(item)
    return groups


def _make_pack(
    *,
    source_document: Any,
    content_pack_name: str,
    project_name: str,
    source_collection: str,
    source_document_id: str,
    field_selection_mode: str,
    include_paths: list[str],
    exclude_paths: list[str],
    alias_map: dict[str, Any],
    type_overrides: dict[str, Any],
    image_policy: str,
    max_inline_text_chars: int,
    max_inline_json_bytes: int,
) -> dict[str, Any]:
    flattened = _flatten_json(source_document)
    mode = _safe_str(field_selection_mode, "auto_safe").lower()
    image_policy = _safe_str(image_policy, "reference").lower()
    used_aliases: set[str] = set()
    fields: list[dict[str, Any]] = []

    for path, value in flattened:
        if not path:
            continue
        if include_paths and not _path_matches(path, include_paths):
            continue
        if exclude_paths and _path_matches(path, exclude_paths):
            continue
        if mode in {"manual_include_paths", "include_paths"} and include_paths and not _path_matches(path, include_paths):
            continue

        comfy_type, json_type = _classify_value(value, path)
        override = type_overrides.get(path) if isinstance(type_overrides, dict) else None
        if override:
            comfy_type = _normalize_type(override, comfy_type)

        # Avoid capturing giant non-image binary-ish strings unless explicitly included.
        if comfy_type == "STRING" and len(value) > max_inline_text_chars and not include_paths:
            continue

        alias_base = _safe_str(alias_map.get(path)) if isinstance(alias_map, dict) else ""
        if not alias_base:
            alias_base = path.split(".")[-1] or path
            if alias_base.isdigit() and "." in path:
                alias_base = path.replace(".", "_")
        alias = _unique_alias(alias_base, used_aliases)

        storage = "inline"
        stored_value = deepcopy(value)
        asset_ref: dict[str, Any] = {}
        json_ref: dict[str, Any] = {}

        if comfy_type == "IMAGE":
            if image_policy == "ignore":
                continue
            storage = "asset_ref" if isinstance(value, dict) else "inline_base64"
            if isinstance(value, dict):
                asset_ref = deepcopy(value)
                stored_value = None
        elif comfy_type == "JSON":
            json_text = _json_dumps(value, pretty=False)
            if len(json_text.encode("utf-8", errors="ignore")) > max_inline_json_bytes:
                storage = "json_ref_pending"
                json_ref = {"reason": "value exceeded max_inline_json_bytes", "source_path": path}
                stored_value = None

        fields.append({
            "index": len(fields),
            "alias": alias,
            "label": _title_from_alias(alias),
            "source_path": path,
            "comfy_type": comfy_type,
            "json_type": json_type,
            "storage": storage,
            "value": stored_value,
            "asset_ref": asset_ref,
            "json_ref": json_ref,
            "summary": _value_summary(value),
        })

    created_at = _utc_now_iso()
    content_pack: dict[str, Any] = {
        "schema_kind": CONTENT_PACK_SCHEMA_KIND,
        "schema_version": CONTENT_PACK_SCHEMA_VERSION,
        "content_pack_name": _safe_str(content_pack_name) or "content_pack",
        "project_name": _safe_str(project_name) or "default",
        "source": {
            "source_collection": _safe_str(source_collection),
            "source_document_id": _safe_str(source_document_id),
            "field_selection_mode": field_selection_mode,
            "include_paths": include_paths,
            "exclude_paths": exclude_paths,
            "image_policy": image_policy,
        },
        "fields": fields,
        "outputs": _build_field_outputs(fields),
        "created_at": created_at,
        "updated_at": created_at,
        "created_at_unix": time.time(),
        "updated_at_unix": time.time(),
    }
    content_pack["field_count"] = len(fields)
    content_pack["manifest_hash"] = _stable_hash(_manifest_only(content_pack))
    content_pack["content"] = {
        "markdown": _preview_markdown(content_pack),
        "summary": f"Content pack '{content_pack['content_pack_name']}' contains {len(fields)} typed field(s).",
    }
    return content_pack


def _session_create_doc(session: Any, collection: str, document: dict[str, Any]) -> dict[str, Any]:
    if session is None:
        return {"success": False, "message": "session is required", "data": {}, "error": {"msg": "Missing session"}}
    if hasattr(session, "create_doc") and callable(session.create_doc):
        return session.create_doc(collection=collection, document=document)
    return {"success": False, "message": "session does not expose create_doc", "data": {}, "error": {"msg": "Missing method"}}


def _session_update_doc(session: Any, collection: str, query: dict[str, Any], document: dict[str, Any], document_id: str = "") -> dict[str, Any]:
    if session is None:
        return {"success": False, "message": "session is required", "data": {}, "error": {"msg": "Missing session"}}
    if hasattr(session, "update_doc") and callable(session.update_doc):
        return session.update_doc(collection=collection, query=query, document_id=document_id, update={"$set": document}, upsert=True)
    return _session_create_doc(session, collection, document)


def _session_query_docs(session: Any, collection: str, query: dict[str, Any], document_id: str = "", limit: int = 50) -> dict[str, Any]:
    if session is None:
        return {"success": False, "message": "session is required", "data": {}, "error": {"msg": "Missing session"}}
    if hasattr(session, "query_docs") and callable(session.query_docs):
        return session.query_docs(
            collection=collection,
            query=query,
            document_id=document_id,
            many=True,
            limit=limit,
            sort=[["updated_at_unix", -1]],
            cache=False,
        )
    if document_id and hasattr(session, "get_doc") and callable(session.get_doc):
        return session.get_doc(collection=collection, document_id=document_id, cache=False)
    return {"success": False, "message": "session does not expose query_docs/get_doc", "data": {}, "error": {"msg": "Missing method"}}


class ZMongoContentPackBuildV3(AlwaysDirtyMixin):
    CATEGORY = "ZMongo/09 Content Packs"
    FUNCTION = "build"
    RETURN_TYPES = (CONTENT_PACK_TYPE, "STRING", "STRING", "STRING", "*", "*", "STRING", "BOOLEAN")
    RETURN_NAMES = ("content_pack", "content_pack_json", "manifest_json", "field_report_json", "aliases", "data_types", "indexed", "success")
    OUTPUT_IS_LIST = (False, False, False, False, True, True, False, False)

    @classmethod
    def INPUT_TYPES(cls):
        return {
            "required": {
                "document_json": ("STRING", {"default": "{}", "multiline": True}),
                "content_pack_name": ("STRING", {"default": "content_pack"}),
                "project_name": ("STRING", {"default": "default"}),
                "field_selection_mode": (["auto_safe", "select_all_safe", "manual_include_paths", "include_paths"], {"default": "auto_safe"}),
                "image_policy": (["reference", "copy_reference", "ignore"], {"default": "reference"}),
            },
            "optional": {
                "source_collection": ("STRING", {"default": ""}),
                "source_document_id": ("STRING", {"default": ""}),
                "include_paths_csv": ("STRING", {"default": "", "multiline": True}),
                "exclude_paths_csv": ("STRING", {"default": "", "multiline": True}),
                "alias_map_json": ("STRING", {"default": "{}", "multiline": True}),
                "type_overrides_json": ("STRING", {"default": "{}", "multiline": True}),
                "max_inline_text_chars": ("INT", {"default": 3000, "min": 1, "max": 200000}),
                "max_inline_json_bytes": ("INT", {"default": 8192, "min": 1, "max": 2000000}),
                "refresh_token": ("STRING", {"default": ""}),
            },
        }

    def build(
        self,
        document_json: str,
        content_pack_name: str,
        project_name: str,
        field_selection_mode: str,
        image_policy: str,
        source_collection: str = "",
        source_document_id: str = "",
        include_paths_csv: str = "",
        exclude_paths_csv: str = "",
        alias_map_json: str = "{}",
        type_overrides_json: str = "{}",
        max_inline_text_chars: int = 3000,
        max_inline_json_bytes: int = 8192,
        refresh_token: str = "",
    ):
        source = _parse_json(document_json, default={})
        if not isinstance(source, (dict, list)):
            source = {"value": source}

        alias_map = _parse_json(alias_map_json, default={})
        if not isinstance(alias_map, dict):
            alias_map = {}
        type_overrides = _parse_json(type_overrides_json, default={})
        if not isinstance(type_overrides, dict):
            type_overrides = {}

        pack = _make_pack(
            source_document=source,
            content_pack_name=content_pack_name,
            project_name=project_name,
            source_collection=source_collection,
            source_document_id=source_document_id,
            field_selection_mode=field_selection_mode,
            include_paths=_csv_list(include_paths_csv),
            exclude_paths=_csv_list(exclude_paths_csv),
            alias_map=alias_map,
            type_overrides=type_overrides,
            image_policy=image_policy,
            max_inline_text_chars=_safe_int(max_inline_text_chars, 3000),
            max_inline_json_bytes=_safe_int(max_inline_json_bytes, 8192),
        )
        manifest = _manifest_only(pack)
        report = {
            "success": True,
            "field_count": len(_field_list(pack)),
            "outputs": pack.get("outputs", {}),
            "manifest_hash": pack.get("manifest_hash", ""),
            "content_pack_name": pack.get("content_pack_name", ""),
            "project_name": pack.get("project_name", ""),
        }
        aliases = _content_pack_alias_items(pack)
        data_types = _content_pack_type_items(pack)
        return (pack, _json_dumps(pack), _json_dumps(manifest), _json_dumps(report), _as_comfy_list(aliases), _as_comfy_list(data_types), _content_pack_indexed_aliases_text(pack), True)


class ZMongoContentPackAliasEditorV3(AlwaysDirtyMixin):
    CATEGORY = "ZMongo/09 Content Packs"
    FUNCTION = "edit"
    RETURN_TYPES = (CONTENT_PACK_TYPE, "STRING", "STRING", "*", "*", "STRING", "BOOLEAN")
    RETURN_NAMES = ("content_pack", "manifest_json", "preview_markdown", "aliases", "data_types", "indexed", "success")
    OUTPUT_IS_LIST = (False, False, False, True, True, False, False)

    @classmethod
    def INPUT_TYPES(cls):
        return {
            "required": {
                "content_pack": (CONTENT_PACK_TYPE,),
            },
            "optional": {
                "alias_map_json": ("STRING", {"default": "{}", "multiline": True}),
                "label_map_json": ("STRING", {"default": "{}", "multiline": True}),
                "type_overrides_json": ("STRING", {"default": "{}", "multiline": True}),
                "drop_fields_csv": ("STRING", {"default": "", "multiline": True}),
                "refresh_token": ("STRING", {"default": ""}),
            },
        }

    def edit(
        self,
        content_pack: Any,
        alias_map_json: str = "{}",
        label_map_json: str = "{}",
        type_overrides_json: str = "{}",
        drop_fields_csv: str = "",
        refresh_token: str = "",
    ):
        pack = deepcopy(_as_content_pack(content_pack))
        if not pack:
            return ({}, "{}", "No valid content pack.", _as_comfy_list([]), _as_comfy_list([]), _content_pack_indexed_aliases_text({}), False)

        alias_map = _parse_json(alias_map_json, default={})
        label_map = _parse_json(label_map_json, default={})
        type_overrides = _parse_json(type_overrides_json, default={})
        drops = set(_csv_list(drop_fields_csv))
        used: set[str] = set()
        edited_fields: list[dict[str, Any]] = []

        for field in _field_list(pack):
            old_alias = _safe_str(field.get("alias"))
            source_path = _safe_str(field.get("source_path"))
            if old_alias in drops or source_path in drops:
                continue
            new_field = deepcopy(field)
            alias_raw = alias_map.get(old_alias) or alias_map.get(source_path) if isinstance(alias_map, dict) else ""
            alias = _unique_alias(_safe_str(alias_raw) or old_alias or source_path, used)
            new_field["alias"] = alias
            if isinstance(label_map, dict):
                label = label_map.get(old_alias) or label_map.get(source_path) or label_map.get(alias)
                if label:
                    new_field["label"] = _safe_str(label)
                else:
                    new_field["label"] = _title_from_alias(alias)
            if isinstance(type_overrides, dict):
                override = type_overrides.get(old_alias) or type_overrides.get(source_path) or type_overrides.get(alias)
                if override:
                    new_field["comfy_type"] = _normalize_type(override, new_field.get("comfy_type", "ANY"))
            new_field["index"] = len(edited_fields)
            edited_fields.append(new_field)

        pack["fields"] = edited_fields
        pack["outputs"] = _build_field_outputs(edited_fields)
        pack["field_count"] = len(edited_fields)
        pack["updated_at"] = _utc_now_iso()
        pack["updated_at_unix"] = time.time()
        pack["manifest_hash"] = _stable_hash(_manifest_only(pack))
        pack["content"] = {"markdown": _preview_markdown(pack), "summary": f"Content pack '{pack.get('content_pack_name')}' contains {len(edited_fields)} typed field(s)."}
        aliases = _content_pack_alias_items(pack)
        data_types = _content_pack_type_items(pack)
        return (pack, _json_dumps(_manifest_only(pack)), _preview_markdown(pack), _as_comfy_list(aliases), _as_comfy_list(data_types), _content_pack_indexed_aliases_text(pack), True)


class ZMongoContentPackSaveV3(AlwaysDirtyMixin):
    CATEGORY = "ZMongo/09 Content Packs"
    FUNCTION = "save"
    RETURN_TYPES = (CONTENT_PACK_REF_TYPE, "STRING", "STRING", "BOOLEAN", "STRING")
    RETURN_NAMES = ("pack_ref", "document_id", "content_pack_json", "success", "refresh")

    @classmethod
    def INPUT_TYPES(cls):
        return {
            "required": {
                "session": ("ZMONGO_API_SESSION",),
                "content_pack": (CONTENT_PACK_TYPE,),
                "target_collection": ("STRING", {"default": DEFAULT_CONTENT_PACK_COLLECTION}),
                "overwrite_mode": (["replace_by_name", "create_version", "create_new", "replace_by_document_id"], {"default": "replace_by_name"}),
            },
            "optional": {
                "document_id": ("STRING", {"default": ""}),
                "refresh_token": ("STRING", {"default": ""}),
            },
        }

    def save(self, session: Any, content_pack: Any, target_collection: str, overwrite_mode: str, document_id: str = "", refresh_token: str = ""):
        collection = _safe_str(target_collection) or DEFAULT_CONTENT_PACK_COLLECTION
        mode = _safe_str(overwrite_mode, "replace_by_name")
        pack = deepcopy(_as_content_pack(content_pack))
        if not pack:
            return ({}, "", "{}", False, f"save_content_pack_failed::{time.time_ns()}")

        now = _utc_now_iso()
        pack["updated_at"] = now
        pack["updated_at_unix"] = time.time()
        pack.setdefault("created_at", now)
        pack.setdefault("created_at_unix", pack["updated_at_unix"])
        pack["field_count"] = len(_field_list(pack))
        pack["manifest_hash"] = _stable_hash(_manifest_only(pack))

        name = _safe_str(pack.get("content_pack_name")) or "content_pack"
        project = _safe_str(pack.get("project_name")) or "default"

        if mode == "replace_by_document_id" and _safe_str(document_id):
            payload = _session_update_doc(session, collection, {}, pack, document_id=_safe_str(document_id))
        elif mode == "replace_by_name":
            query = {"schema_kind": CONTENT_PACK_SCHEMA_KIND, "content_pack_name": name, "project_name": project}
            payload = _session_update_doc(session, collection, query, pack)
        else:
            if mode == "create_version":
                pack["version_id"] = uuid.uuid4().hex
            payload = _session_create_doc(session, collection, pack)

        data = payload.get("data") if isinstance(payload, dict) else {}
        if not isinstance(data, dict):
            data = {}
        saved_doc = _select_best_content_pack_doc(_extract_documents_from_payload(payload)) or pack
        saved_id = _safe_str(data.get("document_id") or data.get("inserted_id") or data.get("_id") or saved_doc.get("_id") or document_id)
        if saved_id:
            saved_doc["_id"] = saved_id
        success = bool(payload.get("success")) if isinstance(payload, dict) else False
        ref = {
            "schema_kind": "zmongo_content_pack_ref",
            "schema_version": CONTENT_PACK_SCHEMA_VERSION,
            "collection": collection,
            "document_id": saved_id,
            "content_pack_name": name,
            "project_name": project,
            "manifest_hash": saved_doc.get("manifest_hash") or pack.get("manifest_hash"),
            "success": success,
            "api_payload": payload,
        }
        refresh = f"content_pack_v3_save::{name}:{project}:{saved_id}:{time.time_ns()}"
        return (ref, saved_id, _json_dumps(saved_doc), success, refresh)


class ZMongoContentPackLoadV3(AlwaysDirtyMixin):
    CATEGORY = "ZMongo/09 Content Packs"
    FUNCTION = "load"
    RETURN_TYPES = (CONTENT_PACK_TYPE, "STRING", "STRING", "STRING", "STRING", "*", "*", "STRING", "BOOLEAN")
    RETURN_NAMES = (
        "content_pack",
        "json",
        "manifest_json",
        "summary",
        "document_id",
        "aliases",
        "data_types",
        "indexed",
        "success",
    )
    OUTPUT_IS_LIST = (False, False, False, False, False, True, True, False, False)

    @classmethod
    def INPUT_TYPES(cls):
        return {
            "required": {
                "session": ("ZMONGO_API_SESSION",),
                "source_collection": ("STRING", {"default": DEFAULT_CONTENT_PACK_COLLECTION}),
            },
            "optional": {
                "content_pack_name": ("STRING", {"default": ""}),
                "project_name": ("STRING", {"default": ""}),
                "document_id": ("STRING", {"default": ""}),
                "fallback_to_latest": ("BOOLEAN", {"default": False}),
                "refresh_token": ("STRING", {"default": ""}),
            },
        }

    def load(
        self,
        session: Any,
        source_collection: str,
        content_pack_name: str = "",
        project_name: str = "",
        document_id: str = "",
        fallback_to_latest: bool = False,
        refresh_token: str = "",
    ):
        collection = _safe_str(source_collection) or DEFAULT_CONTENT_PACK_COLLECTION
        doc_id = _safe_str(document_id)
        query: dict[str, Any] = {"schema_kind": CONTENT_PACK_SCHEMA_KIND}
        if _safe_str(content_pack_name):
            query["content_pack_name"] = _safe_str(content_pack_name)
        if _safe_str(project_name):
            query["project_name"] = _safe_str(project_name)

        payload = _session_query_docs(session, collection, query, document_id=doc_id, limit=100)
        docs = _extract_documents_from_payload(payload)
        doc = _select_best_content_pack_doc(docs)

        if not doc and fallback_to_latest:
            payload = _session_query_docs(session, collection, {"schema_kind": CONTENT_PACK_SCHEMA_KIND}, document_id="", limit=100)
            docs = _extract_documents_from_payload(payload)
            doc = _select_best_content_pack_doc(docs)

        if not doc:
            summary = f"No content pack found in {collection}."
            return ({}, "{}", "{}", summary, "", _as_comfy_list([]), _as_comfy_list([]), _content_pack_indexed_aliases_text({}), False)

        manifest = _manifest_only(doc)
        aliases = _content_pack_alias_items(doc)
        data_types = _content_pack_type_items(doc)
        summary = doc.get("content", {}).get("summary") if isinstance(doc.get("content"), dict) else ""
        if not summary:
            summary = f"Loaded content pack '{doc.get('content_pack_name', '')}' with {len(_field_list(doc))} field(s)."

        return (
            doc,
            _json_dumps(doc),
            _json_dumps(manifest),
            _safe_str(summary),
            _safe_str(doc.get("_id") or doc_id),
            _as_comfy_list(aliases),
            _as_comfy_list(data_types),
            _content_pack_indexed_aliases_text(doc),
            True,
        )

class ZMongoContentPackManifestPreviewV3(AlwaysDirtyMixin):
    CATEGORY = "ZMongo/09 Content Packs"
    FUNCTION = "preview"
    RETURN_TYPES = ("STRING", "STRING", "*", "*", "STRING", "BOOLEAN")
    RETURN_NAMES = ("markdown", "field_table_json", "aliases", "data_types", "indexed", "success")
    OUTPUT_IS_LIST = (False, False, True, True, False, False)

    @classmethod
    def INPUT_TYPES(cls):
        return {"required": {"content_pack": (CONTENT_PACK_TYPE,)}}

    def preview(self, content_pack: Any):
        pack = _as_content_pack(content_pack)
        if not pack:
            return ("No valid content pack.", "[]", _as_comfy_list([]), _as_comfy_list([]), _content_pack_indexed_aliases_text({}), False)
        fields = _field_list(pack)
        table = [
            {
                "index": field.get("index"),
                "alias": field.get("alias"),
                "label": field.get("label"),
                "source_path": field.get("source_path"),
                "comfy_type": field.get("comfy_type"),
                "storage": field.get("storage"),
                "summary": field.get("summary"),
            }
            for field in fields
        ]
        aliases = _content_pack_alias_items(pack)
        data_types = _content_pack_type_items(pack)
        return (_preview_markdown(pack), _json_dumps(table), _as_comfy_list(aliases), _as_comfy_list(data_types), _content_pack_indexed_aliases_text(pack), True)


class _BaseContentPackGetV3(AlwaysDirtyMixin):
    CATEGORY = "ZMongo/09 Content Packs/Get"
    EXPECTED_TYPE = "ANY"
    DEFAULT_VALUE: Any = None

    @classmethod
    def INPUT_TYPES(cls):
        return {
            "required": {
                "content_pack": (CONTENT_PACK_TYPE,),
                "field_alias": ("STRING", {"default": ""}),
                "strict_type": ("BOOLEAN", {"default": True}),
            },
            "optional": {
                "default_value": ("STRING", {"default": ""}),
            },
        }

    def _get_field(self, content_pack: Any, field_alias: str, strict_type: bool) -> tuple[Optional[dict[str, Any]], bool, str]:
        field = _find_field(content_pack, field_alias, self.EXPECTED_TYPE if strict_type else "")
        if not field:
            return None, False, f"Field not found: {field_alias}"
        actual = _normalize_type(field.get("comfy_type"), "ANY")
        if strict_type and actual != self.EXPECTED_TYPE:
            return field, False, f"Type mismatch for {field_alias}: expected {self.EXPECTED_TYPE}, got {actual}"
        return field, True, "OK"


class ZMongoContentPackGetStringV3(_BaseContentPackGetV3):
    EXPECTED_TYPE = "STRING"
    FUNCTION = "get_value"
    RETURN_TYPES = ("STRING", "STRING", "BOOLEAN")
    RETURN_NAMES = ("value", "field_info_json", "found")

    def get_value(self, content_pack: Any, field_alias: str, strict_type: bool = True, default_value: str = ""):
        field, ok, message = self._get_field(content_pack, field_alias, strict_type)
        if not field or not ok:
            return (_safe_str(default_value), _json_dumps({"message": message, "field_alias": field_alias, "field": field or {}}), False)
        value = _coerce_field_value(field)
        if isinstance(value, str):
            text = value
        elif value is None:
            text = _safe_str(default_value)
        else:
            text = _json_dumps(value, pretty=False) if isinstance(value, (dict, list)) else str(value)
        return (text, _json_dumps(field), True)


class ZMongoContentPackGetIntV3(_BaseContentPackGetV3):
    EXPECTED_TYPE = "INT"
    FUNCTION = "get_value"
    RETURN_TYPES = ("INT", "STRING", "BOOLEAN")
    RETURN_NAMES = ("value", "field_info_json", "found")

    def get_value(self, content_pack: Any, field_alias: str, strict_type: bool = True, default_value: str = "0"):
        field, ok, message = self._get_field(content_pack, field_alias, strict_type)
        if not field or not ok:
            return (_safe_int(default_value, 0), _json_dumps({"message": message, "field_alias": field_alias, "field": field or {}}), False)
        return (_safe_int(_coerce_field_value(field), _safe_int(default_value, 0)), _json_dumps(field), True)


class ZMongoContentPackGetFloatV3(_BaseContentPackGetV3):
    EXPECTED_TYPE = "FLOAT"
    FUNCTION = "get_value"
    RETURN_TYPES = ("FLOAT", "STRING", "BOOLEAN")
    RETURN_NAMES = ("value", "field_info_json", "found")

    def _get_field(self, content_pack: Any, field_alias: str, strict_type: bool) -> tuple[Optional[dict[str, Any]], bool, str]:
        field = _find_field(content_pack, field_alias, "")
        if not field:
            return None, False, f"Field not found: {field_alias}"
        actual = _normalize_type(field.get("comfy_type"), "ANY")
        if strict_type and actual not in {"FLOAT", "INT"}:
            return field, False, f"Type mismatch for {field_alias}: expected FLOAT-compatible, got {actual}"
        return field, True, "OK"

    def get_value(self, content_pack: Any, field_alias: str, strict_type: bool = True, default_value: str = "0.0"):
        field, ok, message = self._get_field(content_pack, field_alias, strict_type)
        if not field or not ok:
            return (_safe_float(default_value, 0.0), _json_dumps({"message": message, "field_alias": field_alias, "field": field or {}}), False)
        return (_safe_float(_coerce_field_value(field), _safe_float(default_value, 0.0)), _json_dumps(field), True)


class ZMongoContentPackGetBooleanV3(_BaseContentPackGetV3):
    EXPECTED_TYPE = "BOOLEAN"
    FUNCTION = "get_value"
    RETURN_TYPES = ("BOOLEAN", "STRING", "BOOLEAN")
    RETURN_NAMES = ("value", "field_info_json", "found")

    def get_value(self, content_pack: Any, field_alias: str, strict_type: bool = True, default_value: str = "false"):
        field, ok, message = self._get_field(content_pack, field_alias, strict_type)
        fallback = _safe_str(default_value).lower() in {"1", "true", "yes", "on"}
        if not field or not ok:
            return (fallback, _json_dumps({"message": message, "field_alias": field_alias, "field": field or {}}), False)
        value = _coerce_field_value(field)
        if isinstance(value, bool):
            return (value, _json_dumps(field), True)
        if not strict_type:
            return (_safe_str(value).lower() in {"1", "true", "yes", "on"}, _json_dumps(field), True)
        return (fallback, _json_dumps({"message": "Stored value is not boolean", "field": field}), False)


class ZMongoContentPackGetJSONV3(_BaseContentPackGetV3):
    EXPECTED_TYPE = "JSON"
    FUNCTION = "get_value"
    RETURN_TYPES = ("STRING", "STRING", "BOOLEAN")
    RETURN_NAMES = ("json_text", "field_info_json", "found")

    def get_value(self, content_pack: Any, field_alias: str, strict_type: bool = True, default_value: str = "{}"):
        field, ok, message = self._get_field(content_pack, field_alias, strict_type)
        if not field or not ok:
            return (_safe_str(default_value) or "{}", _json_dumps({"message": message, "field_alias": field_alias, "field": field or {}}), False)
        value = _coerce_field_value(field)
        return (_json_dumps(value), _json_dumps(field), True)


class ZMongoContentPackGetImageV3(_BaseContentPackGetV3):
    EXPECTED_TYPE = "IMAGE"
    CATEGORY = "ZMongo/09 Content Packs/Get"
    FUNCTION = "get_image"
    RETURN_TYPES = ("IMAGE", "STRING", "BOOLEAN")
    RETURN_NAMES = ("image", "field_info_json", "found")

    @classmethod
    def INPUT_TYPES(cls):
        return {
            "required": {
                "content_pack": (CONTENT_PACK_TYPE,),
                "field_alias": ("STRING", {"default": ""}),
                "strict_type": ("BOOLEAN", {"default": True}),
            },
            "optional": {
                "session": ("ZMONGO_API_SESSION",),
                "master_key_hex": ("STRING", {"default": ""}),
            },
        }

    def get_image(self, content_pack: Any, field_alias: str, strict_type: bool = True, session: Any = None, master_key_hex: str = ""):
        field, ok, message = self._get_field(content_pack, field_alias, strict_type)
        if not field or not ok:
            return (_empty_image(), _json_dumps({"message": message, "field_alias": field_alias, "field": field or {}}), False)

        try:
            storage = _safe_str(field.get("storage"))
            value = _coerce_field_value(field)
            asset_ref = field.get("asset_ref") if isinstance(field.get("asset_ref"), dict) else {}
            image_bytes: Optional[bytes] = None

            if storage == "inline_base64" or (isinstance(value, str) and value.strip().startswith("data:image/")):
                image_bytes = _decode_image_bytes_from_value(value)
            elif isinstance(value, (str, bytes, bytearray, dict, list)) and not asset_ref:
                image_bytes = _decode_image_bytes_from_value(value)
            elif asset_ref:
                collection = _safe_str(
                    asset_ref.get("collection")
                    or asset_ref.get("asset_collection")
                    or asset_ref.get("original", {}).get("asset_collection") if isinstance(asset_ref.get("original"), dict) else ""
                )
                document_id = _safe_str(
                    asset_ref.get("document_id")
                    or asset_ref.get("file_id")
                    or asset_ref.get("_id")
                    or asset_ref.get("original", {}).get("file_id") if isinstance(asset_ref.get("original"), dict) else ""
                )
                field_path = _safe_str(asset_ref.get("field_path") or field.get("source_path") or "image_data")
                if not session or not hasattr(session, "fetch_image_field"):
                    raise RuntimeError("Image field is an asset reference, but no session with fetch_image_field was provided.")
                image_bytes, _source = session.fetch_image_field(collection=collection, document_id=document_id, field_path=field_path, master_key_hex=master_key_hex)
            else:
                image_bytes = _decode_image_bytes_from_value(value)

            image = _image_bytes_to_tensor(image_bytes or b"")
            return (image, _json_dumps(field), True)
        except Exception as exc:
            info = deepcopy(field)
            info["image_error"] = {"type": exc.__class__.__name__, "message": str(exc)}
            return (_empty_image(), _json_dumps(info), False)


class ZMongoContentPackGetSelectedV3(AlwaysDirtyMixin):
    """Route a selected alias + selected data type to the matching fixed output.

    ComfyUI backend schemas cannot create a brand-new output type from a runtime
    STRING value. This node therefore exposes all supported fixed outputs and
    activates the one matching data_type. Use the same selected index from the
    aliases and data_types outputs.
    """

    CATEGORY = "ZMongo/09 Content Packs/Get"
    FUNCTION = "get_selected"
    RETURN_TYPES = ("STRING", "INT", "FLOAT", "BOOLEAN", "IMAGE", "STRING", "STRING", "BOOLEAN")
    RETURN_NAMES = ("string", "int", "float", "boolean", "image", "json", "selected_type", "found")

    @classmethod
    def INPUT_TYPES(cls):
        return {
            "required": {
                "content_pack": (CONTENT_PACK_TYPE,),
                "field_alias": ("STRING", {"default": ""}),
                "data_type": (["STRING", "INT", "FLOAT", "BOOLEAN", "IMAGE", "JSON", "ANY"], {"default": "STRING"}),
                "strict_type": ("BOOLEAN", {"default": True}),
            },
            "optional": {
                "session": ("ZMONGO_API_SESSION",),
                "default_string": ("STRING", {"default": ""}),
                "default_int": ("INT", {"default": 0, "min": -2147483648, "max": 2147483647}),
                "default_float": ("FLOAT", {"default": 0.0, "min": -1.0e12, "max": 1.0e12}),
                "default_boolean": ("BOOLEAN", {"default": False}),
                "default_json": ("STRING", {"default": "{}", "multiline": True}),
                "master_key_hex": ("STRING", {"default": ""}),
            },
        }

    def get_selected(
        self,
        content_pack: Any,
        field_alias: str,
        data_type: str,
        strict_type: bool = True,
        session: Any = None,
        default_string: str = "",
        default_int: int = 0,
        default_float: float = 0.0,
        default_boolean: bool = False,
        default_json: str = "{}",
        master_key_hex: str = "",
    ):
        selected_type = _normalize_type(data_type, "ANY")
        alias = _safe_str(field_alias)
        image_default = _empty_image()

        field = _find_field(content_pack, alias, selected_type if strict_type and selected_type != "ANY" else "")
        if not field:
            return (
                _safe_str(default_string),
                _safe_int(default_int, 0),
                _safe_float(default_float, 0.0),
                bool(default_boolean),
                image_default,
                _safe_str(default_json) or "{}",
                selected_type,
                False,
            )

        actual_type = _normalize_type(field.get("comfy_type"), "ANY")
        if strict_type and selected_type != "ANY" and actual_type != selected_type and not (selected_type == "FLOAT" and actual_type == "INT"):
            return (
                _safe_str(default_string),
                _safe_int(default_int, 0),
                _safe_float(default_float, 0.0),
                bool(default_boolean),
                image_default,
                _json_dumps({"error": "type mismatch", "selected_type": selected_type, "actual_type": actual_type, "field": field}),
                actual_type,
                False,
            )

        value = _coerce_field_value(field)
        string_value = _safe_str(default_string)
        int_value = _safe_int(default_int, 0)
        float_value = _safe_float(default_float, 0.0)
        boolean_value = bool(default_boolean)
        image_value = image_default
        json_value = _safe_str(default_json) or "{}"

        try:
            if actual_type == "STRING" or selected_type == "STRING":
                if isinstance(value, str):
                    string_value = value
                elif value is None:
                    string_value = _safe_str(default_string)
                else:
                    string_value = _json_dumps(value, pretty=False) if isinstance(value, (dict, list)) else str(value)
            elif actual_type == "INT" or selected_type == "INT":
                int_value = _safe_int(value, _safe_int(default_int, 0))
            elif actual_type == "FLOAT" or selected_type == "FLOAT":
                float_value = _safe_float(value, _safe_float(default_float, 0.0))
            elif actual_type == "BOOLEAN" or selected_type == "BOOLEAN":
                if isinstance(value, bool):
                    boolean_value = value
                else:
                    boolean_value = _safe_str(value).lower() in {"1", "true", "yes", "on"}
            elif actual_type == "IMAGE" or selected_type == "IMAGE":
                storage = _safe_str(field.get("storage"))
                asset_ref = field.get("asset_ref") if isinstance(field.get("asset_ref"), dict) else {}
                image_bytes: Optional[bytes] = None
                if storage == "inline_base64" or (isinstance(value, str) and value.strip().startswith("data:image/")):
                    image_bytes = _decode_image_bytes_from_value(value)
                elif isinstance(value, (str, bytes, bytearray, dict, list)) and not asset_ref:
                    image_bytes = _decode_image_bytes_from_value(value)
                elif asset_ref:
                    collection = _safe_str(
                        asset_ref.get("collection")
                        or asset_ref.get("asset_collection")
                        or asset_ref.get("original", {}).get("asset_collection") if isinstance(asset_ref.get("original"), dict) else ""
                    )
                    document_id = _safe_str(
                        asset_ref.get("document_id")
                        or asset_ref.get("file_id")
                        or asset_ref.get("_id")
                        or asset_ref.get("original", {}).get("file_id") if isinstance(asset_ref.get("original"), dict) else ""
                    )
                    field_path = _safe_str(asset_ref.get("field_path") or field.get("source_path") or "image_data")
                    if not session or not hasattr(session, "fetch_image_field"):
                        raise RuntimeError("Image field is an asset reference, but no session with fetch_image_field was provided.")
                    image_bytes, _source = session.fetch_image_field(collection=collection, document_id=document_id, field_path=field_path, master_key_hex=master_key_hex)
                else:
                    image_bytes = _decode_image_bytes_from_value(value)
                image_value = _image_bytes_to_tensor(image_bytes or b"")
            else:
                json_value = _json_dumps(value)

            if actual_type == "JSON" or selected_type in {"JSON", "ANY"}:
                json_value = _json_dumps(value)

            return (string_value, int_value, float_value, boolean_value, image_value, json_value, actual_type, True)
        except Exception as exc:
            return (
                _safe_str(default_string),
                _safe_int(default_int, 0),
                _safe_float(default_float, 0.0),
                bool(default_boolean),
                image_default,
                _json_dumps({"error": {"type": exc.__class__.__name__, "message": str(exc)}, "field": field}),
                actual_type,
                False,
            )


def _comfy_image_to_png_data_uri(image: Any) -> str:
    """Convert the first image in a Comfy IMAGE tensor to an inline PNG data URI."""
    if Image is None or np is None:
        raise RuntimeError("Pillow and numpy are required to encode content pack images.")

    value = image
    if isinstance(value, (list, tuple)) and value:
        value = value[0]

    if torch is not None and hasattr(value, "detach"):
        array = value.detach().cpu().numpy()
    else:
        array = np.asarray(value)

    if array.ndim == 4:
        array = array[0]
    if array.ndim == 2:
        array = np.stack([array, array, array], axis=-1)
    if array.ndim != 3:
        raise ValueError(f"Unsupported IMAGE tensor shape for content pack image: {array.shape!r}")

    if array.shape[-1] == 1:
        array = np.repeat(array, 3, axis=-1)
    elif array.shape[-1] > 4:
        array = array[..., :3]

    array = np.nan_to_num(array)
    if array.dtype != np.uint8:
        if float(np.nanmax(array)) <= 1.0:
            array = array * 255.0
        array = np.clip(array, 0, 255).astype(np.uint8)

    if array.shape[-1] == 4:
        pil_image = Image.fromarray(array, mode="RGBA")
    else:
        pil_image = Image.fromarray(array[..., :3], mode="RGB")

    buffer = io.BytesIO()
    pil_image.save(buffer, format="PNG")
    encoded = base64.b64encode(buffer.getvalue()).decode("ascii")
    return "data:image/png;base64," + encoded


class ZMongoContentPackAddImageV3(AlwaysDirtyMixin):
    CATEGORY = "ZMongo/09 Content Packs"
    FUNCTION = "add_image"
    RETURN_TYPES = (CONTENT_PACK_TYPE, "STRING", "STRING", "STRING", "*", "*", "STRING", "BOOLEAN")
    RETURN_NAMES = ("content_pack", "content_pack_json", "manifest_json", "preview_markdown", "aliases", "data_types", "indexed", "success")
    OUTPUT_IS_LIST = (False, False, False, False, True, True, False, False)

    @classmethod
    def INPUT_TYPES(cls):
        return {
            "required": {
                "content_pack": (CONTENT_PACK_TYPE,),
                "image": ("IMAGE",),
                "field_alias": ("STRING", {"default": "hero_image"}),
                "label": ("STRING", {"default": "Hero Image"}),
                "source_path": ("STRING", {"default": "load_image.image"}),
                "storage_mode": (["inline_base64_png"], {"default": "inline_base64_png"}),
                "replace_existing": ("BOOLEAN", {"default": True}),
            },
            "optional": {
                "refresh_token": ("STRING", {"default": ""}),
            },
        }

    def add_image(
        self,
        content_pack: Any,
        image: Any,
        field_alias: str,
        label: str,
        source_path: str,
        storage_mode: str = "inline_base64_png",
        replace_existing: bool = True,
        refresh_token: str = "",
    ):
        pack = deepcopy(_as_content_pack(content_pack))
        if not pack:
            now = _utc_now_iso()
            pack = {
                "schema_kind": CONTENT_PACK_SCHEMA_KIND,
                "schema_version": CONTENT_PACK_SCHEMA_VERSION,
                "content_pack_name": "content_pack_with_image",
                "project_name": "default",
                "source": {},
                "fields": [],
                "outputs": {},
                "created_at": now,
                "created_at_unix": time.time(),
            }

        alias = _slug_alias(field_alias or "hero_image", "hero_image")
        display_label = _safe_str(label) or _title_from_alias(alias)
        source = _safe_str(source_path) or "load_image.image"

        try:
            data_uri = _comfy_image_to_png_data_uri(image)
        except Exception as exc:
            preview = f"# Add Image Failed\n\n- Error: `{exc.__class__.__name__}`\n- Message: {exc}"
            aliases = _content_pack_alias_items(pack)
            data_types = _content_pack_type_items(pack)
            return (pack, _json_dumps(pack), _json_dumps(_manifest_only(pack)), preview, _as_comfy_list(aliases), _as_comfy_list(data_types), _content_pack_indexed_aliases_text(pack), False)

        fields = _field_list(pack)
        if replace_existing:
            fields = [field for field in fields if _safe_str(field.get("alias")) != alias]

        field = {
            "index": len(fields),
            "alias": alias,
            "label": display_label,
            "source_path": source,
            "comfy_type": "IMAGE",
            "json_type": "image_asset",
            "storage": "inline_base64",
            "value": data_uri,
            "asset_ref": {},
            "json_ref": {},
            "summary": {
                "display": f"Inline PNG image saved in content pack as '{alias}'.",
                "length": len(data_uri),
                "truncated": False,
            },
        }
        fields.append(field)

        for index, item in enumerate(fields):
            item["index"] = index

        now = _utc_now_iso()
        pack["fields"] = fields
        pack["outputs"] = _build_field_outputs(fields)
        pack["field_count"] = len(fields)
        pack["updated_at"] = now
        pack["updated_at_unix"] = time.time()
        pack["manifest_hash"] = _stable_hash(_manifest_only(pack))
        pack["content"] = {
            "markdown": _preview_markdown(pack),
            "summary": f"Content pack '{pack.get('content_pack_name', 'content_pack')}' contains {len(fields)} typed field(s), including image '{alias}'.",
        }
        aliases = _content_pack_alias_items(pack)
        data_types = _content_pack_type_items(pack)
        return (pack, _json_dumps(pack), _json_dumps(_manifest_only(pack)), _preview_markdown(pack), _as_comfy_list(aliases), _as_comfy_list(data_types), _content_pack_indexed_aliases_text(pack), True)



# -----------------------------------------------------------------------------
# Portable Content Pack JSON File Nodes
# -----------------------------------------------------------------------------

def _portable_default_output_dir() -> Path:
    """Resolve ComfyUI output directory without hard dependency during import."""
    try:
        import folder_paths  # type: ignore
        return Path(folder_paths.get_output_directory()).resolve()
    except Exception:
        return (Path.cwd() / "output").resolve()


def _safe_filename_stem(value: Any, fallback: str = "content_pack") -> str:
    text = _safe_str(value) or fallback
    text = re.sub(r"[^A-Za-z0-9_.-]+", "_", text).strip("._-")
    return text or fallback


def _first_image_alias_from_pack(content_pack: Any, fallback: str = "hero_image") -> str:
    """Return the first IMAGE field alias from a content pack or portable envelope."""
    for field in _field_list(content_pack):
        if _normalize_type(field.get("comfy_type"), "ANY") == "IMAGE":
            alias = _safe_str(field.get("alias"))
            if alias:
                return alias
    return fallback


def _portable_pack_to_comfy_workflow_json(
    portable_pack: dict[str, Any],
    *,
    workflow_name: str = "ZMongo Content Pack Workflow",
    image_alias: str = "",
) -> dict[str, Any]:
    """
    Wrap a portable content pack envelope in a normal ComfyUI/LiteGraph workflow.

    This is intentionally a workflow file, not a raw ZMongo content-pack file.
    It lets users open the exported JSON through ComfyUI's normal workflow loader.
    """
    portable_text = _json_dumps(portable_pack, pretty=True)
    alias = _safe_str(image_alias) or _first_image_alias_from_pack(portable_pack)
    name = _safe_str(workflow_name) or _safe_str(portable_pack.get("content_pack_name")) or "ZMongo Content Pack Workflow"

    loader_id = 1
    image_getter_id = 2
    link_id = 1

    return {
        "last_node_id": image_getter_id,
        "last_link_id": link_id,
        "nodes": [
            {
                "id": loader_id,
                "type": "ZMongoContentPackJSONTextLoaderV3",
                "pos": [240, 220],
                "size": [560, 360],
                "flags": {},
                "order": 0,
                "mode": 0,
                "inputs": [],
                "outputs": [
                    {"name": "content_pack", "type": CONTENT_PACK_TYPE, "links": [link_id], "shape": 3, "slot_index": 0},
                    {"name": "json", "type": "STRING", "links": None, "shape": 3, "slot_index": 1},
                    {"name": "manifest_json", "type": "STRING", "links": None, "shape": 3, "slot_index": 2},
                    {"name": "aliases", "type": "*", "links": None, "shape": 3, "slot_index": 3},
                    {"name": "data_types", "type": "*", "links": None, "shape": 3, "slot_index": 4},
                    {"name": "indexed", "type": "STRING", "links": None, "shape": 3, "slot_index": 5},
                    {"name": "success", "type": "BOOLEAN", "links": None, "shape": 3, "slot_index": 6},
                ],
                "properties": {"Node name for S&R": "ZMongoContentPackJSONTextLoaderV3"},
                "widgets_values": [
                    portable_text,
                    True,
                    "",
                ],
            },
            {
                "id": image_getter_id,
                "type": "ZMongoContentPackGetImageV3",
                "pos": [900, 260],
                "size": [380, 220],
                "flags": {},
                "order": 1,
                "mode": 0,
                "inputs": [
                    {"name": "content_pack", "type": CONTENT_PACK_TYPE, "link": link_id},
                    {"name": "session", "type": "ZMONGO_API_SESSION", "link": None},
                ],
                "outputs": [
                    {"name": "image", "type": "IMAGE", "links": None, "shape": 3, "slot_index": 0},
                    {"name": "field_info_json", "type": "STRING", "links": None, "shape": 3, "slot_index": 1},
                    {"name": "found", "type": "BOOLEAN", "links": None, "shape": 3, "slot_index": 2},
                ],
                "properties": {"Node name for S&R": "ZMongoContentPackGetImageV3"},
                "widgets_values": [
                    alias,
                    True,
                    "",
                ],
            },
        ],
        "links": [
            [link_id, loader_id, 0, image_getter_id, 0, CONTENT_PACK_TYPE],
        ],
        "groups": [],
        "config": {},
        "extra": {
            "ds": {"scale": 1, "offset": [0, 0]},
            "zmongo": {
                "workflow_kind": "portable_content_pack_workflow",
                "workflow_name": name,
                "content_pack_name": portable_pack.get("content_pack_name", ""),
                "project_name": portable_pack.get("project_name", ""),
                "field_count": portable_pack.get("field_count", 0),
                "image_alias": alias,
            },
        },
        "version": 0.4,
    }


def _content_pack_portable_envelope(
    content_pack: Any,
    *,
    export_mode: str = "portable_inline",
    include_images: bool = True,
    include_metadata: bool = True,
    session: Any = None,
    master_key_hex: str = "",
) -> dict[str, Any]:
    """
    Convert a normalized ZMONGO_CONTENT_PACK into a portable JSON envelope.

    export_mode:
    - portable_inline: keep scalars inline and keep inline_base64 images inline.
      If an image is asset_ref and a session is supplied, try to resolve it to inline base64.
    - asset_refs: keep image asset references instead of embedding them.
    - manifest_only: include aliases/types/source metadata only; omit actual values.
    """
    pack = deepcopy(_as_content_pack(content_pack))
    if not pack:
        raise ValueError("content_pack is empty or invalid.")

    mode = _safe_str(export_mode) or "portable_inline"
    if mode not in {"portable_inline", "asset_refs", "manifest_only"}:
        mode = "portable_inline"

    fields_out: list[dict[str, Any]] = []
    for index, source_field in enumerate(_field_list(pack)):
        field = deepcopy(source_field)
        comfy_type = _normalize_type(field.get("comfy_type"), "ANY")
        storage = _safe_str(field.get("storage")) or "inline"

        portable_field: dict[str, Any] = {
            "index": index,
            "alias": _safe_str(field.get("alias")) or f"field_{index}",
            "label": _safe_str(field.get("label")) or _title_from_alias(field.get("alias") or f"field_{index}"),
            "comfy_type": comfy_type,
            "json_type": _safe_str(field.get("json_type")) or comfy_type.lower(),
            "storage": storage,
        }

        if include_metadata:
            portable_field.update({
                "source_path": _safe_str(field.get("source_path")),
                "summary": field.get("summary") if isinstance(field.get("summary"), dict) else _value_summary(field.get("value")),
            })
        else:
            portable_field.update({"source_path": "", "summary": {}})

        if mode == "manifest_only":
            portable_field["storage"] = "manifest_only"
            fields_out.append(portable_field)
            continue

        if comfy_type == "IMAGE":
            if not include_images:
                portable_field["storage"] = "image_omitted"
                fields_out.append(portable_field)
                continue

            value = field.get("value")
            asset_ref = field.get("asset_ref") if isinstance(field.get("asset_ref"), dict) else {}

            if mode == "asset_refs":
                portable_field["storage"] = "asset_ref"
                portable_field["asset_ref"] = asset_ref
                portable_field["value"] = value if storage == "inline_base64" else None
                fields_out.append(portable_field)
                continue

            # portable_inline
            try:
                if isinstance(value, str) and value.strip().startswith("data:image/"):
                    portable_field["storage"] = "inline_base64"
                    portable_field["value"] = value.strip()
                    portable_field["content_type"] = value.split(";", 1)[0].replace("data:", "")
                    portable_field["filename"] = _safe_str(field.get("filename")) or f"{portable_field['alias']}.png"
                elif storage == "inline_base64" and value:
                    portable_field["storage"] = "inline_base64"
                    portable_field["value"] = value
                    portable_field["content_type"] = _safe_str(field.get("content_type")) or "image/png"
                    portable_field["filename"] = _safe_str(field.get("filename")) or f"{portable_field['alias']}.png"
                elif asset_ref and session is not None:
                    collection = _safe_str(
                        asset_ref.get("collection")
                        or asset_ref.get("asset_collection")
                        or (asset_ref.get("original", {}) or {}).get("asset_collection") if isinstance(asset_ref.get("original"), dict) else ""
                    )
                    document_id = _safe_str(
                        asset_ref.get("document_id")
                        or asset_ref.get("file_id")
                        or asset_ref.get("_id")
                        or (asset_ref.get("original", {}) or {}).get("file_id") if isinstance(asset_ref.get("original"), dict) else ""
                    )
                    field_path = _safe_str(asset_ref.get("field_path") or asset_ref.get("path") or "image_data")
                    if not collection or not document_id:
                        raise ValueError("asset_ref missing collection/document id")
                    image_bytes, _where = session.fetch_image_field(
                        collection=collection,
                        document_id=document_id,
                        field_path=field_path,
                        master_key_hex=master_key_hex,
                    )
                    portable_field["storage"] = "inline_base64"
                    portable_field["content_type"] = "image/png"
                    portable_field["filename"] = f"{portable_field['alias']}.png"
                    portable_field["value"] = "data:image/png;base64," + base64.b64encode(image_bytes).decode("ascii")
                else:
                    portable_field["storage"] = "asset_ref"
                    portable_field["asset_ref"] = asset_ref
                    portable_field["value"] = value if value else None
                    portable_field.setdefault("warning", "Image was not inlined because no inline value or resolvable session asset was available.")
            except Exception as exc:
                portable_field["storage"] = "asset_ref"
                portable_field["asset_ref"] = asset_ref
                portable_field["value"] = None
                portable_field["warning"] = f"Failed to inline image: {exc}"

            fields_out.append(portable_field)
            continue

        # Non-image values.
        portable_field["storage"] = "inline"
        portable_field["value"] = field.get("value")
        fields_out.append(portable_field)

    envelope = {
        "schema_kind": PORTABLE_CONTENT_PACK_SCHEMA_KIND,
        "schema_version": CONTENT_PACK_SCHEMA_VERSION,
        "export_format": mode,
        "portable": True,
        "content_pack_name": pack.get("content_pack_name") or "portable_content_pack",
        "project_name": pack.get("project_name") or "default",
        "source": {
            "source_mode": "portable_content_pack_export",
            "original_schema_kind": pack.get("schema_kind"),
            "original_manifest_hash": pack.get("manifest_hash", ""),
        },
        "field_count": len(fields_out),
        "fields": fields_out,
        "outputs": _build_field_outputs(fields_out),
        "content": {
            "summary": f"Portable content pack '{pack.get('content_pack_name') or 'portable_content_pack'}' exported with {len(fields_out)} field(s).",
            "markdown": _preview_markdown({**pack, "fields": fields_out}),
        },
        "created_at": pack.get("created_at") or _utc_now_iso(),
        "created_at_unix": pack.get("created_at_unix") or time.time(),
        "updated_at": _utc_now_iso(),
        "updated_at_unix": time.time(),
        "exported_at": _utc_now_iso(),
    }
    envelope["manifest_hash"] = _stable_hash(_manifest_only({**envelope, "schema_kind": CONTENT_PACK_SCHEMA_KIND}))
    return envelope



def _extract_portable_pack_from_workflow_json(value: Any) -> dict[str, Any]:
    """Extract embedded portable content-pack JSON from a ComfyUI workflow JSON.

    This lets the loader nodes accept either:
    - raw zmongo_portable_content_pack JSON, or
    - workflow_json files produced by ZMongoContentPackExportJSONFileV3.
    """
    parsed = _parse_json(value, default={})
    if not isinstance(parsed, dict):
        return {}

    extra = parsed.get("extra") if isinstance(parsed.get("extra"), dict) else {}
    zmongo_extra = extra.get("zmongo") if isinstance(extra.get("zmongo"), dict) else {}
    is_zmongo_workflow = _safe_str(zmongo_extra.get("workflow_kind")) == "portable_content_pack_workflow"

    nodes = parsed.get("nodes")
    if not isinstance(nodes, list):
        return {}

    for node in nodes:
        if not isinstance(node, dict):
            continue
        if _safe_str(node.get("type")) != "ZMongoContentPackJSONTextLoaderV3":
            continue
        widgets = node.get("widgets_values")
        if not isinstance(widgets, list) or not widgets:
            continue
        candidate = widgets[0]
        candidate_parsed = _parse_json(candidate, default={})
        if (
            isinstance(candidate_parsed, dict)
            and candidate_parsed.get("schema_kind") in {CONTENT_PACK_SCHEMA_KIND, PORTABLE_CONTENT_PACK_SCHEMA_KIND}
        ):
            return candidate_parsed

    # If the workflow marker is present but the loader JSON could not be found,
    # return an empty dict so the caller reports the normal unsupported schema error.
    if is_zmongo_workflow:
        return {}

    return {}

def _portable_to_content_pack(value: Any, *, validate_schema: bool = True) -> dict[str, Any]:
    parsed = _parse_json(value, default={})
    if not isinstance(parsed, dict):
        raise ValueError("Portable content pack JSON must parse to an object.")

    embedded = _extract_portable_pack_from_workflow_json(parsed)
    if embedded:
        parsed = embedded

    schema_kind = _safe_str(parsed.get("schema_kind"))
    if schema_kind == CONTENT_PACK_SCHEMA_KIND:
        pack = deepcopy(parsed)
    elif schema_kind == PORTABLE_CONTENT_PACK_SCHEMA_KIND:
        pack = deepcopy(parsed)
        pack["schema_kind"] = CONTENT_PACK_SCHEMA_KIND
        pack.setdefault("source", {})
        if isinstance(pack.get("source"), dict):
            pack["source"]["source_mode"] = "portable_content_pack_import"
    else:
        if validate_schema:
            raise ValueError(f"Unsupported schema_kind: {schema_kind!r}")
        pack = deepcopy(parsed)
        pack["schema_kind"] = CONTENT_PACK_SCHEMA_KIND

    fields = _field_list(pack)
    for index, field in enumerate(fields):
        field["index"] = index
        field["alias"] = _slug_alias(field.get("alias") or field.get("source_path") or f"field_{index}", f"field_{index}")
        field["label"] = _safe_str(field.get("label")) or _title_from_alias(field["alias"])
        field["comfy_type"] = _normalize_type(field.get("comfy_type"), "ANY")
        field.setdefault("storage", "inline")
        field.setdefault("source_path", field["alias"])
        field.setdefault("summary", _value_summary(field.get("value")))

    now = _utc_now_iso()
    pack["fields"] = fields
    pack["outputs"] = _build_field_outputs(fields)
    pack["field_count"] = len(fields)
    pack["updated_at"] = pack.get("updated_at") or now
    pack["updated_at_unix"] = pack.get("updated_at_unix") or time.time()
    pack["manifest_hash"] = pack.get("manifest_hash") or _stable_hash(_manifest_only(pack))
    pack["content"] = {
        "summary": f"Content pack '{pack.get('content_pack_name') or 'portable_content_pack'}' loaded from portable JSON with {len(fields)} field(s).",
        "markdown": _preview_markdown(pack),
    }
    return pack


class ZMongoContentPackExportJSONFileV3(AlwaysDirtyMixin):
    CATEGORY = "ZMongo/09 Content Packs/Portable"
    FUNCTION = "export_json_file"
    RETURN_TYPES = ("STRING", "STRING", "STRING", "BOOLEAN", "STRING")
    RETURN_NAMES = ("json", "file_path", "filename", "success", "refresh")

    @classmethod
    def INPUT_TYPES(cls):
        return {
            "required": {
                "content_pack": (CONTENT_PACK_TYPE,),
                "filename_prefix": ("STRING", {"default": "content_pack"}),
                "export_mode": (["workflow_json", "portable_inline", "asset_refs", "manifest_only"], {"default": "workflow_json"}),
                "include_images": ("BOOLEAN", {"default": True}),
                "include_metadata": ("BOOLEAN", {"default": True}),
                "pretty_json": ("BOOLEAN", {"default": True}),
            },
            "optional": {
                "session": ("ZMONGO_API_SESSION",),
                "output_subfolder": ("STRING", {"default": "content_packs"}),
                "master_key_hex": ("STRING", {"default": ""}),
                "refresh_token": ("STRING", {"default": ""}),
            },
        }

    def export_json_file(
        self,
        content_pack: Any,
        filename_prefix: str = "content_pack",
        export_mode: str = "workflow_json",
        include_images: bool = True,
        include_metadata: bool = True,
        pretty_json: bool = True,
        session: Any = None,
        output_subfolder: str = "content_packs",
        master_key_hex: str = "",
        refresh_token: str = "",
    ):
        refresh = _stable_hash([time.time(), filename_prefix, export_mode, refresh_token])[:16]
        try:
            portable_export_mode = "portable_inline" if export_mode == "workflow_json" else export_mode
            envelope = _content_pack_portable_envelope(
                content_pack,
                export_mode=portable_export_mode,
                include_images=include_images,
                include_metadata=include_metadata,
                session=session,
                master_key_hex=master_key_hex,
            )
            export_document: dict[str, Any] = envelope
            if export_mode == "workflow_json":
                export_document = _portable_pack_to_comfy_workflow_json(
                    envelope,
                    workflow_name=_safe_str(filename_prefix) or envelope.get("content_pack_name") or "content_pack",
                    image_alias=_first_image_alias_from_pack(envelope),
                )

            out_dir = _portable_default_output_dir()
            sub = _safe_filename_stem(output_subfolder, "content_packs")
            if sub:
                out_dir = out_dir / sub
            out_dir.mkdir(parents=True, exist_ok=True)

            stem = _safe_filename_stem(filename_prefix or envelope.get("content_pack_name") or "content_pack")
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"{stem}_{timestamp}.json"
            path = out_dir / filename
            text = _json_dumps(export_document, pretty=pretty_json)
            path.write_text(text, encoding="utf-8")

            payload = {
                "success": True,
                "message": "Content pack JSON exported.",
                "data": {
                    "file_path": str(path),
                    "filename": filename,
                    "export_mode": export_mode,
                    "portable_export_mode": portable_export_mode,
                    "opens_as_comfy_workflow": export_mode == "workflow_json",
                    "field_count": len(_field_list(envelope)),
                    "refresh": refresh,
                },
                "error": None,
            }
            return {
                "ui": {
                    "portable_json": [text],
                    "filename": [filename],
                    "file_path": [str(path)],
                    "message": ["Content pack JSON exported."],
                },
                "result": (_json_dumps(payload), str(path), filename, True, refresh),
            }
        except Exception as exc:
            payload = {
                "success": False,
                "message": f"Portable export failed: {exc}",
                "data": {"refresh": refresh},
                "error": {"type": exc.__class__.__name__, "msg": str(exc)},
            }
            return (_json_dumps(payload), "", "", False, refresh)


class ZMongoContentPackLoadJSONFileV3(AlwaysDirtyMixin):
    CATEGORY = "ZMongo/09 Content Packs/Portable"
    FUNCTION = "load_json_file"
    RETURN_TYPES = (CONTENT_PACK_TYPE, "STRING", "STRING", "*", "*", "STRING", "BOOLEAN")
    RETURN_NAMES = ("content_pack", "json", "manifest_json", "aliases", "data_types", "indexed", "success")
    OUTPUT_IS_LIST = (False, False, False, True, True, False, False)

    @classmethod
    def INPUT_TYPES(cls):
        return {
            "required": {
                "content_pack_file": ("STRING", {"default": "", "multiline": False}),
                "validate_schema": ("BOOLEAN", {"default": True}),
                "image_policy": (["load_inline", "ignore_images", "require_images"], {"default": "load_inline"}),
            },
            "optional": {
                "refresh_token": ("STRING", {"default": ""}),
            },
        }

    def load_json_file(self, content_pack_file: str = "", validate_schema: bool = True, image_policy: str = "load_inline", refresh_token: str = ""):
        try:
            path_text = _safe_str(content_pack_file)
            if not path_text:
                raise ValueError("content_pack_file is required.")

            if path_text.strip().startswith("{"):
                raw = path_text
            else:
                path = Path(os.path.expanduser(path_text)).resolve()
                if not path.exists():
                    raise FileNotFoundError(f"Portable content pack file not found: {path}")
                raw = path.read_text(encoding="utf-8")

            pack = _portable_to_content_pack(raw, validate_schema=validate_schema)
            if image_policy == "ignore_images":
                fields = []
                for field in _field_list(pack):
                    item = deepcopy(field)
                    if _normalize_type(item.get("comfy_type"), "ANY") == "IMAGE":
                        item["storage"] = "image_ignored"
                        item["value"] = None
                    fields.append(item)
                pack["fields"] = fields
                pack["outputs"] = _build_field_outputs(fields)
            elif image_policy == "require_images":
                missing = [field.get("alias") for field in _field_list(pack) if _normalize_type(field.get("comfy_type"), "ANY") == "IMAGE" and not field.get("value") and not field.get("asset_ref")]
                if missing:
                    raise ValueError(f"Image fields missing portable image data: {missing}")

            aliases = _content_pack_alias_items(pack)
            data_types = _content_pack_type_items(pack)
            return (pack, _json_dumps(pack), _json_dumps(_manifest_only(pack)), _as_comfy_list(aliases), _as_comfy_list(data_types), _content_pack_indexed_aliases_text(pack), True)
        except Exception as exc:
            payload = {"success": False, "message": f"Load portable file failed: {exc}", "error": {"type": exc.__class__.__name__, "msg": str(exc)}}
            return ({}, _json_dumps(payload), _json_dumps({}), [], [], _indexed_list_text([]), False)


class ZMongoContentPackJSONTextLoaderV3(AlwaysDirtyMixin):
    CATEGORY = "ZMongo/09 Content Packs/Portable"
    FUNCTION = "load_json_text"
    RETURN_TYPES = (CONTENT_PACK_TYPE, "STRING", "STRING", "*", "*", "STRING", "BOOLEAN")
    RETURN_NAMES = ("content_pack", "json", "manifest_json", "aliases", "data_types", "indexed", "success")
    OUTPUT_IS_LIST = (False, False, False, True, True, False, False)

    @classmethod
    def INPUT_TYPES(cls):
        return {
            "required": {
                "content_pack_json": ("STRING", {"default": "{}", "multiline": True}),
                "validate_schema": ("BOOLEAN", {"default": True}),
            },
            "optional": {
                "refresh_token": ("STRING", {"default": ""}),
            },
        }

    def load_json_text(self, content_pack_json: str = "{}", validate_schema: bool = True, refresh_token: str = ""):
        try:
            pack = _portable_to_content_pack(content_pack_json, validate_schema=validate_schema)
            aliases = _content_pack_alias_items(pack)
            data_types = _content_pack_type_items(pack)
            return (pack, _json_dumps(pack), _json_dumps(_manifest_only(pack)), _as_comfy_list(aliases), _as_comfy_list(data_types), _content_pack_indexed_aliases_text(pack), True)
        except Exception as exc:
            payload = {"success": False, "message": f"Load portable JSON text failed: {exc}", "error": {"type": exc.__class__.__name__, "msg": str(exc)}}
            return ({}, _json_dumps(payload), _json_dumps({}), [], [], _indexed_list_text([]), False)

NODE_CLASS_MAPPINGS = {
    "ZMongoContentPackBuildV3": ZMongoContentPackBuildV3,
    "ZMongoContentPackAddImageV3": ZMongoContentPackAddImageV3,
    "ZMongoContentPackAliasEditorV3": ZMongoContentPackAliasEditorV3,
    "ZMongoContentPackSaveV3": ZMongoContentPackSaveV3,
    "ZMongoContentPackLoadV3": ZMongoContentPackLoadV3,
    "ZMongoContentPackManifestPreviewV3": ZMongoContentPackManifestPreviewV3,
    "ZMongoContentPackGetStringV3": ZMongoContentPackGetStringV3,
    "ZMongoContentPackGetIntV3": ZMongoContentPackGetIntV3,
    "ZMongoContentPackGetFloatV3": ZMongoContentPackGetFloatV3,
    "ZMongoContentPackGetBooleanV3": ZMongoContentPackGetBooleanV3,
    "ZMongoContentPackGetJSONV3": ZMongoContentPackGetJSONV3,
    "ZMongoContentPackGetImageV3": ZMongoContentPackGetImageV3,
    "ZMongoContentPackGetSelectedV3": ZMongoContentPackGetSelectedV3,
    "ZMongoContentPackExportJSONFileV3": ZMongoContentPackExportJSONFileV3,
    "ZMongoContentPackLoadJSONFileV3": ZMongoContentPackLoadJSONFileV3,
    "ZMongoContentPackJSONTextLoaderV3": ZMongoContentPackJSONTextLoaderV3,
}

NODE_DISPLAY_NAME_MAPPINGS = {
    "ZMongoContentPackBuildV3": "09 ZMongo Content Pack Build",
    "ZMongoContentPackAddImageV3": "09 ZMongo Content Pack Add Image",
    "ZMongoContentPackAliasEditorV3": "09 ZMongo Content Pack Alias Editor",
    "ZMongoContentPackSaveV3": "09 ZMongo Content Pack Save",
    "ZMongoContentPackLoadV3": "09 ZMongo Content Pack Load",
    "ZMongoContentPackManifestPreviewV3": "09 ZMongo Content Pack Manifest Preview",
    "ZMongoContentPackGetStringV3": "09 ZMongo Content Pack Get String",
    "ZMongoContentPackGetIntV3": "09 ZMongo Content Pack Get Int",
    "ZMongoContentPackGetFloatV3": "09 ZMongo Content Pack Get Float",
    "ZMongoContentPackGetBooleanV3": "ZMongo Content Pack Get Boolean",
    "ZMongoContentPackGetJSONV3": "09 ZMongo Content Pack Get JSON",
    "ZMongoContentPackGetImageV3": "09 ZMongo Content Pack Get Image",
    "ZMongoContentPackGetSelectedV3": "09 ZMongo Content Pack Get Selected",
    "ZMongoContentPackExportJSONFileV3": "09 ZMongo Content Pack Export JSON File",
    "ZMongoContentPackLoadJSONFileV3": "09 ZMongo Content Pack Load JSON File",
    "ZMongoContentPackJSONTextLoaderV3": "09 ZMongo Content Pack JSON Text Loader",
}

__all__ = ["NODE_CLASS_MAPPINGS", "NODE_DISPLAY_NAME_MAPPINGS"]