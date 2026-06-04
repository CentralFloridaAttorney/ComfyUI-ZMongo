from __future__ import annotations

# pylint: disable=too-many-lines,too-many-locals,too-many-branches,too-many-statements,broad-exception-caught,line-too-long

import base64
import io
import json
import re
import time
import uuid
from datetime import datetime, timezone
from typing import Any, Optional

try:
    import torch
except Exception:
    torch = None  # type: ignore[assignment]

try:
    import numpy as np
except Exception:
    np = None  # type: ignore[assignment]

try:
    from PIL import Image, ImageOps
except Exception:
    Image = None  # type: ignore[assignment]
    ImageOps = None  # type: ignore[assignment]

try:
    from .generic_helpers import (
        AlwaysDirtyMixin,
        _dirty_token,
        _json_text,
        _error_payload,
        _success_payload,
        _session_api_request,
        _safe_get_by_path,
        _image_field_candidates,
        _decode_image_bytes_from_value,
        _pil_to_comfy_image,
    )
except Exception:
    class AlwaysDirtyMixin:
        @classmethod
        def IS_CHANGED(cls, *args, **kwargs):
            return float("nan")

    def _dirty_token(*parts: Any) -> str:
        return "::".join(str(part) for part in parts if part is not None)

    def _json_text(value: Any) -> str:
        try:
            return json.dumps(value, indent=2, ensure_ascii=False, default=str)
        except Exception:
            return str(value)

    def _error_payload(
        message: str,
        data: Optional[dict[str, Any]] = None,
        error_type: str = "Error",
        status_code: int = 0,
    ) -> dict[str, Any]:
        return {
            "success": False,
            "message": message,
            "data": data or {},
            "error": {"type": error_type, "msg": message},
            "status_code": status_code,
        }

    def _success_payload(
        message: str,
        data: Optional[dict[str, Any]] = None,
        status_code: int = 200,
    ) -> dict[str, Any]:
        return {
            "success": True,
            "message": message,
            "data": data or {},
            "error": None,
            "status_code": status_code,
        }

    def _safe_get_by_path(data: Any, path: str, default: Any = None) -> Any:
        if not path:
            return data

        node = data
        for part in str(path or "").split("."):
            part = part.strip()
            if not part:
                continue

            if isinstance(node, dict):
                if part not in node:
                    return default
                node = node[part]
                continue

            if isinstance(node, list):
                try:
                    index = int(part)
                except Exception:
                    return default
                if not (0 <= index < len(node)):
                    return default
                node = node[index]
                continue

            return default

        return node

    def _session_api_request(
        session: Any,
        method: str,
        path: str,
        json_body: dict[str, Any] | None = None,
        *,
        prefix: str = "",
        gemini_prefix: str = "",
        zmongo_prefix: str = "",
    ) -> dict[str, Any]:
        request_fn = getattr(session, "request", None)
        if not callable(request_fn):
            return _error_payload("Session does not expose request().")

        clean_prefix = str(prefix or gemini_prefix or zmongo_prefix or "").strip().rstrip("/")
        if clean_prefix and not clean_prefix.startswith("/"):
            clean_prefix = "/" + clean_prefix

        clean_path = str(path or "").strip()
        if clean_path and not clean_path.startswith("/"):
            clean_path = "/" + clean_path

        return request_fn(method, clean_prefix, clean_path, json_body=json_body or {})

    def _image_field_candidates(field_path: str, default: str = "image_data") -> list[str]:
        clean = str(field_path or default).strip().strip(".") or default
        candidates = [clean]
        if not clean.endswith(".data"):
            candidates.append(f"{clean}.data")
        return list(dict.fromkeys(candidates))

    def _decode_image_bytes_from_value(value: Any) -> bytes:
        if isinstance(value, bytes):
            return value

        if isinstance(value, bytearray):
            return bytes(value)

        if isinstance(value, str):
            raw = value.strip()
            if raw.startswith("data:") and "," in raw:
                raw = raw.split(",", 1)[1]
            return base64.b64decode(raw, validate=False)

        if isinstance(value, dict):
            if value.get("__type__") == "bytes" and value.get("data"):
                return _decode_image_bytes_from_value(value["data"])

            for key in ("data", "base64", "b64", "bytes", "image", "content", "payload"):
                if key in value:
                    return _decode_image_bytes_from_value(value[key])

        raise ValueError(f"Unsupported image value type: {type(value).__name__}")

    def _pil_to_comfy_image(image: Any) -> Any:
        if torch is None or np is None:
            return None

        image = image.convert("RGB")
        array = np.asarray(image).astype(np.float32) / 255.0
        return torch.from_numpy(array)[None,]


CONTENT_PACK_CATEGORY = "ZMongo/08 Content Packs"

CONTENT_PACK_SCHEMA_KIND = "zmongo_content_pack"
CONTENT_PACK_SOURCE_SCHEMA_KIND = "zmongo_content_pack_source"
CONTENT_PACK_SCHEMA_VERSION = "1.0.1"

DEFAULT_CONTENT_PACK_COLLECTION = "text_agent_context_packs"
DEFAULT_CONTENT_PACK_NAME = "latest_context_pack"

TEXT_HINTS = {
    "text",
    "prompt",
    "positive",
    "negative",
    "caption",
    "summary",
    "notes",
    "description",
    "markdown",
    "content",
    "message",
    "title",
    "name",
    "filename",
    "model",
    "workflow_name",
    "project_name",
}

IMAGE_HINTS = {
    "image",
    "image_data",
    "images",
    "mask",
    "mask_data",
    "thumbnail",
    "preview",
    "original",
    "asset",
    "asset_ref",
    "file_data",
    "bytes",
    "base64",
    "b64",
    "local_path",
    "url",
    "view_url",
    "download_url",
    "preview_url",
    "src",
}

BINARY_HINTS = {
    "data",
    "bytes",
    "base64",
    "b64",
    "file_data",
    "$binary",
}


# -----------------------------------------------------------------------------
# Generic helpers
# -----------------------------------------------------------------------------

def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _clean(value: Any, default: str = "") -> str:
    text = str(value or "").strip()
    return text or default


def _safe_name(value: Any, default: str = "content_pack") -> str:
    clean = re.sub(r"[^A-Za-z0-9_.-]+", "_", str(value or "").strip()).strip("._-")
    return clean or default


def _safe_bool(value: Any, default: bool = False) -> bool:
    if isinstance(value, bool):
        return value

    if value is None:
        return default

    if isinstance(value, (int, float)):
        return bool(value)

    text = str(value).strip().lower()
    if text in {"1", "true", "yes", "y", "on"}:
        return True
    if text in {"0", "false", "no", "n", "off"}:
        return False

    return default




def _csv_list(value: Any) -> list[str]:
    """Return a clean, de-duplicated list from comma/newline separated text,
    a JSON list string, or an existing Python list/tuple/set.
    """
    if value is None:
        return []

    if isinstance(value, (list, tuple, set)):
        raw_items = list(value)
    else:
        text = str(value or "").strip()
        if not text:
            return []

        parsed: Any = None
        try:
            parsed = json.loads(text)
        except Exception:
            parsed = None

        if isinstance(parsed, list):
            raw_items = parsed
        elif isinstance(parsed, (tuple, set)):
            raw_items = list(parsed)
        elif isinstance(parsed, str):
            raw_items = re.split(r"[,\n\r]+", parsed)
        else:
            raw_items = re.split(r"[,\n\r]+", text)

    items: list[str] = []
    seen: set[str] = set()

    for item in raw_items:
        clean = str(item or "").strip().strip(". ")
        if not clean or clean in seen:
            continue
        seen.add(clean)
        items.append(clean)

    return items


def _shorten(value: Any, max_chars: int = 2000) -> str:
    text = str(value if value is not None else "")
    if len(text) <= max_chars:
        return text
    return text[: max(0, max_chars - 32)].rstrip() + "\n...[truncated]"


def _json_loads_any(value: Any, default: Any = None) -> Any:
    if isinstance(value, (dict, list, int, float, bool)) or value is None:
        return value if value is not None else default

    text = str(value or "").strip()
    if not text:
        return default

    try:
        return json.loads(text)
    except Exception:
        return default


def _value_json_size(value: Any) -> int:
    try:
        return len(json.dumps(value, ensure_ascii=False, default=str).encode("utf-8"))
    except Exception:
        return len(str(value).encode("utf-8", errors="replace"))


def _normalize_bson_json(value: Any) -> Any:
    if isinstance(value, dict):
        keys = set(value.keys())

        if keys == {"$oid"}:
            return str(value["$oid"])

        if keys == {"$numberLong"}:
            try:
                return int(value["$numberLong"])
            except Exception:
                return str(value["$numberLong"])

        if keys == {"$numberInt"}:
            try:
                return int(value["$numberInt"])
            except Exception:
                return str(value["$numberInt"])

        if keys == {"$numberDouble"}:
            try:
                return float(value["$numberDouble"])
            except Exception:
                return str(value["$numberDouble"])

        if keys == {"$date"}:
            return value["$date"]

        return {str(key): _normalize_bson_json(item) for key, item in value.items()}

    if isinstance(value, list):
        return [_normalize_bson_json(item) for item in value]

    return value


def _payload_data(payload: Any) -> dict[str, Any]:
    if isinstance(payload, dict) and isinstance(payload.get("data"), dict):
        return payload["data"]
    return {}


def _extract_document_from_payload_or_json(value: Any) -> dict[str, Any]:
    parsed = _normalize_bson_json(_json_loads_any(value, default={}))

    if not isinstance(parsed, dict):
        return {}

    data = parsed.get("data")

    if isinstance(data, dict):
        for key in ("document", "doc", "result"):
            item = data.get(key)
            if isinstance(item, dict):
                return _normalize_bson_json(item)

        api_response = data.get("api_response")
        if isinstance(api_response, dict):
            api_data = api_response.get("data")
            if isinstance(api_data, dict):
                for key in ("document", "doc", "result"):
                    item = api_data.get(key)
                    if isinstance(item, dict):
                        return _normalize_bson_json(item)

        docs = data.get("documents") or data.get("docs") or data.get("results") or data.get("items")
        if isinstance(docs, list) and docs and isinstance(docs[0], dict):
            return _normalize_bson_json(docs[0])

        # Important fallback:
        # Easy Save Image often returns an API result payload, not the final document.
        # This still contains enough information to build a reference-only content pack.
        if any(
            key in data
            for key in (
                "_id",
                "document_id",
                "inserted_id",
                "collection",
                "collection_name",
                "field_path",
                "filename",
                "api_response",
            )
        ):
            return _normalize_bson_json(data)

    for key in ("document", "doc", "result"):
        item = parsed.get(key)
        if isinstance(item, dict):
            return _normalize_bson_json(item)

    docs = parsed.get("documents") or parsed.get("docs") or parsed.get("results") or parsed.get("items")
    if isinstance(docs, list) and docs and isinstance(docs[0], dict):
        return _normalize_bson_json(docs[0])

    return parsed


def _deep_first_value(data: Any, keys: tuple[str, ...]) -> Any:
    if isinstance(data, dict):
        for key in keys:
            value = data.get(key)
            if value not in (None, ""):
                return value

        for value in data.values():
            found = _deep_first_value(value, keys)
            if found not in (None, ""):
                return found

    elif isinstance(data, list):
        for item in data:
            found = _deep_first_value(item, keys)
            if found not in (None, ""):
                return found

    return None


def _extract_source_collection(value: Any, explicit: str = "") -> str:
    if explicit:
        return explicit

    parsed = _json_loads_any(value, default={})
    doc = _extract_document_from_payload_or_json(value)

    candidates = (
        doc.get("collection_name") if isinstance(doc, dict) else None,
        doc.get("collection") if isinstance(doc, dict) else None,
        doc.get("coll") if isinstance(doc, dict) else None,
        _deep_first_value(parsed, ("collection_name", "collection", "coll")),
    )

    for candidate in candidates:
        if candidate:
            return str(candidate)

    return ""


def _extract_source_document_id(value: Any, explicit: str = "") -> str:
    if explicit:
        return explicit

    parsed = _json_loads_any(value, default={})
    doc = _extract_document_from_payload_or_json(value)

    keys = ("document_id", "inserted_id", "_id", "id", "upserted_id", "doc_id")

    if isinstance(doc, dict):
        for key in keys:
            candidate = doc.get(key)
            if candidate:
                return str(_normalize_bson_json(candidate))

    found = _deep_first_value(parsed, keys)
    return str(_normalize_bson_json(found)) if found else ""


def _extract_source_field_path(value: Any, explicit: str = "") -> str:
    if explicit:
        return explicit

    parsed = _json_loads_any(value, default={})
    doc = _extract_document_from_payload_or_json(value)

    if isinstance(doc, dict):
        candidate = doc.get("field_path") or doc.get("target_field_path")
        if candidate:
            return str(candidate)

    found = _deep_first_value(parsed, ("field_path", "target_field_path"))
    return str(found) if found else "image_data"


def _extract_source_filename(value: Any) -> str:
    parsed = _json_loads_any(value, default={})
    doc = _extract_document_from_payload_or_json(value)

    if isinstance(doc, dict):
        candidate = doc.get("filename")
        if candidate:
            return str(candidate)

    found = _deep_first_value(parsed, ("filename", "name"))
    return str(found) if found else ""


def _path_parts(path: str) -> list[str]:
    return [part.strip().lower() for part in str(path or "").split(".") if part.strip()]


def _path_contains_hint(path: str, hints: set[str]) -> bool:
    parts = _path_parts(path)
    joined = ".".join(parts)

    for hint in hints:
        clean_hint = hint.lower()
        if clean_hint in parts:
            return True
        if clean_hint in joined:
            return True

    return False


def _is_binary_envelope(value: Any) -> bool:
    if not isinstance(value, dict):
        return False

    if value.get("__type__") == "bytes" and value.get("data"):
        return True

    if "$binary" in value:
        return True

    if value.get("encoding") == "base64" and value.get("data"):
        return True

    return False


def _looks_like_base64_image_string(value: Any) -> bool:
    if not isinstance(value, str):
        return False

    text = value.strip()
    if text.startswith("data:image/") and "," in text:
        return True

    if len(text) < 256:
        return False

    return text.startswith(("iVBOR", "/9j/", "UklGR", "R0lGOD"))


def _is_url_like(value: Any) -> bool:
    if not isinstance(value, str):
        return False

    text = value.strip().lower()
    return text.startswith(("http://", "https://", "local://", "/"))


def _is_image_like_value(value: Any) -> bool:
    if isinstance(value, (bytes, bytearray, memoryview)):
        return True

    if _looks_like_base64_image_string(value):
        return True

    if not isinstance(value, dict):
        return False

    content_type = str(value.get("content_type") or value.get("mime_type") or "").strip().lower()
    if content_type.startswith("image/"):
        return True

    if _is_binary_envelope(value):
        return True

    if value.get("local_path"):
        return True

    for key in ("url", "view_url", "download_url", "preview_url", "src"):
        if _is_url_like(value.get(key)):
            return True

    if isinstance(value.get("preview"), dict) or isinstance(value.get("original"), dict):
        return True

    if value.get("storage_policy") or value.get("file_id"):
        return True

    return False


def _is_image_like_field(path: str, value: Any) -> bool:
    if _path_contains_hint(path, IMAGE_HINTS):
        return True
    return _is_image_like_value(value)


def _is_probably_raw_binary_field(path: str, value: Any) -> bool:
    if _path_contains_hint(path, BINARY_HINTS) and (_is_binary_envelope(value) or _looks_like_base64_image_string(value)):
        return True

    if isinstance(value, str) and _looks_like_base64_image_string(value):
        return True

    if isinstance(value, (bytes, bytearray, memoryview)):
        return True

    return False


def _extract_asset_summary(value: Any, filename_hint: str = "") -> dict[str, Any]:
    summary: dict[str, Any] = {}

    if filename_hint:
        summary["filename"] = filename_hint

    if isinstance(value, dict):
        for key in (
            "filename",
            "content_type",
            "mime_type",
            "size_bytes",
            "storage_policy",
            "asset_type",
            "file_id",
            "content_hash",
            "created_at",
            "updated_at",
            "created_new_document",
        ):
            if value.get(key) is not None:
                summary[key] = value.get(key)

        for parent_key in ("preview", "original"):
            item = value.get(parent_key)
            if isinstance(item, dict):
                summary[parent_key] = {
                    sub_key: item.get(sub_key)
                    for sub_key in (
                        "backend",
                        "file_id",
                        "asset_collection",
                        "content_type",
                        "size_bytes",
                        "url",
                        "view_url",
                        "download_url",
                        "local_path",
                    )
                    if item.get(sub_key) is not None
                }

        for key in ("url", "view_url", "download_url", "preview_url", "local_path"):
            if value.get(key):
                summary[key] = value.get(key)

        if _is_binary_envelope(value):
            summary["binary_envelope"] = True
            if value.get("size_bytes") is not None:
                summary["size_bytes"] = value.get("size_bytes")
            elif isinstance(value.get("data"), str):
                summary["base64_length"] = len(value["data"])

    elif isinstance(value, str):
        if _is_url_like(value):
            summary["url"] = value
        elif _looks_like_base64_image_string(value):
            summary["base64_length"] = len(value)
            summary["binary_envelope"] = True

    elif isinstance(value, (bytes, bytearray, memoryview)):
        summary["size_bytes"] = len(bytes(value))
        summary["binary_envelope"] = True

    return summary


def _infer_value_type(value: Any) -> str:
    if isinstance(value, bool):
        return "boolean"
    if isinstance(value, int) and not isinstance(value, bool):
        return "int"
    if isinstance(value, float):
        return "float"
    if isinstance(value, str):
        return "string"
    if isinstance(value, list):
        return "list"
    if isinstance(value, dict):
        return "dict"
    if value is None:
        return "null"
    return type(value).__name__


def _flatten_values(value: Any, parent_path: str = "") -> dict[str, Any]:
    flat: dict[str, Any] = {}

    if parent_path and _is_binary_envelope(value):
        flat[parent_path] = value
        return flat

    if isinstance(value, dict):
        if parent_path and _is_image_like_value(value):
            flat[parent_path] = value

            for key, item in value.items():
                child_path = f"{parent_path}.{key}"
                if str(key).lower() in BINARY_HINTS:
                    flat[child_path] = item
                elif isinstance(item, (dict, list)):
                    flat.update(_flatten_values(item, child_path))
                else:
                    flat[child_path] = item

            return flat

        if not value and parent_path:
            flat[parent_path] = value

        for key, item in value.items():
            path = f"{parent_path}.{key}" if parent_path else str(key)
            flat.update(_flatten_values(item, path))

        return flat

    if isinstance(value, list):
        if not value and parent_path:
            flat[parent_path] = value

        for index, item in enumerate(value):
            path = f"{parent_path}.{index}" if parent_path else str(index)
            flat.update(_flatten_values(item, path))

        return flat

    if parent_path:
        flat[parent_path] = value

    return flat


def _classify_field(
    *,
    path: str,
    value: Any,
    collection_name: str,
    document_id: str,
    max_inline_text_chars: int,
    max_inline_json_bytes: int,
    filename_hint: str = "",
) -> dict[str, Any]:
    value_type = _infer_value_type(value)

    if _is_image_like_field(path, value):
        clean_path = path or "image_data"
        return {
            "path": clean_path,
            "kind": "image_asset",
            "value_type": "image",
            "storage": "reference",
            "inline": False,
            "ref": {
                "kind": "zmongo_image_field_ref",
                "collection_name": collection_name,
                "document_id": document_id,
                "field_path": clean_path,
                "field_candidates": _image_field_candidates(clean_path, "image_data"),
            },
            "summary": _extract_asset_summary(value, filename_hint=filename_hint),
        }

    if _is_probably_raw_binary_field(path, value):
        return {
            "path": path,
            "kind": "binary_blob",
            "value_type": value_type,
            "storage": "omitted",
            "inline": False,
            "summary": {
                "reason": "Raw binary/base64 data omitted from content pack.",
                "json_size_bytes": _value_json_size(value),
            },
        }

    if isinstance(value, str):
        kind = "text" if (_path_contains_hint(path, TEXT_HINTS) or len(value) > 40) else "metadata"
        return {
            "path": path,
            "kind": kind,
            "value_type": "string",
            "storage": "inline",
            "inline": True,
            "value": _shorten(value, max_inline_text_chars),
            "summary": {
                "length": len(value),
                "truncated": len(value) > max_inline_text_chars,
            },
        }

    if isinstance(value, bool):
        return {
            "path": path,
            "kind": "boolean",
            "value_type": "boolean",
            "storage": "inline",
            "inline": True,
            "value": bool(value),
            "summary": {},
        }

    if isinstance(value, int) and not isinstance(value, bool):
        return {
            "path": path,
            "kind": "number",
            "value_type": "int",
            "storage": "inline",
            "inline": True,
            "value": int(value),
            "summary": {},
        }

    if isinstance(value, float):
        return {
            "path": path,
            "kind": "number",
            "value_type": "float",
            "storage": "inline",
            "inline": True,
            "value": float(value),
            "summary": {},
        }

    if value is None:
        return {
            "path": path,
            "kind": "metadata",
            "value_type": "null",
            "storage": "inline",
            "inline": True,
            "value": None,
            "summary": {},
        }

    size = _value_json_size(value)
    if size <= max_inline_json_bytes:
        return {
            "path": path,
            "kind": "json",
            "value_type": value_type,
            "storage": "inline",
            "inline": True,
            "value": value,
            "summary": {"json_size_bytes": size},
        }

    return {
        "path": path,
        "kind": "large_json",
        "value_type": value_type,
        "storage": "reference",
        "inline": False,
        "ref": {
            "kind": "zmongo_field_ref",
            "collection_name": collection_name,
            "document_id": document_id,
            "field_path": path,
        },
        "summary": {
            "json_size_bytes": size,
            "reason": "Large JSON value referenced instead of copied.",
        },
    }


def _build_synthetic_image_field_from_save_payload(
    *,
    payload_or_document: Any,
    collection_name: str,
    document_id: str,
    field_path: str,
    filename: str,
) -> Optional[dict[str, Any]]:
    if not collection_name or not document_id:
        return None

    path = field_path or "image_data"
    parsed = _json_loads_any(payload_or_document, default={})
    doc = _extract_document_from_payload_or_json(payload_or_document)

    if isinstance(doc, dict):
        existing = _safe_get_by_path(doc, path)
        if existing not in (None, ""):
            return None

    return {
        "path": path,
        "kind": "image_asset",
        "value_type": "image",
        "storage": "reference",
        "inline": False,
        "ref": {
            "kind": "zmongo_image_field_ref",
            "collection_name": collection_name,
            "document_id": document_id,
            "field_path": path,
            "field_candidates": _image_field_candidates(path, "image_data"),
        },
        "summary": {
            "filename": filename,
            "source_payload_type": "api_save_result",
            "note": "Image reference synthesized from save-image API payload.",
            "request_summary": {
                "success": parsed.get("success") if isinstance(parsed, dict) else None,
                "message": parsed.get("message") if isinstance(parsed, dict) else None,
                "status_code": parsed.get("status_code") if isinstance(parsed, dict) else None,
            },
        },
    }


def _field_report_from_index(field_index: list[dict[str, Any]]) -> dict[str, Any]:
    report = {
        "text_fields": [],
        "metadata_fields": [],
        "number_fields": [],
        "boolean_fields": [],
        "image_asset_fields": [],
        "binary_fields": [],
        "json_fields": [],
        "referenced_fields": [],
        "counts": {},
    }

    for item in field_index:
        path = item.get("path")
        kind = item.get("kind")

        if not path:
            continue

        if kind == "image_asset":
            report["image_asset_fields"].append(path)
        elif kind == "binary_blob":
            report["binary_fields"].append(path)
        elif kind == "text":
            report["text_fields"].append(path)
        elif kind == "number":
            report["number_fields"].append(path)
        elif kind == "boolean":
            report["boolean_fields"].append(path)
        elif kind in {"json", "large_json"}:
            report["json_fields"].append(path)
        else:
            report["metadata_fields"].append(path)

        if item.get("storage") == "reference":
            report["referenced_fields"].append(path)

    report["counts"] = {
        "total": len(field_index),
        "text": len(report["text_fields"]),
        "metadata": len(report["metadata_fields"]),
        "number": len(report["number_fields"]),
        "boolean": len(report["boolean_fields"]),
        "image_asset": len(report["image_asset_fields"]),
        "binary": len(report["binary_fields"]),
        "json": len(report["json_fields"]),
        "referenced": len(report["referenced_fields"]),
    }

    return report


def _make_markdown_preview(
    *,
    content_pack_name: str,
    project_name: str,
    source: dict[str, Any],
    field_report: dict[str, Any],
    field_index: list[dict[str, Any]],
    max_items: int = 40,
) -> str:
    counts = field_report.get("counts", {}) if isinstance(field_report, dict) else {}

    lines = [
        f"# Content Pack: {content_pack_name}",
        "",
        f"- Project: `{project_name}`",
        f"- Source collection: `{source.get('collection_name', '')}`",
        f"- Source document: `{source.get('document_id', '')}`",
        f"- Total fields: `{counts.get('total', 0)}`",
        f"- Text fields: `{counts.get('text', 0)}`",
        f"- Image assets: `{counts.get('image_asset', 0)}`",
        f"- Referenced fields: `{counts.get('referenced', 0)}`",
        "",
        "## Field Index Preview",
        "",
    ]

    for index, item in enumerate(field_index[:max_items]):
        path = item.get("path", "")
        kind = item.get("kind", "")
        storage = item.get("storage", "")
        value_preview = ""

        if item.get("inline"):
            value = item.get("value")
            if isinstance(value, (dict, list)):
                value_preview = _shorten(json.dumps(value, ensure_ascii=False, default=str), 160)
            else:
                value_preview = _shorten(value, 160)
        else:
            summary = item.get("summary") or {}
            value_preview = _shorten(json.dumps(summary, ensure_ascii=False, default=str), 160)

        lines.append(f"{index}. `{path}` — **{kind}** / `{storage}`")
        if value_preview:
            lines.append(f"   - {value_preview}")

    if len(field_index) > max_items:
        lines.append("")
        lines.append(f"... {len(field_index) - max_items} more field(s) omitted from preview.")

    return "\n".join(lines)


def _extract_field_index(value: Any) -> list[dict[str, Any]]:
    parsed = _json_loads_any(value, default={})

    if isinstance(parsed, dict):
        if isinstance(parsed.get("field_index"), list):
            return [item for item in parsed["field_index"] if isinstance(item, dict)]

        data = parsed.get("data")
        if isinstance(data, dict):
            if isinstance(data.get("field_index"), list):
                return [item for item in data["field_index"] if isinstance(item, dict)]

            for key in ("document", "content_pack", "content_pack_source"):
                item = data.get(key)
                if isinstance(item, dict) and isinstance(item.get("field_index"), list):
                    return [entry for entry in item["field_index"] if isinstance(entry, dict)]

    return []


def _extract_content_pack_document(value: Any) -> dict[str, Any]:
    parsed = _json_loads_any(value, default={})

    if isinstance(parsed, dict):
        data = parsed.get("data")
        if isinstance(data, dict):
            for key in ("document", "content_pack", "content_pack_source"):
                item = data.get(key)
                if isinstance(item, dict):
                    return item

        return parsed

    return {}


def _extract_document_id_from_payload(payload: Any) -> str:
    if not isinstance(payload, dict):
        return ""

    data = payload.get("data")
    if isinstance(data, dict):
        for key in ("document_id", "inserted_id", "_id", "id", "upserted_id"):
            if data.get(key):
                return str(data[key])

        doc = data.get("document")
        if isinstance(doc, dict):
            for key in ("_id", "document_id", "id"):
                if doc.get(key):
                    return str(doc[key])

    for key in ("document_id", "inserted_id", "_id", "id", "upserted_id"):
        if payload.get(key):
            return str(payload[key])

    return ""


def _extract_documents_from_payload(payload: Any) -> list[dict[str, Any]]:
    if not isinstance(payload, dict):
        return []

    data = payload.get("data")

    if isinstance(data, dict):
        for key in ("documents", "docs", "results", "items"):
            value = data.get(key)
            if isinstance(value, list):
                return [item for item in value if isinstance(item, dict)]

        for key in ("document", "doc", "result"):
            value = data.get(key)
            if isinstance(value, dict):
                return [value]

    if isinstance(data, list):
        return [item for item in data if isinstance(item, dict)]

    return []

def _content_pack_doc_score(doc: Any) -> tuple[int, int, float, str]:
    """
    Prefer usable/new content packs over stale empty duplicates.

    Local File Store query_docs currently ignores the requested sort list, so
    load_content_pack must not blindly use docs[0]. This score intentionally
    prefers a document with real fields/outputs, then the newest timestamp.
    """
    if not isinstance(doc, dict):
        return (-1, -1, 0.0, "")

    schema_bonus = 1000 if doc.get("schema_kind") == CONTENT_PACK_SCHEMA_KIND else 0

    field_index = doc.get("field_index")
    field_count = len(field_index) if isinstance(field_index, list) else 0

    outputs = doc.get("outputs") if isinstance(doc.get("outputs"), dict) else {}
    output_count = 0
    if isinstance(outputs, dict):
        for key in ("images", "texts", "numbers", "booleans", "any"):
            value = outputs.get(key)
            if isinstance(value, list):
                output_count += len(value)

    report_count = 0
    field_report = doc.get("field_report") if isinstance(doc.get("field_report"), dict) else {}
    counts = field_report.get("counts") if isinstance(field_report.get("counts"), dict) else {}
    try:
        report_count = int(counts.get("total") or 0)
    except Exception:
        report_count = 0

    usable_count = max(field_count, output_count, report_count)
    usable_bonus = 500 if usable_count > 0 else 0

    timestamp = 0.0
    for key in ("updated_at_unix", "created_at_unix"):
        try:
            timestamp = max(timestamp, float(doc.get(key) or 0.0))
        except Exception:
            pass

    updated_text = str(doc.get("updated_at") or doc.get("created_at") or "")

    return (schema_bonus + usable_bonus, usable_count, timestamp, updated_text)


def _select_best_content_pack_doc(docs: list[dict[str, Any]]) -> dict[str, Any]:
    candidates = [doc for doc in docs if isinstance(doc, dict)]
    if not candidates:
        return {}
    return max(candidates, key=_content_pack_doc_score)

def _join_route_path(prefix: str = "", path: str = "") -> str:
    """
    Build the canonical backend route path used by ComfyZMongoRoutes.

    The ZMongo API session request method accepts only (method, path) as
    positional arguments.  Older node code passed (method, prefix, path), which
    breaks with:
        ZMongoApiSession.request() takes 3 positional arguments but 4 were given
    """
    clean_prefix = str(prefix or "").strip().rstrip("/")
    clean_path = str(path or "").strip()

    if clean_prefix and not clean_prefix.startswith("/"):
        clean_prefix = "/" + clean_prefix
    if clean_path and not clean_path.startswith("/"):
        clean_path = "/" + clean_path

    if not clean_prefix:
        return clean_path or "/"
    if clean_path == "/":
        return clean_prefix or "/"
    return clean_prefix + clean_path


def _route_api_request(
    session: Any,
    method: str,
    path: str,
    json_body: dict[str, Any] | None = None,
    *,
    prefix: str = "",
    gemini_prefix: str = "",
    zmongo_prefix: str = "",
) -> dict[str, Any]:
    """
    Route-aligned, signature-safe request adapter for ZMongoApiSession.

    Canonical ComfyZMongoRoutes paths after the optional mount prefix:
      - POST /api/query
      - POST /api/doc/create
      - POST /api/doc/update
      - GET  /api/doc/<coll>/<doc_id>
      - GET  /api/image/<coll>/<doc_id>?field_path=...

    This wrapper intentionally combines the prefix and path into one path before
    calling session.request(method, full_path, ...).  It also tolerates older
    session helpers that used body/json/payload instead of json_body.
    """
    request_fn = getattr(session, "request", None)
    if not callable(request_fn):
        return _error_payload("Session does not expose request().")

    route_prefix = str(prefix or gemini_prefix or zmongo_prefix or "").strip()
    full_path = _join_route_path(route_prefix, path)
    body = json_body or {}
    method_text = str(method or "GET").upper()

    call_attempts = (
        lambda: request_fn(method_text, full_path, json_body=body),
        lambda: request_fn(method_text, full_path, body=body),
        lambda: request_fn(method_text, full_path, json=body),
        lambda: request_fn(method_text, full_path, payload=body),
        lambda: request_fn(method_text, full_path, data=body),
        lambda: request_fn(method_text, full_path),
    )

    last_type_error: Exception | None = None
    for attempt in call_attempts:
        try:
            result = attempt()
            if isinstance(result, dict):
                return result
            if isinstance(result, (bytes, bytearray)):
                return _success_payload(
                    "Binary response received.",
                    data={"bytes": bytes(result), "path": full_path},
                )
            return _success_payload(
                "Request completed.",
                data={"result": result, "path": full_path},
            )
        except TypeError as exc:
            last_type_error = exc
            continue
        except Exception as exc:
            return _error_payload(
                f"Session request failed: {exc}",
                error_type=exc.__class__.__name__,
                data={"method": method_text, "path": full_path},
            )

    return _error_payload(
        "Session request signature did not match any supported adapter call.",
        error_type="RequestSignatureMismatch",
        data={
            "method": method_text,
            "path": full_path,
            "last_type_error": str(last_type_error) if last_type_error else "",
            "expected_signature": "request(method, path, *, json_body=...) or equivalent",
        },
    )

def _query_content_pack(
    session: Any,
    *,
    collection_name: str,
    content_pack_name: str,
    project_name: str,
    zmongo_prefix: str = "",
) -> dict[str, Any]:
    query: dict[str, Any] = {
        "schema_kind": CONTENT_PACK_SCHEMA_KIND,
        "content_pack_name": content_pack_name,
    }

    if project_name:
        query["project_name"] = project_name

    query_docs = getattr(session, "query_docs", None)
    if callable(query_docs):
        try:
            return query_docs(
                collection=collection_name,
                query=query,
                many=True,
                limit=10,
                skip=0,
                sort=[["updated_at_unix", -1]],
                cache=False,
            )
        except TypeError:
            pass
        except Exception as exc:
            return _error_payload(
                f"Session query_docs failed: {exc}",
                error_type=exc.__class__.__name__,
                data={"collection_name": collection_name, "query": query},
            )

    return _route_api_request(
        session,
        "POST",
        "/api/query",
        json_body={
            "collection": collection_name,
            "collection_name": collection_name,
            "query": query,
            "many": True,
            "limit": 10,
            "skip": 0,
            "sort": [["updated_at_unix", -1]],
            "cache": False,
        },
        zmongo_prefix=zmongo_prefix,
    )


def _create_content_pack_doc(
    session: Any,
    *,
    collection_name: str,
    document: dict[str, Any],
    zmongo_prefix: str = "",
) -> dict[str, Any]:
    create_doc = getattr(session, "create_doc", None)
    if callable(create_doc):
        try:
            return create_doc(collection=collection_name, document=document)
        except TypeError:
            pass
        except Exception as exc:
            return _error_payload(
                f"Session create_doc failed: {exc}",
                error_type=exc.__class__.__name__,
                data={"collection_name": collection_name},
            )

    return _route_api_request(
        session,
        "POST",
        "/api/doc/create",
        json_body={
            "collection": collection_name,
            "collection_name": collection_name,
            "document": document,
        },
        zmongo_prefix=zmongo_prefix,
    )


def _save_content_pack_doc_by_name(
    session: Any,
    *,
    collection_name: str,
    content_pack_name: str,
    project_name: str,
    document: dict[str, Any],
    zmongo_prefix: str = "",
) -> dict[str, Any]:
    query: dict[str, Any] = {
        "schema_kind": CONTENT_PACK_SCHEMA_KIND,
        "content_pack_name": content_pack_name,
    }

    if project_name:
        query["project_name"] = project_name

    update_doc = getattr(session, "update_doc", None)
    if callable(update_doc):
        try:
            return update_doc(
                collection=collection_name,
                query=query,
                update={"$set": document},
                upsert=True,
            )
        except TypeError:
            pass
        except Exception as exc:
            return _error_payload(
                f"Session update_doc failed: {exc}",
                error_type=exc.__class__.__name__,
                data={"collection_name": collection_name, "query": query},
            )

    save_value = getattr(session, "save_value", None)
    if callable(save_value):
        try:
            return save_value(
                collection=collection_name,
                query=query,
                document_id="",
                field_path="",
                value=document,
                upsert_if_missing=True,
                parse_json_strings=False,
                normalize_for_storage=False,
            )
        except TypeError:
            pass
        except Exception as exc:
            return _error_payload(
                f"Session save_value failed: {exc}",
                error_type=exc.__class__.__name__,
                data={"collection_name": collection_name, "query": query},
            )

    return _route_api_request(
        session,
        "POST",
        "/api/doc/update",
        json_body={
            "collection": collection_name,
            "collection_name": collection_name,
            "query": query,
            "update": {"$set": document},
            "upsert": True,
        },
        zmongo_prefix=zmongo_prefix,
    )


def _empty_image_tensor(width: int = 64, height: int = 64):
    if torch is None:
        return None
    return torch.zeros((1, max(1, height), max(1, width), 3), dtype=torch.float32)


def _image_bytes_to_tensor(image_bytes: bytes):
    if torch is None or np is None or Image is None:
        return _empty_image_tensor()

    if not image_bytes:
        return _empty_image_tensor()

    image = Image.open(io.BytesIO(image_bytes))
    if ImageOps is not None:
        image = ImageOps.exif_transpose(image)

    image = image.convert("RGB")

    try:
        return _pil_to_comfy_image(image)
    except Exception:
        array = np.asarray(image).astype(np.float32) / 255.0
        return torch.from_numpy(array)[None,]


def _load_doc_for_image_ref(session: Any, collection_name: str, document_id: str, *, zmongo_prefix: str = "") -> tuple[dict[str, Any], dict[str, Any]]:
    get_doc = getattr(session, "get_doc", None)
    if callable(get_doc):
        try:
            payload = get_doc(collection=collection_name, document_id=document_id, cache=False)
            document = _extract_document_from_payload_or_json(payload)
            if document:
                return document, payload if isinstance(payload, dict) else {}
        except TypeError:
            pass
        except Exception as exc:
            return {}, _error_payload(
                f"Session get_doc failed: {exc}",
                error_type=exc.__class__.__name__,
                data={"collection_name": collection_name, "document_id": document_id},
            )

    payload = _route_api_request(
        session,
        "GET",
        f"/api/doc/{collection_name}/{document_id}",
        json_body={},
        zmongo_prefix=zmongo_prefix,
    )

    return _extract_document_from_payload_or_json(payload), payload


def _load_image_tensor_from_ref(session: Any, ref: dict[str, Any], *, zmongo_prefix: str = "") -> tuple[Any, dict[str, Any]]:
    if session is None:
        return _empty_image_tensor(), _error_payload("No session provided for image reference load.")

    collection_name = _clean(ref.get("collection_name"))
    document_id = _clean(ref.get("document_id"))
    field_path = _clean(ref.get("field_path"), "image_data")

    if not collection_name or not document_id:
        return _empty_image_tensor(), _error_payload(
            "Image reference is missing collection_name or document_id.",
            data={"ref": ref},
        )

    fetch_image_field = getattr(session, "fetch_image_field", None)
    if callable(fetch_image_field):
        try:
            image_bytes, source = fetch_image_field(
                collection=collection_name,
                document_id=document_id,
                field_path=field_path,
                master_key_hex="",
            )
            return _image_bytes_to_tensor(image_bytes), _success_payload(
                "Loaded image through session.fetch_image_field.",
                {
                    "collection_name": collection_name,
                    "document_id": document_id,
                    "field_path": field_path,
                    "source": source,
                },
            )
        except Exception:
            pass

    document, doc_payload = _load_doc_for_image_ref(session, collection_name, document_id, zmongo_prefix=zmongo_prefix)
    if not document:
        return _empty_image_tensor(), _error_payload(
            "Could not load image reference document.",
            data={
                "collection_name": collection_name,
                "document_id": document_id,
                "doc_payload": doc_payload,
            },
            status_code=404,
        )

    errors: list[str] = []
    candidates = ref.get("field_candidates")
    if not isinstance(candidates, list) or not candidates:
        candidates = _image_field_candidates(field_path, "image_data")

    for candidate in candidates:
        try:
            value = _safe_get_by_path(document, str(candidate))
            if value is None:
                errors.append(f"{candidate}: missing")
                continue

            image_bytes = _decode_image_bytes_from_value(value)
            return _image_bytes_to_tensor(image_bytes), _success_payload(
                "Loaded image from referenced document field.",
                {
                    "collection_name": collection_name,
                    "document_id": document_id,
                    "field_path": candidate,
                },
            )
        except Exception as exc:
            errors.append(f"{candidate}: {exc}")

    return _empty_image_tensor(), _error_payload(
        "Could not decode image from referenced document.",
        data={
            "collection_name": collection_name,
            "document_id": document_id,
            "field_path": field_path,
            "field_errors": errors,
            "doc_payload_status_code": doc_payload.get("status_code") if isinstance(doc_payload, dict) else None,
        },
        status_code=404,
    )


# -----------------------------------------------------------------------------
# Nodes
# -----------------------------------------------------------------------------

class ZMongoContentPackFromDocumentJSONNode(AlwaysDirtyMixin):
    @classmethod
    def INPUT_TYPES(cls):
        return {
            "required": {
                "document_json": ("STRING", {"default": "{}", "multiline": True}),
                "content_pack_name": ("STRING", {"default": DEFAULT_CONTENT_PACK_NAME}),
                "project_name": ("STRING", {"default": "default"}),
                "collection_name": ("STRING", {"default": ""}),
                "document_id": ("STRING", {"default": ""}),
                "image_reference_mode": (
                    ["reference_only", "metadata_only", "caption_if_possible", "embed_small_preview"],
                    {"default": "reference_only"},
                ),
                "field_selection_mode": (
                    ["auto", "select_all_safe", "text_only", "image_assets_only", "manual_include_paths"],
                    {"default": "auto"},
                ),
            },
            "optional": {
                "field_path": ("STRING", {"default": "image_data"}),
                "include_paths_csv": ("STRING", {"default": ""}),
                "exclude_paths_csv": ("STRING", {"default": ""}),
                "max_inline_text_chars": ("INT", {"default": 3000, "min": 128, "max": 100000}),
                "max_inline_json_bytes": ("INT", {"default": 8192, "min": 256, "max": 1048576}),
                "refresh_token": ("STRING", {"default": ""}),
            },
        }

    RETURN_TYPES = (
        "STRING",
        "STRING",
        "STRING",
        "*",
        "STRING",
        "INT",
        "INT",
        "BOOLEAN",
        "STRING",
    )
    RETURN_NAMES = (
        "content_pack_source_json",
        "field_report_json",
        "preview_markdown",
        "selectable_paths",
        "indexed_paths",
        "image_asset_count",
        "text_field_count",
        "success",
        "refresh",
    )
    OUTPUT_IS_LIST = (False, False, False, True, False, False, False, False, False)

    FUNCTION = "from_document_json"
    CATEGORY = f"{CONTENT_PACK_CATEGORY}/Document"
    OUTPUT_NODE = True

    def from_document_json(
        self,
        document_json: str,
        content_pack_name: str = DEFAULT_CONTENT_PACK_NAME,
        project_name: str = "default",
        collection_name: str = "",
        document_id: str = "",
        image_reference_mode: str = "reference_only",
        field_selection_mode: str = "auto",
        field_path: str = "image_data",
        include_paths_csv: str = "",
        exclude_paths_csv: str = "",
        max_inline_text_chars: int = 3000,
        max_inline_json_bytes: int = 8192,
        refresh_token: str = "",
    ):
        refresh = _dirty_token("content_pack_from_document_json", refresh_token, content_pack_name, project_name)

        try:
            document = _extract_document_from_payload_or_json(document_json)
            if not document:
                payload = _error_payload(
                    "document_json did not contain a JSON document or API result payload.",
                    data={"refresh": refresh},
                    error_type="MissingDocument",
                    status_code=400,
                )
                return (_json_text(payload), _json_text({}), "", [], "[]", 0, 0, False, refresh)

            resolved_collection = _extract_source_collection(document_json, explicit=_clean(collection_name))
            resolved_document_id = _extract_source_document_id(document_json, explicit=_clean(document_id))
            resolved_field_path = _extract_source_field_path(document_json, explicit=_clean(field_path, "image_data"))
            resolved_filename = _extract_source_filename(document_json)

            include_paths = set(_csv_list(include_paths_csv))
            exclude_paths = set(_csv_list(exclude_paths_csv))

            flat_values = _flatten_values(document)
            field_index: list[dict[str, Any]] = []

            for path, value in sorted(flat_values.items(), key=lambda item: item[0].lower()):
                if include_paths and path not in include_paths:
                    continue
                if path in exclude_paths:
                    continue
                if any(path.startswith(f"{excluded}.") for excluded in exclude_paths):
                    continue

                item = _classify_field(
                    path=path,
                    value=value,
                    collection_name=resolved_collection,
                    document_id=resolved_document_id,
                    max_inline_text_chars=int(max_inline_text_chars),
                    max_inline_json_bytes=int(max_inline_json_bytes),
                    filename_hint=resolved_filename,
                )

                if field_selection_mode == "text_only" and item.get("kind") not in {"text", "metadata", "number", "boolean"}:
                    continue

                if field_selection_mode == "image_assets_only" and item.get("kind") != "image_asset":
                    continue

                if field_selection_mode == "manual_include_paths" and not include_paths:
                    continue

                if image_reference_mode == "metadata_only" and item.get("kind") == "image_asset":
                    item["storage"] = "metadata_only"
                    item["inline"] = False

                field_index.append(item)

            synthetic_image_field = _build_synthetic_image_field_from_save_payload(
                payload_or_document=document_json,
                collection_name=resolved_collection,
                document_id=resolved_document_id,
                field_path=resolved_field_path,
                filename=resolved_filename,
            )

            if synthetic_image_field:
                existing_paths = {str(item.get("path") or "") for item in field_index}
                if synthetic_image_field["path"] not in existing_paths:
                    field_index.insert(0, synthetic_image_field)

            field_report = _field_report_from_index(field_index)

            source = {
                "schema_kind": CONTENT_PACK_SOURCE_SCHEMA_KIND,
                "schema_version": CONTENT_PACK_SCHEMA_VERSION,
                "source_mode": "document_json_or_api_payload",
                "collection_name": resolved_collection,
                "document_id": resolved_document_id,
                "field_path": resolved_field_path,
                "filename": resolved_filename,
                "image_reference_mode": image_reference_mode,
                "field_selection_mode": field_selection_mode,
            }

            content_pack_source = {
                "schema_kind": CONTENT_PACK_SOURCE_SCHEMA_KIND,
                "schema_version": CONTENT_PACK_SCHEMA_VERSION,
                "content_pack_name": _safe_name(content_pack_name, DEFAULT_CONTENT_PACK_NAME),
                "project_name": _clean(project_name, "default"),
                "source": source,
                "field_index": field_index,
                "field_report": field_report,
                "created_at": _utc_now(),
                "updated_at": _utc_now(),
                "refresh": refresh,
            }

            preview = _make_markdown_preview(
                content_pack_name=content_pack_source["content_pack_name"],
                project_name=content_pack_source["project_name"],
                source=source,
                field_report=field_report,
                field_index=field_index,
            )

            selectable_paths = [item.get("path", "") for item in field_index if item.get("path")]
            indexed_paths = _json_text([f"{index}: {path}" for index, path in enumerate(selectable_paths)])

            return (
                _json_text(content_pack_source),
                _json_text(field_report),
                preview,
                selectable_paths,
                indexed_paths,
                int(field_report["counts"]["image_asset"]),
                int(field_report["counts"]["text"]),
                True,
                refresh,
            )

        except Exception as exc:
            payload = _error_payload(
                f"Content pack document classification failed: {exc}",
                data={"refresh": refresh},
                error_type=exc.__class__.__name__,
            )
            return (_json_text(payload), _json_text({}), "", [], "[]", 0, 0, False, refresh)


class ZMongoContentPackFieldSelectorNode(AlwaysDirtyMixin):
    @classmethod
    def INPUT_TYPES(cls):
        return {
            "required": {
                "content_pack_source_json": ("STRING", {"default": "{}", "multiline": True}),
                "index": ("INT", {"default": 0, "min": 0, "max": 4096}),
                "semantic_hint": (
                    [
                        "auto",
                        "field_path",
                        "text_field",
                        "image_field",
                        "metadata_field",
                        "number_field",
                        "boolean_field",
                        "json_field",
                        "ignore_field",
                    ],
                    {"default": "auto"},
                ),
            },
            "optional": {
                "fallback": ("STRING", {"default": ""}),
                "refresh_token": ("STRING", {"default": ""}),
            },
        }

    RETURN_TYPES = ("STRING", "STRING", "STRING", "BOOLEAN", "BOOLEAN", "STRING", "STRING", "BOOLEAN")
    RETURN_NAMES = (
        "selected_path",
        "selected_value_json",
        "selected_type",
        "is_image_like",
        "is_text_like",
        "status",
        "json",
        "success",
    )
    FUNCTION = "select_field"
    CATEGORY = f"{CONTENT_PACK_CATEGORY}/Selector"
    OUTPUT_NODE = True

    def select_field(
        self,
        content_pack_source_json: str,
        index: int = 0,
        semantic_hint: str = "auto",
        fallback: str = "",
        refresh_token: str = "",
    ):
        try:
            field_index = _extract_field_index(content_pack_source_json)
            hint = str(semantic_hint or "auto").strip()
            filtered = field_index

            if hint == "text_field":
                filtered = [item for item in field_index if item.get("kind") == "text"]
            elif hint == "image_field":
                filtered = [item for item in field_index if item.get("kind") == "image_asset"]
            elif hint == "metadata_field":
                filtered = [item for item in field_index if item.get("kind") == "metadata"]
            elif hint == "number_field":
                filtered = [item for item in field_index if item.get("kind") == "number"]
            elif hint == "boolean_field":
                filtered = [item for item in field_index if item.get("kind") == "boolean"]
            elif hint == "json_field":
                filtered = [item for item in field_index if item.get("kind") in {"json", "large_json"}]
            elif hint == "ignore_field":
                filtered = [item for item in field_index if item.get("kind") in {"binary_blob", "large_json"}]

            if not filtered:
                payload = _error_payload(
                    "No matching content-pack field was found.",
                    data={"semantic_hint": hint, "fallback": fallback},
                    error_type="NoMatchingField",
                    status_code=404,
                )
                return (fallback, "{}", "", False, False, payload["message"], _json_text(payload), False)

            safe_index = max(0, min(int(index or 0), len(filtered) - 1))
            item = filtered[safe_index]

            path = str(item.get("path") or fallback or "")
            kind = str(item.get("kind") or "")
            value_type = str(item.get("value_type") or "")
            is_image = kind == "image_asset"
            is_text = kind in {"text", "metadata"} and value_type == "string"

            if item.get("inline"):
                value = item.get("value")
            else:
                value = {
                    "ref": item.get("ref"),
                    "summary": item.get("summary"),
                    "storage": item.get("storage"),
                    "kind": kind,
                    "value_type": value_type,
                }

            payload = _success_payload(
                "Selected content-pack field.",
                {
                    "selected_index": safe_index,
                    "semantic_hint": hint,
                    "selected": item,
                    "path": path,
                    "kind": kind,
                    "value_type": value_type,
                },
            )

            return (
                path,
                _json_text(value),
                value_type or kind,
                bool(is_image),
                bool(is_text),
                payload["message"],
                _json_text(payload),
                True,
            )

        except Exception as exc:
            payload = _error_payload(
                f"Content pack field selection failed: {exc}",
                error_type=exc.__class__.__name__,
            )
            return (fallback, "{}", "", False, False, payload["message"], _json_text(payload), False)


class ZMongoContentPackSaveNode(AlwaysDirtyMixin):
    @classmethod
    def INPUT_TYPES(cls):
        return {
            "required": {
                "session": ("ZMONGO_API_SESSION",),
                "content_pack_source_json": ("STRING", {"default": "{}", "multiline": True}),
                "content_pack_name": ("STRING", {"default": DEFAULT_CONTENT_PACK_NAME}),
                "target_collection": ("STRING", {"default": DEFAULT_CONTENT_PACK_COLLECTION}),
                "save_mode": (
                    ["reference_only", "copy_text_reference_images", "copy_all_small_values"],
                    {"default": "reference_only"},
                ),
                "overwrite_mode": (
                    ["create_new", "replace_by_name", "versioned_snapshot", "latest_alias"],
                    {"default": "replace_by_name"},
                ),
            },
            "optional": {
                "project_name": ("STRING", {"default": ""}),
                "markdown_override": ("STRING", {"default": "", "multiline": True}),
                "zmongo_prefix": ("STRING", {"default": ""}),
                "refresh_token": ("STRING", {"default": ""}),
            },
        }

    RETURN_TYPES = ("STRING", "STRING", "STRING", "BOOLEAN", "STRING")
    RETURN_NAMES = ("json", "document_id", "saved_name", "success", "refresh")
    FUNCTION = "save_content_pack"
    CATEGORY = f"{CONTENT_PACK_CATEGORY}/Save & Load"
    OUTPUT_NODE = True

    def save_content_pack(
        self,
        session,
        content_pack_source_json: str,
        content_pack_name: str = DEFAULT_CONTENT_PACK_NAME,
        target_collection: str = DEFAULT_CONTENT_PACK_COLLECTION,
        save_mode: str = "reference_only",
        overwrite_mode: str = "replace_by_name",
        project_name: str = "",
        markdown_override: str = "",
        zmongo_prefix: str = "",
        refresh_token: str = "",
    ):
        refresh = _dirty_token("save_content_pack", refresh_token, content_pack_name, target_collection)

        if session is None:
            payload = _error_payload("No API session provided. Connect a ZMongo API Key Session node.", data={"refresh": refresh})
            return (_json_text(payload), "", "", False, refresh)

        try:
            source_doc = _extract_content_pack_document(content_pack_source_json)
            if not source_doc:
                payload = _error_payload("content_pack_source_json did not contain a content-pack source object.", data={"refresh": refresh})
                return (_json_text(payload), "", "", False, refresh)

            source_name = _clean(source_doc.get("content_pack_name"), DEFAULT_CONTENT_PACK_NAME)
            saved_name = _safe_name(content_pack_name or source_name, DEFAULT_CONTENT_PACK_NAME)
            resolved_project = _clean(project_name) or _clean(source_doc.get("project_name"), "default")
            collection = _clean(target_collection, DEFAULT_CONTENT_PACK_COLLECTION)

            field_index = _extract_field_index(source_doc)
            field_report = _field_report_from_index(field_index)

            now = _utc_now()
            markdown = markdown_override.strip()
            if not markdown:
                markdown = _make_markdown_preview(
                    content_pack_name=saved_name,
                    project_name=resolved_project,
                    source=source_doc.get("source") if isinstance(source_doc.get("source"), dict) else {},
                    field_report=field_report,
                    field_index=field_index,
                )

            content_pack_document = {
                "schema_kind": CONTENT_PACK_SCHEMA_KIND,
                "schema_version": CONTENT_PACK_SCHEMA_VERSION,
                "content_pack_name": saved_name,
                "project_name": resolved_project,
                "save_mode": save_mode,
                "overwrite_mode": overwrite_mode,
                "source": source_doc.get("source") or {},
                "field_count": len(field_index),  # Added like Presets
                "field_index": field_index,  # The single source of truth
                "field_report": field_report,
                "content": {
                    "markdown": markdown,
                    "summary": f"Content pack {saved_name!r} built from {field_report.get('counts', {}).get('total', 0)} field(s).",
                },
                "created_at": source_doc.get("created_at") or now,
                "updated_at": now,
                "updated_at_unix": time.time(),
            }

            if overwrite_mode in {"replace_by_name", "latest_alias"}:
                save_payload = _save_content_pack_doc_by_name(
                    session,
                    collection_name=collection,
                    content_pack_name=saved_name,
                    project_name=resolved_project,
                    document=content_pack_document,
                    zmongo_prefix=zmongo_prefix,
                )
            elif overwrite_mode == "versioned_snapshot":
                snapshot_name = f"{saved_name}_{time.strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:8]}"
                content_pack_document["content_pack_name"] = snapshot_name
                content_pack_document["base_content_pack_name"] = saved_name
                save_payload = _create_content_pack_doc(
                    session,
                    collection_name=collection,
                    document=content_pack_document,
                    zmongo_prefix=zmongo_prefix,
                )
            else:
                save_payload = _create_content_pack_doc(
                    session,
                    collection_name=collection,
                    document=content_pack_document,
                    zmongo_prefix=zmongo_prefix,
                )

            document_id = _extract_document_id_from_payload(save_payload)
            success = bool(save_payload.get("success"))

            merged = _success_payload(
                "Content pack save attempted.",
                {
                    "save_payload": save_payload,
                    "document_id": document_id,
                    "saved_name": content_pack_document.get("content_pack_name"),
                    "target_collection": collection,
                    "content_pack_document": content_pack_document,
                    "refresh": refresh,
                },
                status_code=int(save_payload.get("status_code") or 200),
            )
            merged["success"] = success
            if not success:
                merged["message"] = "Content pack save failed."

            return (
                _json_text(merged),
                document_id,
                str(content_pack_document.get("content_pack_name") or saved_name),
                success,
                refresh,
            )

        except Exception as exc:
            payload = _error_payload(
                f"Content pack save failed: {exc}",
                data={"refresh": refresh},
                error_type=exc.__class__.__name__,
            )
            return (_json_text(payload), "", "", False, refresh)


class ZMongoContentPackLoadNode(AlwaysDirtyMixin):
    @classmethod
    def INPUT_TYPES(cls):
        return {
            "required": {
                "session": ("ZMONGO_API_SESSION",),
                "content_pack_name": ("STRING", {"default": DEFAULT_CONTENT_PACK_NAME}),
                "source_collection": ("STRING", {"default": DEFAULT_CONTENT_PACK_COLLECTION}),
            },
            "optional": {
                "project_name": ("STRING", {"default": ""}),
                "fallback_to_latest": ("BOOLEAN", {"default": True}),
                "zmongo_prefix": ("STRING", {"default": ""}),
                "refresh_token": ("STRING", {"default": ""}),
            },
        }

    RETURN_TYPES = ("STRING", "STRING", "STRING", "STRING", "BOOLEAN", "STRING")
    RETURN_NAMES = ("content_pack_json", "markdown", "summary", "document_id", "success", "json")
    FUNCTION = "load_content_pack"
    CATEGORY = f"{CONTENT_PACK_CATEGORY}/Save & Load"
    OUTPUT_NODE = True

    def load_content_pack(
        self,
        session,
        content_pack_name: str = DEFAULT_CONTENT_PACK_NAME,
        source_collection: str = DEFAULT_CONTENT_PACK_COLLECTION,
        project_name: str = "",
        fallback_to_latest: bool = True,
        zmongo_prefix: str = "",
        refresh_token: str = "",
    ):
        if session is None:
            payload = _error_payload("No API session provided. Connect a ZMongo API Key Session node.")
            return ("{}", "", "", "", False, _json_text(payload))

        try:
            collection = _clean(source_collection, DEFAULT_CONTENT_PACK_COLLECTION)
            name = _safe_name(content_pack_name, DEFAULT_CONTENT_PACK_NAME)
            project = _clean(project_name)

            payload = _query_content_pack(
                session,
                collection_name=collection,
                content_pack_name=name,
                project_name=project,
                zmongo_prefix=zmongo_prefix,
            )

            docs = _extract_documents_from_payload(payload)

            if not docs and fallback_to_latest and name != DEFAULT_CONTENT_PACK_NAME:
                payload = _query_content_pack(
                    session,
                    collection_name=collection,
                    content_pack_name=DEFAULT_CONTENT_PACK_NAME,
                    project_name=project,
                    zmongo_prefix=zmongo_prefix,
                )
                docs = _extract_documents_from_payload(payload)

            if not docs:
                merged = _error_payload(
                    "Content pack not found.",
                    data={
                        "content_pack_name": name,
                        "source_collection": collection,
                        "project_name": project,
                        "query_payload": payload,
                    },
                    error_type="ContentPackNotFound",
                    status_code=404,
                )
                return ("{}", "", "", "", False, _json_text(merged))

            doc = _select_best_content_pack_doc(docs)
            document_id = _extract_source_document_id(doc)
            content = doc.get("content") if isinstance(doc.get("content"), dict) else {}
            markdown = str(content.get("markdown") or doc.get("markdown") or "")
            summary = str(content.get("summary") or doc.get("summary") or "")

            merged = _success_payload(
                "Loaded content pack.",
                {
                    "document_id": document_id,
                    "content_pack_name": doc.get("content_pack_name"),
                    "source_collection": collection,
                    "query_payload": payload,
                },
            )

            return (_json_text(doc), markdown, summary, document_id, True, _json_text(merged))

        except Exception as exc:
            payload = _error_payload(
                f"Content pack load failed: {exc}",
                error_type=exc.__class__.__name__,
            )
            return ("{}", "", "", "", False, _json_text(payload))


class ZMongoDynamicContentPackOutputs(AlwaysDirtyMixin):
    """
    Redesigned Dynamic Content Pack Output Node.
    Modeled precisely after ZMongoDynamicPresetOutputs for strictly linear, 1:1 socket alignment.
    """

    MAX_OUTPUTS = 64

    RETURN_TYPES = tuple(["*"] * MAX_OUTPUTS)
    RETURN_NAMES = tuple([f"out_{index:02d}" for index in range(MAX_OUTPUTS)])
    FUNCTION = "hydrate"
    CATEGORY = f"{CONTENT_PACK_CATEGORY}/Dynamic Outputs"
    OUTPUT_NODE = True

    @classmethod
    def INPUT_TYPES(cls):
        return {
            "required": {
                "content_pack_json": ("STRING", {"default": "{}", "multiline": True}),
                "include_images": ("BOOLEAN", {"default": True}),
                "include_metadata": ("BOOLEAN", {"default": True}),
            },
            "optional": {
                "session": ("ZMONGO_API_SESSION",),
                "zmongo_prefix": ("STRING", {"default": ""}),
                "refresh_token": ("STRING", {"default": ""}),
                "cached_content_pack_json": ("STRING", {"default": "{}", "multiline": True}),
                "resolved_content_pack_json": ("STRING", {"default": "{}", "multiline": True}),
                "runtime_content_pack_json": ("STRING", {"default": "{}", "multiline": True}),
                "dynamic_status": ("STRING", {"default": "", "multiline": True}),
            },
        }

    @staticmethod
    def _unwrap_value(value: Any) -> Any:
        """Safeguard: Strips single-item arrays (e.g., [4.5] -> 4.5) to prevent ComfyUI tuple crashes."""
        if isinstance(value, list) and len(value) == 1:
            return value[0]
        return value

    def hydrate(
        self,
        content_pack_json: str = "{}",
        include_images: bool = True,
        include_metadata: bool = True,
        session=None,
        zmongo_prefix: str = "",
        cached_content_pack_json: str = "",
        resolved_content_pack_json: str = "",
        runtime_content_pack_json: str = "",
        dynamic_status: str = "",
        refresh_token: str = "",
        **_ignored: Any,
    ):
        output_values = [None] * self.MAX_OUTPUTS

        try:
            payload = _select_best_content_pack_doc([
                _extract_content_pack_document(content_pack_json),
                _extract_content_pack_document(cached_content_pack_json),
                _extract_content_pack_document(resolved_content_pack_json),
                _extract_content_pack_document(runtime_content_pack_json),
            ])
            if not payload:
                return tuple(output_values)
            # 1. Grab the flat field index (analogous to the 'fields' list in Presets)
            field_index = _extract_field_index(payload)

            # 2. Linear Filter (Matches the Javascript UI socket generation 1:1)
            visible_fields = []
            for field in field_index:
                if not isinstance(field, dict):
                    continue

                kind = str(field.get("kind") or "")
                path = str(field.get("path") or field.get("name") or "")

                if not include_images and kind == "image_asset":
                    continue

                is_meta = path.startswith("metadata.") or path.startswith("_")
                if not include_metadata and is_meta:
                    continue

                visible_fields.append(field)

            # 3. Linear Hydration
            for index, field in enumerate(visible_fields[: self.MAX_OUTPUTS]):
                kind = str(field.get("kind") or "")
                raw_val = (
                    field.get("value")
                    if field.get("inline", True)
                    else field.get("ref")
                )

                if kind == "image_asset":
                    ref = field.get("ref")
                    if isinstance(ref, dict):
                        image_tensor, _ = _load_image_tensor_from_ref(
                            session, ref, zmongo_prefix=zmongo_prefix
                        )
                        output_values[index] = image_tensor
                    else:
                        output_values[index] = _empty_image_tensor()
                else:
                    # Unwrap primitives to prevent float/int cast crashes
                    val = self._unwrap_value(raw_val)

                    if kind == "number":
                        val_type = str(field.get("value_type") or "").lower()
                        if val_type == "int" or (
                            isinstance(val, int) and not isinstance(val, bool)
                        ):
                            try:
                                output_values[index] = int(val)
                            except:
                                output_values[index] = 0
                        else:
                            try:
                                output_values[index] = float(val)
                            except:
                                output_values[index] = 0.0

                    elif kind == "boolean":
                        output_values[index] = _safe_bool(val, False)

                    elif kind in {"text", "metadata"}:
                        if val is None:
                            val = field.get("summary") or field.get("ref") or ""
                        output_values[index] = (
                            _json_text(val)
                            if isinstance(val, (dict, list))
                            else str(val)
                        )

                    else:
                        # Fallback for ANY or JSON types
                        output_values[index] = val

            return tuple(output_values)

        except Exception as exc:
            print(f"[ComfyUI-ZMongo] Dynamic content-pack hydration failed: {exc}")
            return tuple(output_values)


# -----------------------------------------------------------------------------
# ComfyUI mappings
# -----------------------------------------------------------------------------

NODE_CLASS_MAPPINGS = {
    "ZMongoContentPackFromDocumentJSONNode": ZMongoContentPackFromDocumentJSONNode,
    "ZMongoContentPackFieldSelectorNode": ZMongoContentPackFieldSelectorNode,
    "ZMongoContentPackSaveNode": ZMongoContentPackSaveNode,
    "ZMongoContentPackLoadNode": ZMongoContentPackLoadNode,
    "ZMongoDynamicContentPackOutputs": ZMongoDynamicContentPackOutputs,
}

NODE_DISPLAY_NAME_MAPPINGS = {
    "ZMongoContentPackFromDocumentJSONNode": "08 Content Pack From Document JSON",
    "ZMongoContentPackFieldSelectorNode": "08 Content Pack Field Selector",
    "ZMongoContentPackSaveNode": "08 Save Content Pack",
    "ZMongoContentPackLoadNode": "08 Load Content Pack",
    "ZMongoDynamicContentPackOutputs": "08 Dynamic Content Pack Outputs",
}

__all__ = [
    "NODE_CLASS_MAPPINGS",
    "NODE_DISPLAY_NAME_MAPPINGS",
]
