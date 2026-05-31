from __future__ import annotations

# pylint: disable=too-many-lines,too-many-locals,too-many-branches,too-many-statements,too-many-return-statements,broad-exception-caught,import-error,no-name-in-module,unused-argument,protected-access,line-too-long,missing-module-docstring,missing-class-docstring,missing-function-docstring,invalid-name

import base64
import io
import json
import mimetypes
import os
import re
import time
import urllib.parse
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

try:
    import requests
except Exception:  # pragma: no cover
    requests = None  # type: ignore[assignment]

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

try:
    from dotenv import load_dotenv
except Exception:  # pragma: no cover
    def load_dotenv(*args: Any, **kwargs: Any) -> bool:
        return False

try:
    import folder_paths  # type: ignore
except Exception:  # pragma: no cover
    folder_paths = None  # type: ignore[assignment]

ENV_PATH1 = Path.home() / ".resources" / ".env"
ENV_PATH2 = Path.home() / ".resources" / ".secrets"
load_dotenv(ENV_PATH1)
load_dotenv(ENV_PATH2)

DEFAULT_BASE_URL = os.getenv("BPA_BASE_URL", os.getenv("ZMONGO_BASE_URL", os.getenv("ZTAROT_BASE_URL",
                                                                                    "https://businessprocessapplications.com"))).rstrip(
    "/")
DEFAULT_TIMEOUT = int(
    os.getenv("BPA_TIMEOUT_SECONDS", os.getenv("ZMONGO_TIMEOUT_SECONDS", os.getenv("ZTAROT_TIMEOUT_SECONDS", "30"))))
DEFAULT_COMFY_ZMONGO_PREFIX = os.getenv("COMFY_ZMONGO_API_PREFIX", "/comfy-zmongo").strip().rstrip(
    "/") or "/comfy-zmongo"
DEFAULT_GEMINI_PREFIX = os.getenv("GEMINI_API_PREFIX", "/gemini").strip().rstrip("/") or "/gemini"
DEFAULT_FLEET_PREFIX = os.getenv("ZFLEET_API_PREFIX", "/fleet").strip().rstrip("/") or "/fleet"
DEFAULT_COMFY_ZMONGO_FLEET_PREFIX = os.getenv("COMFY_ZMONGO_FLEET_PREFIX", "/comfy-zmongo-fleet").strip().rstrip(
    "/") or "/comfy-zmongo-fleet"
DEFAULT_DOCUMENT_PREFIX = os.getenv("ZMONGO_DOCUMENT_API_PREFIX", DEFAULT_COMFY_ZMONGO_PREFIX).strip().rstrip(
    "/") or DEFAULT_COMFY_ZMONGO_PREFIX
DEFAULT_MAX_UPLOAD_BYTES = int(os.getenv("ZMONGO_MAX_UPLOAD_BYTES", str(50 * 1024 * 1024)))
DEFAULT_LOCAL_MAX_VALUE_BYTES = int(os.getenv("ZMONGO_LOCAL_MAX_VALUE_BYTES", str(256 * 1024)))
DEFAULT_LOCAL_MAX_DOCUMENT_BYTES = int(os.getenv("ZMONGO_LOCAL_MAX_DOCUMENT_BYTES", str(1024 * 1024)))

DOCUMENT_FILE_EXTENSIONS = tuple(
    ext.strip().lower()
    for ext in os.getenv("ZMONGO_DOCUMENT_NODE_EXTENSIONS", ".pdf,.txt,.md,.docx,.rtf,.csv,.json").split(",")
    if ext.strip()
)

IMMUTABLE_DOCUMENT_FIELD_PATHS = {
    "_id", "id", "document_id", "system_id", "uuid", "owner", "username", "created_by", "created_at",
}

SELECTABLE_RETURN_TYPES = ("*", "STRING", "INT")
SELECTABLE_RETURN_NAMES = ("selectable_items", "indexed_items", "item_count")
SELECTABLE_OUTPUT_IS_LIST = (True, False, False)

ZMONGO_DOCUMENT_ID = "ZMONGO_DOCUMENT_ID"
ZMONGO_FIELD_PATH = "ZMONGO_FIELD_PATH"
ZMONGO_FILE_PATH = "ZMONGO_FILE_PATH"
ZMONGO_FILENAME = "ZMONGO_FILENAME"
ZMONGO_TEXT = "ZMONGO_TEXT"
ZMONGO_STATUS = "ZMONGO_STATUS"


# -----------------------------------------------------------------------------
# Generic helpers for ZMongo API and session access
# -----------------------------------------------------------------------------

import json
from typing import Any

# Safely ensure any payload is a dict
def _ensure_payload_dict(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return value
    return {"success": False, "message": str(value), "data": value}

# Extract the main document from a response payload
def _extract_document_from_response(payload: Any) -> dict[str, Any]:
    if not isinstance(payload, dict):
        return {}
    data = payload.get("data")
    if isinstance(data, dict):
        if "document" in data and isinstance(data["document"], dict):
            return data["document"]
        if "documents" in data and isinstance(data["documents"], list) and data["documents"]:
            first_doc = data["documents"][0]
            if isinstance(first_doc, dict):
                return first_doc
    return {}

# Fallback GET document via session API request
def _session_get_doc(session: Any, collection_name: str, document_id: str, cache: bool = True) -> dict[str, Any]:
    """Fetch a single document using the session object."""
    if hasattr(session, "get_doc") and callable(session.get_doc):
        try:
            payload = session.get_doc(collection=collection_name, document_id=document_id, cache=cache)
            return _ensure_payload_dict(payload)
        except Exception:
            pass
    return {}

# Fallback generic API request helper
def _session_api_request(session: Any, method: str, path: str, json_body: dict[str, Any] | None = None) -> dict[str, Any]:
    """Perform a raw API request through a session object."""
    if hasattr(session, "api_request") and callable(session.api_request):
        try:
            payload = session.api_request(method, path, json=json_body)
            return _ensure_payload_dict(payload)
        except Exception:
            pass
    return {"success": False, "message": "API request failed", "data": None}


def _clean_field_path(value: str, default: str = "image_data") -> str:
    cleaned = (value or "").strip().strip(".")
    return cleaned or default


def _legacy_data_path(field_path: str) -> str:
    cleaned = _clean_field_path(field_path)
    return cleaned if cleaned.endswith(".data") else f"{cleaned}.data"


def _strip_known_image_leaf(field_path: str, default: str = "image_data") -> str:
    """Return the public image field, not an internal envelope member.

    ZMongo byte envelopes are normally stored as:
        image_data = {"__type__": "bytes", "encoding": "base64", "data": "..."}

    A user may accidentally type image_data.data after inspecting the JSON, but
    the backend route must receive image_data so it can decode the envelope.
    """
    cleaned = _clean_field_path(field_path, default)
    known_leafs = ("data", "base64", "b64", "bytes", "image", "content", "payload", "url")
    parts = [part for part in cleaned.split(".") if part]

    if len(parts) > 1 and parts[-1] in known_leafs:
        return ".".join(parts[:-1]) or default

    return cleaned


def _route_image_field_path(field_path: str, default: str = "image_data") -> str:
    """Field path to send to server image routes.

    Never send image_data.data as the preferred route field.  The .data member
    is part of the ZMongo binary envelope and should be decoded by the route,
    not treated as the public image field.
    """
    return _strip_known_image_leaf(field_path, default)


def _image_field_candidates(field_path: str, default: str = "image_data") -> list[str]:
    """
    Return the old working read candidates in priority order.

    The save node and display node must agree on the same public field path.
    For ZMongo byte envelopes, the image is stored at the field itself:
        image_data = {"__type__": "bytes", "encoding": "base64", "data": "..."}

    The legacy '<field>.data' path is only a fallback for older documents or
    direct base64 members. Do not use it as the preferred public route field.
    """
    exact = _clean_field_path(field_path, default)
    public_field = _strip_known_image_leaf(exact, default)
    legacy = _legacy_data_path(public_field)
    candidates = [public_field]
    if exact != public_field:
        candidates.append(exact)
    candidates.append(legacy)
    return list(dict.fromkeys(path for path in candidates if path))


def _empty_comfy_image(width: int = 1, height: int = 1) -> torch.Tensor:
    return torch.zeros((1, max(1, height), max(1, width), 3), dtype=torch.float32)


def _raise_display_image_error(message: str) -> None:
    # PreviewImage makes a zero tensor look like a legitimate black image.
    # Raising keeps route/field errors visible in ComfyUI instead of hiding them.
    raise RuntimeError(message)


def _pil_to_comfy_image(image: Image.Image) -> torch.Tensor:
    image = ImageOps.exif_transpose(image).convert("RGB")
    np_image = np.asarray(image).astype(np.float32) / 255.0
    return torch.from_numpy(np_image)[None,]


def _decode_base64_text(value: str) -> bytes:
    stripped = value.strip()
    if stripped.startswith("data:") and "," in stripped:
        stripped = stripped.split(",", 1)[1]
    stripped = stripped.strip()
    return base64.b64decode(stripped, validate=False)


def _decode_image_bytes_from_value(value: Any) -> bytes:
    if value is None:
        raise ValueError("Image field value is empty.")

    if isinstance(value, bytes):
        return value

    if isinstance(value, bytearray):
        return bytes(value)

    if isinstance(value, str):
        return _decode_base64_text(value)

    if isinstance(value, list):
        if not value:
            raise ValueError("Image list is empty.")
        return _decode_image_bytes_from_value(value[0])

    if isinstance(value, dict):
        if value.get("__type__") == "bytes" and value.get("encoding") == "base64" and value.get("data"):
            return _decode_base64_text(str(value["data"]))

        for key in ("data", "image_data", "base64", "b64", "bytes", "image", "content", "payload"):
            if key in value:
                return _decode_image_bytes_from_value(value[key])

        if "$binary" in value:
            binary_value = value["$binary"]
            if isinstance(binary_value, dict) and "base64" in binary_value:
                return _decode_base64_text(str(binary_value["base64"]))
            return _decode_image_bytes_from_value(binary_value)

    raise ValueError(f"Unsupported image field type: {type(value).__name__}")


def _is_probable_url(value: Any) -> bool:
    if not isinstance(value, str):
        return False
    text = value.strip().lower()
    return text.startswith("http://") or text.startswith("https://") or text.startswith("/")


def _image_url_from_value(value: Any) -> str:
    if isinstance(value, str):
        stripped = value.strip()
        if stripped.startswith(("http://", "https://", "/")):
            return stripped

    if isinstance(value, dict):
        for key in ("url", "view_url", "download_url", "preview_url", "href", "src"):
            candidate = value.get(key)
            if isinstance(candidate, str) and candidate.strip():
                return candidate.strip()

    return ""


def _find_first_bytes_envelope(node: Any, path: str = "") -> tuple[str, dict[str, Any]] | None:
    if isinstance(node, dict):
        if node.get("__type__") == "bytes" and node.get("data"):
            return path or "root", node
        for key, value in node.items():
            found = _find_first_bytes_envelope(value, f"{path}.{key}" if path else str(key))
            if found:
                return found
    elif isinstance(node, list):
        for index, value in enumerate(node):
            found = _find_first_bytes_envelope(value, f"{path}.{index}" if path else str(index))
            if found:
                return found
    return None


def _document_key_summary(document: Any) -> dict[str, Any]:
    if not isinstance(document, dict):
        return {"type": type(document).__name__}

    keys = sorted(str(key) for key in document.keys())
    field_types = {str(key): type(value).__name__ for key, value in document.items()}
    likely_refs: dict[str, Any] = {}
    for key in (
        "_id", "file_id", "image_id", "asset_id", "gridfs_id", "blob_id",
        "filename", "path", "filepath", "file_path", "url", "view_url",
        "download_url", "preview_url", "r2_key", "object_key", "collection",
    ):
        if key in document:
            likely_refs[key] = document.get(key)

    return {
        "top_level_keys": keys,
        "field_types": field_types,
        "likely_image_reference_fields": likely_refs,
    }


def _string_value_from_path(document: dict[str, Any], path: str) -> str:
    value = safe_get_by_path(document, path)
    if value is None:
        return ""
    if isinstance(value, str):
        return value.strip()
    return str(value).strip()


def _image_reference_candidates(document: dict[str, Any]) -> list[dict[str, str]]:
    """Return image/file-id references found in metadata-only documents.

    Some workflows save an image document as metadata or a pointer to the
    encrypted vault file, not as an inline image_data envelope.  This lets the
    display node follow those pointers before failing.
    """
    if not isinstance(document, dict):
        return []

    candidates: list[dict[str, str]] = []

    pointer_paths = (
        "file_id", "image_id", "asset_id", "gridfs_id", "blob_id",
        "payload.file_id", "payload.image_id", "payload.asset_id",
        "metadata.file_id", "metadata.image_id", "metadata.asset_id",
        "result.file_id", "data.file_id",
    )
    for path in pointer_paths:
        value = _string_value_from_path(document, path)
        if value:
            candidates.append({"collection": "zai_fleet_files", "document_id": value, "field_path": "image_data", "source": path})

    # If the selected document is itself a GridFS/zai_fleet_files metadata
    # record, its _id is the image id even if the selected collection was
    # named images by an older workflow.
    looks_like_file_doc = any(key in document for key in ("filename", "length", "chunkSize", "uploadDate"))
    object_id = _string_value_from_path(document, "_id")
    if looks_like_file_doc and object_id:
        candidates.append({"collection": "zai_fleet_files", "document_id": object_id, "field_path": "image_data", "source": "metadata._id"})

    # Absolute/relative URLs can be loaded directly by the node.
    for path in ("url", "view_url", "download_url", "preview_url", "payload.url", "metadata.url"):
        value = _string_value_from_path(document, path)
        if value.startswith(("http://", "https://", "/")):
            candidates.append({"collection": "", "document_id": value, "field_path": "", "source": path})

    deduped: list[dict[str, str]] = []
    seen: set[tuple[str, str, str]] = set()
    for item in candidates:
        key = (item.get("collection", ""), item.get("document_id", ""), item.get("field_path", ""))
        if key not in seen:
            seen.add(key)
            deduped.append(item)
    return deduped


def dirty_token(*parts: Any) -> str:
    prefix = ":".join(str(part) for part in parts if part is not None)
    return f"{prefix}:{time.time_ns()}:{uuid.uuid4().hex}" if prefix else f"{time.time_ns()}:{uuid.uuid4().hex}"


def json_text(value: Any) -> str:
    return json.dumps(value, indent=2, ensure_ascii=False, default=str)


def success_payload(message: str, data: Optional[dict[str, Any]] = None, *, status_code: int = 200) -> dict[str, Any]:
    return {"success": True, "message": message, "data": data or {}, "error": None, "status_code": int(status_code)}


def error_payload(
        message: str,
        *,
        data: Optional[dict[str, Any]] = None,
        status_code: int = 0,
        error_type: str = "Error",
) -> dict[str, Any]:
    return {
        "success": False,
        "message": message,
        "data": data or {},
        "error": {"type": error_type, "msg": message},
        "status_code": int(status_code),
    }


def ensure_payload_dict(payload: Any) -> dict[str, Any]:
    if isinstance(payload, dict):
        result = dict(payload)
    else:
        result = {
            "success": False,
            "message": "Payload was not a JSON object.",
            "data": payload,
            "error": {"msg": f"Unexpected payload type: {type(payload).__name__}"},
            "status_code": 0,
        }
    result.setdefault("success", False)
    result.setdefault("message", "OK" if result.get("success") else "")
    result.setdefault("data", {})
    result.setdefault("error", None)
    result.setdefault("status_code", 0)
    return result


def normalize_base_url(raw_base_url: str) -> str:
    base = (raw_base_url or DEFAULT_BASE_URL).strip().rstrip("/")
    suffixes = (
        "/api/comfy-zmongo", "/comfy_zmongo", "/comfy-zmongo", "/gemini",
        "/user/manager/api", "/user/manager", "/user/api-manager",
        "/comfy-zmongo-fleet", "/fleet", "/api/fleet",
        "/user/login", "/user/dashboard", "/user/settings", "/user/profile", "/user",
    )
    changed = True
    while changed:
        changed = False
        for suffix in suffixes:
            if base.endswith(suffix):
                base = base[: -len(suffix)].rstrip("/")
                changed = True
    return base or DEFAULT_BASE_URL


def clean_prefix(value: str, default: str = DEFAULT_COMFY_ZMONGO_PREFIX) -> str:
    cleaned = (value or default).strip().rstrip("/")
    if not cleaned.startswith("/"):
        cleaned = f"/{cleaned}"
    return cleaned or default


def clean_scalar(value: Any) -> str:
    if isinstance(value, (list, tuple)):
        value = value[0] if value else ""
    if value is None:
        return ""
    text = str(value).strip()
    for _ in range(6):
        before = text
        if text.startswith("[") and text.endswith("]"):
            text = text[1:-1].strip()
        if text.startswith("(") and text.endswith(")"):
            text = text[1:-1].strip()
        if text.endswith(","):
            text = text[:-1].strip()
        if len(text) >= 2 and text[0] == text[-1] and text[0] in {"'", '"'}:
            text = text[1:-1].strip()
        if text == before:
            break
    return text.strip()


def strip_index_prefix(value: Any) -> str:
    text = clean_scalar(value)
    match = re.match(r"^\s*\d+\s*:\s*(.+?)\s*$", text, flags=re.DOTALL)
    return match.group(1).strip() if match else text


def dedupe_strings(values: Any) -> list[str]:
    cleaned: list[str] = []
    seen: set[str] = set()
    for value in values or []:
        if value is None:
            continue
        text = str(value).strip()
        if not text or text in seen:
            continue
        seen.add(text)
        cleaned.append(text)
    return cleaned


def as_bool(value: Any, default: bool = False) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes", "y", "on"}
    return default


def as_comfy_list(values: Any) -> list[Any]:
    if values is None:
        return []
    if isinstance(values, list):
        return values
    if isinstance(values, tuple):
        return list(values)
    return [values]


def indexed_list_text(values: list[Any]) -> str:
    return json_text([f"{index}: {value}" for index, value in enumerate(values)])


def selectable_tail(items: list[Any]) -> tuple[list[str], str, int]:
    cleaned = dedupe_strings(items)
    return as_comfy_list(cleaned), indexed_list_text(cleaned), len(cleaned)


def parse_json_object(value: Any, field_name: str = "json") -> dict[str, Any]:
    text = clean_scalar(value)
    if not text:
        return {}
    parsed = json.loads(text)
    if not isinstance(parsed, dict):
        raise ValueError(f"{field_name} must be a JSON object.")
    return parsed


def parse_json_list(value: Any, field_name: str = "json") -> list[Any]:
    text = strip_index_prefix(value)
    if not text:
        return []

    def default_for_field() -> list[Any]:
        return [["updated_at", -1]] if field_name == "sort_json" else []

    def parse_tags_fragment(fragment: str) -> list[str]:
        fragment = strip_index_prefix(fragment).strip()
        if not fragment:
            return []
        double_quote = chr(34)
        single_quote = chr(39)
        candidates = [fragment]
        if not fragment.startswith("["):
            candidates.append("[" + fragment + "]")
        if not fragment.startswith((double_quote, single_quote, "[")) and (
                double_quote in fragment or single_quote in fragment):
            candidates.append("[" + double_quote + fragment + double_quote + "]")
        for candidate in candidates:
            try:
                parsed_candidate = json.loads(candidate)
                if isinstance(parsed_candidate, list):
                    return [str(item).strip() for item in parsed_candidate if str(item).strip()]
                if isinstance(parsed_candidate, str) and parsed_candidate.strip():
                    return [parsed_candidate.strip()]
            except Exception:
                continue
        if "," in fragment:
            return [part.strip().strip(double_quote).strip(single_quote).strip() for part in fragment.split(",") if
                    part.strip().strip(double_quote).strip(single_quote).strip()]
        item = fragment.strip().strip(double_quote).strip(single_quote).strip()
        return [item] if item else []

    try:
        parsed = json.loads(text)
    except json.JSONDecodeError:
        first_line = next((line.strip() for line in text.splitlines() if line.strip()), "")
        parsed = None
        if first_line and first_line != text:
            try:
                parsed = json.loads(strip_index_prefix(first_line))
            except Exception:
                parsed = None
        if parsed is None:
            return parse_tags_fragment(text) if field_name == "tags_json" else default_for_field()

    if not isinstance(parsed, list):
        if field_name == "tags_json" and isinstance(parsed, str):
            return parse_tags_fragment(parsed)
        return default_for_field()
    return parsed


def parse_any_json(value: Any, parse_json: bool = True) -> Any:
    if not parse_json:
        return clean_scalar(value)
    text = clean_scalar(value)
    if not text:
        return ""
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        return text


def safe_get_by_path(obj: Any, path: str, default: Any = None) -> Any:
    if not path:
        return obj
    current = obj
    for part in str(path).split("."):
        if part == "":
            continue
        if isinstance(current, dict):
            if part in current:
                current = current[part]
            else:
                return default
        elif isinstance(current, list) and part.isdigit():
            index = int(part)
            if 0 <= index < len(current):
                current = current[index]
            else:
                return default
        else:
            return default
    return current


def set_by_path(obj: dict[str, Any], path: str, value: Any) -> None:
    cleaned = clean_scalar(path)
    if not cleaned:
        raise ValueError("field_path is required.")
    parts = [part for part in cleaned.split(".") if part]
    current: Any = obj
    for part in parts[:-1]:
        if not isinstance(current, dict):
            raise ValueError(f"Cannot set nested path through non-object: {part}")
        if part not in current or not isinstance(current[part], dict):
            current[part] = {}
        current = current[part]
    if not isinstance(current, dict):
        raise ValueError(f"Cannot set field path on non-object parent: {cleaned}")
    current[parts[-1]] = value


def flatten_document_paths(value: Any, *, parent_key: str = "") -> list[str]:
    paths: list[str] = []
    if isinstance(value, dict):
        if not value and parent_key:
            return [parent_key]
        for key, child in value.items():
            child_key = f"{parent_key}.{key}" if parent_key else str(key)
            paths.extend(flatten_document_paths(child, parent_key=child_key))
        return paths
    if isinstance(value, list):
        if not value and parent_key:
            return [parent_key]
        for index, child in enumerate(value):
            child_key = f"{parent_key}.{index}" if parent_key else str(index)
            paths.extend(flatten_document_paths(child, parent_key=child_key))
        return paths
    return [parent_key] if parent_key else []


def flatten_path_keys(
        value: Any,
        *,
        parent_key: str = "",
        sep: str = ".",
        include_leaf_values: bool = False,
        max_value_preview: int = 120,
) -> dict[str, Any]:
    flat: dict[str, Any] = {}
    if isinstance(value, dict) and value.get("__type__") == "bytes":
        flat[parent_key or "root"] = {
            "type": "bytes_envelope",
            "size_bytes": value.get("size_bytes"),
            "encoding": value.get("encoding"),
        } if include_leaf_values else None
        return flat
    if isinstance(value, dict):
        if not value and parent_key:
            flat[parent_key] = {} if include_leaf_values else None
            return flat
        for key, child in value.items():
            child_key = f"{parent_key}{sep}{key}" if parent_key else str(key)
            flat.update(flatten_path_keys(child, parent_key=child_key, sep=sep, include_leaf_values=include_leaf_values,
                                          max_value_preview=max_value_preview))
        return flat
    if isinstance(value, list):
        if not value and parent_key:
            flat[parent_key] = [] if include_leaf_values else None
            return flat
        for index, child in enumerate(value):
            child_key = f"{parent_key}{sep}{index}" if parent_key else str(index)
            flat.update(flatten_path_keys(child, parent_key=child_key, sep=sep, include_leaf_values=include_leaf_values,
                                          max_value_preview=max_value_preview))
        return flat
    if parent_key:
        if include_leaf_values:
            if isinstance(value, str):
                preview = value[:max_value_preview]
                flat[parent_key] = preview + ("..." if len(value) > max_value_preview else "")
            else:
                flat[parent_key] = value
        else:
            flat[parent_key] = None
    return flat


def coerce_document_id(value: Any) -> str:
    if isinstance(value, dict):
        for key in ("_id", "document_id", "system_id", "id", "uuid", "inserted_id"):
            item = value.get(key)
            if item:
                return clean_scalar(item)
        return ""
    if isinstance(value, (list, tuple)):
        return coerce_document_id(value[0]) if value else ""
    text = strip_index_prefix(value)
    if not text:
        return ""
    try:
        parsed = json.loads(text)
    except Exception:
        return clean_scalar(text)
    return coerce_document_id(parsed) if isinstance(parsed, (dict, list, tuple)) else clean_scalar(parsed)


def coerce_field_path(value: Any) -> str:
    text = strip_index_prefix(value)
    if not text:
        return ""
    try:
        parsed = json.loads(text)
        if isinstance(parsed, str):
            return parsed.strip().strip(".")
    except Exception:
        pass
    return text.strip().strip(".")


def looks_like_document_id(value: Any) -> bool:
    text = clean_scalar(value)
    return bool(text and (re.fullmatch(r"[0-9a-fA-F]{24}", text) or re.fullmatch(
        r"[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}", text)))


def coerce_document_id_link(value: Any) -> str:
    if isinstance(value, dict):
        return coerce_document_id(value)
    if isinstance(value, (list, tuple)):
        return coerce_document_id_link(value[0]) if value else ""
    text = strip_index_prefix(value)
    if not text:
        return ""
    try:
        parsed = json.loads(text)
    except Exception:
        return text if looks_like_document_id(text) else ""
    if isinstance(parsed, dict):
        return coerce_document_id(parsed)
    if isinstance(parsed, list):
        return coerce_document_id_link(parsed[0]) if parsed else ""
    if isinstance(parsed, str):
        return parsed.strip() if looks_like_document_id(parsed) else ""
    return ""


def coerce_file_path_link(value: Any) -> str:
    text = strip_index_prefix(value)
    if not text or text in {"_id", "id", "document_id", "system_id", "filename", "text"}:
        return ""
    expanded = os.path.expanduser(text)
    if "/" in expanded or "\\" in expanded:
        return expanded
    return expanded if Path(expanded).suffix.lower() in DOCUMENT_FILE_EXTENSIONS else ""


def coerce_filename_link(value: Any) -> str:
    text = strip_index_prefix(value)
    if not text:
        return ""
    if "/" in text or "\\" in text:
        return Path(text).name
    return text if Path(text).suffix.lower() in DOCUMENT_FILE_EXTENSIONS else ""


def coerce_text_link(value: Any) -> str:
    text = strip_index_prefix(value)
    if not text or coerce_document_id_link(text) or coerce_file_path_link(text):
        return ""
    if re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*(\.[A-Za-z0-9_]+)*", text):
        return ""
    return text


def is_immutable_document_field_path(field_path: Any) -> bool:
    path = coerce_field_path(field_path)
    if not path:
        return False
    root = path.split(".", 1)[0].strip()
    return path in IMMUTABLE_DOCUMENT_FIELD_PATHS or root == "_id"


def blocked_immutable_field_payload(*, document_id: str, field_path: str, refresh: str) -> dict[str, Any]:
    return ensure_payload_dict({
        "success": False,
        "message": f"Save skipped. The field path {field_path!r} is immutable/protected and cannot be updated. Select a writable field path such as metadata.note, metadata.review_status, title, status, or text.",
        "data": {
            "document_id": document_id,
            "field_path": field_path,
            "blocked": True,
            "blocked_reason": "immutable_or_protected_field",
            "safe_examples": ["metadata.note", "metadata.review_status", "metadata.workflow_review", "title", "status",
                              "text"],
            "refresh": refresh,
        },
        "error": {"type": "ImmutableFieldPath",
                  "msg": f"Refusing to update immutable/protected field path {field_path!r}."},
        "status_code": 400,
    })


def extract_data(payload: dict[str, Any]) -> dict[str, Any]:
    data = payload.get("data") if isinstance(payload, dict) else None
    return data if isinstance(data, dict) else {}


def extract_documents(payload: dict[str, Any]) -> list[dict[str, Any]]:
    data = payload.get("data", {}) if isinstance(payload, dict) else {}
    if isinstance(data, dict):
        documents = data.get("documents")
        if isinstance(documents, list):
            return [item for item in documents if isinstance(item, dict)]
        document = data.get("document")
        if isinstance(document, dict):
            return [document]
        if isinstance(data.get("results"), list):
            return [item for item in data["results"] if isinstance(item, dict)]
        if "_id" in data or "document_id" in data or "system_id" in data:
            return [data]
    if isinstance(data, list):
        return [item for item in data if isinstance(item, dict)]
    return []


def extract_document(payload: dict[str, Any]) -> dict[str, Any]:
    docs = extract_documents(payload)
    return docs[0] if docs else {}


def extract_doc_id(payload: dict[str, Any]) -> str:
    data = extract_data(payload)
    for key in ("document_id", "inserted_id", "_id", "id", "system_id", "uuid"):
        value = data.get(key)
        if value:
            return coerce_document_id(value)
    return coerce_document_id(extract_document(payload))


def extract_doc_ids_from_documents(docs: list[dict[str, Any]]) -> list[str]:
    return dedupe_strings(coerce_document_id(doc) for doc in docs)


def extract_doc_ids(payload: dict[str, Any]) -> list[str]:
    return extract_doc_ids_from_documents(extract_documents(payload))


def extract_filenames_from_documents(docs: list[dict[str, Any]]) -> list[str]:
    names: list[str] = []
    for doc in docs:
        for key in ("filename", "original_name", "title"):
            value = doc.get(key)
            if value:
                names.append(str(value))
                break
    return dedupe_strings(names)


def extract_collections(payload: dict[str, Any]) -> list[str]:
    data = payload.get("data", {}) if isinstance(payload, dict) else {}
    if isinstance(data, dict):
        collections = data.get("collections") or data.get("collection_names")
        if isinstance(collections, list):
            return [str(item) for item in collections]
    if isinstance(data, list):
        return [str(item) for item in data]
    return []


def extract_count(payload: dict[str, Any]) -> int:
    data = payload.get("data", {}) if isinstance(payload, dict) else {}
    if isinstance(data, dict):
        for key in ("count", "document_count", "total", "matched_count", "modified_count", "deleted_count"):
            if key in data:
                try:
                    return int(data.get(key) or 0)
                except Exception:
                    return 0
    return 0


def extract_text(payload: dict[str, Any]) -> str:
    data = extract_data(payload)
    if "text" in data:
        return str(data.get("text") or "")
    doc = extract_document(payload)
    if "text" in doc:
        return str(doc.get("text") or "")
    return ""


def extract_field_paths(payload: dict[str, Any]) -> list[str]:
    data = extract_data(payload)
    for key in ("field_paths", "paths"):
        paths = data.get(key)
        if isinstance(paths, list):
            return dedupe_strings(str(item) for item in paths if str(item).strip())
    return []


def extract_text_from_gemini_payload(payload: dict[str, Any]) -> str:
    data = payload.get("data", {}) if isinstance(payload, dict) else {}
    if isinstance(data, dict):
        for key in ("text", "response_text", "output", "content"):
            value = data.get(key)
            if value is not None:
                return str(value)
        response = data.get("response") or data.get("raw")
        if isinstance(response, dict):
            for key in ("text", "output", "content"):
                value = response.get(key)
                if value is not None:
                    return str(value)
    return ""


def extract_models_from_payload(payload: dict[str, Any]) -> list[str]:
    data = payload.get("data", {}) if isinstance(payload, dict) else {}
    candidates: Any = []
    if isinstance(data, dict):
        candidates = data.get("models") or data.get("items") or data.get("results") or []
    elif isinstance(data, list):
        candidates = data
    models: list[str] = []
    if isinstance(candidates, list):
        for item in candidates:
            if isinstance(item, str):
                models.append(item)
            elif isinstance(item, dict):
                name = item.get("name") or item.get("model") or item.get("id")
                if name:
                    models.append(str(name))
    return models


def value_items(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, list):
        return dedupe_strings(json_text(item) if isinstance(item, (dict, list, tuple)) else str(item) for item in value)
    if isinstance(value, tuple):
        return value_items(list(value))
    if isinstance(value, dict):
        return [json_text(value)]
    return dedupe_strings([str(value)])


def document_summary(document: dict[str, Any]) -> str:
    if not isinstance(document, dict) or not document:
        return ""
    keys = sorted(str(k) for k in document.keys())
    doc_id = coerce_document_id(document)
    filename = document.get("filename") or document.get("original_name") or ""
    text = str(document.get("text") or "")
    return json_text({"document_id": doc_id, "filename": filename, "text_length": len(text), "top_level_keys": keys})


def prefer_link_value(primary: Any, link_value: Any) -> Any:
    if link_value is None:
        return primary
    if isinstance(link_value, (list, tuple)):
        if not link_value:
            return primary
        first = link_value[0]
        return first if clean_scalar(first) else primary
    if isinstance(link_value, dict):
        return link_value if link_value else primary
    linked = clean_scalar(link_value)
    return link_value if linked else primary


def raise_if_failed(payload: dict[str, Any], action: str) -> None:
    if payload.get("success"):
        return
    message = payload.get("message") or payload.get("error") or f"{action} failed."
    raise RuntimeError(f"{action} failed: {message}\n{json_text(payload)}")


def get_comfy_input_directory() -> Path:
    if folder_paths is not None:
        get_input_directory = getattr(folder_paths, "get_input_directory", None)
        if callable(get_input_directory):
            return Path(get_input_directory()).expanduser().resolve()
        input_directory = getattr(folder_paths, "input_directory", None)
        if input_directory:
            return Path(input_directory).expanduser().resolve()
    return (Path.cwd() / "input").expanduser().resolve()


def get_default_documents_directory() -> Path:
    configured = os.getenv("ZMONGO_DOCUMENT_BROWSER_ROOT", "").strip()
    if configured:
        return Path(os.path.expanduser(configured)).resolve()
    documents_dir = (Path.home() / "Documents").expanduser().resolve()
    if documents_dir.exists() and documents_dir.is_dir():
        return documents_dir
    return get_comfy_input_directory()


def get_document_browser_root(root_mode: str = "Documents", custom_root: str = "") -> Path:
    if custom_root and str(custom_root).strip():
        return Path(os.path.expanduser(str(custom_root).strip())).resolve()
    mode = str(root_mode or "Documents").strip().lower()
    if mode in {"documents", "~/documents", "home_documents"}:
        return get_default_documents_directory()
    if mode in {"comfy input", "comfy_input", "input"}:
        return get_comfy_input_directory()
    return get_default_documents_directory()


def list_document_files(root_mode: str = "Documents", custom_root: str = "") -> list[str]:
    sentinel = "__NO_DOCUMENT_FILES_FOUND__"
    base = get_document_browser_root(root_mode=root_mode, custom_root=custom_root)
    if not base.exists() or not base.is_dir():
        return [sentinel]
    files: list[str] = [sentinel]
    for candidate in sorted(base.rglob("*")):
        if not candidate.is_file():
            continue
        if DOCUMENT_FILE_EXTENSIONS and candidate.suffix.lower() not in DOCUMENT_FILE_EXTENSIONS:
            continue
        try:
            files.append(candidate.relative_to(base).as_posix())
        except ValueError:
            files.append(candidate.name)
    return dedupe_strings(files)


def read_file_as_base64(path: str) -> tuple[str, str, str, int]:
    file_path = Path(os.path.expanduser(clean_scalar(path))).resolve()
    if not file_path.exists() or not file_path.is_file():
        raise FileNotFoundError(f"Document file not found: {file_path}")
    size_bytes = file_path.stat().st_size
    if size_bytes > DEFAULT_MAX_UPLOAD_BYTES:
        raise ValueError(
            f"File is too large for this node: {size_bytes} bytes. Limit is {DEFAULT_MAX_UPLOAD_BYTES} bytes.")
    content_type = mimetypes.guess_type(str(file_path))[0] or "application/octet-stream"
    return file_path.name, content_type, base64.b64encode(file_path.read_bytes()).decode("ascii"), size_bytes


def resolve_document_file_path(selected_file: str, manual_path: str = "", root_mode: str = "Documents",
                               custom_root: str = "") -> Path:
    manual = clean_scalar(manual_path)
    if manual:
        return Path(os.path.expanduser(manual)).resolve()
    selected = clean_scalar(selected_file)
    if not selected or selected == "__NO_DOCUMENT_FILES_FOUND__":
        raise FileNotFoundError(
            "No document file selected. Put the document in ~/Documents, set ZMONGO_DOCUMENT_BROWSER_ROOT, choose Comfy Input, or provide manual_path.")
    base = get_document_browser_root(root_mode=root_mode, custom_root=custom_root)
    candidate = (base / selected).resolve()
    try:
        candidate.relative_to(base)
    except ValueError as exc:
        raise PermissionError(f"Selected file escapes document browser root: {candidate}") from exc
    return candidate


def local_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def local_safe_name(value: Any, default: str = "documents") -> str:
    text = clean_scalar(value) or default
    result = "".join(char if (char.isalnum() or char in {"_", "-", "."}) else "_" for char in text).strip("._-")
    return result or default


def local_new_id() -> str:
    return f"local_{time.strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:8]}"


def matches_query(document: dict[str, Any], query: Optional[dict[str, Any]]) -> bool:
    if not query:
        return True
    if not isinstance(query, dict):
        return False
    for key, expected in query.items():
        if str(key).startswith("$"):
            continue
        actual = safe_get_by_path(document, str(key))
        if isinstance(expected, dict):
            if "$eq" in expected and actual != expected["$eq"]:
                return False
            if "$ne" in expected and actual == expected["$ne"]:
                return False
            if "$in" in expected and isinstance(expected["$in"], list) and actual not in expected["$in"]:
                return False
            if "$exists" in expected and bool(expected["$exists"]) != (actual is not None):
                return False
        elif actual != expected and str(actual) != str(expected):
            return False
    return True


def session_request(session: Any, method: str, prefix: str, path: str, *, json_body: Optional[dict[str, Any]] = None,
                    params: Optional[dict[str, Any]] = None) -> dict[str, Any]:
    if session is None:
        return error_payload("Missing ZMONGO_API_SESSION input.")
    prefix = clean_prefix(prefix)
    path = path if path.startswith("/") else f"/{path}"
    request_method = getattr(session, "request", None)
    if callable(request_method):
        return ensure_payload_dict(request_method(method, prefix, path, json_body=json_body, params=params))
    return error_payload("Session object does not expose request(method, prefix, path, ...).",
                         data={"session_type": type(session).__name__})


def session_api_request(
        session: Any,
        method: str,
        path: str,
        *,
        json_body: Optional[dict[str, Any]] = None,
        params: Optional[dict[str, Any]] = None,
        gemini_prefix: str = DEFAULT_GEMINI_PREFIX,
) -> dict[str, Any]:
    if session is None:
        return error_payload("No ZMongo API session provided.")
    prefix = clean_prefix(gemini_prefix, DEFAULT_GEMINI_PREFIX)
    clean_path = path if path.startswith("/") else f"/{path}"
    request_method = getattr(session, "request", None)
    if callable(request_method):
        try:
            return ensure_payload_dict(
                request_method(method.upper(), prefix, clean_path, json_body=json_body, params=params))
        except TypeError:
            pass
        try:
            kwargs: dict[str, Any] = {"json_body": json_body}
            if params:
                query = "&".join(f"{key}={value}" for key, value in params.items() if value is not None)
                old_path = f"{prefix}{clean_path}?{query}" if query else f"{prefix}{clean_path}"
            else:
                old_path = f"{prefix}{clean_path}"
            return ensure_payload_dict(request_method(method.upper(), old_path, **kwargs))
        except TypeError:
            pass
        except Exception as exc:
            return error_payload(f"Route request failed through session.request: {exc}")
    if requests is None:
        return error_payload("The requests package is not available for direct session fallback.")
    base_url = normalize_base_url(str(getattr(session, "base_url", DEFAULT_BASE_URL)))
    timeout = int(getattr(session, "timeout", DEFAULT_TIMEOUT) or DEFAULT_TIMEOUT)
    verify_tls = bool(getattr(session, "verify_tls", True))
    url = f"{base_url}{prefix}{clean_path}"
    headers = {"Accept": "application/json", "Content-Type": "application/json",
               "User-Agent": "comfyui-zmongo-generic-helpers/1.0"}
    api_key = str(getattr(session, "zai_api_key", "") or getattr(session, "api_key", "") or "").strip()
    username = str(getattr(session, "username", "") or "").strip()
    if api_key:
        headers["ZAI_API_KEY"] = api_key
    if username:
        headers["ZAI_USER"] = username
    bearer = str(getattr(session, "bearer_token", "") or getattr(session, "jwt_token", "") or "").strip()
    if bearer and "Authorization" not in headers:
        headers["Authorization"] = f"Bearer {bearer}"
    requests_session = getattr(session, "session", None)
    if not isinstance(requests_session, requests.Session):
        requests_session = requests.Session()
    try:
        response = requests_session.request(method=method.upper(), url=url, headers=headers, json=json_body,
                                            params=params, timeout=timeout, verify=verify_tls)
        try:
            payload = response.json()
        except ValueError:
            payload = {"success": response.ok,
                       "message": response.reason or ("OK" if response.ok else "Request failed"), "data": {},
                       "error": None if response.ok else {"msg": response.text[:1200]}, "raw_text": response.text}
        payload = ensure_payload_dict(payload)
        payload["status_code"] = response.status_code
        return payload
    except requests.RequestException as exc:
        return error_payload(f"Route request failed: {exc}")


def session_get_doc(session: Any, collection_name: str, document_id: str, cache: bool = False) -> dict[str, Any]:
    get_doc = getattr(session, "get_doc", None)
    if callable(get_doc):
        try:
            return ensure_payload_dict(
                get_doc(collection=(collection_name or "").strip(), document_id=(document_id or "").strip(),
                        cache=bool(cache)))
        except TypeError:
            pass
        try:
            return ensure_payload_dict(get_doc((collection_name or "").strip(), (document_id or "").strip()))
        except Exception as exc:
            return error_payload(f"get_doc failed: {exc}")
    return session_api_request(session, "GET", f"/api/doc/{collection_name}/{document_id}",
                               gemini_prefix=DEFAULT_COMFY_ZMONGO_PREFIX)


def session_save_value(
        session: Any,
        *,
        collection_name: str,
        document_id: str,
        query: Optional[dict[str, Any]],
        field_path: str,
        value: Any,
        upsert: bool,
) -> dict[str, Any]:
    save_value_by_query = getattr(session, "save_value_by_query", None)
    if callable(save_value_by_query):
        try:
            return ensure_payload_dict(save_value_by_query(collection_name=collection_name,
                                                           query=query or ({"_id": document_id} if document_id else {}),
                                                           field_path=field_path, value=value,
                                                           upsert_if_missing=upsert))
        except Exception as exc:
            return error_payload(f"save_value_by_query failed: {exc}")
    body: dict[str, Any] = {"collection": collection_name, "field_path": field_path, "value": value,
                            "upsert_if_missing": bool(upsert)}
    if document_id:
        body["document_id"] = document_id
    elif query:
        body["query"] = query
    else:
        return error_payload("Save requires document_id or query_json.")
    return session_api_request(session, "POST", "/api/save-value", json_body=body,
                               gemini_prefix=DEFAULT_COMFY_ZMONGO_PREFIX)


def comfy_image_to_png_bytes(frame_tensor: Any) -> bytes:
    if torch is None or np is None or Image is None:
        raise RuntimeError("torch, numpy, and Pillow are required to convert ComfyUI image tensors.")
    tensor = frame_tensor.detach().cpu().clamp(0.0, 1.0).numpy()
    np_image = (tensor * 255.0).round().astype(np.uint8)
    pil_image = Image.fromarray(np_image, mode="RGB")
    buffer = io.BytesIO()
    pil_image.save(buffer, format="PNG")
    return buffer.getvalue()


def build_binary_envelope(image_bytes: bytes, filename: str) -> dict[str, Any]:
    return {
        "__type__": "bytes",
        "encoding": "base64",
        "size_bytes": len(image_bytes),
        "data": base64.b64encode(image_bytes).decode("ascii"),
        "filename": filename,
        "content_type": "image/png",
        "storage_mode": "inline_zmongo_binary_envelope",
        "saved_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
    }


def extract_tensor_recursively(obj: Any) -> Any:
    if torch is not None and isinstance(obj, torch.Tensor):
        return obj
    if isinstance(obj, dict):
        for key in ("images", "image"):
            if key in obj:
                result = extract_tensor_recursively(obj[key])
                if result is not None:
                    return result
        for value in obj.values():
            result = extract_tensor_recursively(value)
            if result is not None:
                return result
    if isinstance(obj, (list, tuple)):
        for item in obj:
            result = extract_tensor_recursively(item)
            if result is not None:
                return result
    for attr in ("images", "image", "data"):
        if hasattr(obj, attr):
            try:
                result = extract_tensor_recursively(getattr(obj, attr))
                if result is not None:
                    return result
            except Exception:
                pass
    return None


class AlwaysDirtyMixin:
    @classmethod
    def IS_CHANGED(cls, *args: Any, **kwargs: Any) -> str:
        return dirty_token(cls.__name__)


# Backwards-compatible private aliases for existing ComfyUI node modules.
_dirty_token = dirty_token
_json_text = json_text
_success_payload = success_payload
_error_payload = error_payload
_ensure_payload_dict = ensure_payload_dict
_normalize_base_url = normalize_base_url
_clean_prefix = clean_prefix
_clean_scalar = clean_scalar
_clean_scalar_string = clean_scalar
_strip_index_prefix = strip_index_prefix
_dedupe_strings = dedupe_strings
_as_bool = as_bool
_as_comfy_list = as_comfy_list
_indexed_list_text = indexed_list_text
_selectable_tail = selectable_tail
_parse_json_object = parse_json_object
_parse_json_list = parse_json_list
_parse_any_json = parse_any_json
_safe_get_by_path = safe_get_by_path
_flatten_document_paths = flatten_document_paths
_flatten_path_keys = flatten_path_keys
_coerce_document_id = coerce_document_id
_coerce_field_path = coerce_field_path
_looks_like_document_id = looks_like_document_id
_coerce_document_id_link = coerce_document_id_link
_coerce_file_path_link = coerce_file_path_link
_coerce_filename_link = coerce_filename_link
_coerce_text_link = coerce_text_link
_is_immutable_document_field_path = is_immutable_document_field_path
_blocked_immutable_field_payload = blocked_immutable_field_payload
_extract_data = extract_data
_extract_documents = extract_documents
_extract_document = extract_document
_extract_document_from_payload = extract_document
_extract_doc_id = extract_doc_id
_extract_doc_ids = extract_doc_ids
_extract_doc_ids_from_documents = extract_doc_ids_from_documents
_extract_filenames_from_documents = extract_filenames_from_documents
_extract_collections = extract_collections
_extract_count = extract_count
_extract_text = extract_text
_extract_field_paths = extract_field_paths
_extract_text_from_gemini_payload = extract_text_from_gemini_payload
_extract_models_from_payload = extract_models_from_payload
_value_items = value_items
_document_summary = document_summary
_prefer_link_value = prefer_link_value
_raise_if_failed = raise_if_failed
_get_comfy_input_directory = get_comfy_input_directory
_get_default_documents_directory = get_default_documents_directory
_get_document_browser_root = get_document_browser_root
_list_document_files = list_document_files
_read_file_as_base64 = read_file_as_base64
_resolve_document_file_path = resolve_document_file_path
_session_request = session_request
_session_save_value = session_save_value
_comfy_image_to_png_bytes = comfy_image_to_png_bytes
_build_binary_envelope = build_binary_envelope
_extract_tensor_recursively = extract_tensor_recursively
_local_now_iso = local_now_iso
_local_clean_scalar = clean_scalar
_local_safe_name = local_safe_name
_local_new_id = local_new_id
_local_get_by_path = safe_get_by_path
_local_set_by_path = set_by_path
_local_matches_query = matches_query
_local_payload_ok = success_payload
_local_payload_error = lambda message, data=None, status_code=400, error_type="LocalFileStoreError": error_payload(
    message, data=data, status_code=status_code, error_type=error_type)


def _local_json_size_bytes(value: Any) -> int:
    return len(json.dumps(value, ensure_ascii=False, default=str).encode("utf-8"))


def _local_limit_payload(kind: str, size_bytes: int, max_bytes: int, extra: Optional[dict[str, Any]] = None) -> dict[str, Any]:
    data = {
        "storage_backend": "local_file_store",
        "kind": kind,
        "size_bytes": int(size_bytes),
        "max_bytes": int(max_bytes),
        "recommended_backend": "https://businessprocessapplications.com",
        "recommended_pipeline": "Use hosted ZMongo for large documents/files; backend ZEmbedder.py handles chunking and embeddings.",
    }
    if extra:
        data.update(extra)
    return _local_payload_error(
        f"Local File Store {kind} is too large: {size_bytes} bytes exceeds {max_bytes} bytes.",
        data=data,
        status_code=413,
        error_type="LocalStorageItemTooLarge",
    )


class LocalZMongoSession:
    """
    Local file-backed ZMongo-compatible session.

    Stores documents as JSON files under:
        ComfyUI/custom_nodes/ComfyUI-ZMongo/local_store/collections/<collection>/<id>.document.json

    This exposes the same node-facing methods as ZMongoApiSession:
        health, whoami, list_collections, create_collection, delete_collection,
        list_docs, get_doc, query_docs, count_docs, create_doc, update_doc,
        delete_doc, save_value, fetch_image_field, fetch_absolute_or_relative_bytes.
    """

    storage_backend = "local_file_store"
    base_url = "local://comfyui-zmongo"
    username = "local_user"
    zai_api_key = ""
    comfy_zmongo_prefix = "/local-file-store"
    fleet_prefix = "/local-disabled"
    comfy_zmongo_fleet_prefix = "/local-disabled"
    max_value_bytes = DEFAULT_LOCAL_MAX_VALUE_BYTES
    max_document_bytes = DEFAULT_LOCAL_MAX_DOCUMENT_BYTES

    def _limit_info(self) -> dict[str, Any]:
        return {
            "local_max_value_bytes": int(self.max_value_bytes),
            "local_max_document_bytes": int(self.max_document_bytes),
            "large_document_backend": "https://businessprocessapplications.com",
            "large_document_chunker": "ZEmbedder.py",
        }

    def __init__(self, root_dir: Optional[str | Path] = None) -> None:
        plugin_root = Path(__file__).resolve().parent
        self.root_dir = Path(root_dir).expanduser().resolve() if root_dir else (plugin_root / "local_store").resolve()
        self.collections_dir = self.root_dir / "collections"
        self.manifest_path = self.root_dir / "manifest.json"
        self.root_dir.mkdir(parents=True, exist_ok=True)
        self.collections_dir.mkdir(parents=True, exist_ok=True)
        self._ensure_manifest()

    def close(self) -> None:
        return None

    def _ensure_manifest(self) -> None:
        if self.manifest_path.exists():
            return
        self._write_json(self.manifest_path, {
            "storage_backend": "local_file_store",
            "created_at": _local_now_iso(),
            "updated_at": _local_now_iso(),
            "root_dir": str(self.root_dir),
            "collections_dir": str(self.collections_dir),
            "message": "Local File Store mode. Files are stored on this machine only.",
        })

    @staticmethod
    def _read_json(path: Path, default: Any = None) -> Any:
        if not path.exists():
            return default
        with path.open("r", encoding="utf-8") as handle:
            return json.load(handle)

    @staticmethod
    def _write_json(path: Path, value: Any) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp = path.with_suffix(path.suffix + ".tmp")
        with tmp.open("w", encoding="utf-8") as handle:
            json.dump(value, handle, indent=2, ensure_ascii=False, default=str)
        tmp.replace(path)

    def _collection_dir(self, collection: str) -> Path:
        path = self.collections_dir / _local_safe_name(collection, "documents")
        path.mkdir(parents=True, exist_ok=True)
        return path

    def _doc_path(self, collection: str, document_id: str) -> Path:
        return self._collection_dir(collection) / f"{_local_safe_name(document_id, 'document')}.document.json"

    def _iter_doc_paths(self, collection: str) -> list[Path]:
        return sorted(self._collection_dir(collection).glob("*.document.json"))

    def _load_doc_path(self, path: Path) -> dict[str, Any]:
        data = self._read_json(path, default={})
        return data if isinstance(data, dict) else {}

    def _load_doc(self, collection: str, document_id: str) -> dict[str, Any]:
        path = self._doc_path(collection, document_id)
        if not path.exists():
            return {}
        return self._load_doc_path(path)

    def _save_doc(self, collection: str, document: dict[str, Any]) -> dict[str, Any]:
        clean_collection = _local_safe_name(collection, "documents")
        doc = dict(document or {})
        doc_id = _local_clean_scalar(doc.get("_id") or doc.get("document_id") or doc.get("id")) or _local_new_id()
        now = _local_now_iso()
        doc["_id"] = doc_id
        doc["collection"] = clean_collection
        doc.setdefault("created_at", now)
        doc["updated_at"] = now
        doc["storage_backend"] = "local_file_store"
        self._write_json(self._doc_path(clean_collection, doc_id), doc)
        return doc

    def health(self) -> dict[str, Any]:
        return _local_payload_ok("Local File Store is available.", {
            "ok": True,
            "mode": "Local File Store",
            "storage_backend": "local_file_store",
            "root_dir": str(self.root_dir),
            "manifest": str(self.manifest_path),
            "message": "This stores small JSON items on this machine only. Large document storage, chunking, embeddings, R2, dashboard, sync, metering, and backup require hosted ZMongo.",
            **self._limit_info(),
        })

    def whoami(self) -> dict[str, Any]:
        return _local_payload_ok("Local File Store session.", {
            "username": "local_user",
            "db_name": "local_file_store",
            "silo_db_name": "local_file_store",
            "api_key_present": False,
            "hosted_backend": False,
            "storage_backend": "local_file_store",
            "root_dir": str(self.root_dir),
        })

    def list_collections(self) -> dict[str, Any]:
        collections = sorted(p.name for p in self.collections_dir.iterdir() if p.is_dir())
        return _local_payload_ok("Listed local collections.", {
            "collections": collections,
            "collection_names": collections,
            "count": len(collections),
            "storage_backend": "local_file_store",
        })

    def create_collection(self, collection: str) -> dict[str, Any]:
        clean = _local_safe_name(collection, "documents")
        path = self._collection_dir(clean)
        return _local_payload_ok("Created local collection.", {
            "collection": clean,
            "collection_name": clean,
            "path": str(path),
            "storage_backend": "local_file_store",
        })

    def delete_collection(self, collection: str) -> dict[str, Any]:
        clean = _local_safe_name(collection, "")
        if not clean:
            return _local_payload_error("collection is required.", status_code=400)
        path = self.collections_dir / clean
        if not path.exists():
            return _local_payload_error("Local collection does not exist.", {"collection": clean, "path": str(path)},
                                        status_code=404)
        import shutil
        shutil.rmtree(path)
        return _local_payload_ok("Deleted local collection.", {
            "collection": clean,
            "collection_name": clean,
            "path": str(path),
            "storage_backend": "local_file_store",
        })

    def list_docs(self, *, collection: str, limit: int = 50, skip: int = 0, query: Optional[dict[str, Any]] = None) -> \
    dict[str, Any]:
        clean = _local_safe_name(collection, "documents")
        safe_limit = max(1, min(int(limit or 50), 500))
        safe_skip = max(0, int(skip or 0))
        docs = []
        for path in self._iter_doc_paths(clean):
            doc = self._load_doc_path(path)
            if _local_matches_query(doc, query or {}):
                docs.append(doc)
        page = docs[safe_skip:safe_skip + safe_limit]
        ids = [str(doc.get("_id")) for doc in page if doc.get("_id")]
        return _local_payload_ok("Listed local documents.", {
            "collection": clean,
            "collection_name": clean,
            "query": query or {},
            "limit": safe_limit,
            "skip": safe_skip,
            "count": len(page),
            "total": len(docs),
            "documents": page,
            "results": page,
            "document_ids": ids,
            "ids": ids,
            "storage_backend": "local_file_store",
        })

    def get_doc(self, *, collection: str, document_id: str, cache: bool = False) -> dict[str, Any]:
        clean = _local_safe_name(collection, "documents")
        clean_id = _local_clean_scalar(document_id)
        doc = self._load_doc(clean, clean_id)
        if not doc:
            return _local_payload_error("Local document not found.", {
                "collection": clean,
                "collection_name": clean,
                "document_id": clean_id,
                "path": str(self._doc_path(clean, clean_id)),
                "storage_backend": "local_file_store",
            }, status_code=404)
        return _local_payload_ok("Loaded local document.", {
            "document": doc,
            "document_id": clean_id,
            "collection": clean,
            "collection_name": clean,
            "cache_hit": False,
            "storage_backend": "local_file_store",
        })

    def query_docs(
            self,
            *,
            collection: str,
            query: Optional[dict[str, Any]] = None,
            document_id: str = "",
            many: bool = True,
            limit: int = 50,
            skip: int = 0,
            projection: Optional[dict[str, Any]] = None,
            sort: Optional[list[Any]] = None,
            cache: bool = False,
    ) -> dict[str, Any]:
        clean = _local_safe_name(collection, "documents")
        clean_id = _local_clean_scalar(document_id)
        if clean_id:
            payload = self.get_doc(collection=clean, document_id=clean_id, cache=cache)
            if not payload.get("success"):
                return payload
            doc = payload["data"]["document"]
            return _local_payload_ok("Queried one local document.", {
                "collection": clean,
                "collection_name": clean,
                "document": doc,
                "documents": [doc],
                "results": [doc],
                "document_id": clean_id,
                "count": 1,
                "total": 1,
                "storage_backend": "local_file_store",
            })
        listed = self.list_docs(collection=clean, query=query or {}, limit=limit, skip=skip)
        docs = listed.get("data", {}).get("documents", [])
        if sort:
            for item in reversed(sort):
                try:
                    key = item[0]
                    direction = int(item[1])
                    docs.sort(key=lambda d: str(_local_get_by_path(d, str(key), "")), reverse=direction < 0)
                except Exception:
                    pass
        if not many:
            docs = docs[:1]
        data = dict(listed.get("data", {}))
        data["documents"] = docs
        data["results"] = docs
        data["count"] = len(docs)
        if docs:
            data["document"] = docs[0]
        return _local_payload_ok("Queried local documents.", data)

    def count_docs(self, *, collection: str, query: Optional[dict[str, Any]] = None, document_id: str = "",
                   cache: bool = False) -> dict[str, Any]:
        clean = _local_safe_name(collection, "documents")
        clean_id = _local_clean_scalar(document_id)
        if clean_id:
            count = 1 if self._load_doc(clean, clean_id) else 0
        else:
            count = 0
            for path in self._iter_doc_paths(clean):
                if _local_matches_query(self._load_doc_path(path), query or {}):
                    count += 1
        return _local_payload_ok("Counted local documents.", {
            "collection": clean,
            "collection_name": clean,
            "query": query or {},
            "document_id": clean_id,
            "count": count,
            "document_count": count,
            "total": count,
            "storage_backend": "local_file_store",
        })

    def create_doc(self, *, collection: str, document: dict[str, Any]) -> dict[str, Any]:
        clean = _local_safe_name(collection, "documents")
        candidate = dict(document or {})
        candidate_size = _local_json_size_bytes(candidate)
        if candidate_size > self.max_document_bytes:
            return _local_limit_payload("document", candidate_size, self.max_document_bytes, {"collection": clean})
        saved = self._save_doc(clean, candidate)
        doc_id = str(saved["_id"])
        return _local_payload_ok("Created local document.", {
            "document": saved,
            "document_id": doc_id,
            "inserted_id": doc_id,
            "_id": doc_id,
            "collection": clean,
            "collection_name": clean,
            "storage_backend": "local_file_store",
            **self._limit_info(),
        })

    def update_doc(
            self,
            *,
            collection: str,
            query: Optional[dict[str, Any]] = None,
            document_id: str = "",
            update: Optional[dict[str, Any]] = None,
            field_path: str = "",
            value: Any = None,
            upsert: bool = False,
    ) -> dict[str, Any]:
        clean = _local_safe_name(collection, "documents")
        clean_id = _local_clean_scalar(document_id)
        doc = self._load_doc(clean, clean_id) if clean_id else {}
        if not doc and query:
            docs = self.query_docs(collection=clean, query=query, many=False, limit=1).get("data", {}).get("documents",
                                                                                                           [])
            if docs:
                doc = docs[0]
                clean_id = str(doc.get("_id") or "")
        if not doc and not upsert:
            return _local_payload_error("Local update target not found.", {
                "collection": clean,
                "query": query or {},
                "document_id": clean_id,
                "upsert": bool(upsert),
                "storage_backend": "local_file_store",
            }, status_code=404)
        if not doc:
            doc = {}
            if clean_id:
                doc["_id"] = clean_id
            if query:
                for key, item in query.items():
                    if isinstance(key, str) and not key.startswith("$") and not isinstance(item, dict):
                        _local_set_by_path(doc, key, item)
        if update is not None:
            if "$set" in update and isinstance(update["$set"], dict):
                for key, item in update["$set"].items():
                    _local_set_by_path(doc, key, item)
            else:
                for key, item in update.items():
                    if not str(key).startswith("$"):
                        _local_set_by_path(doc, key, item)
        else:
            _local_set_by_path(doc, field_path, value)
        doc_size = _local_json_size_bytes(doc)
        if doc_size > self.max_document_bytes:
            return _local_limit_payload("document", doc_size, self.max_document_bytes, {
                "collection": clean,
                "document_id": clean_id,
            })
        saved = self._save_doc(clean, doc)
        doc_id = str(saved["_id"])
        return _local_payload_ok("Updated local document.", {
            "document": saved,
            "document_id": doc_id,
            "matched_count": 1,
            "modified_count": 1,
            "collection": clean,
            "collection_name": clean,
            "storage_backend": "local_file_store",
        })

    def delete_doc(self, *, collection: str, query: Optional[dict[str, Any]] = None, document_id: str = "") -> dict[
        str, Any]:
        clean = _local_safe_name(collection, "documents")
        clean_id = _local_clean_scalar(document_id)
        deleted = []
        if clean_id:
            path = self._doc_path(clean, clean_id)
            if path.exists():
                path.unlink()
                deleted.append(clean_id)
        elif query:
            for path in self._iter_doc_paths(clean):
                doc = self._load_doc_path(path)
                if _local_matches_query(doc, query):
                    deleted.append(str(doc.get("_id") or path.stem.replace(".document", "")))
                    path.unlink()
        else:
            return _local_payload_error("delete_doc requires document_id or query.", status_code=400)
        return _local_payload_ok("Deleted local document(s).", {
            "collection": clean,
            "collection_name": clean,
            "document_id": clean_id,
            "deleted_ids": deleted,
            "deleted_count": len(deleted),
            "storage_backend": "local_file_store",
        })

    def save_value(
            self,
            *,
            collection: str,
            query: Optional[dict[str, Any]] = None,
            document_id: str = "",
            field_path: str = "",
            value: Any = None,
            upsert_if_missing: bool = True,
            parse_json_strings: bool = True,
            normalize_for_storage: bool = False,
    ) -> dict[str, Any]:
        clean = _local_safe_name(collection, "documents")
        clean_id = _local_clean_scalar(document_id)
        clean_field = _local_clean_scalar(field_path)
        if not clean_field:
            return _local_payload_error("field_path is required.", status_code=400)
        doc = self._load_doc(clean, clean_id) if clean_id else {}
        if not doc and query:
            docs = self.query_docs(collection=clean, query=query, many=False, limit=1).get("data", {}).get("documents",
                                                                                                           [])
            if docs:
                doc = docs[0]
                clean_id = str(doc.get("_id") or "")
        if not doc:
            if not upsert_if_missing:
                return _local_payload_error("query or document_id is required.", {
                    "collection": clean,
                    "query": query or {},
                    "document_id": clean_id,
                    "field_path": clean_field,
                    "upsert_if_missing": bool(upsert_if_missing),
                }, status_code=400)
            doc = {}
            if clean_id:
                doc["_id"] = clean_id
            if query:
                for key, item in query.items():
                    if isinstance(key, str) and not key.startswith("$") and not isinstance(item, dict):
                        _local_set_by_path(doc, key, item)
        if parse_json_strings and isinstance(value, str) and value.strip():
            try:
                value = json.loads(value)
            except Exception:
                pass
        value_size = _local_json_size_bytes(value)
        if value_size > self.max_value_bytes:
            return _local_limit_payload("value", value_size, self.max_value_bytes, {
                "collection": clean,
                "document_id": clean_id,
                "field_path": clean_field,
            })
        _local_set_by_path(doc, clean_field, value)
        doc_size = _local_json_size_bytes(doc)
        if doc_size > self.max_document_bytes:
            return _local_limit_payload("document", doc_size, self.max_document_bytes, {
                "collection": clean,
                "document_id": clean_id,
                "field_path": clean_field,
            })
        saved = self._save_doc(clean, doc)
        doc_id = str(saved["_id"])
        return _local_payload_ok("Saved local value.", {
            "operation": "updated_existing",
            "document_id": doc_id,
            "inserted_id": doc_id,
            "_id": doc_id,
            "collection": clean,
            "collection_name": clean,
            "field_path": clean_field,
            "saved_value": value,
            "document": saved,
            "document_path": str(self._doc_path(clean, doc_id).relative_to(self.root_dir)),
            "storage_backend": "local_file_store",
            **self._limit_info(),
        })

    def fetch_image_field(
        self,
        *,
        collection: str,
        document_id: str,
        field_path: str,
        master_key_hex: str = "",
    ) -> tuple[bytes, str]:
        """Load image bytes from a local ZMongo-style document field.

        This method intentionally does not import zmongo_image_nodes or
        zmongo_images_nodes. Those modules import this helper module, so importing
        them here creates a Pylint R0401 cyclic-import warning.

        The required image helpers are already defined in this file:
        _image_field_candidates() and _decode_image_bytes_from_value().
        """
        del master_key_hex  # Local file store does not use hosted encryption keys.

        clean = _local_safe_name(collection, "images")
        clean_id = _local_clean_scalar(document_id)
        doc = self._load_doc(clean, clean_id)

        if not doc:
            raise ValueError(f"Local document not found: {clean}/{clean_id}")

        errors: list[str] = []

        for candidate in _image_field_candidates(field_path, "image_data"):
            try:
                value = _local_get_by_path(doc, candidate)
                data = _decode_image_bytes_from_value(value)
                return data, f"local_file_store:{clean}/{clean_id}:{candidate}"
            except Exception as exc:
                errors.append(f"{candidate}: {exc}")

        raise ValueError("No decodable local image field found. " + " | ".join(errors))


    def fetch_absolute_or_relative_bytes(self, url: str) -> bytes:
        text = _local_clean_scalar(url)
        if text.startswith("local://"):
            text = text.replace("local://", "", 1)
        path = Path(text)
        if not path.is_absolute():
            path = self.root_dir / text.lstrip("/\\")
        path = path.resolve()
        if not path.exists():
            raise FileNotFoundError(f"Local file not found: {path}")
        return path.read_bytes()

    def request(self, method: str, prefix: str, path: str, *, json_body: Optional[dict[str, Any]] = None,
                params: Optional[dict[str, Any]] = None) -> dict[str, Any]:
        """Route a node-style request into the local file store.

        This intentionally supports both route families used by ComfyUI-ZMongo:

        1. Generic ZMongo routes:
           /api/doc/<collection>/<id>, /api/query, /api/save-value, etc.

        2. Document API node routes:
           /health, /api/list, /api/get/<id>, /api/text/<id>,
           /api/field-paths/<id>, /api/save-value/<id>, etc.

        The document nodes default to the "documents" collection unless the
        request body explicitly provides collection/collection_name.
        """
        method = (method or "GET").upper()
        path = path if path.startswith("/") else f"/{path}"
        body = json_body or {}
        params = params or {}

        def body_collection(default: str = "documents") -> str:
            return body.get("collection") or body.get("collection_name") or default

        def tail_after(marker: str) -> str:
            return urllib.parse.unquote(path.rsplit(marker, 1)[1].strip("/"))

        def payload_text_from_document(doc: dict[str, Any]) -> str:
            text_value = doc.get("text")
            if text_value is not None:
                return str(text_value or "")

            file_data = doc.get("file_data")
            content_type = str(doc.get("content_type") or "")
            if isinstance(file_data, str) and (
                content_type.startswith("text/")
                or str(doc.get("filename") or "").lower().endswith((".txt", ".md", ".csv", ".json", ".rtf"))
            ):
                try:
                    return base64.b64decode(file_data.encode("ascii"), validate=False).decode("utf-8", errors="replace")
                except Exception:
                    return ""
            return ""

        try:
            # -----------------------------------------------------------------
            # Shared health/account routes
            # -----------------------------------------------------------------
            if method == "GET" and (path.endswith("/health") or path.endswith("/api/health")):
                return self.health()
            if method == "GET" and path.endswith("/api/whoami"):
                return self.whoami()
            if method == "GET" and path.endswith("/api/collections"):
                return self.list_collections()

            # -----------------------------------------------------------------
            # Generic ZMongo collection/document routes
            # -----------------------------------------------------------------
            if method == "POST" and path.endswith("/api/collection/create"):
                return self.create_collection(
                    body.get("name") or body.get("collection") or body.get("collection_name") or "")
            if method == "POST" and path.endswith("/api/collection/delete"):
                return self.delete_collection(
                    body.get("name") or body.get("collection") or body.get("collection_name") or "")
            if method == "POST" and path.endswith("/api/doc/create"):
                return self.create_doc(collection=body_collection(), document=body.get("document") or {})
            if method == "POST" and path.endswith("/api/query"):
                return self.query_docs(
                    collection=body_collection(),
                    query=body.get("query") or {},
                    document_id=body.get("document_id") or "",
                    many=body.get("many", True),
                    limit=body.get("limit", 50),
                    skip=body.get("skip", 0),
                    projection=body.get("projection") or {},
                    sort=body.get("sort") or [],
                    cache=body.get("cache", False),
                )
            if method == "POST" and path.endswith("/api/count"):
                return self.count_docs(
                    collection=body_collection(),
                    query=body.get("query") or {},
                    document_id=body.get("document_id") or "",
                    cache=body.get("cache", False),
                )
            if method == "POST" and path.endswith("/api/doc/update"):
                return self.update_doc(
                    collection=body_collection(),
                    query=body.get("query") or {},
                    document_id=body.get("document_id") or "",
                    update=body.get("update"),
                    field_path=body.get("field_path") or "",
                    value=body.get("value"),
                    upsert=body.get("upsert", False),
                )
            if method == "POST" and path.endswith("/api/doc/delete"):
                return self.delete_doc(
                    collection=body_collection(),
                    query=body.get("query") or {},
                    document_id=body.get("document_id") or "",
                )
            if method == "POST" and path.endswith("/api/save-value"):
                return self.save_value(
                    collection=body_collection(),
                    query=body.get("query") or {},
                    document_id=body.get("document_id") or "",
                    field_path=body.get("field_path") or "",
                    value=body.get("value"),
                    upsert_if_missing=body.get("upsert_if_missing", True),
                    parse_json_strings=body.get("parse_json_strings", True),
                    normalize_for_storage=body.get("normalize_for_storage", False),
                )
            if method == "GET" and "/api/docs/" in path:
                collection = tail_after("/api/docs/")
                query = {}
                if params.get("query_json"):
                    try:
                        query = json.loads(params.get("query_json") or "{}")
                    except Exception:
                        query = {}
                return self.list_docs(collection=collection, query=query, limit=int(params.get("limit", 50)),
                                      skip=int(params.get("skip", 0)))
            if method == "GET" and "/api/doc/" in path:
                tail = tail_after("/api/doc/")
                parts = tail.split("/", 1)
                if len(parts) == 2:
                    return self.get_doc(collection=parts[0],
                                        document_id=parts[1],
                                        cache=str(params.get("cache", "false")).lower() == "true")

            # -----------------------------------------------------------------
            # Document API node routes. These are the routes used by
            # document_api_nodes.py and are backed locally by the "documents"
            # collection.
            # -----------------------------------------------------------------
            if method == "POST" and path.endswith("/api/upload"):
                filename = _local_clean_scalar(body.get("filename")) or "uploaded_document"
                file_data = body.get("file_data") or ""
                text_value = body.get("text") or ""
                metadata = body.get("metadata") if isinstance(body.get("metadata"), dict) else {}
                document = body.get("document") if isinstance(body.get("document"), dict) else {}
                document.update({
                    "filename": filename,
                    "content_type": body.get("content_type") or mimetypes.guess_type(filename)[0] or "application/octet-stream",
                    "file_data": file_data,
                    "size_bytes": len(base64.b64decode(file_data.encode("ascii"), validate=False)) if isinstance(file_data, str) and file_data else 0,
                    "text": text_value,
                    "case_id": body.get("case_id"),
                    "metadata": metadata,
                })
                return self.create_doc(collection=body_collection(), document=document)

            if method == "POST" and path.endswith("/api/create"):
                filename = _local_clean_scalar(body.get("filename")) or "manual_document.txt"
                metadata = body.get("metadata") if isinstance(body.get("metadata"), dict) else {}
                document = body.get("document") if isinstance(body.get("document"), dict) else {}
                document.update({
                    "filename": filename,
                    "content_type": body.get("content_type") or mimetypes.guess_type(filename)[0] or "text/plain",
                    "text": body.get("text") or "",
                    "case_id": body.get("case_id"),
                    "metadata": metadata,
                    "text_length": len(str(body.get("text") or "")),
                })
                payload = self.create_doc(collection=body_collection(), document=document)
                if payload.get("success"):
                    data = payload.setdefault("data", {})
                    data["filename"] = filename
                    data["text_length"] = len(str(body.get("text") or ""))
                return payload

            if method == "POST" and path.endswith("/api/list"):
                return self.query_docs(
                    collection=body_collection(),
                    query=body.get("query") or {},
                    many=True,
                    limit=body.get("limit", 50),
                    skip=body.get("skip", 0),
                    sort=body.get("sort") or [],
                    cache=False,
                )

            if method == "GET" and "/api/get/" in path:
                document_id = tail_after("/api/get/")
                return self.get_doc(collection=body_collection(), document_id=document_id,
                                    cache=str(params.get("cache", "false")).lower() == "true")

            if method == "GET" and "/api/text/" in path:
                document_id = tail_after("/api/text/")
                payload = self.get_doc(collection=body_collection(), document_id=document_id, cache=False)
                if not payload.get("success"):
                    return payload
                doc = payload.get("data", {}).get("document", {})
                text_value = payload_text_from_document(doc if isinstance(doc, dict) else {})
                return _local_payload_ok("Loaded local document text.", {
                    "document_id": document_id,
                    "text": text_value,
                    "text_length": len(text_value),
                    "storage_backend": "local_file_store",
                })

            if method == "GET" and "/api/field-paths/" in path:
                document_id = tail_after("/api/field-paths/")
                collection = params.get("collection") or params.get("collection_name") or body_collection()
                payload = self.get_doc(collection=str(collection), document_id=document_id, cache=False)
                if not payload.get("success"):
                    return payload
                doc = payload.get("data", {}).get("document", {})
                paths = dedupe_strings(flatten_document_paths(doc if isinstance(doc, dict) else {}))
                return _local_payload_ok("Listed local document field paths.", {
                    "collection": collection,
                    "collection_name": collection,
                    "document_id": document_id,
                    "field_paths": paths,
                    "paths": paths,
                    "count": len(paths),
                    "storage_backend": "local_file_store",
                })

            if method == "POST" and "/api/save-text/" in path:
                document_id = tail_after("/api/save-text/")
                text_value = str(body.get("text") or "")
                return self.update_doc(
                    collection=body_collection(),
                    document_id=document_id,
                    field_path="text",
                    value=text_value,
                    upsert=False,
                )

            if method == "POST" and "/api/save-value/" in path:
                document_id = tail_after("/api/save-value/")
                return self.save_value(
                    collection=body_collection(),
                    document_id=document_id,
                    field_path=body.get("field_path") or "",
                    value=body.get("value"),
                    upsert_if_missing=False,
                    parse_json_strings=body.get("parse_json_strings", True),
                    normalize_for_storage=body.get("normalize_for_storage", False),
                )

            if method == "POST" and "/api/metadata/" in path:
                document_id = tail_after("/api/metadata/")
                update_values: dict[str, Any] = {}
                if isinstance(body.get("metadata"), dict):
                    update_values["metadata"] = body.get("metadata")
                for key in ("case_id", "status", "title", "description", "tags"):
                    if key in body and body.get(key) not in (None, ""):
                        update_values[key] = body.get(key)
                return self.update_doc(
                    collection=body_collection(),
                    document_id=document_id,
                    update={"$set": update_values},
                    upsert=False,
                )

            if method == "POST" and "/api/extract-text/" in path:
                document_id = tail_after("/api/extract-text/")
                text_payload = self.request("GET", prefix, f"/api/text/{document_id}")
                if not text_payload.get("success"):
                    return text_payload
                if body.get("save", True):
                    text_value = str(text_payload.get("data", {}).get("text") or "")
                    self.update_doc(collection=body_collection(), document_id=document_id,
                                    field_path="text", value=text_value, upsert=False)
                return text_payload

            if method == "POST" and "/api/ocr/queue/" in path:
                document_id = tail_after("/api/ocr/queue/")
                payload = self.update_doc(
                    collection=body_collection(),
                    document_id=document_id,
                    update={"$set": {"ocr": {
                        "status": "queued",
                        "priority": int(body.get("priority") or 50),
                        "source": body.get("source") or "comfyui_zmongo_document_node",
                        "queued_at": _local_now_iso(),
                    }}},
                    upsert=False,
                )
                if payload.get("success"):
                    payload["data"]["status"] = "queued"
                return payload

            if method == "GET" and "/api/ocr/status/" in path:
                document_id = tail_after("/api/ocr/status/")
                payload = self.get_doc(collection=body_collection(), document_id=document_id, cache=False)
                if not payload.get("success"):
                    return payload
                doc = payload.get("data", {}).get("document", {})
                ocr = doc.get("ocr", {}) if isinstance(doc, dict) and isinstance(doc.get("ocr"), dict) else {}
                text_value = payload_text_from_document(doc if isinstance(doc, dict) else {})
                return _local_payload_ok("Loaded local OCR status.", {
                    "document_id": document_id,
                    "status": ocr.get("status") or ("complete" if text_value else "not_queued"),
                    "has_text": bool(text_value),
                    "last_error": ocr.get("last_error") or "",
                    "ocr": ocr,
                    "storage_backend": "local_file_store",
                })

            if method == "POST" and "/api/delete/" in path:
                document_id = tail_after("/api/delete/")
                return self.delete_doc(collection=body_collection(), document_id=document_id)

            return _local_payload_error("Local request route is not implemented.", {
                "method": method,
                "prefix": prefix,
                "path": path,
                "json_body": body,
                "params": params,
                "storage_backend": "local_file_store",
            }, status_code=404)
        except Exception as exc:
            return _local_payload_error(f"Local request failed: {exc}", {
                "method": method,
                "prefix": prefix,
                "path": path,
                "json_body": body,
                "params": params,
                "storage_backend": "local_file_store",
            }, status_code=0, error_type=exc.__class__.__name__)


class ZMongoLocalFileStoreSessionNode(AlwaysDirtyMixin):
    @classmethod
    def INPUT_TYPES(cls):
        return {
            "required": {
                "local_store_root": ("STRING", {"default": "", "multiline": False}),
                "test_health": ("BOOLEAN", {"default": True}),
            }
        }

    RETURN_TYPES = ("ZMONGO_API_SESSION", "STRING", "STRING")
    RETURN_NAMES = ("session", "json", "status")
    FUNCTION = "connect"
    CATEGORY = "ZMongo/00 Auth"

    def connect(self, local_store_root: str = "", test_health: bool = True):
        try:
            root_text = _local_clean_scalar(local_store_root)
            root_dir = Path(root_text).expanduser().resolve() if root_text else None
            session = LocalZMongoSession(root_dir=root_dir)
            payload = session.health() if test_health else _local_payload_ok("Local File Store session created.", {
                "storage_backend": "local_file_store",
                "root_dir": str(session.root_dir),
                "mode": "Local File Store",
            })
            return (session, _json_text(payload), payload.get("message", "Local File Store session created."))
        except Exception as exc:
            payload = _local_payload_error(f"Local File Store session failed: {exc}", {
                "local_store_root": local_store_root,
                "error_type": exc.__class__.__name__,
            }, status_code=0, error_type=exc.__class__.__name__)
            return (None, _json_text(payload), payload["message"])


NODE_CLASS_MAPPINGS = {
    "ZMongoLocalFileStoreSessionNode": ZMongoLocalFileStoreSessionNode,
}

NODE_DISPLAY_NAME_MAPPINGS = {
    "ZMongoLocalFileStoreSessionNode": "00 Local File Store Session",
}

__all__ = [
    "AlwaysDirtyMixin",
    "LocalZMongoSession",
    "ZMongoLocalFileStoreSessionNode",
    "NODE_CLASS_MAPPINGS",
    "NODE_DISPLAY_NAME_MAPPINGS",
    "DOCUMENT_FILE_EXTENSIONS",
    "IMMUTABLE_DOCUMENT_FIELD_PATHS",
    "SELECTABLE_RETURN_TYPES",
    "SELECTABLE_RETURN_NAMES",
    "SELECTABLE_OUTPUT_IS_LIST",
    "ZMONGO_DOCUMENT_ID",
    "ZMONGO_FIELD_PATH",
    "ZMONGO_FILE_PATH",
    "ZMONGO_FILENAME",
    "ZMONGO_TEXT",
    "ZMONGO_STATUS",
    "dirty_token",
    "json_text",
    "success_payload",
    "error_payload",
    "ensure_payload_dict",
    "normalize_base_url",
    "clean_prefix",
    "clean_scalar",
    "strip_index_prefix",
    "dedupe_strings",
    "as_bool",
    "as_comfy_list",
    "indexed_list_text",
    "selectable_tail",
    "parse_json_object",
    "parse_json_list",
    "parse_any_json",
    "safe_get_by_path",
    "set_by_path",
    "flatten_document_paths",
    "flatten_path_keys",
    "coerce_document_id",
    "coerce_field_path",
    "looks_like_document_id",
    "coerce_document_id_link",
    "coerce_file_path_link",
    "coerce_filename_link",
    "coerce_text_link",
    "is_immutable_document_field_path",
    "blocked_immutable_field_payload",
    "extract_data",
    "extract_documents",
    "extract_document",
    "extract_doc_id",
    "extract_doc_ids",
    "extract_doc_ids_from_documents",
    "extract_filenames_from_documents",
    "extract_collections",
    "extract_count",
    "extract_text",
    "extract_field_paths",
    "extract_text_from_gemini_payload",
    "extract_models_from_payload",
    "value_items",
    "document_summary",
    "prefer_link_value",
    "raise_if_failed",
    "get_comfy_input_directory",
    "get_default_documents_directory",
    "get_document_browser_root",
    "list_document_files",
    "read_file_as_base64",
    "resolve_document_file_path",
    "session_request",
    "session_api_request",
    "session_get_doc",
    "session_save_value",
    "comfy_image_to_png_bytes",
    "build_binary_envelope",
    "extract_tensor_recursively",
    "local_now_iso",
    "local_safe_name",
    "local_new_id",
    "matches_query",
]