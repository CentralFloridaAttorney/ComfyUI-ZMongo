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
import shutil
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

def _ensure_payload_dict(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return value
    return {"success": False, "message": str(value), "data": value}


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


def _session_get_doc(session: Any, collection_name: str, document_id: str, cache: bool = True) -> dict[str, Any]:
    if hasattr(session, "get_doc") and callable(session.get_doc):
        try:
            payload = session.get_doc(collection=collection_name, document_id=document_id, cache=cache)
            return _ensure_payload_dict(payload)
        except Exception:
            pass
    return {}


def _session_api_request(session: Any, method: str, path: str, json_body: dict[str, Any] | None = None) -> dict[
    str, Any]:
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
    cleaned = _clean_field_path(field_path, default)
    known_leafs = ("data", "base64", "b64", "bytes", "image", "content", "payload", "url")
    parts = [part for part in cleaned.split(".") if part]

    if len(parts) > 1 and parts[-1] in known_leafs:
        return ".".join(parts[:-1]) or default

    return cleaned


def _route_image_field_path(field_path: str, default: str = "image_data") -> str:
    return _strip_known_image_leaf(field_path, default)


def _image_field_candidates(field_path: str, default: str = "image_data") -> list[str]:
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
            candidates.append(
                {"collection": "zai_fleet_files", "document_id": value, "field_path": "image_data", "source": path})

    looks_like_file_doc = any(key in document for key in ("filename", "length", "chunkSize", "uploadDate"))
    object_id = _string_value_from_path(document, "_id")
    if looks_like_file_doc and object_id:
        candidates.append({"collection": "zai_fleet_files", "document_id": object_id, "field_path": "image_data",
                           "source": "metadata._id"})

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


def _local_limit_payload(kind: str, size_bytes: int, max_bytes: int, extra: Optional[dict[str, Any]] = None) -> dict[
    str, Any]:
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


__all__ = [
    "AlwaysDirtyMixin",
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
