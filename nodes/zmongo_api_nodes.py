from __future__ import annotations

import base64
import io
import json
import os
import time
import uuid
import urllib.parse
from pathlib import Path
from typing import Any, Optional

import numpy as np
import requests
import torch
from dotenv import load_dotenv
from PIL import Image, ImageOps, UnidentifiedImageError


ENV_PATH1 = Path.home() / ".resources" / ".env"
ENV_PATH2 = Path.home() / ".resources" / ".secrets"
load_dotenv(ENV_PATH1)
load_dotenv(ENV_PATH2)

DEFAULT_BASE_URL = os.getenv("ZTAROT_BASE_URL", "https://ztarot.app").rstrip("/")
DEFAULT_TIMEOUT = int(os.getenv("ZTAROT_TIMEOUT_SECONDS", "30"))
DEFAULT_COMFY_ZMONGO_PREFIX = os.getenv("COMFY_ZMONGO_API_PREFIX", "/comfy-zmongo")
DEFAULT_FLEET_PREFIX = os.getenv("ZFLEET_API_PREFIX", "/fleet")
DEFAULT_COMFY_ZMONGO_FLEET_PREFIX = os.getenv("COMFY_ZMONGO_FLEET_PREFIX", "/comfy-zmongo-fleet")


# -----------------------------------------------------------------------------
# Generic helpers
# -----------------------------------------------------------------------------

def _dirty_token(*parts: Any) -> str:
    prefix = ":".join(str(part) for part in parts if part is not None)
    return f"{prefix}:{time.time_ns()}:{uuid.uuid4().hex}"


def _json_text(value: Any) -> str:
    return json.dumps(value, indent=2, ensure_ascii=False, default=str)


def _error_payload(message: str, *, status_code: int = 0) -> dict[str, Any]:
    return {
        "success": False,
        "message": message,
        "data": {},
        "error": {"msg": message},
        "status_code": status_code,
    }


def _success_payload(message: str, data: Optional[dict[str, Any]] = None, *, status_code: int = 200) -> dict[str, Any]:
    return {
        "success": True,
        "message": message,
        "data": data or {},
        "error": None,
        "status_code": status_code,
    }


def _ensure_payload_dict(payload: Any) -> dict[str, Any]:
    if isinstance(payload, dict):
        result = dict(payload)
    else:
        result = {
            "success": False,
            "message": "Payload was not a JSON object.",
            "data": payload,
            "error": {"msg": f"Unexpected payload type: {type(payload).__name__}"},
        }

    result.setdefault("success", False)
    result.setdefault("message", "OK" if result.get("success") else "")
    result.setdefault("data", {})
    result.setdefault("error", None)
    result.setdefault("status_code", 0)
    return result


def _normalize_base_url(raw_base_url: str) -> str:
    base = (raw_base_url or DEFAULT_BASE_URL).strip().rstrip("/")
    suffixes = (
        "/api/comfy-zmongo",
        "/comfy_zmongo",
        "/comfy-zmongo",
        "/user/manager/api",
        "/user/manager",
        "/user/api-manager",
        "/comfy-zmongo-fleet",
        "/fleet",
        "/api/fleet",
        "/user/login",
        "/user/dashboard",
        "/user/settings",
        "/user/profile",
        "/user",
    )

    changed = True
    while changed:
        changed = False
        for suffix in suffixes:
            if base.endswith(suffix):
                base = base[: -len(suffix)].rstrip("/")
                changed = True

    return base or DEFAULT_BASE_URL


def _clean_prefix(value: str, default: str) -> str:
    cleaned = (value or default).strip().rstrip("/")
    if not cleaned.startswith("/"):
        cleaned = f"/{cleaned}"
    return cleaned or default


def _parse_json_object(value: str, field_name: str = "json") -> dict[str, Any]:
    text = (value or "").strip()
    if not text:
        return {}

    parsed = json.loads(text)
    if not isinstance(parsed, dict):
        raise ValueError(f"{field_name} must be a JSON object.")
    return parsed


def _parse_json_list(value: str, field_name: str = "json") -> list[Any]:
    text = (value or "").strip()
    if not text:
        return []

    parsed = json.loads(text)
    if not isinstance(parsed, list):
        raise ValueError(f"{field_name} must be a JSON list.")
    return parsed


def _parse_any_json(value: str, parse_json: bool = True) -> Any:
    if not parse_json:
        return value

    text = (value or "").strip()
    if not text:
        return ""

    try:
        return json.loads(text)
    except json.JSONDecodeError:
        return value


def _as_comfy_list(values: Any) -> list[Any]:
    if values is None:
        return []
    if isinstance(values, list):
        return values
    if isinstance(values, tuple):
        return list(values)
    return [values]


def _indexed_list_text(values: list[Any]) -> str:
    return _json_text([f"{index}: {value}" for index, value in enumerate(values)])


def safe_get_by_path(obj: Any, path: str, default: Any = None) -> Any:
    if not path:
        return obj

    current = obj
    for part in path.split("."):
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


def _extract_documents(payload: dict[str, Any]) -> list[dict[str, Any]]:
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

        if "_id" in data:
            return [data]

    if isinstance(data, list):
        return [item for item in data if isinstance(item, dict)]

    return []


def _extract_document_from_payload(payload: dict[str, Any]) -> dict[str, Any]:
    docs = _extract_documents(payload)
    return docs[0] if docs else {}


def _extract_doc_ids(payload: dict[str, Any]) -> list[str]:
    ids: list[str] = []
    for doc in _extract_documents(payload):
        doc_id = doc.get("_id") or doc.get("id") or doc.get("document_id")
        if doc_id is not None:
            ids.append(str(doc_id))
    return ids


def _extract_collections(payload: dict[str, Any]) -> list[str]:
    data = payload.get("data", {}) if isinstance(payload, dict) else {}

    if isinstance(data, dict):
        collections = data.get("collections") or data.get("collection_names")
        if isinstance(collections, list):
            return [str(item) for item in collections]

    if isinstance(data, list):
        return [str(item) for item in data]

    return []


def _extract_count(payload: dict[str, Any]) -> int:
    data = payload.get("data", {}) if isinstance(payload, dict) else {}

    if isinstance(data, dict):
        for key in ("count", "document_count", "total", "matched_count", "modified_count", "deleted_count"):
            if key in data:
                try:
                    return int(data.get(key) or 0)
                except Exception:
                    return 0

    return 0


class AlwaysDirtyMixin:
    @classmethod
    def IS_CHANGED(cls, *args, **kwargs):
        return _dirty_token(cls.__name__)


# -----------------------------------------------------------------------------
# Image helpers
# -----------------------------------------------------------------------------

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


# -----------------------------------------------------------------------------
# HTTP client
# -----------------------------------------------------------------------------

class ZMongoApiSession:
    def __init__(
        self,
        *,
        base_url: str = DEFAULT_BASE_URL,
        zai_api_key: str = "",
        username: str = "",
        comfy_zmongo_prefix: str = DEFAULT_COMFY_ZMONGO_PREFIX,
        fleet_prefix: str = DEFAULT_FLEET_PREFIX,
        comfy_zmongo_fleet_prefix: str = DEFAULT_COMFY_ZMONGO_FLEET_PREFIX,
        timeout: int = DEFAULT_TIMEOUT,
        verify_tls: bool = True,
    ) -> None:
        self.base_url = _normalize_base_url(base_url)
        self.zai_api_key = (zai_api_key or os.getenv("ZAI_API_KEY", "")).strip()
        self.username = (username or os.getenv("ZTAROT_USERNAME", "")).strip()
        self.comfy_zmongo_prefix = _clean_prefix(comfy_zmongo_prefix, DEFAULT_COMFY_ZMONGO_PREFIX)
        self.fleet_prefix = _clean_prefix(fleet_prefix, DEFAULT_FLEET_PREFIX)
        self.comfy_zmongo_fleet_prefix = _clean_prefix(comfy_zmongo_fleet_prefix, DEFAULT_COMFY_ZMONGO_FLEET_PREFIX)
        self.timeout = max(1, int(timeout or DEFAULT_TIMEOUT))
        self.verify_tls = bool(verify_tls)
        self.session = requests.Session()

    def close(self) -> None:
        self.session.close()

    def _headers(self, *, json_content: bool = True) -> dict[str, str]:
        headers = {
            "Accept": "application/json",
            "User-Agent": "comfyui-zmongo-api-nodes/1.0",
            "Origin": self.base_url,
        }

        if json_content:
            headers["Content-Type"] = "application/json"

        if self.zai_api_key:
            headers["ZAI_API_KEY"] = self.zai_api_key
            headers["Authorization"] = f"Bearer {self.zai_api_key}"

        if self.username:
            headers["X-AGENT-USERNAME"] = self.username

        return headers

    def _normalize_response(self, response: requests.Response) -> dict[str, Any]:
        content_type = (response.headers.get("Content-Type") or "").lower()

        try:
            if "application/json" in content_type:
                payload = response.json()
            else:
                payload = {
                    "success": response.ok,
                    "message": response.reason or ("OK" if response.ok else "Request failed"),
                    "data": {},
                    "error": None if response.ok else {"msg": "Non-JSON response"},
                    "raw_text": response.text,
                }
        except Exception:
            payload = {
                "success": False,
                "message": "Response was not valid JSON.",
                "data": {},
                "error": {"msg": "Response was not valid JSON."},
                "raw_text": response.text,
            }

        payload = _ensure_payload_dict(payload)
        payload["status_code"] = response.status_code

        if payload.get("message") == "" and response.ok:
            payload["message"] = "OK"

        return payload

    def request(
        self,
        method: str,
        prefix: str,
        path: str,
        *,
        json_body: Optional[dict[str, Any]] = None,
        params: Optional[dict[str, Any]] = None,
    ) -> dict[str, Any]:
        normalized_path = path if path.startswith("/") else f"/{path}"
        url = f"{self.base_url}{prefix}{normalized_path}"

        try:
            response = self.session.request(
                method=method.upper(),
                url=url,
                headers=self._headers(json_content=json_body is not None),
                json=json_body,
                params=params,
                timeout=self.timeout,
                verify=self.verify_tls,
                allow_redirects=True,
            )
            return self._normalize_response(response)
        except requests.RequestException as exc:
            return _ensure_payload_dict(
                {
                    "success": False,
                    "message": f"Request failed: {exc}",
                    "data": {},
                    "error": {"msg": str(exc), "type": exc.__class__.__name__},
                    "status_code": 0,
                }
            )

    def request_bytes(
        self,
        method: str,
        prefix: str,
        path: str,
        *,
        params: Optional[dict[str, Any]] = None,
        accept: str = "image/*,*/*",
        extra_headers: Optional[dict[str, str]] = None,
    ) -> tuple[bytes, int, str]:
        normalized_path = path if path.startswith("/") else f"/{path}"
        url = f"{self.base_url}{prefix}{normalized_path}"
        headers = self._headers(json_content=False)
        headers["Accept"] = accept
        if extra_headers:
            headers.update({k: v for k, v in extra_headers.items() if v})

        response = self.session.request(
            method=method.upper(),
            url=url,
            headers=headers,
            params=params,
            timeout=self.timeout,
            verify=self.verify_tls,
            allow_redirects=True,
        )
        response.raise_for_status()
        return response.content, response.status_code, response.headers.get("Content-Type", "")

    def fetch_absolute_or_relative_bytes(self, url: str) -> bytes:
        target = url.strip()
        if target.startswith("/"):
            target = f"{self.base_url}{target}"

        response = self.session.get(
            target,
            headers=self._headers(json_content=False),
            timeout=self.timeout,
            verify=self.verify_tls,
            allow_redirects=True,
        )
        response.raise_for_status()
        return response.content

    # ------------------------------------------------------------------
    # Canonical Comfy-ZMongo routes
    # ------------------------------------------------------------------

    def health(self) -> dict[str, Any]:
        return self.request("GET", self.comfy_zmongo_prefix, "/api/health")

    def whoami(self) -> dict[str, Any]:
        return self.request("GET", self.comfy_zmongo_prefix, "/api/whoami")

    def list_collections(self) -> dict[str, Any]:
        return self.request("GET", self.comfy_zmongo_prefix, "/api/collections")

    def create_collection(self, collection: str) -> dict[str, Any]:
        return self.request(
            "POST",
            self.comfy_zmongo_prefix,
            "/api/collection/create",
            json_body={"name": collection},
        )

    def delete_collection(self, collection: str) -> dict[str, Any]:
        return self.request(
            "POST",
            self.comfy_zmongo_prefix,
            "/api/collection/delete",
            json_body={"name": collection},
        )

    def list_docs(
        self,
        *,
        collection: str,
        limit: int = 50,
        skip: int = 0,
        query: Optional[dict[str, Any]] = None,
    ) -> dict[str, Any]:
        quoted = urllib.parse.quote(collection, safe="")
        params: dict[str, Any] = {
            "limit": max(1, min(int(limit or 50), 500)),
            "skip": max(0, int(skip or 0)),
        }
        if query:
            params["query_json"] = json.dumps(query, ensure_ascii=False, default=str)

        return self.request("GET", self.comfy_zmongo_prefix, f"/api/docs/{quoted}", params=params)

    def get_doc(self, *, collection: str, document_id: str, cache: bool = False) -> dict[str, Any]:
        quoted_coll = urllib.parse.quote(collection, safe="")
        quoted_doc = urllib.parse.quote(document_id, safe="")
        return self.request(
            "GET",
            self.comfy_zmongo_prefix,
            f"/api/doc/{quoted_coll}/{quoted_doc}",
            params={"cache": "true" if cache else "false"},
        )

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
        body: dict[str, Any] = {
            "collection": collection,
            "query": query or {},
            "many": bool(many),
            "limit": max(1, min(int(limit or 50), 500)),
            "skip": max(0, int(skip or 0)),
            "cache": bool(cache),
        }

        if document_id:
            body["document_id"] = document_id
        if projection:
            body["projection"] = projection
        if sort:
            body["sort"] = sort

        return self.request("POST", self.comfy_zmongo_prefix, "/api/query", json_body=body)

    def count_docs(
        self,
        *,
        collection: str,
        query: Optional[dict[str, Any]] = None,
        document_id: str = "",
        cache: bool = False,
    ) -> dict[str, Any]:
        body: dict[str, Any] = {"collection": collection, "query": query or {}, "cache": bool(cache)}
        if document_id:
            body["document_id"] = document_id
        return self.request("POST", self.comfy_zmongo_prefix, "/api/count", json_body=body)

    def create_doc(self, *, collection: str, document: dict[str, Any]) -> dict[str, Any]:
        return self.request(
            "POST",
            self.comfy_zmongo_prefix,
            "/api/doc/create",
            json_body={"collection": collection, "document": document},
        )

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
        body: dict[str, Any] = {"collection": collection, "query": query or {}, "upsert": bool(upsert)}

        if document_id:
            body["document_id"] = document_id

        if update is not None:
            body["update"] = update
        else:
            body["field_path"] = field_path
            body["value"] = value

        return self.request("POST", self.comfy_zmongo_prefix, "/api/doc/update", json_body=body)

    def delete_doc(self, *, collection: str, query: Optional[dict[str, Any]] = None, document_id: str = "") -> dict[str, Any]:
        body: dict[str, Any] = {"collection": collection, "query": query or {}}
        if document_id:
            body["document_id"] = document_id
        return self.request("POST", self.comfy_zmongo_prefix, "/api/doc/delete", json_body=body)

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
        body: dict[str, Any] = {
            "collection": collection,
            "query": query or {},
            "field_path": field_path,
            "value": value,
            "upsert_if_missing": bool(upsert_if_missing),
            "parse_json_strings": bool(parse_json_strings),
            "normalize_for_storage": bool(normalize_for_storage),
        }
        if document_id:
            body["document_id"] = document_id
        return self.request("POST", self.comfy_zmongo_prefix, "/api/save-value", json_body=body)

    def fetch_image_field(
        self,
        *,
        collection: str,
        document_id: str,
        field_path: str,
        master_key_hex: str = "",
    ) -> tuple[bytes, str]:
        """
        Fetch an image through the actual server routes.

        ComfyZMongoRoutes registers:
            GET <comfy_zmongo_prefix>/api/image/<coll>/<doc_id>?field=<field_path>

        ManagerRoutes registers browser/dashboard compatibility routes:
            GET /user/manager/api/image-field/view/<coll>/<doc_id>?field_path=<field_path>

        The Comfy route is tried first because this file is the ComfyUI node
        client. The manager route is only a compatibility fallback.
        """
        quoted_coll = urllib.parse.quote(collection, safe="")
        quoted_doc = urllib.parse.quote(document_id, safe="")
        clean_field = _route_image_field_path(field_path, "image_data")

        extra_headers = {"X-Master-Key": (master_key_hex or os.getenv("ZAI_MASTER_KEY") or os.getenv("ZMONGO_KEY") or "").strip()}

        attempts: list[tuple[str, str, dict[str, Any]]] = [
            (
                self.comfy_zmongo_prefix,
                f"/api/image/{quoted_coll}/{quoted_doc}",
                {"field": clean_field},
            ),
            (
                "/user/manager",
                f"/api/image-field/view/{quoted_coll}/{quoted_doc}",
                {"field_path": clean_field},
            ),
        ]

        errors: list[str] = []
        for prefix, path, params in attempts:
            try:
                data, _, content_type = self.request_bytes("GET", prefix, path, params=params, extra_headers=extra_headers)
                if data:
                    return data, f"route:{prefix}{path}; params:{params}; content_type:{content_type}"
                errors.append(f"{prefix}{path}: empty response body")
            except Exception as exc:
                errors.append(f"{prefix}{path}: {exc}")

        raise ValueError("Image route fetch failed: " + " | ".join(errors))

    # ------------------------------------------------------------------
    # Fleet routes
    # ------------------------------------------------------------------

    def fleet_status(self) -> dict[str, Any]:
        return self.request("GET", self.fleet_prefix, "/status")

    def fleet_agents(self) -> dict[str, Any]:
        return self.request("GET", self.fleet_prefix, "/agents")

    def fleet_dispatch(
        self,
        *,
        intent: str,
        payload: dict[str, Any],
        dispatch_id: str = "",
        timeout: float = 60.0,
        cost_usd: str = "",
    ) -> dict[str, Any]:
        body: dict[str, Any] = {"intent": intent, "payload": payload, "timeout": timeout}
        if dispatch_id:
            body["dispatch_id"] = dispatch_id
        if cost_usd:
            body["cost_usd"] = cost_usd
        return self.request("POST", self.fleet_prefix, "/dispatch", json_body=body)

    def fleet_send_chat(self, *, message: str, dispatch_id: str = "", timeout: float = 60.0, cost_usd: str = "") -> dict[str, Any]:
        body: dict[str, Any] = {"message": message, "timeout": timeout}
        if dispatch_id:
            body["dispatch_id"] = dispatch_id
        if cost_usd:
            body["cost_usd"] = cost_usd
        return self.request("POST", self.fleet_prefix, "/send-chat", json_body=body)

    # ------------------------------------------------------------------
    # Comfy-ZMongo fleet inspection / dispatch routes
    # ------------------------------------------------------------------

    def comfy_fleet_ping(self) -> dict[str, Any]:
        return self.request("GET", self.comfy_zmongo_fleet_prefix, "/ping")

    def comfy_fleet_connections(self) -> dict[str, Any]:
        return self.request("GET", self.comfy_zmongo_fleet_prefix, "/connections")

    def comfy_fleet_connection(self, connection_id: str) -> dict[str, Any]:
        quoted = urllib.parse.quote(connection_id, safe="")
        return self.request("GET", self.comfy_zmongo_fleet_prefix, f"/connections/{quoted}")

    def comfy_fleet_stats(self) -> dict[str, Any]:
        return self.request("GET", self.comfy_zmongo_fleet_prefix, "/stats")

    def comfy_fleet_dispatch(
        self,
        *,
        connection_id: str = "",
        message_type: str = "dispatch",
        payload: Optional[dict[str, Any]] = None,
    ) -> dict[str, Any]:
        body: dict[str, Any] = {"type": message_type or "dispatch", "payload": payload or {}}
        if connection_id:
            body["connection_id"] = connection_id
        return self.request("POST", self.comfy_zmongo_fleet_prefix, "/dispatch", json_body=body)


# -----------------------------------------------------------------------------
# 00 Auth nodes
# -----------------------------------------------------------------------------

class ZMongoApiKeySessionNode(AlwaysDirtyMixin):
    @classmethod
    def INPUT_TYPES(cls):
        return {
            "required": {
                "base_url": ("STRING", {"default": DEFAULT_BASE_URL}),
                "zai_api_key": ("STRING", {"default": "", "multiline": False}),
                "username": ("STRING", {"default": ""}),
                "comfy_zmongo_prefix": ("STRING", {"default": DEFAULT_COMFY_ZMONGO_PREFIX}),
                "fleet_prefix": ("STRING", {"default": DEFAULT_FLEET_PREFIX}),
                "comfy_zmongo_fleet_prefix": ("STRING", {"default": DEFAULT_COMFY_ZMONGO_FLEET_PREFIX}),
                "timeout_seconds": ("INT", {"default": DEFAULT_TIMEOUT, "min": 1, "max": 300}),
                "verify_tls": ("BOOLEAN", {"default": True}),
                "test_whoami": ("BOOLEAN", {"default": True}),
            }
        }

    RETURN_TYPES = ("ZMONGO_API_SESSION", "STRING", "STRING")
    RETURN_NAMES = ("session", "json", "status")
    FUNCTION = "connect"
    CATEGORY = "ZMongo/API/00 Auth"

    def connect(
        self,
        base_url: str,
        zai_api_key: str,
        username: str,
        comfy_zmongo_prefix: str,
        fleet_prefix: str,
        comfy_zmongo_fleet_prefix: str,
        timeout_seconds: int,
        verify_tls: bool,
        test_whoami: bool,
    ):
        try:
            session = ZMongoApiSession(
                base_url=base_url,
                zai_api_key=zai_api_key,
                username=username,
                comfy_zmongo_prefix=comfy_zmongo_prefix,
                fleet_prefix=fleet_prefix,
                comfy_zmongo_fleet_prefix=comfy_zmongo_fleet_prefix,
                timeout=timeout_seconds,
                verify_tls=verify_tls,
            )

            if test_whoami:
                payload = session.whoami()
                status = payload.get("message") or "API session created."
                return (session, _json_text(payload), status)

            payload = _success_payload(
                "API session created.",
                {
                    "base_url": session.base_url,
                    "username": session.username,
                    "comfy_zmongo_prefix": session.comfy_zmongo_prefix,
                    "fleet_prefix": session.fleet_prefix,
                    "comfy_zmongo_fleet_prefix": session.comfy_zmongo_fleet_prefix,
                },
            )
            return (session, _json_text(payload), "API session created.")
        except Exception as exc:
            payload = _error_payload(str(exc))
            return (None, _json_text(payload), f"API session failed: {exc}")


class ZMongoApiCloseSessionNode(AlwaysDirtyMixin):
    @classmethod
    def INPUT_TYPES(cls):
        return {"required": {"session": ("ZMONGO_API_SESSION",)}}

    RETURN_TYPES = ("STRING",)
    RETURN_NAMES = ("json",)
    FUNCTION = "close_session"
    CATEGORY = "ZMongo/API/00 Auth"

    def close_session(self, session):
        if session is None:
            return (_json_text(_error_payload("No session provided.")),)

        try:
            session.close()
            return (_json_text(_success_payload("Session closed.")),)
        except Exception as exc:
            return (_json_text(_error_payload(str(exc))),)


# -----------------------------------------------------------------------------
# 01 Service nodes
# -----------------------------------------------------------------------------

class ZMongoApiHealthNode(AlwaysDirtyMixin):
    @classmethod
    def INPUT_TYPES(cls):
        return {
            "required": {"session": ("ZMONGO_API_SESSION",)},
            "optional": {"refresh_token": ("STRING", {"default": ""})},
        }

    RETURN_TYPES = ("STRING", "BOOLEAN")
    RETURN_NAMES = ("json", "success")
    FUNCTION = "health"
    CATEGORY = "ZMongo/API/01 Service"

    def health(self, session, refresh_token: str = ""):
        if session is None:
            payload = _error_payload("No session provided.")
            return (_json_text(payload), False)
        payload = session.health()
        return (_json_text(payload), bool(payload.get("success")))


class ZMongoApiWhoamiNode(AlwaysDirtyMixin):
    @classmethod
    def INPUT_TYPES(cls):
        return {
            "required": {"session": ("ZMONGO_API_SESSION",)},
            "optional": {"refresh_token": ("STRING", {"default": ""})},
        }

    RETURN_TYPES = ("STRING", "STRING", "STRING", "BOOLEAN")
    RETURN_NAMES = ("json", "username", "db_name", "success")
    FUNCTION = "whoami"
    CATEGORY = "ZMongo/API/01 Service"

    def whoami(self, session, refresh_token: str = ""):
        if session is None:
            payload = _error_payload("No session provided.")
            return (_json_text(payload), "", "", False)

        payload = session.whoami()
        data = payload.get("data", {}) if isinstance(payload, dict) else {}
        username = str(data.get("username") or "") if isinstance(data, dict) else ""
        db_name = str(data.get("silo_db_name") or data.get("db_name") or "") if isinstance(data, dict) else ""
        return (_json_text(payload), username, db_name, bool(payload.get("success")))


# -----------------------------------------------------------------------------
# 02 Collections nodes
# -----------------------------------------------------------------------------

class ZMongoApiListCollectionsNode(AlwaysDirtyMixin):
    @classmethod
    def INPUT_TYPES(cls):
        return {
            "required": {"session": ("ZMONGO_API_SESSION",)},
            "optional": {"refresh_token": ("STRING", {"default": ""})},
        }

    RETURN_TYPES = ("STRING", "*", "STRING")
    RETURN_NAMES = ("json", "collections", "indexed")
    OUTPUT_IS_LIST = (False, True, False)
    FUNCTION = "list_collections"
    CATEGORY = "ZMongo/API/02 Collections"

    def list_collections(self, session, refresh_token: str = ""):
        if session is None:
            payload = _error_payload("No session provided.")
            return (_json_text(payload), [], _indexed_list_text([]))

        payload = session.list_collections()
        collections = _extract_collections(payload)
        return (_json_text(payload), _as_comfy_list(collections), _indexed_list_text(collections))


class ZMongoApiCreateCollectionNode(AlwaysDirtyMixin):
    @classmethod
    def INPUT_TYPES(cls):
        return {"required": {"session": ("ZMONGO_API_SESSION",), "collection_name": ("STRING", {"default": ""})}}

    RETURN_TYPES = ("STRING", "STRING")
    RETURN_NAMES = ("json", "refresh")
    FUNCTION = "create_collection"
    CATEGORY = "ZMongo/API/02 Collections"

    def create_collection(self, session, collection_name: str):
        token = _dirty_token("create_collection", collection_name)
        if session is None:
            return (_json_text(_error_payload("No session provided.")), token)
        payload = session.create_collection((collection_name or "").strip())
        return (_json_text(payload), token)


class ZMongoApiDeleteCollectionNode(AlwaysDirtyMixin):
    @classmethod
    def INPUT_TYPES(cls):
        return {
            "required": {
                "session": ("ZMONGO_API_SESSION",),
                "collection_name": ("STRING", {"default": ""}),
                "confirm_collection_name": ("STRING", {"default": ""}),
            }
        }

    RETURN_TYPES = ("STRING", "STRING")
    RETURN_NAMES = ("json", "refresh")
    FUNCTION = "delete_collection"
    CATEGORY = "ZMongo/API/02 Collections"

    def delete_collection(self, session, collection_name: str, confirm_collection_name: str):
        token = _dirty_token("delete_collection", collection_name)
        if session is None:
            return (_json_text(_error_payload("No session provided.")), token)

        cleaned = (collection_name or "").strip()
        confirmed = (confirm_collection_name or "").strip()
        if not cleaned or cleaned != confirmed:
            payload = _error_payload("Collection deletion requires matching collection_name and confirm_collection_name.")
            return (_json_text(payload), token)

        payload = session.delete_collection(cleaned)
        return (_json_text(payload), token)


# -----------------------------------------------------------------------------
# 03 Document nodes
# -----------------------------------------------------------------------------

class ZMongoApiListDocsNode(AlwaysDirtyMixin):
    @classmethod
    def INPUT_TYPES(cls):
        return {
            "required": {
                "session": ("ZMONGO_API_SESSION",),
                "collection_name": ("STRING", {"default": ""}),
                "query_json": ("STRING", {"default": "{}", "multiline": True}),
                "limit": ("INT", {"default": 50, "min": 1, "max": 500}),
                "skip": ("INT", {"default": 0, "min": 0, "max": 1000000}),
            },
            "optional": {"refresh_token": ("STRING", {"default": ""})},
        }

    RETURN_TYPES = ("STRING", "*", "STRING")
    RETURN_NAMES = ("json", "ids", "indexed")
    OUTPUT_IS_LIST = (False, True, False)
    FUNCTION = "list_docs"
    CATEGORY = "ZMongo/API/03 Docs"

    def list_docs(self, session, collection_name: str, query_json: str, limit: int, skip: int, refresh_token: str = ""):
        if session is None:
            payload = _error_payload("No session provided.")
            return (_json_text(payload), [], _indexed_list_text([]))

        try:
            query = _parse_json_object(query_json, "query_json")
            payload = session.list_docs(collection=(collection_name or "").strip(), limit=limit, skip=skip, query=query)
            ids = _extract_doc_ids(payload)
            return (_json_text(payload), _as_comfy_list(ids), _indexed_list_text(ids))
        except Exception as exc:
            payload = _error_payload(str(exc))
            return (_json_text(payload), [], _indexed_list_text([]))


class ZMongoApiGetDocNode(AlwaysDirtyMixin):
    @classmethod
    def INPUT_TYPES(cls):
        return {
            "required": {
                "session": ("ZMONGO_API_SESSION",),
                "collection_name": ("STRING", {"default": ""}),
                "document_id": ("STRING", {"default": ""}),
                "cache": ("BOOLEAN", {"default": False}),
            },
            "optional": {"refresh_token": ("STRING", {"default": ""})},
        }

    RETURN_TYPES = ("STRING", "BOOLEAN")
    RETURN_NAMES = ("json", "success")
    FUNCTION = "get_doc"
    CATEGORY = "ZMongo/API/03 Docs"

    def get_doc(self, session, collection_name: str, document_id: str, cache: bool, refresh_token: str = ""):
        if session is None:
            payload = _error_payload("No session provided.")
            return (_json_text(payload), False)

        payload = session.get_doc(collection=(collection_name or "").strip(), document_id=(document_id or "").strip(), cache=cache)
        return (_json_text(payload), bool(payload.get("success")))


class ZMongoApiQueryDocsNode(AlwaysDirtyMixin):
    @classmethod
    def INPUT_TYPES(cls):
        return {
            "required": {
                "session": ("ZMONGO_API_SESSION",),
                "collection_name": ("STRING", {"default": ""}),
                "query_json": ("STRING", {"default": "{}", "multiline": True}),
                "document_id": ("STRING", {"default": ""}),
                "many": ("BOOLEAN", {"default": True}),
                "limit": ("INT", {"default": 50, "min": 1, "max": 500}),
                "skip": ("INT", {"default": 0, "min": 0, "max": 1000000}),
                "projection_json": ("STRING", {"default": "{}", "multiline": True}),
                "sort_json": ("STRING", {"default": "[]", "multiline": True}),
                "cache": ("BOOLEAN", {"default": False}),
            },
            "optional": {"refresh_token": ("STRING", {"default": ""})},
        }

    RETURN_TYPES = ("STRING", "*", "STRING")
    RETURN_NAMES = ("json", "ids", "indexed")
    OUTPUT_IS_LIST = (False, True, False)
    FUNCTION = "query_docs"
    CATEGORY = "ZMongo/API/03 Docs"

    def query_docs(
        self,
        session,
        collection_name: str,
        query_json: str,
        document_id: str,
        many: bool,
        limit: int,
        skip: int,
        projection_json: str,
        sort_json: str,
        cache: bool,
        refresh_token: str = "",
    ):
        if session is None:
            payload = _error_payload("No session provided.")
            return (_json_text(payload), [], _indexed_list_text([]))

        try:
            query = _parse_json_object(query_json, "query_json")
            projection = _parse_json_object(projection_json, "projection_json")
            sort = _parse_json_list(sort_json, "sort_json")
            payload = session.query_docs(
                collection=(collection_name or "").strip(),
                query=query,
                document_id=(document_id or "").strip(),
                many=many,
                limit=limit,
                skip=skip,
                projection=projection,
                sort=sort,
                cache=cache,
            )
            ids = _extract_doc_ids(payload)
            return (_json_text(payload), _as_comfy_list(ids), _indexed_list_text(ids))
        except Exception as exc:
            payload = _error_payload(str(exc))
            return (_json_text(payload), [], _indexed_list_text([]))


class ZMongoApiCountDocsNode(AlwaysDirtyMixin):
    @classmethod
    def INPUT_TYPES(cls):
        return {
            "required": {
                "session": ("ZMONGO_API_SESSION",),
                "collection_name": ("STRING", {"default": ""}),
                "query_json": ("STRING", {"default": "{}", "multiline": True}),
                "document_id": ("STRING", {"default": ""}),
                "cache": ("BOOLEAN", {"default": False}),
            },
            "optional": {"refresh_token": ("STRING", {"default": ""})},
        }

    RETURN_TYPES = ("STRING", "INT")
    RETURN_NAMES = ("json", "count")
    FUNCTION = "count_docs"
    CATEGORY = "ZMongo/API/03 Docs"

    def count_docs(self, session, collection_name: str, query_json: str, document_id: str, cache: bool, refresh_token: str = ""):
        if session is None:
            payload = _error_payload("No session provided.")
            return (_json_text(payload), 0)

        try:
            query = _parse_json_object(query_json, "query_json")
            payload = session.count_docs(
                collection=(collection_name or "").strip(),
                query=query,
                document_id=(document_id or "").strip(),
                cache=cache,
            )
            return (_json_text(payload), _extract_count(payload))
        except Exception as exc:
            payload = _error_payload(str(exc))
            return (_json_text(payload), 0)


class ZMongoApiCreateDocNode(AlwaysDirtyMixin):
    @classmethod
    def INPUT_TYPES(cls):
        return {
            "required": {
                "session": ("ZMONGO_API_SESSION",),
                "collection_name": ("STRING", {"default": ""}),
                "document_json": ("STRING", {"default": "{}", "multiline": True}),
            }
        }

    RETURN_TYPES = ("STRING", "STRING", "STRING")
    RETURN_NAMES = ("json", "document_id", "refresh")
    FUNCTION = "create_doc"
    CATEGORY = "ZMongo/API/03 Docs"

    def create_doc(self, session, collection_name: str, document_json: str):
        token = _dirty_token("create_doc", collection_name)
        if session is None:
            return (_json_text(_error_payload("No session provided.")), "", token)

        try:
            document = _parse_json_object(document_json, "document_json")
            payload = session.create_doc(collection=(collection_name or "").strip(), document=document)
            data = payload.get("data", {}) if isinstance(payload, dict) else {}
            document_id = ""
            if isinstance(data, dict):
                document_id = str(data.get("document_id") or data.get("inserted_id") or data.get("_id") or "")
            return (_json_text(payload), document_id, token)
        except Exception as exc:
            payload = _error_payload(str(exc))
            return (_json_text(payload), "", token)


class ZMongoApiUpdateDocNode(AlwaysDirtyMixin):
    @classmethod
    def INPUT_TYPES(cls):
        return {
            "required": {
                "session": ("ZMONGO_API_SESSION",),
                "collection_name": ("STRING", {"default": ""}),
                "query_json": ("STRING", {"default": "{}", "multiline": True}),
                "document_id": ("STRING", {"default": ""}),
                "update_json": ("STRING", {"default": "", "multiline": True}),
                "field_path": ("STRING", {"default": ""}),
                "value_json": ("STRING", {"default": "", "multiline": True}),
                "parse_value_json": ("BOOLEAN", {"default": True}),
                "upsert": ("BOOLEAN", {"default": False}),
            }
        }

    RETURN_TYPES = ("STRING", "STRING")
    RETURN_NAMES = ("json", "refresh")
    FUNCTION = "update_doc"
    CATEGORY = "ZMongo/API/03 Docs"

    def update_doc(
        self,
        session,
        collection_name: str,
        query_json: str,
        document_id: str,
        update_json: str,
        field_path: str,
        value_json: str,
        parse_value_json: bool,
        upsert: bool,
    ):
        token = _dirty_token("update_doc", collection_name, document_id, field_path)
        if session is None:
            return (_json_text(_error_payload("No session provided.")), token)

        try:
            query = _parse_json_object(query_json, "query_json")
            cleaned_update_json = (update_json or "").strip()
            if cleaned_update_json:
                update = _parse_json_object(cleaned_update_json, "update_json")
                payload = session.update_doc(
                    collection=(collection_name or "").strip(),
                    query=query,
                    document_id=(document_id or "").strip(),
                    update=update,
                    upsert=upsert,
                )
            else:
                value = _parse_any_json(value_json, parse_value_json)
                payload = session.update_doc(
                    collection=(collection_name or "").strip(),
                    query=query,
                    document_id=(document_id or "").strip(),
                    field_path=(field_path or "").strip(),
                    value=value,
                    upsert=upsert,
                )
            return (_json_text(payload), token)
        except Exception as exc:
            payload = _error_payload(str(exc))
            return (_json_text(payload), token)


class ZMongoApiDeleteDocNode(AlwaysDirtyMixin):
    @classmethod
    def INPUT_TYPES(cls):
        return {
            "required": {
                "session": ("ZMONGO_API_SESSION",),
                "collection_name": ("STRING", {"default": ""}),
                "query_json": ("STRING", {"default": "{}", "multiline": True}),
                "document_id": ("STRING", {"default": ""}),
                "confirm_document_id": ("STRING", {"default": ""}),
            }
        }

    RETURN_TYPES = ("STRING", "STRING")
    RETURN_NAMES = ("json", "refresh")
    FUNCTION = "delete_doc"
    CATEGORY = "ZMongo/API/03 Docs"

    def delete_doc(self, session, collection_name: str, query_json: str, document_id: str, confirm_document_id: str):
        token = _dirty_token("delete_doc", collection_name, document_id)
        if session is None:
            return (_json_text(_error_payload("No session provided.")), token)

        try:
            query = _parse_json_object(query_json, "query_json")
            cleaned_id = (document_id or "").strip()
            confirmed_id = (confirm_document_id or "").strip()
            if cleaned_id and cleaned_id != confirmed_id:
                payload = _error_payload("Document deletion by document_id requires matching confirm_document_id.")
                return (_json_text(payload), token)
            if not cleaned_id and not query:
                payload = _error_payload("Delete requires document_id or non-empty query_json.")
                return (_json_text(payload), token)
            payload = session.delete_doc(collection=(collection_name or "").strip(), query=query, document_id=cleaned_id)
            return (_json_text(payload), token)
        except Exception as exc:
            payload = _error_payload(str(exc))
            return (_json_text(payload), token)


class ZMongoApiSaveValueNode(AlwaysDirtyMixin):
    @classmethod
    def INPUT_TYPES(cls):
        return {
            "required": {
                "session": ("ZMONGO_API_SESSION",),
                "collection_name": ("STRING", {"default": ""}),
                "query_json": ("STRING", {"default": "{}", "multiline": True}),
                "document_id": ("STRING", {"default": ""}),
                "field_path": ("STRING", {"default": ""}),
                "value_json": ("STRING", {"default": "", "multiline": True}),
                "parse_value_json": ("BOOLEAN", {"default": True}),
                "upsert_if_missing": ("BOOLEAN", {"default": True}),
                "parse_json_strings": ("BOOLEAN", {"default": True}),
                "normalize_for_storage": ("BOOLEAN", {"default": False}),
            }
        }

    RETURN_TYPES = ("STRING", "STRING")
    RETURN_NAMES = ("json", "refresh")
    FUNCTION = "save_value"
    CATEGORY = "ZMongo/API/03 Docs"

    def save_value(
        self,
        session,
        collection_name: str,
        query_json: str,
        document_id: str,
        field_path: str,
        value_json: str,
        parse_value_json: bool,
        upsert_if_missing: bool,
        parse_json_strings: bool,
        normalize_for_storage: bool,
    ):
        token = _dirty_token("save_value", collection_name, field_path)
        if session is None:
            return (_json_text(_error_payload("No session provided.")), token)

        try:
            query = _parse_json_object(query_json, "query_json")
            value = _parse_any_json(value_json, parse_value_json)
            payload = session.save_value(
                collection=(collection_name or "").strip(),
                query=query,
                document_id=(document_id or "").strip(),
                field_path=(field_path or "").strip(),
                value=value,
                upsert_if_missing=upsert_if_missing,
                parse_json_strings=parse_json_strings,
                normalize_for_storage=normalize_for_storage,
            )
            return (_json_text(payload), token)
        except Exception as exc:
            payload = _error_payload(str(exc))
            return (_json_text(payload), token)


# -----------------------------------------------------------------------------
# 04 Image nodes
# -----------------------------------------------------------------------------

class ZMongoDisplayImageNode(AlwaysDirtyMixin):
    @classmethod
    def INPUT_TYPES(cls):
        return {
            "required": {
                "session": ("ZMONGO_API_SESSION",),
                "collection_name": ("STRING", {"default": ""}),
                "document_id": ("STRING", {"default": ""}),
                "field_path": ("STRING", {"default": "image_data"}),
            },
            "optional": {
                "master_key_hex": ("STRING", {"default": "", "multiline": False}),
                "cache": ("BOOLEAN", {"default": False}),
                "refresh_token": ("STRING", {"default": ""}),
            },
        }

    RETURN_TYPES = ("IMAGE", "STRING", "STRING")
    RETURN_NAMES = ("image", "status", "json")
    FUNCTION = "display_image"
    CATEGORY = "ZMongo/API/04 Images"

    def _open_image_bytes(self, image_bytes: bytes) -> tuple[torch.Tensor, Image.Image]:
        if not image_bytes:
            raise ValueError("Image bytes were empty.")
        if image_bytes[:1] in (b"{", b"["):
            raise ValueError(f"Got JSON/text instead of image bytes: {image_bytes[:500]!r}")
        image = Image.open(io.BytesIO(image_bytes))
        return _pil_to_comfy_image(image), image

    @staticmethod
    def _safe_preview_text(value: Any, max_chars: int = 300) -> str:
        try:
            text = str(value)
        except Exception:
            text = f"<{type(value).__name__}>"
        text = text.replace("\n", " ").replace("\r", " ").strip()
        return text[:max_chars] + ("..." if len(text) > max_chars else "")

    @classmethod
    def _diagnostic_image(cls, lines: list[str], *, width: int = 1024, height: int = 512) -> torch.Tensor:
        """
        Returns a visible non-black placeholder image with troubleshooting text.
        This prevents PreviewImage from showing a misleading black frame when the
        selected document simply does not contain an image.
        """
        import textwrap
        from PIL import ImageDraw

        image = Image.new("RGB", (width, height), "white")
        draw = ImageDraw.Draw(image)

        margin = 28
        y = margin
        line_height = 18
        max_line_chars = 108

        title = "ZMongo image not found"
        draw.text((margin, y), title, fill="black")
        y += line_height * 2

        for raw_line in lines:
            if y > height - margin - line_height:
                draw.text((margin, y), "... see node status/json output for full details", fill="black")
                break

            wrapped = textwrap.wrap(str(raw_line), width=max_line_chars) or [""]
            for line in wrapped:
                if y > height - margin - line_height:
                    draw.text((margin, y), "... see node status/json output for full details", fill="black")
                    return _pil_to_comfy_image(image)
                draw.text((margin, y), line, fill="black")
                y += line_height
            y += 4

        return _pil_to_comfy_image(image)

    @staticmethod
    def _auth_hint(session) -> dict[str, Any]:
        if session is None:
            return {"session": "missing"}

        api_key = getattr(session, "zai_api_key", "") or ""
        username = getattr(session, "username", "") or ""
        base_url = getattr(session, "base_url", "") or ""
        comfy_prefix = getattr(session, "comfy_zmongo_prefix", "") or ""

        return {
            "base_url": base_url,
            "comfy_zmongo_prefix": comfy_prefix,
            "username": username,
            "api_key_present": bool(api_key),
            "api_key_preview": f"{api_key[:8]}...{api_key[-6:]}" if len(api_key) >= 16 else ("present" if api_key else "missing"),
        }

    @classmethod
    def _failure_payload(
        cls,
        *,
        session,
        collection: str,
        document_id: str,
        requested_field_path: str,
        route_field_path: str,
        field_errors: list[str],
        route_errors: list[str],
        document: dict[str, Any],
        api_payload: dict[str, Any],
        refresh_token: str,
    ) -> dict[str, Any]:
        document_diag = _document_key_summary(document) if document else {"document": "not found or empty"}

        first_envelope = _find_first_bytes_envelope(document) if document else None
        first_envelope_data = None
        if first_envelope:
            first_envelope_data = {
                "path": first_envelope[0],
                "keys": sorted(str(k) for k in first_envelope[1].keys()),
                "size_bytes": first_envelope[1].get("size_bytes"),
                "suggested_field_path": first_envelope[0],
            }

        checks = [
            "Verify the selected collection_name is the image collection, usually 'images'.",
            "Verify the selected document_id is an image document, not metadata or another record type.",
            "Verify field_path points to the image envelope, usually 'image_data', not 'image_data.data'.",
            "Run the '04 Debug Image Document' node on the same collection/document/field.",
            "Check that the API session username matches the user silo that owns the document.",
            "Check that the API key belongs to that username and is active in auth.api_keys.",
            "Check base_url and comfy_zmongo_prefix, usually https://ztarot.app and /comfy-zmongo.",
            "If this is encrypted vault/GridFS storage, supply master_key_hex or use the vault/image-id loader path.",
        ]

        return {
            "success": False,
            "message": "No image could be loaded from the selected ZMongo document.",
            "data": {
                "collection_name": collection,
                "document_id": document_id,
                "requested_field_path": requested_field_path,
                "route_field_path": route_field_path,
                "field_candidates": _image_field_candidates(requested_field_path, "image_data"),
                "auth_hint": cls._auth_hint(session),
                "api_payload_success": api_payload.get("success") if isinstance(api_payload, dict) else None,
                "api_payload_status_code": api_payload.get("status_code") if isinstance(api_payload, dict) else None,
                "api_payload_message": api_payload.get("message") if isinstance(api_payload, dict) else None,
                "document_diagnostic": document_diag,
                "first_bytes_envelope": first_envelope_data,
                "field_errors": field_errors,
                "route_errors": route_errors,
                "user_checks": checks,
                "refresh_token": refresh_token,
            },
            "error": {
                "type": "ImageNotFound",
                "msg": "Selected document did not contain decodable image data at the requested field path.",
            },
            "status_code": 404,
        }

    @classmethod
    def _failure_status(cls, payload: dict[str, Any]) -> str:
        data = payload.get("data", {}) if isinstance(payload, dict) else {}
        auth = data.get("auth_hint", {}) if isinstance(data, dict) else {}
        first = data.get("first_bytes_envelope") if isinstance(data, dict) else None

        parts = [
            "No ZMongo image found.",
            f"collection={data.get('collection_name', '')!r}",
            f"document_id={data.get('document_id', '')!r}",
            f"field_path={data.get('requested_field_path', '')!r}",
            f"username={auth.get('username', '')!r}",
            f"api_key={auth.get('api_key_preview', 'missing')}",
            f"prefix={auth.get('comfy_zmongo_prefix', '')!r}",
        ]

        if first:
            parts.append(f"Found bytes envelope at {first.get('path')!r}; try that field_path.")
        else:
            parts.append("Run '04 Debug Image Document' to inspect available image fields.")

        return " ".join(parts)

    @classmethod
    def _failure_lines(cls, payload: dict[str, Any]) -> list[str]:
        data = payload.get("data", {}) if isinstance(payload, dict) else {}
        auth = data.get("auth_hint", {}) if isinstance(data, dict) else {}
        doc_diag = data.get("document_diagnostic", {}) if isinstance(data, dict) else {}
        first = data.get("first_bytes_envelope") if isinstance(data, dict) else None

        lines = [
            f"Collection: {data.get('collection_name', '')}",
            f"Document _id: {data.get('document_id', '')}",
            f"Field path: {data.get('requested_field_path', '')}",
            f"Username: {auth.get('username', '')}",
            f"API key: {auth.get('api_key_preview', 'missing')}",
            f"Base URL: {auth.get('base_url', '')}",
            f"Prefix: {auth.get('comfy_zmongo_prefix', '')}",
        ]

        if first:
            lines.append(f"Found image bytes envelope at: {first.get('path')}. Try that as field_path.")
        else:
            top_keys = doc_diag.get("top_level_keys") if isinstance(doc_diag, dict) else None
            if top_keys:
                lines.append("Document top-level keys: " + ", ".join(str(k) for k in top_keys[:18]))
            lines.append("No bytes envelope found at image_data or image_data.data.")

        lines.extend([
            "Check: collection name, document _id, field_path, username silo, API key, base_url, and route prefix.",
            "Use '04 Debug Image Document' for a full JSON field report.",
        ])
        return lines

    def display_image(
        self,
        session,
        collection_name: str,
        document_id: str,
        field_path: str,
        master_key_hex: str = "",
        cache: bool = False,
        refresh_token: str = "",
    ):
        """
        Load image using the old proven behavior:
        1. Fetch document JSON.
        2. Decode exact field path first.
        3. Decode legacy <field>.data second.
        4. Use backend image route as fallback.

        Failure mode:
        Returns a readable diagnostic placeholder image plus a status/json report,
        instead of a black image or a generic RuntimeError.
        """
        cleaned_collection = (collection_name or "").strip()
        cleaned_document_id = (document_id or "").strip()
        requested_field_path = _clean_field_path(field_path, "image_data")
        route_field_path = _route_image_field_path(requested_field_path, "image_data")

        field_errors: list[str] = []
        route_errors: list[str] = []
        document: dict[str, Any] = {}
        api_payload: dict[str, Any] = {}

        def return_failure(extra_error: str | None = None):
            if extra_error:
                field_errors.append(extra_error)
            payload = self._failure_payload(
                session=session,
                collection=cleaned_collection,
                document_id=cleaned_document_id,
                requested_field_path=requested_field_path,
                route_field_path=route_field_path,
                field_errors=field_errors,
                route_errors=route_errors,
                document=document,
                api_payload=api_payload,
                refresh_token=refresh_token,
            )
            status = self._failure_status(payload)
            diagnostic_image = self._diagnostic_image(self._failure_lines(payload))
            return (diagnostic_image, status, _json_text(payload))

        if session is None:
            return return_failure("No API session provided. Connect an API Key Session node.")
        if not cleaned_collection:
            return return_failure("collection_name is required. Select a collection first.")
        if not cleaned_document_id:
            return return_failure("document_id is required. Select a document first.")

        # ------------------------------------------------------------------
        # 1. Old working behavior: fetch JSON document and decode locally.
        # ------------------------------------------------------------------
        try:
            api_payload = session.get_doc(
                collection=cleaned_collection,
                document_id=cleaned_document_id,
                cache=cache,
            )
            document = _extract_document_from_payload(api_payload)
        except Exception as exc:
            field_errors.append(f"get_doc failed: {exc}")

        if document:
            for candidate_field_path in _image_field_candidates(requested_field_path, "image_data"):
                try:
                    image_value = safe_get_by_path(document, candidate_field_path)
                    image_bytes = _decode_image_bytes_from_value(image_value)
                    comfy_image, pil_image = self._open_image_bytes(image_bytes)
                    payload = _success_payload(
                        "Image loaded from document JSON.",
                        {
                            "collection": cleaned_collection,
                            "document_id": cleaned_document_id,
                            "requested_field_path": requested_field_path,
                            "resolved_field_path": candidate_field_path,
                            "source": "document_json",
                            "width": pil_image.width,
                            "height": pil_image.height,
                            "refresh_token": refresh_token,
                        },
                    )
                    return (
                        comfy_image,
                        f"Loaded image from document field {candidate_field_path}",
                        _json_text(payload),
                    )
                except Exception as exc:
                    field_errors.append(f"{candidate_field_path}: {exc}")
        else:
            field_errors.append(
                "Document not found in API payload or payload did not contain data.document. "
                "Check collection_name, document_id, username, and API key."
            )

        # ------------------------------------------------------------------
        # 2. Route fallback, using the public field only.
        # ------------------------------------------------------------------
        try:
            image_bytes, source = session.fetch_image_field(
                collection=cleaned_collection,
                document_id=cleaned_document_id,
                field_path=route_field_path,
                master_key_hex=(master_key_hex or "").strip(),
            )
            comfy_image, pil_image = self._open_image_bytes(image_bytes)
            payload = _success_payload(
                "Image loaded from backend route fallback.",
                {
                    "collection": cleaned_collection,
                    "document_id": cleaned_document_id,
                    "requested_field_path": requested_field_path,
                    "route_field_path": route_field_path,
                    "source": source,
                    "width": pil_image.width,
                    "height": pil_image.height,
                    "refresh_token": refresh_token,
                },
            )
            return (
                comfy_image,
                f"Loaded image from route {source}",
                _json_text(payload),
            )
        except Exception as exc:
            route_errors.append(str(exc))

        return return_failure()



class ZMongoApiEasySaveImageNode(AlwaysDirtyMixin):
    """
    Easy image saver for API-key ComfyUI-ZMongo workflows.

    Rule:
    - If document_id cleans to a non-empty value, update that exact document only.
    - Create a new document only when document_id is empty after cleanup.

    This fixes connected document IDs that arrive as strings like:
        (69f420e535aff093fc71e58f)
        ('69f420e535aff093fc71e58f',)
        [69f420e535aff093fc71e58f]
    """

    @classmethod
    def INPUT_TYPES(cls):
        return {
            "required": {
                "session": ("ZMONGO_API_SESSION",),
                "image": ("IMAGE",),
                "collection_name": ("STRING", {"default": "images"}),
                "field_path": ("STRING", {"default": "image_data"}),
                "filename": ("STRING", {"default": "comfy_image.png"}),
            },
            "optional": {
                "document_id": ("STRING", {"default": ""}),
                "doc_key": ("STRING", {"default": ""}),
                "metadata_json": ("STRING", {"default": "{}", "multiline": True}),
            },
        }

    RETURN_TYPES = ("STRING", "STRING", "STRING", "STRING", "BOOLEAN")
    RETURN_NAMES = ("json", "document_id", "field_path", "refresh", "created_new_document")
    FUNCTION = "save_image"
    CATEGORY = "ZMongo/API/04 Images"

    @staticmethod
    def _unwrap_connected_value(value: Any) -> str:
        """
        Normalize scalar values coming from ComfyUI connections.

        Handles:
        - ["abc"] -> "abc"
        - ("abc",) -> "abc"
        - "(abc)" -> "abc"
        - "('abc',)" -> "abc"
        - '["abc"]' -> "abc"
        - trailing tuple/list punctuation
        """
        if isinstance(value, (list, tuple)):
            if not value:
                return ""
            value = value[0]

        if value is None:
            return ""

        text = str(value).strip()

        # Peel repeated single-item wrappers, but avoid stripping meaningful chars inside ObjectId.
        for _ in range(4):
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

    @staticmethod
    def _parse_metadata(metadata_json: Any) -> dict[str, Any]:
        text = ZMongoApiEasySaveImageNode._unwrap_connected_value(metadata_json)
        if not text:
            return {}
        parsed = json.loads(text)
        if not isinstance(parsed, dict):
            raise ValueError("metadata_json must be a JSON object.")
        return parsed

    @staticmethod
    def _first_image_from_batch(image: torch.Tensor) -> torch.Tensor:
        if image is None:
            raise ValueError("No image was provided.")
        if not isinstance(image, torch.Tensor):
            raise TypeError(f"Expected ComfyUI IMAGE tensor, got {type(image).__name__}.")

        tensor = image.detach().cpu()
        if tensor.ndim == 4:
            if tensor.shape[0] < 1:
                raise ValueError("Image batch is empty.")
            tensor = tensor[0]
        if tensor.ndim != 3:
            raise ValueError(f"Expected IMAGE tensor with 3 or 4 dims, got shape {tuple(tensor.shape)}.")
        return tensor

    @classmethod
    def _image_to_png_bytes(cls, image: torch.Tensor) -> bytes:
        tensor = cls._first_image_from_batch(image).clamp(0.0, 1.0).numpy()
        np_image = (tensor * 255.0).round().astype(np.uint8)
        pil_image = Image.fromarray(np_image, mode="RGB")
        buffer = io.BytesIO()
        pil_image.save(buffer, format="PNG")
        return buffer.getvalue()

    @staticmethod
    def _build_image_envelope(
        *,
        image_bytes: bytes,
        filename: str,
        metadata: dict[str, Any],
    ) -> dict[str, Any]:
        safe_filename = (filename or "comfy_image.png").strip() or "comfy_image.png"
        return {
            "__type__": "bytes",
            "encoding": "base64",
            "size_bytes": len(image_bytes),
            "data": base64.b64encode(image_bytes).decode("ascii"),
            "filename": safe_filename,
            "content_type": "image/png",
            "source": "comfyui",
            "storage_mode": "inline_zmongo_binary_envelope",
            "metadata": metadata or {},
            "saved_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        }

    @staticmethod
    def _extract_inserted_id(payload: dict[str, Any]) -> str:
        data = payload.get("data", {}) if isinstance(payload, dict) else {}
        if isinstance(data, dict):
            for key in ("document_id", "inserted_id", "_id", "id"):
                value = data.get(key)
                if value:
                    return ZMongoApiEasySaveImageNode._unwrap_connected_value(value)

            for nested_key in ("result", "document"):
                nested = data.get(nested_key)
                if isinstance(nested, dict):
                    for key in ("document_id", "inserted_id", "_id", "id"):
                        value = nested.get(key)
                        if value:
                            return ZMongoApiEasySaveImageNode._unwrap_connected_value(value)
        return ""

    def save_image(
        self,
        session,
        image,
        collection_name: str,
        field_path: str,
        filename: str,
        document_id: str = "",
        doc_key: str = "",
        metadata_json: str = "{}",
    ):
        cleaned_collection = self._unwrap_connected_value(collection_name) or "images"
        cleaned_field_path = _clean_field_path(self._unwrap_connected_value(field_path), "image_data")
        cleaned_filename = self._unwrap_connected_value(filename) or "comfy_image.png"
        cleaned_document_id = self._unwrap_connected_value(document_id)
        cleaned_doc_key = self._unwrap_connected_value(doc_key)

        refresh = _dirty_token("easy_save_image", cleaned_collection, cleaned_document_id, cleaned_field_path)

        if session is None:
            payload = _error_payload("No API session provided.")
            return (_json_text(payload), cleaned_document_id, cleaned_field_path, refresh, False)

        try:
            metadata = self._parse_metadata(metadata_json)
            image_bytes = self._image_to_png_bytes(image)
            image_envelope = self._build_image_envelope(
                image_bytes=image_bytes,
                filename=cleaned_filename,
                metadata=metadata,
            )

            # Existing document path: never upsert/create here.
            # IMPORTANT: send an explicit _id query. Some backend save-value
            # route versions do not convert document_id into query when
            # upsert_if_missing=False, which causes:
            #   "No query provided and upsert is False"
            if cleaned_document_id:
                payload = session.save_value(
                    collection=cleaned_collection,
                    query={"_id": cleaned_document_id},
                    document_id="",
                    field_path=cleaned_field_path,
                    value=image_envelope,
                    upsert_if_missing=False,
                    parse_json_strings=False,
                    normalize_for_storage=False,
                )

                success = bool(payload.get("success")) if isinstance(payload, dict) else False
                result_payload = {
                    "success": success,
                    "message": (
                        f"Updated existing image document {cleaned_collection}/{cleaned_document_id} "
                        f"at {cleaned_field_path}."
                        if success
                        else f"Failed to update existing document {cleaned_collection}/{cleaned_document_id}."
                    ),
                    "data": {
                        "operation": "update_existing_document",
                        "collection_name": cleaned_collection,
                        "document_id": cleaned_document_id,
                        "query_used": {"_id": cleaned_document_id},
                        "field_path": cleaned_field_path,
                        "filename": image_envelope["filename"],
                        "size_bytes": image_envelope["size_bytes"],
                        "created_new_document": False,
                        "refresh": refresh,
                        "api_response": payload,
                        "checks": [
                            "The connected document_id was cleaned before save.",
                            "A new document is not created when cleaned document_id is non-empty.",
                            "Confirm the document_id exists in the selected collection if update fails.",
                            "Confirm the API key belongs to the same username silo.",
                        ],
                    },
                    "error": None if success else (payload.get("error") if isinstance(payload, dict) else "Save failed."),
                    "status_code": payload.get("status_code", 0) if isinstance(payload, dict) else 0,
                }
                return (_json_text(result_payload), cleaned_document_id, cleaned_field_path, refresh, False)

            # New document path: only when document_id is empty after cleanup.
            document: dict[str, Any] = {
                cleaned_field_path: image_envelope,
                "source": "comfyui",
                "filename": image_envelope["filename"],
                "content_type": "image/png",
                "size_bytes": image_envelope["size_bytes"],
            }
            if cleaned_doc_key:
                document["doc_key"] = cleaned_doc_key
            if metadata:
                document["metadata"] = metadata

            payload = session.create_doc(collection=cleaned_collection, document=document)
            new_document_id = self._extract_inserted_id(payload)
            success = bool(payload.get("success")) and bool(new_document_id)

            result_payload = {
                "success": success,
                "message": (
                    f"Created new image document {cleaned_collection}/{new_document_id} "
                    f"with image at {cleaned_field_path}."
                    if success
                    else "Failed to create new image document."
                ),
                "data": {
                    "operation": "create_new_document",
                    "collection_name": cleaned_collection,
                    "document_id": new_document_id,
                    "field_path": cleaned_field_path,
                    "filename": image_envelope["filename"],
                    "size_bytes": image_envelope["size_bytes"],
                    "created_new_document": True,
                    "refresh": refresh,
                    "api_response": payload,
                },
                "error": None if success else (payload.get("error") if isinstance(payload, dict) else "Create failed."),
                "status_code": payload.get("status_code", 0) if isinstance(payload, dict) else 0,
            }
            return (_json_text(result_payload), new_document_id, cleaned_field_path, refresh, True)

        except Exception as exc:
            payload = {
                "success": False,
                "message": f"Easy Save Image failed: {exc}",
                "data": {
                    "operation": "failed",
                    "collection_name": cleaned_collection,
                    "document_id": cleaned_document_id,
                    "field_path": cleaned_field_path,
                    "filename": cleaned_filename,
                    "created_new_document": False,
                    "refresh": refresh,
                    "checks": [
                        "Confirm the API session is connected.",
                        "Confirm collection_name is correct.",
                        "Confirm the image input is connected.",
                        "Confirm metadata_json is valid JSON.",
                        "If document_id is connected, confirm it is not an empty string after cleanup.",
                    ],
                },
                "error": {"type": exc.__class__.__name__, "msg": str(exc)},
                "status_code": 0,
            }
            return (_json_text(payload), cleaned_document_id, cleaned_field_path, refresh, False)

class ZMongoApiDocumentImageDebugNode(AlwaysDirtyMixin):
    @classmethod
    def INPUT_TYPES(cls):
        return {
            "required": {
                "session": ("ZMONGO_API_SESSION",),
                "collection_name": ("STRING", {"default": ""}),
                "document_id": ("STRING", {"default": ""}),
                "field_path": ("STRING", {"default": "image_data"}),
            },
            "optional": {
                "cache": ("BOOLEAN", {"default": False}),
                "refresh_token": ("STRING", {"default": ""}),
            },
        }

    RETURN_TYPES = ("STRING", "*", "STRING")
    RETURN_NAMES = ("json", "candidate_paths", "summary")
    OUTPUT_IS_LIST = (False, True, False)
    FUNCTION = "debug_image_document"
    CATEGORY = "ZMongo/API/04 Images"

    def debug_image_document(
        self,
        session,
        collection_name: str,
        document_id: str,
        field_path: str,
        cache: bool = False,
        refresh_token: str = "",
    ):
        if session is None:
            payload = _error_payload("No session provided.")
            return (_json_text(payload), [], payload["message"])

        cleaned_collection = (collection_name or "").strip()
        cleaned_document_id = (document_id or "").strip()
        requested_field_path = _clean_field_path(field_path, "image_data")
        candidates = _image_field_candidates(requested_field_path, "image_data")

        try:
            payload = session.get_doc(
                collection=cleaned_collection,
                document_id=cleaned_document_id,
                cache=cache,
            )
            document = _extract_document_from_payload(payload)
            candidate_report: list[dict[str, Any]] = []

            for candidate in candidates:
                value = safe_get_by_path(document, candidate)
                item = {
                    "path": candidate,
                    "type": type(value).__name__,
                    "is_empty": value is None or value == "" or value == {},
                }
                if isinstance(value, dict):
                    item["keys"] = sorted(str(k) for k in value.keys())
                    item["is_zmongo_bytes_envelope"] = bool(value.get("__type__") == "bytes" and value.get("data"))
                    item["size_bytes"] = value.get("size_bytes")
                elif isinstance(value, str):
                    item["length"] = len(value)
                    item["starts_with"] = value[:40]
                candidate_report.append(item)

            found_envelope = _find_first_bytes_envelope(document)
            debug_payload = _success_payload(
                "Document image debug complete.",
                {
                    "collection": cleaned_collection,
                    "document_id": cleaned_document_id,
                    "requested_field_path": requested_field_path,
                    "route_field_path": _route_image_field_path(requested_field_path, "image_data"),
                    "candidate_report": candidate_report,
                    "first_bytes_envelope": {
                        "path": found_envelope[0],
                        "keys": sorted(str(k) for k in found_envelope[1].keys()),
                        "size_bytes": found_envelope[1].get("size_bytes"),
                    } if found_envelope else None,
                    "document_diagnostic": _document_key_summary(document),
                    "refresh_token": refresh_token,
                },
            )
            summary = "first_bytes_envelope=" + (found_envelope[0] if found_envelope else "None")
            return (_json_text(debug_payload), _as_comfy_list(candidates), summary)
        except Exception as exc:
            payload = _error_payload(str(exc))
            return (_json_text(payload), _as_comfy_list(candidates), str(exc))


class ZMongoApiImageFieldCandidatesNode(AlwaysDirtyMixin):
    @classmethod
    def INPUT_TYPES(cls):
        return {
            "required": {
                "field_path": ("STRING", {"default": "image_data"}),
            }
        }

    RETURN_TYPES = ("STRING", "*", "STRING")
    RETURN_NAMES = ("json", "paths", "indexed")
    OUTPUT_IS_LIST = (False, True, False)
    FUNCTION = "image_field_candidates"
    CATEGORY = "ZMongo/API/04 Images"

    def image_field_candidates(self, field_path: str):
        paths = _image_field_candidates(field_path, "image_data")
        payload = _success_payload("Image field candidates generated.", {"paths": paths})
        return (_json_text(payload), _as_comfy_list(paths), _indexed_list_text(paths))


# -----------------------------------------------------------------------------
# 05 Fleet nodes
# -----------------------------------------------------------------------------

class ZMongoApiFleetStatusNode(AlwaysDirtyMixin):
    @classmethod
    def INPUT_TYPES(cls):
        return {
            "required": {"session": ("ZMONGO_API_SESSION",)},
            "optional": {"refresh_token": ("STRING", {"default": ""})},
        }

    RETURN_TYPES = ("STRING", "BOOLEAN")
    RETURN_NAMES = ("json", "success")
    FUNCTION = "fleet_status"
    CATEGORY = "ZMongo/API/05 Fleet"

    def fleet_status(self, session, refresh_token: str = ""):
        if session is None:
            payload = _error_payload("No session provided.")
            return (_json_text(payload), False)
        payload = session.fleet_status()
        return (_json_text(payload), bool(payload.get("success")))


class ZMongoApiFleetAgentsNode(AlwaysDirtyMixin):
    @classmethod
    def INPUT_TYPES(cls):
        return {
            "required": {"session": ("ZMONGO_API_SESSION",)},
            "optional": {"refresh_token": ("STRING", {"default": ""})},
        }

    RETURN_TYPES = ("STRING",)
    RETURN_NAMES = ("json",)
    FUNCTION = "fleet_agents"
    CATEGORY = "ZMongo/API/05 Fleet"

    def fleet_agents(self, session, refresh_token: str = ""):
        if session is None:
            return (_json_text(_error_payload("No session provided.")),)
        return (_json_text(session.fleet_agents()),)


class ZMongoApiFleetDispatchNode(AlwaysDirtyMixin):
    @classmethod
    def INPUT_TYPES(cls):
        return {
            "required": {
                "session": ("ZMONGO_API_SESSION",),
                "intent": ("STRING", {"default": "chat"}),
                "payload_json": ("STRING", {"default": "{}", "multiline": True}),
                "timeout_seconds": ("FLOAT", {"default": 60.0, "min": 1.0, "max": 600.0}),
            },
            "optional": {
                "dispatch_id": ("STRING", {"default": ""}),
                "cost_usd": ("STRING", {"default": ""}),
            },
        }

    RETURN_TYPES = ("STRING", "BOOLEAN")
    RETURN_NAMES = ("json", "success")
    FUNCTION = "fleet_dispatch"
    CATEGORY = "ZMongo/API/05 Fleet"

    def fleet_dispatch(self, session, intent: str, payload_json: str, timeout_seconds: float, dispatch_id: str = "", cost_usd: str = ""):
        if session is None:
            payload = _error_payload("No session provided.")
            return (_json_text(payload), False)

        try:
            payload_obj = _parse_json_object(payload_json, "payload_json")
            payload = session.fleet_dispatch(
                intent=(intent or "").strip(),
                payload=payload_obj,
                dispatch_id=(dispatch_id or "").strip(),
                timeout=float(timeout_seconds or 60.0),
                cost_usd=(cost_usd or "").strip(),
            )
            return (_json_text(payload), bool(payload.get("success")))
        except Exception as exc:
            payload = _error_payload(str(exc))
            return (_json_text(payload), False)


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
    CATEGORY = "ZMongo/API/99 Helpers"
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
    CATEGORY = "ZMongo/API/99 Helpers"

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
    # 00 Auth
    "ZMongoApiKeySessionNode": ZMongoApiKeySessionNode,
    "ZMongoApiCloseSessionNode": ZMongoApiCloseSessionNode,

    # 01 Service
    "ZMongoApiHealthNode": ZMongoApiHealthNode,
    "ZMongoApiWhoamiNode": ZMongoApiWhoamiNode,

    # 02 Collections
    "ZMongoApiListCollectionsNode": ZMongoApiListCollectionsNode,
    "ZMongoApiCreateCollectionNode": ZMongoApiCreateCollectionNode,
    "ZMongoApiDeleteCollectionNode": ZMongoApiDeleteCollectionNode,

    # 03 Docs
    "ZMongoApiListDocsNode": ZMongoApiListDocsNode,
    "ZMongoApiGetDocNode": ZMongoApiGetDocNode,
    "ZMongoApiQueryDocsNode": ZMongoApiQueryDocsNode,
    "ZMongoApiCountDocsNode": ZMongoApiCountDocsNode,
    "ZMongoApiCreateDocNode": ZMongoApiCreateDocNode,
    "ZMongoApiUpdateDocNode": ZMongoApiUpdateDocNode,
    "ZMongoApiDeleteDocNode": ZMongoApiDeleteDocNode,
    "ZMongoApiSaveValueNode": ZMongoApiSaveValueNode,

    # 04 Images
    "ZMongoDisplayImageNode": ZMongoDisplayImageNode,
    "ZMongoApiEasySaveImageNode": ZMongoApiEasySaveImageNode,
    "ZMongoApiDocumentImageDebugNode": ZMongoApiDocumentImageDebugNode,
    "ZMongoApiImageFieldCandidatesNode": ZMongoApiImageFieldCandidatesNode,

    # 05 Fleet
    "ZMongoApiFleetStatusNode": ZMongoApiFleetStatusNode,
    "ZMongoApiFleetAgentsNode": ZMongoApiFleetAgentsNode,
    "ZMongoApiFleetDispatchNode": ZMongoApiFleetDispatchNode,

    # 99 Helpers
    "ZMongoApiSelectNthItemNode": ZMongoApiSelectNthItemNode,
    "ZMongoApiJsonPickNode": ZMongoApiJsonPickNode,
}

NODE_DISPLAY_NAME_MAPPINGS = {
    # 00 Auth
    "ZMongoApiKeySessionNode": "00 API Key Session",
    "ZMongoApiCloseSessionNode": "00 Close API Session",

    # 01 Service
    "ZMongoApiHealthNode": "01 Health",
    "ZMongoApiWhoamiNode": "01 Who Am I",

    # 02 Collections
    "ZMongoApiListCollectionsNode": "02 List Collections",
    "ZMongoApiCreateCollectionNode": "02 Create Collection",
    "ZMongoApiDeleteCollectionNode": "02 Delete Collection",

    # 03 Docs
    "ZMongoApiListDocsNode": "03 List Docs",
    "ZMongoApiGetDocNode": "03 Get Doc",
    "ZMongoApiQueryDocsNode": "03 Query Docs",
    "ZMongoApiCountDocsNode": "03 Count Docs",
    "ZMongoApiCreateDocNode": "03 Create Doc",
    "ZMongoApiUpdateDocNode": "03 Update Doc",
    "ZMongoApiDeleteDocNode": "03 Delete Doc",
    "ZMongoApiSaveValueNode": "03 Save Value",

    # 04 Images
    "ZMongoDisplayImageNode": "04 Display Image from ZMongo",
    "ZMongoApiEasySaveImageNode": "04 Easy Save Image",
    "ZMongoApiDocumentImageDebugNode": "04 Debug Image Document",
    "ZMongoApiImageFieldCandidatesNode": "04 Image Field Candidates",

    # 05 Fleet
    "ZMongoApiFleetStatusNode": "05 Fleet Status",
    "ZMongoApiFleetAgentsNode": "05 Fleet Agents",
    "ZMongoApiFleetDispatchNode": "05 Fleet Dispatch",

    # 99 Helpers
    "ZMongoApiSelectNthItemNode": "99 Select Nth Item",
    "ZMongoApiJsonPickNode": "99 JSON Pick",
}

__all__ = [
    "NODE_CLASS_MAPPINGS",
    "NODE_DISPLAY_NAME_MAPPINGS",
]