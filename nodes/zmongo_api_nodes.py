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


def _image_field_candidates(field_path: str, default: str = "image_data") -> list[str]:
    exact = _clean_field_path(field_path, default)
    candidates = [
        exact,
        _legacy_data_path(exact),
        f"{exact}.base64",
        f"{exact}.b64",
        f"{exact}.bytes",
        f"{exact}.image",
        f"{exact}.url",
    ]
    return list(dict.fromkeys(candidates))


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
    if isinstance(value, str) and _is_probable_url(value):
        return value.strip()

    if isinstance(value, dict):
        for key in ("url", "view_url", "download_url", "preview_url", "href"):
            candidate = value.get(key)
            if _is_probable_url(candidate):
                return str(candidate).strip()

    return ""


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
        clean_field = _clean_field_path(field_path, "image_data")

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
        if session is None:
            payload = _error_payload("No session provided.")
            _raise_display_image_error(payload["message"])

        cleaned_collection = (collection_name or "").strip()
        cleaned_document_id = (document_id or "").strip()
        requested_field_path = _clean_field_path(field_path, "image_data")

        if not cleaned_collection:
            payload = _error_payload("collection_name is required.")
            _raise_display_image_error(payload["message"])

        if not cleaned_document_id:
            payload = _error_payload("document_id is required.")
            _raise_display_image_error(payload["message"])

        field_errors: list[str] = []
        route_errors: list[str] = []
        resolved_source = ""

        try:
            payload = session.get_doc(collection=cleaned_collection, document_id=cleaned_document_id, cache=cache)
            document = _extract_document_from_payload(payload)

            if not document:
                field_errors.append("Document not found in API payload.")
            else:
                for candidate_field_path in _image_field_candidates(requested_field_path, "image_data"):
                    image_value = safe_get_by_path(document, candidate_field_path)

                    image_url = _image_url_from_value(image_value)
                    if image_url:
                        try:
                            image_bytes = session.fetch_absolute_or_relative_bytes(image_url)
                            image = Image.open(io.BytesIO(image_bytes))
                            comfy_image = _pil_to_comfy_image(image)
                            resolved_source = f"url field {candidate_field_path}"
                            result_payload = _success_payload(
                                "Image loaded.",
                                {
                                    "collection": cleaned_collection,
                                    "document_id": cleaned_document_id,
                                    "field_path": candidate_field_path,
                                    "source": resolved_source,
                                    "width": image.width,
                                    "height": image.height,
                                },
                            )
                            return (comfy_image, f"Loaded image from {resolved_source}", _json_text(result_payload))
                        except Exception as exc:
                            field_errors.append(f"{candidate_field_path} URL: {exc}")
                            continue

                    try:
                        image_bytes = _decode_image_bytes_from_value(image_value)
                        image = Image.open(io.BytesIO(image_bytes))
                        comfy_image = _pil_to_comfy_image(image)
                        resolved_source = f"document field {candidate_field_path}"
                        result_payload = _success_payload(
                            "Image loaded.",
                            {
                                "collection": cleaned_collection,
                                "document_id": cleaned_document_id,
                                "field_path": candidate_field_path,
                                "source": resolved_source,
                                "width": image.width,
                                "height": image.height,
                            },
                        )
                        return (comfy_image, f"Loaded image from {resolved_source}", _json_text(result_payload))
                    except Exception as exc:
                        field_errors.append(f"{candidate_field_path}: {exc}")

            try:
                image_bytes, resolved_source = session.fetch_image_field(
                    collection=cleaned_collection,
                    document_id=cleaned_document_id,
                    field_path=requested_field_path,
                    master_key_hex=(master_key_hex or "").strip(),
                )
                if image_bytes[:1] in (b"{", b"["):
                    raise ValueError(f"Image route returned JSON/text instead of image bytes: {image_bytes[:500]!r}")
                image = Image.open(io.BytesIO(image_bytes))
                comfy_image = _pil_to_comfy_image(image)
                result_payload = _success_payload(
                    "Image loaded from image route fallback.",
                    {
                        "collection": cleaned_collection,
                        "document_id": cleaned_document_id,
                        "field_path": requested_field_path,
                        "source": resolved_source,
                        "width": image.width,
                        "height": image.height,
                    },
                )
                return (comfy_image, f"Loaded image from {resolved_source}", _json_text(result_payload))
            except Exception as exc:
                route_errors.append(str(exc))

            message = "Could not load image. " + " | ".join(field_errors + route_errors)
            error_payload = _error_payload(message)
            _raise_display_image_error(message)

        except UnidentifiedImageError as exc:
            message = f"Image bytes were loaded but PIL could not identify the file format: {exc}"
            error_payload = _error_payload(message)
            _raise_display_image_error(message)
        except Exception as exc:
            message = f"Display image failed: {exc}"
            error_payload = _error_payload(message)
            _raise_display_image_error(message)


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

    def select_nth_item(self, items_list, index: int, fallback: str):
        items = _as_comfy_list(items_list)
        cleaned = [str(item).strip() for item in items if str(item).strip()]

        if not cleaned:
            return ((fallback or ""), "Input list was empty.")

        safe_index = max(0, min(int(index), len(cleaned) - 1))
        selected = cleaned[safe_index]
        return (selected, f"Selected {safe_index + 1}/{len(cleaned)}: {selected}")


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