from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Optional

import requests

from .generic_helpers import (
    AlwaysDirtyMixin,
    DEFAULT_BASE_URL,
    _json_text,
    _error_payload,
    _success_payload,
)


# -----------------------------------------------------------------------------
# Shared helpers
# -----------------------------------------------------------------------------


def _safe_result_to_dict(result: Any) -> dict[str, Any]:
    if isinstance(result, dict):
        return result
    if hasattr(result, "to_dict") and callable(result.to_dict):
        payload = result.to_dict()
        return payload if isinstance(payload, dict) else {"success": False, "message": "Invalid result payload.", "data": {}, "error": str(payload), "status_code": 500}
    return {
        "success": False,
        "message": f"Unsupported result type: {type(result).__name__}",
        "data": {},
        "error": {"type": type(result).__name__, "msg": str(result)},
        "status_code": 500,
    }


def _extract_document_from_local_payload(payload: dict[str, Any]) -> dict[str, Any]:
    data = payload.get("data") if isinstance(payload, dict) else None
    if not isinstance(data, dict):
        return {}

    document = data.get("document")
    if isinstance(document, dict):
        return document

    documents = data.get("documents")
    if isinstance(documents, list) and documents and isinstance(documents[0], dict):
        return documents[0]

    if data.get("_id") is not None:
        return data

    return {}


def _set_dot_path(document: dict[str, Any], path: str, value: Any) -> bool:
    path = str(path or "").strip()
    if not path:
        return False

    target: Any = document
    parts = path.split(".")

    for part in parts[:-1]:
        if not isinstance(target, dict):
            return False
        if part not in target or not isinstance(target.get(part), dict):
            target[part] = {}
        target = target[part]

    if not isinstance(target, dict):
        return False

    target[parts[-1]] = value
    return True


def _get_dot_path(document: dict[str, Any], path: str, default: Any = None) -> Any:
    if not path:
        return document

    node: Any = document
    for part in str(path or "").split("."):
        if isinstance(node, dict):
            node = node.get(part, default)
        elif isinstance(node, list) and part.isdigit():
            idx = int(part)
            node = node[idx] if 0 <= idx < len(node) else default
        else:
            return default

        if node is default:
            return default

    return node


# -----------------------------------------------------------------------------
# Hosted Comfy-ZMongo API session
# -----------------------------------------------------------------------------


class ZMongoApiSession:
    """
    Shared hosted API session used by the simplified API-key auth node and the
    main ZMongo API nodes.

    Final URL shape:

        https://businessprocessapplications.com/comfy-zmongo/api/...

    The auth node should pass only:

        https://businessprocessapplications.com

    This class adds /comfy-zmongo/api internally.
    """

    COMFY_PREFIX = "/comfy-zmongo"
    API_PREFIX = "/api"
    STORAGE_BACKEND = "zmongo_api"

    def __init__(
        self,
        *,
        base_url: str = DEFAULT_BASE_URL,
        zai_api_key: str = "",
        verify_tls: bool = True,
    ) -> None:
        self.base_url = self._normalize_base_url(base_url)
        self.zai_api_key = (zai_api_key or "").strip()
        self.verify_tls = bool(verify_tls)
        self.session = requests.Session()
        self.storage_backend = self.STORAGE_BACKEND
        self.username = ""
        self.comfy_zmongo_prefix = self.COMFY_PREFIX

    @classmethod
    def _normalize_base_url(cls, base_url: str) -> str:
        cleaned = (base_url or "").strip() or DEFAULT_BASE_URL
        cleaned = cleaned.rstrip("/")

        if cleaned.endswith(cls.COMFY_PREFIX):
            cleaned = cleaned[: -len(cls.COMFY_PREFIX)]

        return cleaned.rstrip("/")

    @staticmethod
    def _clean_scalar(value: Any) -> str:
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

            if ": " in text and text.split(": ", 1)[0].strip().isdigit():
                text = text.split(": ", 1)[1].strip()

            if text == before:
                break

        return text.strip()

    @staticmethod
    def _safe_int(value: Any, default: int) -> int:
        try:
            return int(value)
        except Exception:
            return int(default)

    def _join_path(self, prefix: str, path: str) -> str:
        clean_prefix = (prefix or "").strip()
        clean_path = (path or "").strip()

        if clean_prefix and not clean_prefix.startswith("/"):
            clean_prefix = "/" + clean_prefix

        if clean_path and not clean_path.startswith("/"):
            clean_path = "/" + clean_path

        return f"{self.COMFY_PREFIX}{clean_prefix}{clean_path}"

    def _build_url(self, prefix: str, path: str) -> str:
        return f"{self.base_url}{self._join_path(prefix, path)}"

    def close(self) -> None:
        self.session.close()

    def _headers(self) -> dict[str, str]:
        return {
            "Accept": "application/json",
            "Content-Type": "application/json",
            "Authorization": f"Bearer {self.zai_api_key}",
            "ZAI_API_KEY": self.zai_api_key,
        }

    def _json_response(self, response: requests.Response) -> dict[str, Any]:
        try:
            payload = response.json()
            if isinstance(payload, dict):
                payload.setdefault("status_code", response.status_code)
                return payload
            return {
                "success": False,
                "message": "API returned JSON, but not a JSON object.",
                "data": {"raw_json": payload},
                "error": {"msg": "Non-object JSON response."},
                "status_code": response.status_code,
            }
        except Exception:
            return {
                "success": False,
                "message": "Non-JSON response from ZMongo API.",
                "data": {},
                "error": {"msg": response.text[:1000]},
                "raw_text": response.text[:1000],
                "status_code": response.status_code,
            }

    def request(
        self,
        method: str,
        prefix: str,
        path: str,
        *,
        json_body: Optional[dict[str, Any]] = None,
        params: Optional[dict[str, Any]] = None,
    ) -> dict[str, Any]:
        url = self._build_url(prefix, path)

        try:
            response = self.session.request(
                method.upper(),
                url,
                headers=self._headers(),
                json=json_body,
                params=params,
                verify=self.verify_tls,
                timeout=60,
            )
            payload = self._json_response(response)
            payload.setdefault("data", {})
            if isinstance(payload.get("data"), dict):
                payload["data"].setdefault("request_url", url)
                payload["data"].setdefault("request_method", method.upper())
            return payload
        except Exception as exc:
            return {
                "success": False,
                "message": str(exc),
                "data": {
                    "method": method.upper(),
                    "url": url,
                    "params": params or {},
                    "json_body": json_body or {},
                },
                "error": {"type": exc.__class__.__name__, "msg": str(exc)},
                "status_code": 0,
            }

    def request_bytes(
        self,
        method: str,
        prefix: str,
        path: str,
        *,
        params: Optional[dict[str, Any]] = None,
    ) -> tuple[bytes, int, str]:
        url = self._build_url(prefix, path)
        response = self.session.request(
            method.upper(),
            url,
            headers=self._headers(),
            params=params,
            verify=self.verify_tls,
            timeout=60,
        )
        response.raise_for_status()
        return response.content, response.status_code, response.headers.get("Content-Type", "")

    def _is_route_miss(self, payload: dict[str, Any]) -> bool:
        status_code = int(payload.get("status_code") or 0)
        if status_code in {404, 405, 501}:
            return True

        haystack = " ".join(
            str(payload.get(key) or "").lower()
            for key in ("message", "raw_text", "error")
        )
        return any(marker in haystack for marker in ("not found", "method not allowed", "405", "404", "route"))

    def _try_requests(
        self,
        candidates: list[tuple[str, str, dict[str, Any] | None, dict[str, Any] | None]],
    ) -> dict[str, Any]:
        attempted: list[dict[str, Any]] = []
        last_payload: dict[str, Any] | None = None

        for method, path, json_body, params in candidates:
            url = self._build_url(self.API_PREFIX, path)
            payload = self.request(method, self.API_PREFIX, path, json_body=json_body, params=params)
            last_payload = payload

            attempted.append(
                {
                    "method": method.upper(),
                    "url": url,
                    "path": self._join_path(self.API_PREFIX, path),
                    "status_code": payload.get("status_code"),
                    "success": payload.get("success"),
                    "message": payload.get("message"),
                }
            )

            if bool(payload.get("success")):
                payload.setdefault("data", {})
                if isinstance(payload.get("data"), dict):
                    payload["data"].setdefault("attempted_routes", attempted)
                return payload

            if not self._is_route_miss(payload):
                payload.setdefault("data", {})
                if isinstance(payload.get("data"), dict):
                    payload["data"].setdefault("attempted_routes", attempted)
                return payload

        if last_payload is None:
            return {
                "success": False,
                "message": "No API route candidates were provided.",
                "data": {"attempted_routes": attempted},
                "error": {"msg": "No candidates."},
                "status_code": 0,
            }

        last_payload.setdefault("data", {})
        if isinstance(last_payload.get("data"), dict):
            last_payload["data"].setdefault("attempted_routes", attempted)
        return last_payload

    def health(self) -> dict[str, Any]:
        return self._try_requests([("GET", "/health", None, None), ("GET", "/status", None, None)])

    def whoami(self) -> dict[str, Any]:
        payload = self._try_requests([("GET", "/whoami", None, None), ("GET", "/me", None, None)])
        data = payload.get("data") if isinstance(payload, dict) else None
        if isinstance(data, dict):
            self.username = str(data.get("username") or self.username or "")
        return payload

    def list_collections(self) -> dict[str, Any]:
        return self._try_requests([("GET", "/collections", None, None), ("POST", "/collections/list", {}, None)])

    def create_collection(self, collection_name: str) -> dict[str, Any]:
        cleaned = self._clean_scalar(collection_name)
        body = {"collection_name": cleaned, "collection": cleaned, "name": cleaned}
        return self._try_requests([("POST", "/collection/create", body, None)])

    def delete_collection(self, collection_name: str) -> dict[str, Any]:
        cleaned = self._clean_scalar(collection_name)
        body = {"collection_name": cleaned, "collection": cleaned, "name": cleaned}
        return self._try_requests([("POST", "/collection/delete", body, None)])

    def list_docs(self, *, collection: str, limit: int = 50, skip: int = 0, query: Optional[dict[str, Any]] = None) -> dict[str, Any]:
        cleaned = self._clean_scalar(collection)
        safe_limit = self._safe_int(limit, 50)
        safe_skip = self._safe_int(skip, 0)
        safe_query = query or {}
        params = {"query_json": json.dumps(safe_query), "limit": safe_limit, "skip": safe_skip}
        return self._try_requests([("GET", f"/docs/{cleaned}", None, params)])

    def get_doc(self, *, collection: str, document_id: str, cache: bool = False) -> dict[str, Any]:
        cleaned_collection = self._clean_scalar(collection)
        cleaned_id = self._clean_scalar(document_id)
        if not cleaned_collection or not cleaned_id:
            return {
                "success": False,
                "message": "collection and document_id are required.",
                "data": {"collection_name": cleaned_collection, "document_id": cleaned_id},
                "error": {"msg": "Missing collection or document_id."},
                "status_code": 400,
            }

        params = {"cache": str(bool(cache)).lower()}
        payload = self._try_requests([("GET", f"/doc/{cleaned_collection}/{cleaned_id}", None, params)])
        if not isinstance(payload, dict) or not payload.get("success"):
            return payload

        document = _extract_document_from_local_payload(payload)
        if document:
            return {
                "success": True,
                "message": f"Loaded document {cleaned_id} from {cleaned_collection}.",
                "data": {
                    "collection_name": cleaned_collection,
                    "document_id": cleaned_id,
                    "document": document,
                    "doc": document,
                    "documents": [document],
                    "api_response": payload,
                },
                "error": None,
                "status_code": payload.get("status_code", 200),
            }
        return payload

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
        cleaned = self._clean_scalar(collection)
        cleaned_id = self._clean_scalar(document_id)
        body = {
            "collection": cleaned,
            "collection_name": cleaned,
            "query": query or {},
            "document_id": cleaned_id,
            "_id": cleaned_id,
            "many": bool(many),
            "limit": self._safe_int(limit, 50),
            "skip": self._safe_int(skip, 0),
            "projection": projection or {},
            "sort": sort or [],
            "cache": bool(cache),
        }
        return self._try_requests([("POST", "/query", body, None)])

    def count_docs(self, *, collection: str, query: Optional[dict[str, Any]] = None, document_id: str = "", cache: bool = False) -> dict[str, Any]:
        cleaned = self._clean_scalar(collection)
        cleaned_id = self._clean_scalar(document_id)
        body = {"collection": cleaned, "query": query or {}, "document_id": cleaned_id, "_id": cleaned_id, "cache": bool(cache)}
        return self._try_requests([("POST", "/count", body, None)])

    def create_doc(self, *, collection: str, document: dict[str, Any]) -> dict[str, Any]:
        cleaned = self._clean_scalar(collection)
        return self._try_requests([("POST", "/doc/create", {"collection": cleaned, "document": document or {}}, None)])

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
        cleaned = self._clean_scalar(collection)
        cleaned_id = self._clean_scalar(document_id)
        body = {
            "collection": cleaned,
            "query": query or {},
            "document_id": cleaned_id,
            "_id": cleaned_id,
            "update": update,
            "field_path": self._clean_scalar(field_path),
            "value": value,
            "upsert": bool(upsert),
        }
        return self._try_requests([("POST", "/doc/update", body, None)])

    def delete_doc(self, *, collection: str, query: Optional[dict[str, Any]] = None, document_id: str = "") -> dict[str, Any]:
        cleaned = self._clean_scalar(collection)
        cleaned_id = self._clean_scalar(document_id)
        effective_query = {"_id": cleaned_id} if cleaned_id else (query or {})
        body = {"collection": cleaned, "coll": cleaned, "document_id": cleaned_id, "_id": cleaned_id, "id": cleaned_id, "query": effective_query}
        return self._try_requests([("POST", "/doc/delete", body, None)])

    def save_value(
        self,
        *,
        collection: str,
        query: Optional[dict[str, Any]] = None,
        document_id: str = "",
        field_path: str,
        value: Any,
        upsert_if_missing: bool = False,
        parse_json_strings: bool = True,
        normalize_for_storage: bool = False,
    ) -> dict[str, Any]:
        cleaned = self._clean_scalar(collection)
        body = {
            "collection": cleaned,
            "collection_name": cleaned,
            "query": query or {},
            "document_id": self._clean_scalar(document_id),
            "_id": self._clean_scalar(document_id),
            "field_path": self._clean_scalar(field_path),
            "value": value,
            "upsert_if_missing": bool(upsert_if_missing),
            "upsert": bool(upsert_if_missing),
            "parse_json_strings": bool(parse_json_strings),
            "normalize_for_storage": bool(normalize_for_storage),
        }
        return self._try_requests([("POST", "/save-value", body, None)])

    def fetch_image_field(self, *, collection: str, document_id: str, field_path: str, master_key_hex: str = "") -> tuple[bytes, str]:
        params = {"field_path": self._clean_scalar(field_path)}
        if master_key_hex:
            params["master_key_hex"] = self._clean_scalar(master_key_hex)
        content, _status, _content_type = self.request_bytes("GET", self.API_PREFIX, f"/image/{self._clean_scalar(collection)}/{self._clean_scalar(document_id)}", params=params)
        return content, "hosted_api_image_route"

    def fleet_status(self) -> dict[str, Any]:
        return self._try_requests([("GET", "/fleet/status", None, None)])

    def fleet_agents(self) -> dict[str, Any]:
        return self._try_requests([("GET", "/fleet/agents", None, None)])

    def fleet_dispatch(self, *, intent: str, payload: dict[str, Any], dispatch_id: str = "", timeout: float = 60.0, cost_usd: str = "") -> dict[str, Any]:
        body = {"intent": self._clean_scalar(intent), "payload": payload or {}, "dispatch_id": self._clean_scalar(dispatch_id), "timeout": float(timeout or 60.0), "cost_usd": self._clean_scalar(cost_usd)}
        return self._try_requests([("POST", "/fleet/dispatch", body, None)])


# -----------------------------------------------------------------------------
# Local File Store session
# -----------------------------------------------------------------------------


class ZMongoLocalFileStoreSession:
    """
    Local session that presents the same method surface as ZMongoApiSession while
    storing data through LocalFileStore on disk.

    This is intentionally not local MongoDB. It supports the same core node calls:
    list collections, list docs, get doc, query, count, create/update/delete,
    save value, save image, load image, and fetch image field.
    """

    STORAGE_BACKEND = "local_file_store"

    def __init__(self, *, root_dir: str = "") -> None:
        from .local_file_store import LocalFileStore

        clean_root = str(root_dir or "").strip()
        self.store = LocalFileStore(clean_root or None)
        self.local_store = self.store
        self._store = self.store
        self.storage_backend = self.STORAGE_BACKEND
        self.base_url = "local_file_store"
        self.comfy_zmongo_prefix = "local_file_store"
        self.zai_api_key = ""
        self.username = "local"
        self.root_dir = str(getattr(self.store, "root_dir", clean_root or ""))

    @staticmethod
    def _clean_scalar(value: Any) -> str:
        return ZMongoApiSession._clean_scalar(value)

    @staticmethod
    def _safe_int(value: Any, default: int) -> int:
        return ZMongoApiSession._safe_int(value, default)

    def close(self) -> None:
        return None

    def health(self) -> dict[str, Any]:
        payload = _safe_result_to_dict(self.store.ping())
        payload.setdefault("data", {})
        if isinstance(payload.get("data"), dict):
            payload["data"].setdefault("session_type", "ZMongoLocalFileStoreSession")
        return payload

    def whoami(self) -> dict[str, Any]:
        return {
            "success": True,
            "message": "Local File Store session active.",
            "data": {
                "username": "local",
                "db_name": "local_file_store",
                "silo_db_name": "local_file_store",
                "storage_backend": self.STORAGE_BACKEND,
                "root_dir": self.root_dir,
                "repo_class": self.__class__.__name__,
            },
            "error": None,
            "status_code": 200,
        }

    def list_collections(self) -> dict[str, Any]:
        return _safe_result_to_dict(self.store.list_collections())

    def create_collection(self, collection_name: str) -> dict[str, Any]:
        return _safe_result_to_dict(self.store.create_collection(self._clean_scalar(collection_name)))

    def delete_collection(self, collection_name: str) -> dict[str, Any]:
        # LocalFileStore does not expose a drop-collection primitive. Keep this
        # intentionally guarded to avoid accidental recursive file deletion.
        return {
            "success": False,
            "message": "Local File Store collection deletion is intentionally disabled.",
            "data": {"collection_name": self._clean_scalar(collection_name), "storage_backend": self.STORAGE_BACKEND},
            "error": {"type": "UnsupportedOperation", "msg": "Delete individual documents instead."},
            "status_code": 400,
        }

    def list_docs(self, *, collection: str, limit: int = 50, skip: int = 0, query: Optional[dict[str, Any]] = None) -> dict[str, Any]:
        # LocalFileStore has no skip parameter; apply skip after loading enough.
        safe_limit = max(1, self._safe_int(limit, 50))
        safe_skip = max(0, self._safe_int(skip, 0))
        payload = _safe_result_to_dict(
            self.store.list_documents(
                self._clean_scalar(collection),
                query=query or {},
                limit=safe_limit + safe_skip,
                include_documents=True,
            )
        )
        data = payload.get("data") if isinstance(payload, dict) else None
        if isinstance(data, dict) and isinstance(data.get("documents"), list):
            docs = data["documents"][safe_skip : safe_skip + safe_limit]
            data["documents"] = docs
            data["docs"] = docs
            data["items"] = docs
            data["count"] = len(docs)
            data["limit"] = safe_limit
            data["skip"] = safe_skip
        return payload

    def get_doc(self, *, collection: str, document_id: str, cache: bool = False) -> dict[str, Any]:
        cleaned_collection = self._clean_scalar(collection)
        cleaned_id = self._clean_scalar(document_id)
        payload = _safe_result_to_dict(
            self.store.load_document(cleaned_collection, document_id=cleaned_id)
        )
        document = _extract_document_from_local_payload(payload)
        if payload.get("success") and document:
            return {
                "success": True,
                "message": f"Loaded local document {cleaned_collection}/{cleaned_id}.",
                "data": {
                    "collection_name": cleaned_collection,
                    "document_id": cleaned_id,
                    "document": document,
                    "doc": document,
                    "item": document,
                    "record": document,
                    "documents": [document],
                    "docs": [document],
                    "items": [document],
                    "records": [document],
                    "storage_backend": self.STORAGE_BACKEND,
                    "api_response": payload,
                },
                "error": None,
                "status_code": 200,
            }
        return payload

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
        cleaned_collection = self._clean_scalar(collection)
        cleaned_id = self._clean_scalar(document_id)
        effective_query = dict(query or {})
        if cleaned_id:
            effective_query = {"_id": cleaned_id}

        if many:
            return self.list_docs(collection=cleaned_collection, query=effective_query, limit=limit, skip=skip)

        loaded = self.store.load_document(cleaned_collection, document_id=cleaned_id or None, query=effective_query or None)
        payload = _safe_result_to_dict(loaded)
        document = _extract_document_from_local_payload(payload)
        if payload.get("success") and document:
            payload.setdefault("data", {})
            if isinstance(payload["data"], dict):
                payload["data"].setdefault("document", document)
                payload["data"].setdefault("documents", [document])
        return payload

    def count_docs(self, *, collection: str, query: Optional[dict[str, Any]] = None, document_id: str = "", cache: bool = False) -> dict[str, Any]:
        payload = self.query_docs(collection=collection, query=query, document_id=document_id, many=True, limit=100000, skip=0)
        data = payload.get("data") if isinstance(payload, dict) else None
        docs = data.get("documents") if isinstance(data, dict) else []
        count = len(docs) if isinstance(docs, list) else 0
        return {
            "success": bool(payload.get("success")),
            "message": "Local document count loaded.",
            "data": {"count": count, "collection": self._clean_scalar(collection), "query": query or {}, "storage_backend": self.STORAGE_BACKEND},
            "error": None if payload.get("success") else payload.get("error"),
            "status_code": 200 if payload.get("success") else payload.get("status_code", 500),
        }

    def create_doc(self, *, collection: str, document: dict[str, Any]) -> dict[str, Any]:
        payload = _safe_result_to_dict(self.store.save_document(self._clean_scalar(collection), document or {}, upsert=True))
        data = payload.get("data") if isinstance(payload, dict) else None
        if isinstance(data, dict):
            doc_id = data.get("document_id")
            data.setdefault("inserted_id", doc_id)
            data.setdefault("_id", doc_id)
        return payload

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
        cleaned_collection = self._clean_scalar(collection)
        cleaned_id = self._clean_scalar(document_id)
        effective_query = {"_id": cleaned_id} if cleaned_id else (query or {})

        loaded = self.store.load_document(cleaned_collection, document_id=cleaned_id or None, query=effective_query or None)
        loaded_payload = _safe_result_to_dict(loaded)
        document = _extract_document_from_local_payload(loaded_payload)

        if not document:
            if not upsert:
                return loaded_payload
            document = {}
            if effective_query:
                for key, item in effective_query.items():
                    if not str(key).startswith("$"):
                        _set_dot_path(document, str(key), item)

        if isinstance(update, dict) and update:
            if isinstance(update.get("$set"), dict):
                for key, item in update["$set"].items():
                    _set_dot_path(document, str(key), item)
            else:
                document.update(update)
        elif field_path:
            if not _set_dot_path(document, self._clean_scalar(field_path), value):
                return {
                    "success": False,
                    "message": f"Could not set field_path: {field_path}",
                    "data": {"collection": cleaned_collection, "document_id": cleaned_id, "field_path": field_path},
                    "error": {"type": "ValueError", "msg": f"Could not set field_path: {field_path}"},
                    "status_code": 400,
                }
        else:
            return {
                "success": False,
                "message": "update or field_path is required.",
                "data": {"collection": cleaned_collection, "document_id": cleaned_id},
                "error": {"type": "ValueError", "msg": "Missing update or field_path."},
                "status_code": 400,
            }

        target_id = cleaned_id or str(document.get("_id") or "") or None
        return _safe_result_to_dict(self.store.save_document(cleaned_collection, document, document_id=target_id, upsert=True))

    def delete_doc(self, *, collection: str, query: Optional[dict[str, Any]] = None, document_id: str = "") -> dict[str, Any]:
        cleaned_collection = self._clean_scalar(collection)
        cleaned_id = self._clean_scalar(document_id)
        effective_query = {"_id": cleaned_id} if cleaned_id else (query or {})
        return _safe_result_to_dict(
            self.store.delete_document(
                cleaned_collection,
                document_id=cleaned_id or None,
                query=effective_query or None,
                delete_assets=True,
            )
        )

    def save_value(
        self,
        *,
        collection: str,
        query: Optional[dict[str, Any]] = None,
        document_id: str = "",
        field_path: str,
        value: Any,
        upsert_if_missing: bool = False,
        parse_json_strings: bool = True,
        normalize_for_storage: bool = False,
    ) -> dict[str, Any]:
        return _safe_result_to_dict(
            self.store.save_value(
                self._clean_scalar(collection),
                value,
                document_id=self._clean_scalar(document_id) or None,
                query=query or None,
                field_path=self._clean_scalar(field_path),
                upsert=bool(upsert_if_missing),
                parse_json_strings=bool(parse_json_strings),
            )
        )

    def get_value(self, *, collection: str, field_path: str = "", document_id: str = "", query: Optional[dict[str, Any]] = None, default: Any = None) -> dict[str, Any]:
        return _safe_result_to_dict(
            self.store.get_value(
                self._clean_scalar(collection),
                field_path=self._clean_scalar(field_path),
                document_id=self._clean_scalar(document_id) or None,
                query=query or None,
                default=default,
            )
        )

    def save_image(
        self,
        *,
        collection: str,
        image: Any,
        filename: str = "comfy_image.png",
        document_id: str = "",
        query: Optional[dict[str, Any]] = None,
        field_path: str = "image_data",
        metadata: Optional[dict[str, Any]] = None,
        upsert: bool = True,
    ) -> dict[str, Any]:
        return _safe_result_to_dict(
            self.store.save_image(
                self._clean_scalar(collection),
                image,
                filename=self._clean_scalar(filename) or "comfy_image.png",
                document_id=self._clean_scalar(document_id) or None,
                query=query or None,
                field_path=self._clean_scalar(field_path) or "image_data",
                metadata=metadata or {},
                upsert=bool(upsert),
            )
        )

    def load_image(
        self,
        *,
        collection: str,
        document_id: str = "",
        query: Optional[dict[str, Any]] = None,
        field_path: str = "image_data",
        as_base64: bool = False,
    ) -> dict[str, Any]:
        return _safe_result_to_dict(
            self.store.load_image(
                self._clean_scalar(collection),
                document_id=self._clean_scalar(document_id) or None,
                query=query or None,
                field_path=self._clean_scalar(field_path) or "image_data",
                as_base64=bool(as_base64),
            )
        )

    def fetch_image_field(self, *, collection: str, document_id: str, field_path: str, master_key_hex: str = "") -> tuple[bytes, str]:
        payload = self.load_image(
            collection=collection,
            document_id=document_id,
            field_path=field_path,
            as_base64=False,
        )
        if not payload.get("success"):
            raise RuntimeError(payload.get("message") or payload.get("error") or "Local image load failed.")

        data = payload.get("data") if isinstance(payload, dict) else None
        if not isinstance(data, dict):
            raise RuntimeError("Local image load returned invalid data.")

        image_bytes = data.get("bytes")
        if isinstance(image_bytes, str):
            # SafeResult JSON compatibility may turn bytes into text only when the
            # bytes happen to decode as UTF-8. For images this should normally be
            # a bytes envelope; handle both defensively.
            image_bytes = image_bytes.encode("utf-8")
        elif isinstance(image_bytes, dict) and image_bytes.get("__type__") == "bytes":
            import base64
            image_bytes = base64.b64decode(str(image_bytes.get("data") or ""))

        if not isinstance(image_bytes, (bytes, bytearray, memoryview)):
            # Direct file read fallback from local_path.
            local_path = data.get("local_path")
            if local_path:
                image_record = _get_dot_path(_extract_document_from_local_payload(self.get_doc(collection=collection, document_id=document_id)), field_path, {})
                local_path = image_record.get("local_path") if isinstance(image_record, dict) else local_path
                absolute = Path(self.root_dir) / str(local_path)
                image_bytes = absolute.read_bytes()

        if not isinstance(image_bytes, (bytes, bytearray, memoryview)):
            raise RuntimeError("Local image payload did not contain bytes.")

        return bytes(image_bytes), "local_file_store.load_image"

    def list_flattened_key_paths(self, collection: str, *, document_id: str = "", query: Optional[dict[str, Any]] = None, include_values: bool = False) -> dict[str, Any]:
        return _safe_result_to_dict(
            self.store.list_flattened_key_paths(
                self._clean_scalar(collection),
                document_id=self._clean_scalar(document_id) or None,
                query=query or None,
                include_values=bool(include_values),
            )
        )

    def get_storage_summary(self) -> dict[str, Any]:
        return _safe_result_to_dict(self.store.get_storage_summary())


# -----------------------------------------------------------------------------
# 00 Auth nodes
# -----------------------------------------------------------------------------


class ZMongoApiKeyOnlySessionNode(AlwaysDirtyMixin):
    CATEGORY = "ZMongo/00 Auth"
    FUNCTION = "connect"
    DISPLAY_ONLY = True

    @classmethod
    def INPUT_TYPES(cls):
        return {
            "required": {
                "api_key": ("STRING", {"description": "Your ZMongo API key"}),
            }
        }

    RETURN_TYPES = ("ZMONGO_API_SESSION", "STRING", "STRING")
    RETURN_NAMES = ("session", "json", "status")

    def connect(self, api_key: str):
        try:
            session = ZMongoApiSession(
                base_url="https://businessprocessapplications.com",
                zai_api_key=api_key,
            )
            payload = session.whoami()
            status = payload.get("message") or "API session created successfully."
            return session, _json_text(payload), status
        except Exception as exc:
            payload = _error_payload(str(exc))
            return None, _json_text(payload), f"API session failed: {exc}"


class ZMongoLocalFileStoreSessionNode(AlwaysDirtyMixin):
    CATEGORY = "ZMongo/00 Auth"
    FUNCTION = "connect"
    DISPLAY_ONLY = True

    @classmethod
    def INPUT_TYPES(cls):
        return {
            "required": {
                "local_store_root": (
                    "STRING",
                    {
                        "default": "",
                        "multiline": False,
                        "description": "Optional local store root. Leave blank to use nodes/local_store.",
                    },
                ),
                "test_health": ("BOOLEAN", {"default": True}),
            }
        }

    RETURN_TYPES = ("ZMONGO_API_SESSION", "STRING", "STRING")
    RETURN_NAMES = ("session", "json", "status")

    def connect(self, local_store_root: str = "", test_health: bool = True):
        try:
            session = ZMongoLocalFileStoreSession(root_dir=local_store_root)
            payload = session.health() if test_health else session.whoami()
            status = payload.get("message") or "Local File Store session created."
            return session, _json_text(payload), status
        except Exception as exc:
            payload = _error_payload(str(exc))
            return None, _json_text(payload), f"Local File Store session failed: {exc}"


class ZMongoApiCloseSessionNode(AlwaysDirtyMixin):
    CATEGORY = "ZMongo/00 Auth"
    FUNCTION = "close_session"

    @classmethod
    def INPUT_TYPES(cls):
        return {"required": {"session": ("ZMONGO_API_SESSION",)}}

    RETURN_TYPES = ("STRING",)
    RETURN_NAMES = ("json",)

    def close_session(self, session):
        if session is None:
            return (_json_text(_error_payload("No session provided.")),)
        try:
            close = getattr(session, "close", None)
            if callable(close):
                close()
            return (_json_text(_success_payload("Session closed.")),)
        except Exception as exc:
            return (_json_text(_error_payload(str(exc))),)


NODE_CLASS_MAPPINGS = {
    "ZMongoApiKeyOnlySessionNode": ZMongoApiKeyOnlySessionNode,
    "ZMongoLocalFileStoreSessionNode": ZMongoLocalFileStoreSessionNode,
    "ZMongoApiCloseSessionNode": ZMongoApiCloseSessionNode,
}

NODE_DISPLAY_NAME_MAPPINGS = {
    "ZMongoApiKeyOnlySessionNode": "00 API Key Session",
    "ZMongoLocalFileStoreSessionNode": "00 Local File Store Session",
    "ZMongoApiCloseSessionNode": "00 Close Session",
}

__all__ = [
    "ZMongoApiSession",
    "ZMongoLocalFileStoreSession",
    "ZMongoApiKeyOnlySessionNode",
    "ZMongoLocalFileStoreSessionNode",
    "ZMongoApiCloseSessionNode",
    "NODE_CLASS_MAPPINGS",
    "NODE_DISPLAY_NAME_MAPPINGS",
]