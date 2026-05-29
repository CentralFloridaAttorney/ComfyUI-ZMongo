from __future__ import annotations

import json
import os
import urllib.parse
from pathlib import Path
from typing import Any, Optional

import requests


from .generic_helpers import AlwaysDirtyMixin, DEFAULT_BASE_URL, DEFAULT_COMFY_ZMONGO_PREFIX, DEFAULT_FLEET_PREFIX, \
    DEFAULT_COMFY_ZMONGO_FLEET_PREFIX, DEFAULT_TIMEOUT, _normalize_base_url, _clean_prefix, _json_text, _error_payload, \
    _success_payload, _indexed_list_text, _extract_collections, _as_comfy_list, _dirty_token, _parse_json_object, \
    _parse_json_list, _extract_doc_ids, _extract_count, _parse_any_json, _extract_document_from_payload, \
    ZMongoLocalFileStoreSessionNode, safe_get_by_path, _ensure_payload_dict
from .local_file_store import LocalFileStore


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
            headers["ZAI_USER"] = self.username
            headers["ZAI-USER"] = self.username
            headers["X-ZAI-User"] = self.username
            headers["X-Username"] = self.username
            headers["X-User"] = self.username
            headers["ZAI_USER"] = self.username
            headers["ZAI-USER"] = self.username
            headers["X-ZAI-User"] = self.username
            headers["X-Username"] = self.username
            headers["X-User"] = self.username

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
        from .zmongo_image_nodes import _route_image_field_path

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
# Local File Store session adapter
# -----------------------------------------------------------------------------

class LocalFileStoreSessionAdapter:
    """
    ZMongoApiSession-compatible wrapper around local_file_store.LocalFileStore.

    This keeps existing ZMongo nodes unchanged. Any node that expects methods like
    create_doc(), list_docs(), get_doc(), save_value(), query_docs(), and
    fetch_image_field() can use this local session exactly like the hosted API
    session, but all persistence is handled by local_file_store.py.
    """

    storage_backend = "local_file_store"
    base_url = "local://comfyui-zmongo"
    username = "local_user"
    zai_api_key = ""
    comfy_zmongo_prefix = "/local-file-store"
    fleet_prefix = "/local-disabled"
    comfy_zmongo_fleet_prefix = "/local-disabled"

    def __init__(self, root_dir: Optional[str | Path] = None) -> None:
        if LocalFileStore is None:
            raise RuntimeError(
                "local_file_store.py could not be imported. Confirm it exists next to zmongo_api_nodes.py."
            )
        self.store = LocalFileStore(root_dir=root_dir)
        self.root_dir = self.store.root_dir

    @staticmethod
    def _payload(result: Any) -> dict[str, Any]:
        if hasattr(result, "to_dict") and callable(result.to_dict):
            return _ensure_payload_dict(result.to_dict())
        return _ensure_payload_dict(result)

    @staticmethod
    def _extract_documents_from_payload(payload: dict[str, Any]) -> list[dict[str, Any]]:
        data = payload.get("data") if isinstance(payload, dict) else None
        if isinstance(data, dict):
            docs = data.get("documents")
            if isinstance(docs, list):
                return [doc for doc in docs if isinstance(doc, dict)]
            doc = data.get("document")
            if isinstance(doc, dict):
                return [doc]
        if isinstance(data, list):
            return [doc for doc in data if isinstance(doc, dict)]
        return []

    def close(self) -> None:
        return None

    def health(self) -> dict[str, Any]:
        return self._payload(self.store.ping())

    def whoami(self) -> dict[str, Any]:
        return _success_payload(
            "Local File Store session active.",
            {
                "username": self.username,
                "silo_db_name": "local_file_store",
                "db_name": "local_file_store",
                "root_dir": str(self.root_dir),
                "storage_backend": self.storage_backend,
            },
        )

    def list_collections(self) -> dict[str, Any]:
        return self._payload(self.store.list_collections())

    def create_collection(self, collection: str) -> dict[str, Any]:
        return self._payload(self.store.create_collection(collection))

    def delete_collection(self, collection: str) -> dict[str, Any]:
        # Keep this non-destructive until LocalFileStore has an explicit
        # delete_collection implementation.
        return _error_payload(
            "Local File Store delete_collection is intentionally disabled.",
            data={"collection": collection, "storage_backend": self.storage_backend},
            status_code=405,
        )

    def list_docs(
        self,
        *,
        collection: str,
        limit: int = 50,
        skip: int = 0,
        query: Optional[dict[str, Any]] = None,
    ) -> dict[str, Any]:
        payload = self._payload(
            self.store.list_documents(
                collection,
                query=query or {},
                limit=max(1, min(int(limit or 50), 500)) + max(0, int(skip or 0)),
                include_documents=True,
            )
        )
        if payload.get("success") and int(skip or 0) > 0:
            data = payload.setdefault("data", {})
            docs = data.get("documents") if isinstance(data, dict) else None
            if isinstance(docs, list):
                data["documents"] = docs[int(skip or 0):]
                data["count"] = len(data["documents"])
        return payload

    def get_doc(self, *, collection: str, document_id: str, cache: bool = False) -> dict[str, Any]:
        return self._payload(self.store.load_document(collection, document_id=document_id))

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
        if document_id:
            return self.get_doc(collection=collection, document_id=document_id, cache=cache)

        payload = self.list_docs(collection=collection, query=query or {}, limit=limit, skip=skip)
        if payload.get("success") and not many:
            data = payload.setdefault("data", {})
            docs = data.get("documents") if isinstance(data, dict) else []
            first = docs[0] if isinstance(docs, list) and docs else {}
            data["document"] = first
            data["documents"] = [first] if first else []
            data["count"] = 1 if first else 0
        return payload

    def count_docs(
        self,
        *,
        collection: str,
        query: Optional[dict[str, Any]] = None,
        document_id: str = "",
        cache: bool = False,
    ) -> dict[str, Any]:
        if document_id:
            payload = self.get_doc(collection=collection, document_id=document_id, cache=cache)
            count = 1 if payload.get("success") else 0
        else:
            payload = self.list_docs(collection=collection, query=query or {}, limit=500, skip=0)
            count = len(self._extract_documents_from_payload(payload)) if payload.get("success") else 0
        return _success_payload(
            "Counted local documents.",
            {"collection": collection, "query": query or {}, "count": count, "storage_backend": self.storage_backend},
        )

    def create_doc(self, *, collection: str, document: dict[str, Any]) -> dict[str, Any]:
        payload = self._payload(self.store.save_document(collection, document, upsert=True))
        data = payload.setdefault("data", {})
        if isinstance(data, dict) and data.get("document_id") and not data.get("inserted_id"):
            data["inserted_id"] = data["document_id"]
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
        if update is not None:
            set_values = update.get("$set") if isinstance(update, dict) else None
            if isinstance(set_values, dict):
                target_id = document_id or ""
                response: dict[str, Any] = {}
                for path, path_value in set_values.items():
                    response = self.save_value(
                        collection=collection,
                        query=query or {},
                        document_id=target_id,
                        field_path=str(path),
                        value=path_value,
                        upsert_if_missing=upsert,
                        parse_json_strings=False,
                        normalize_for_storage=False,
                    )
                    if not response.get("success"):
                        return response
                    target_id = str(response.get("data", {}).get("document_id") or target_id)
                return response or _success_payload("No fields to update.")

            if isinstance(update, dict):
                return self._payload(
                    self.store.save_value(
                        collection,
                        update,
                        document_id=document_id or None,
                        query=query or None,
                        field_path=None,
                        upsert=upsert,
                        parse_json_strings=False,
                    )
                )

        return self.save_value(
            collection=collection,
            query=query or {},
            document_id=document_id,
            field_path=field_path,
            value=value,
            upsert_if_missing=upsert,
            parse_json_strings=False,
            normalize_for_storage=False,
        )

    def delete_doc(self, *, collection: str, query: Optional[dict[str, Any]] = None, document_id: str = "") -> dict[str, Any]:
        return self._payload(
            self.store.delete_document(
                collection,
                document_id=document_id or None,
                query=query or None,
                delete_assets=False,
            )
        )

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
        return self._payload(
            self.store.save_value(
                collection,
                value,
                document_id=document_id or None,
                query=query or None,
                field_path=field_path or None,
                upsert=bool(upsert_if_missing),
                parse_json_strings=bool(parse_json_strings),
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
        return self._payload(
            self.store.save_image(
                collection,
                image,
                filename=filename,
                document_id=document_id or None,
                query=query or None,
                field_path=field_path or "image_data",
                metadata=metadata or {},
                upsert=upsert,
            )
        )

    def fetch_image_field(
        self,
        *,
        collection: str,
        document_id: str,
        field_path: str,
        master_key_hex: str = "",
    ) -> tuple[bytes, str]:
        payload = self._payload(
            self.store.load_image(
                collection,
                document_id=document_id,
                field_path=field_path or "image_data",
                as_base64=False,
            )
        )
        if not payload.get("success"):
            raise ValueError(payload.get("message") or "Local image load failed.")
        data = payload.get("data", {})
        image_bytes = data.get("bytes") if isinstance(data, dict) else None
        if not isinstance(image_bytes, (bytes, bytearray)):
            raise ValueError("Local image payload did not contain bytes.")
        return bytes(image_bytes), f"local_file_store:{data.get('local_path') or field_path}"

    def fetch_absolute_or_relative_bytes(self, url: str) -> bytes:
        target = str(url or "").strip()
        if not target:
            raise ValueError("url/path is required.")
        path = Path(target)
        if not path.is_absolute():
            path = (self.root_dir / target).resolve()
        else:
            path = path.resolve()
        root = self.root_dir.resolve()
        if path != root and root not in path.parents:
            raise ValueError(f"Path escapes Local File Store root: {target}")
        return path.read_bytes()


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
            root_text = str(local_store_root or "").strip()
            root_dir = Path(root_text).expanduser().resolve() if root_text else None
            session = LocalFileStoreSessionAdapter(root_dir=root_dir)
            payload = session.health() if test_health else _success_payload(
                "Local File Store session created.",
                {
                    "storage_backend": session.storage_backend,
                    "root_dir": str(session.root_dir),
                    "mode": "Local File Store",
                },
            )
            return (session, _json_text(payload), payload.get("message") or "Local File Store session created.")
        except Exception as exc:
            payload = _error_payload(
                f"Local File Store session failed: {exc}",
                data={"local_store_root": local_store_root, "error_type": exc.__class__.__name__},
                status_code=0,
                error_type=exc.__class__.__name__,
            )
            return (None, _json_text(payload), payload["message"])

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
    CATEGORY = "ZMongo/00 Auth"

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
    CATEGORY = "ZMongo/00 Auth"

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
    CATEGORY = "ZMongo/01 Service"

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
    CATEGORY = "ZMongo/01 Service"

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
    CATEGORY = "ZMongo/02 Collections"

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
    CATEGORY = "ZMongo/02 Collections"

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
    CATEGORY = "ZMongo/02 Collections"

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
    CATEGORY = "ZMongo/03 Docs"

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
    CATEGORY = "ZMongo/03 Docs"

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
    CATEGORY = "ZMongo/03 Docs"

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
    CATEGORY = "ZMongo/03 Docs"

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
    CATEGORY = "ZMongo/03 Docs"

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
    CATEGORY = "ZMongo/03 Docs"

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
    CATEGORY = "ZMongo/03 Docs"

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


class ZMongoApiGetValueNode(AlwaysDirtyMixin):
    """
    Get one value from a ZMongo document by dot-path.

    Examples:
    - field_path = metadata.prompt
    - field_path = image_data.metadata.seed
    - field_path = image_data
    - field_path = ""  -> returns the whole document JSON
    """

    @classmethod
    def INPUT_TYPES(cls):
        return {
            "required": {
                "session": ("ZMONGO_API_SESSION",),
                "collection_name": ("STRING", {"default": ""}),
                "document_id": ("STRING", {"default": ""}),
                "field_path": ("STRING", {"default": ""}),
                "fallback": ("STRING", {"default": ""}),
                "cache": ("BOOLEAN", {"default": False}),
            },
            "optional": {
                "refresh_token": ("STRING", {"default": ""}),
            },
        }

    RETURN_TYPES = ("STRING", "STRING", "BOOLEAN", "STRING", "STRING")
    RETURN_NAMES = ("json", "value", "exists", "value_type", "refresh")
    FUNCTION = "get_value"
    CATEGORY = "ZMongo/03 Docs"

    @staticmethod
    def _clean_scalar(value: Any) -> str:
        """
        Normalize scalar values coming from ComfyUI links.

        Handles:
        - ["abc"] -> "abc"
        - ("abc",) -> "abc"
        - "(abc)" -> "abc"
        - "('abc',)" -> "abc"
        - '["abc"]' -> "abc"
        """
        if isinstance(value, (list, tuple)):
            if not value:
                return ""
            value = value[0]

        if value is None:
            return ""

        text = str(value).strip()

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
    def _stringify_value(value: Any) -> str:
        if value is None:
            return ""

        if isinstance(value, str):
            return value

        if isinstance(value, (dict, list, tuple)):
            return _json_text(value)

        return str(value)

    def get_value(
        self,
        session,
        collection_name: str,
        document_id: str,
        field_path: str,
        fallback: str,
        cache: bool,
        refresh_token: str = "",
    ):
        cleaned_collection = self._clean_scalar(collection_name)
        cleaned_document_id = self._clean_scalar(document_id)
        cleaned_field_path = self._clean_scalar(field_path)
        cleaned_fallback = self._clean_scalar(fallback)

        refresh = _dirty_token(
            "get_value",
            cleaned_collection,
            cleaned_document_id,
            cleaned_field_path,
            refresh_token,
        )

        if session is None:
            payload = _error_payload("No API session provided.")
            return (_json_text(payload), cleaned_fallback, False, "missing_session", refresh)

        if not cleaned_collection:
            payload = _error_payload("collection_name is required.")
            return (_json_text(payload), cleaned_fallback, False, "missing_collection_name", refresh)

        if not cleaned_document_id:
            payload = _error_payload("document_id is required.")
            return (_json_text(payload), cleaned_fallback, False, "missing_document_id", refresh)

        try:
            api_payload = session.get_doc(
                collection=cleaned_collection,
                document_id=cleaned_document_id,
                cache=cache,
            )

            document = _extract_document_from_payload(api_payload)

            if not document:
                payload = {
                    "success": False,
                    "message": "Document not found or API payload did not contain a document.",
                    "data": {
                        "collection_name": cleaned_collection,
                        "document_id": cleaned_document_id,
                        "field_path": cleaned_field_path,
                        "fallback": cleaned_fallback,
                        "api_payload_success": api_payload.get("success") if isinstance(api_payload, dict) else None,
                        "api_payload_status_code": api_payload.get("status_code") if isinstance(api_payload, dict) else None,
                        "api_payload_message": api_payload.get("message") if isinstance(api_payload, dict) else None,
                        "refresh": refresh,
                    },
                    "error": {"msg": "No document returned."},
                    "status_code": 404,
                }
                return (_json_text(payload), cleaned_fallback, False, "missing_document", refresh)

            if cleaned_field_path:
                value = safe_get_by_path(document, cleaned_field_path, default=None)
                exists = value is not None
                resolved_path = cleaned_field_path
            else:
                value = document
                exists = True
                resolved_path = ""

            if not exists:
                payload = {
                    "success": False,
                    "message": f"No value found at field path {cleaned_field_path!r}.",
                    "data": {
                        "collection_name": cleaned_collection,
                        "document_id": cleaned_document_id,
                        "field_path": cleaned_field_path,
                        "fallback": cleaned_fallback,
                        "document_top_level_keys": sorted(str(k) for k in document.keys()),
                        "refresh": refresh,
                        "checks": [
                            "Confirm field_path is spelled correctly.",
                            "Use 04 Metadata Flattened Paths with metadata_field_path blank to list all paths.",
                            "Use 04 Debug Image Document to inspect image document structure.",
                            "Leave field_path blank to return the whole document JSON.",
                        ],
                    },
                    "error": {"msg": "Field path not found."},
                    "status_code": 404,
                }
                return (_json_text(payload), cleaned_fallback, False, "missing_value", refresh)

            value_type = type(value).__name__
            value_text = self._stringify_value(value)

            payload = {
                "success": True,
                "message": (
                    "Loaded whole document."
                    if not cleaned_field_path
                    else f"Loaded value from field path {cleaned_field_path!r}."
                ),
                "data": {
                    "collection_name": cleaned_collection,
                    "document_id": cleaned_document_id,
                    "field_path": cleaned_field_path,
                    "resolved_path": resolved_path,
                    "exists": True,
                    "value_type": value_type,
                    "value": value,
                    "value_text": value_text,
                    "refresh": refresh,
                },
                "error": None,
                "status_code": 200,
            }

            return (_json_text(payload), value_text, True, value_type, refresh)

        except Exception as exc:
            payload = {
                "success": False,
                "message": f"Get Value failed: {exc}",
                "data": {
                    "collection_name": cleaned_collection,
                    "document_id": cleaned_document_id,
                    "field_path": cleaned_field_path,
                    "fallback": cleaned_fallback,
                    "refresh": refresh,
                    "checks": [
                        "Confirm API session is connected.",
                        "Confirm collection_name is correct.",
                        "Confirm document_id exists.",
                        "Confirm field_path is a valid dot-path.",
                    ],
                },
                "error": {
                    "type": exc.__class__.__name__,
                    "msg": str(exc),
                },
                "status_code": 0,
            }
            return (_json_text(payload), cleaned_fallback, False, "error", refresh)


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
                "upsert_if_missing": ("BOOLEAN", {"default": False}),
                "parse_json_strings": ("BOOLEAN", {"default": True}),
                "normalize_for_storage": ("BOOLEAN", {"default": False}),
            }
        }

    RETURN_TYPES = ("STRING", "STRING", "BOOLEAN")
    RETURN_NAMES = ("json", "refresh", "success")
    FUNCTION = "save_value"
    CATEGORY = "ZMongo/03 Docs"

    @staticmethod
    def _clean_scalar(value: Any) -> str:
        """
        Normalize scalar values coming from ComfyUI links.

        Handles:
        - ["abc"] -> "abc"
        - ("abc",) -> "abc"
        - "(abc)" -> "abc"
        - "('abc',)" -> "abc"
        - '["abc"]' -> "abc"
        """
        if isinstance(value, (list, tuple)):
            if not value:
                return ""
            value = value[0]

        if value is None:
            return ""

        text = str(value).strip()

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
    def _parse_query_or_empty(query_json: Any) -> dict[str, Any]:
        text = ZMongoApiSaveValueNode._clean_scalar(query_json)
        if not text:
            return {}

        parsed = json.loads(text)
        if not isinstance(parsed, dict):
            raise ValueError("query_json must be a JSON object.")
        return parsed

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
        cleaned_collection = self._clean_scalar(collection_name)
        cleaned_document_id = self._clean_scalar(document_id)
        cleaned_field_path = self._clean_scalar(field_path)
        cleaned_value_json = self._clean_scalar(value_json)

        token = _dirty_token(
            "save_value",
            cleaned_collection,
            cleaned_document_id,
            cleaned_field_path,
        )

        if session is None:
            payload = _error_payload("No API session provided.")
            return (_json_text(payload), token, False)

        if not cleaned_collection:
            payload = _error_payload("collection_name is required.")
            return (_json_text(payload), token, False)

        if not cleaned_field_path:
            payload = _error_payload("field_path is required.")
            return (_json_text(payload), token, False)

        try:
            query = self._parse_query_or_empty(query_json)

            # ------------------------------------------------------------
            # Critical fix:
            # If a document_id is connected, use it as the explicit target.
            # This avoids backend rejection: "query or document_id is required."
            # ------------------------------------------------------------
            query_used = dict(query)
            document_id_used = cleaned_document_id

            if cleaned_document_id:
                query_used = {"_id": cleaned_document_id}
                document_id_used = ""

            if not query_used and not cleaned_document_id:
                payload = {
                    "success": False,
                    "message": "Save Value requires a selected document_id or a non-empty query_json.",
                    "data": {
                        "collection_name": cleaned_collection,
                        "document_id": cleaned_document_id,
                        "query_json": query_json,
                        "field_path": cleaned_field_path,
                        "refresh": token,
                        "checks": [
                            "Connect document_id from 99 Select Nth Item.",
                            "Or enter query_json such as {\"_id\": \"DOCUMENT_ID_HERE\"}.",
                            "Confirm collection_name is connected from 99 Select Nth Item.",
                            "Confirm field_path is the dot-path you want to write.",
                        ],
                    },
                    "error": {"msg": "query or document_id is required."},
                    "status_code": 400,
                }
                return (_json_text(payload), token, False)

            value = _parse_any_json(cleaned_value_json, parse_value_json)

            payload = session.save_value(
                collection=cleaned_collection,
                query=query_used,
                document_id=document_id_used,
                field_path=cleaned_field_path,
                value=value,
                upsert_if_missing=upsert_if_missing,
                parse_json_strings=parse_json_strings,
                normalize_for_storage=normalize_for_storage,
            )

            success = bool(payload.get("success")) if isinstance(payload, dict) else False

            result_payload = {
                "success": success,
                "message": (
                    f"Saved value to {cleaned_collection} at {cleaned_field_path}."
                    if success
                    else f"Failed to save value to {cleaned_collection} at {cleaned_field_path}."
                ),
                "data": {
                    "operation": "save_value",
                    "collection_name": cleaned_collection,
                    "document_id": cleaned_document_id,
                    "query_used": query_used,
                    "field_path": cleaned_field_path,
                    "value_preview": cleaned_value_json[:300],
                    "upsert_if_missing": bool(upsert_if_missing),
                    "parse_json_strings": bool(parse_json_strings),
                    "normalize_for_storage": bool(normalize_for_storage),
                    "refresh": token,
                    "api_response": payload,
                },
                "error": None if success else (
                    payload.get("error") if isinstance(payload, dict) else "Save failed."
                ),
                "status_code": payload.get("status_code", 0) if isinstance(payload, dict) else 0,
            }

            return (_json_text(result_payload), token, success)

        except Exception as exc:
            payload = {
                "success": False,
                "message": f"Save Value failed: {exc}",
                "data": {
                    "collection_name": cleaned_collection,
                    "document_id": cleaned_document_id,
                    "field_path": cleaned_field_path,
                    "refresh": token,
                    "checks": [
                        "Confirm query_json is valid JSON.",
                        "Confirm value_json is valid JSON when parse_value_json is true.",
                        "Confirm document_id is connected or query_json is non-empty.",
                        "Confirm field_path is not empty.",
                    ],
                },
                "error": {
                    "type": exc.__class__.__name__,
                    "msg": str(exc),
                },
                "status_code": 0,
            }
            return (_json_text(payload), token, False)


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
    CATEGORY = "ZMongo/05 Fleet"

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
    CATEGORY = "ZMongo/05 Fleet"

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
    CATEGORY = "ZMongo/05 Fleet"

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
# ComfyUI mappings
# -----------------------------------------------------------------------------

NODE_CLASS_MAPPINGS = {
    # 00 Auth
    "ZMongoLocalFileStoreSessionNode": ZMongoLocalFileStoreSessionNode,
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
    "ZMongoApiGetValueNode": ZMongoApiGetValueNode,
    "ZMongoApiSaveValueNode": ZMongoApiSaveValueNode,
}

NODE_DISPLAY_NAME_MAPPINGS = {
    # 00 Auth
    "ZMongoLocalFileStoreSessionNode": "00 Local File Store Session",
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
    "ZMongoApiGetValueNode": "03 Get Value",
    "ZMongoApiSaveValueNode": "03 Save Value",
}


__all__ = [
    "NODE_CLASS_MAPPINGS",
    "NODE_DISPLAY_NAME_MAPPINGS",
]