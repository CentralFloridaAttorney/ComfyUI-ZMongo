from __future__ import annotations

"""
Endpoint compatibility patches for the hosted BPA / ComfyUI-ZMongo API.

This module is intentionally small and imported by nodes.__init__ after the node
modules are loaded.  It patches the hosted API session class instead of
rewriting every node class that calls the session surface.

Current hosted API families supported by this patch:
  - /comfy-zmongo/api/doc-image-status/<collection>/<document_id>
  - /comfy-zmongo/api/image/<collection>/<document_id>
  - /comfy-zmongo/api/image-field/view/<collection>/<document_id>
  - /comfy-zmongo/api/doc-field-paths/<collection>/<document_id>
  - /comfy-zmongo/api/doc/<collection>/<document_id>
  - /comfy-zmongo/api/doc
  - /comfy-zmongo/api/create, /update, /delete manager-compatible aliases
"""

from typing import Any, Optional
from urllib.parse import urlparse


def _as_text(value: Any) -> str:
    if isinstance(value, (list, tuple)):
        value = value[0] if value else ""
    if value is None:
        return ""
    return str(value).strip()


def _route_miss(payload: Any) -> bool:
    if not isinstance(payload, dict):
        return True
    try:
        status_code = int(payload.get("status_code") or 0)
    except Exception:
        status_code = 0
    if status_code in {404, 405, 501}:
        return True
    haystack = " ".join(str(payload.get(key) or "").lower() for key in ("message", "raw_text", "error"))
    return any(marker in haystack for marker in ("not found", "method not allowed", "route", "404", "405"))


def _patch_zmongo_api_session() -> bool:
    try:
        from . import zmongo_auth_nodes
    except Exception:
        return False

    session_cls = getattr(zmongo_auth_nodes, "ZMongoApiSession", None)
    if session_cls is None or getattr(session_cls, "_BPA_ENDPOINT_COMPAT_PATCHED", False):
        return False

    def _try_requests_compat(self, candidates: list[tuple[str, str, dict[str, Any] | None, dict[str, Any] | None]]) -> dict[str, Any]:
        attempted: list[dict[str, Any]] = []
        last_payload: dict[str, Any] | None = None

        for method, path, json_body, params in candidates:
            payload = self.request(method, self.API_PREFIX, path, json_body=json_body, params=params)
            last_payload = payload if isinstance(payload, dict) else {"success": False, "message": str(payload), "data": {}}
            attempted.append(
                {
                    "method": method.upper(),
                    "path": self._join_path(self.API_PREFIX, path),
                    "url": self._build_url(self.API_PREFIX, path),
                    "status_code": last_payload.get("status_code"),
                    "success": last_payload.get("success"),
                    "message": last_payload.get("message"),
                }
            )
            if bool(last_payload.get("success")):
                last_payload.setdefault("data", {})
                if isinstance(last_payload.get("data"), dict):
                    last_payload["data"].setdefault("attempted_routes", attempted)
                return last_payload
            if not _route_miss(last_payload):
                last_payload.setdefault("data", {})
                if isinstance(last_payload.get("data"), dict):
                    last_payload["data"].setdefault("attempted_routes", attempted)
                return last_payload

        if last_payload is None:
            last_payload = {"success": False, "message": "No endpoint candidates were attempted.", "data": {}, "status_code": 0}
        last_payload.setdefault("data", {})
        if isinstance(last_payload.get("data"), dict):
            last_payload["data"].setdefault("attempted_routes", attempted)
        return last_payload

    def create_doc(self, *, collection: str, document: dict[str, Any]) -> dict[str, Any]:
        cleaned = self._clean_scalar(collection)
        body = {"collection": cleaned, "collection_name": cleaned, "document": document or {}}
        return self._try_requests(
            [
                ("POST", "/doc", body, None),
                ("POST", "/create", body, None),
                ("POST", "/doc/create", body, None),
            ]
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
        cleaned = self._clean_scalar(collection)
        cleaned_id = self._clean_scalar(document_id)
        cleaned_field = self._clean_scalar(field_path)
        body = {
            "collection": cleaned,
            "collection_name": cleaned,
            "query": query or {},
            "document_id": cleaned_id,
            "_id": cleaned_id,
            "id": cleaned_id,
            "update": update,
            "field_path": cleaned_field,
            "key": cleaned_field,
            "value": value,
            "upsert": bool(upsert),
        }
        if update is None and cleaned_field:
            body["update"] = {"$set": {cleaned_field: value}}
        candidates: list[tuple[str, str, dict[str, Any] | None, dict[str, Any] | None]] = []
        if cleaned and cleaned_id:
            candidates.extend(
                [
                    ("PATCH", f"/doc/{cleaned}/{cleaned_id}", body, None),
                    ("POST", f"/doc/{cleaned}/{cleaned_id}", body, None),
                ]
            )
        candidates.extend(
            [
                ("POST", "/update", body, None),
                ("POST", "/doc/update", body, None),
                ("POST", "/save-value", body, None) if cleaned_field else ("POST", "/doc/update", body, None),
            ]
        )
        return self._try_requests(candidates)

    def delete_doc(self, *, collection: str, query: Optional[dict[str, Any]] = None, document_id: str = "") -> dict[str, Any]:
        cleaned = self._clean_scalar(collection)
        cleaned_id = self._clean_scalar(document_id)
        effective_query = {"_id": cleaned_id} if cleaned_id else (query or {})
        body = {"collection": cleaned, "collection_name": cleaned, "coll": cleaned, "document_id": cleaned_id, "_id": cleaned_id, "id": cleaned_id, "query": effective_query}
        return self._try_requests(
            [
                ("POST", "/delete", body, None),
                ("POST", "/doc/delete", body, None),
            ]
        )

    def doc_image_status(self, *, collection: str, document_id: str, field_path: str = "image_data", variant: str = "preview", master_key_hex: str = "") -> dict[str, Any]:
        cleaned_collection = self._clean_scalar(collection)
        cleaned_id = self._clean_scalar(document_id)
        params = {
            "field_path": self._clean_scalar(field_path) or "image_data",
            "variant": self._clean_scalar(variant) or "preview",
        }
        if master_key_hex:
            params["master_key_hex"] = self._clean_scalar(master_key_hex)
        return self._try_requests(
            [
                ("GET", f"/doc-image-status/{cleaned_collection}/{cleaned_id}", None, params),
                ("GET", f"/image-status/{cleaned_collection}/{cleaned_id}", None, params),
            ]
        )

    def doc_field_paths(self, *, collection: str, document_id: str, include_containers: bool = True) -> dict[str, Any]:
        cleaned_collection = self._clean_scalar(collection)
        cleaned_id = self._clean_scalar(document_id)
        params = {"include_containers": str(bool(include_containers)).lower()}
        return self._try_requests(
            [
                ("GET", f"/doc-field-paths/{cleaned_collection}/{cleaned_id}", None, params),
                ("GET", f"/field-paths/{cleaned_collection}/{cleaned_id}", None, params),
            ]
        )

    def _bytes_from_stream_url(self, stream_url: str) -> tuple[bytes, int, str]:
        if not stream_url:
            raise ValueError("stream_url is empty.")
        parsed = urlparse(stream_url)
        if parsed.scheme in {"http", "https"}:
            url = stream_url
        else:
            url = self.base_url + (stream_url if stream_url.startswith("/") else "/" + stream_url)
        response = self.session.get(url, headers=self._headers(), verify=self.verify_tls, timeout=60)
        response.raise_for_status()
        return response.content, response.status_code, response.headers.get("Content-Type", "")

    def fetch_image_field(self, *, collection: str, document_id: str, field_path: str, master_key_hex: str = "") -> tuple[bytes, str]:
        cleaned_collection = self._clean_scalar(collection)
        cleaned_id = self._clean_scalar(document_id)
        cleaned_field = self._clean_scalar(field_path) or "image_data"
        base_params = {"field_path": cleaned_field}
        if master_key_hex:
            base_params["master_key_hex"] = self._clean_scalar(master_key_hex)

        byte_candidates = [
            ("/image-field/view/{collection}/{document_id}", "preview"),
            ("/image-field/download/{collection}/{document_id}", "original"),
            ("/image/{collection}/{document_id}", "original"),
            ("/image/{collection}/{document_id}", "preview"),
        ]
        errors: list[str] = []
        for template, variant in byte_candidates:
            params = dict(base_params)
            params["variant"] = variant
            path = template.format(collection=cleaned_collection, document_id=cleaned_id)
            try:
                content, status, content_type = self.request_bytes("GET", self.API_PREFIX, path, params=params)
                if content and not content[:1] in (b"{", b"["):
                    return content, f"hosted_api:{path}:{variant}:{status}:{content_type}"
                errors.append(f"{path}:{variant}: returned JSON/text instead of image bytes")
            except Exception as exc:
                errors.append(f"{path}:{variant}: {exc}")

        for variant in ("original", "preview"):
            status_payload = doc_image_status(
                self,
                collection=cleaned_collection,
                document_id=cleaned_id,
                field_path=cleaned_field,
                variant=variant,
                master_key_hex=master_key_hex,
            )
            data = status_payload.get("data") if isinstance(status_payload, dict) else None
            stream_url = data.get("stream_url") if isinstance(data, dict) else ""
            if stream_url:
                try:
                    content, status, content_type = _bytes_from_stream_url(self, stream_url)
                    if content and not content[:1] in (b"{", b"["):
                        return content, f"hosted_api:doc_image_status:{variant}:{status}:{content_type}"
                    errors.append(f"doc-image-status:{variant}: stream_url returned JSON/text")
                except Exception as exc:
                    errors.append(f"doc-image-status:{variant}: {exc}")
            else:
                errors.append(f"doc-image-status:{variant}: no stream_url; message={status_payload.get('message') if isinstance(status_payload, dict) else status_payload}")

        raise RuntimeError("Image field could not be fetched from the hosted BPA API. " + " | ".join(errors[-8:]))

    session_cls._try_requests = _try_requests_compat
    session_cls.create_doc = create_doc
    session_cls.update_doc = update_doc
    session_cls.delete_doc = delete_doc
    session_cls.doc_image_status = doc_image_status
    session_cls.doc_field_paths = doc_field_paths
    session_cls.fetch_image_field = fetch_image_field
    session_cls._BPA_ENDPOINT_COMPAT_PATCHED = True
    return True


def register_bpa_endpoint_compat() -> bool:
    return _patch_zmongo_api_session()
