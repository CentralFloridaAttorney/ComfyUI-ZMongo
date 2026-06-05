from __future__ import annotations
from __future__ import annotations

import base64
import mimetypes
import shutil
import urllib
from pathlib import Path

from .generic_helpers import AlwaysDirtyMixin, _indexed_list_text, _extract_collections, _as_comfy_list, _dirty_token, _parse_json_object, \
    _parse_json_list, _extract_doc_ids, _extract_count, _parse_any_json, _extract_document_from_payload, \
    safe_get_by_path, _local_payload_ok, _local_safe_name, _local_payload_error, \
    _local_clean_scalar, dedupe_strings, flatten_document_paths, _local_now_iso, _image_field_candidates, \
    _local_set_by_path, _local_get_by_path, _decode_image_bytes_from_value

import json
from typing import Any, Optional
from .generic_helpers import AlwaysDirtyMixin, _json_text, _error_payload

class LocalZMongoSession:
    """
    Local file-backed ZMongo-compatible session acting as an exact proxy.

    Stores documents as JSON files via the robust local_file_store.py module.
    Correctly intercepts ZMongo bytes envelopes allowing ComfyUI image nodes
    to interact natively.
    """

    storage_backend = "local_file_store"
    base_url = "local://comfyui-zmongo"
    username = "local_user"
    zai_api_key = ""
    comfy_zmongo_prefix = "/local-file-store"
    fleet_prefix = "/local-disabled"
    comfy_zmongo_fleet_prefix = "/local-disabled"

    def __init__(self, root_dir: Optional[str | Path] = None) -> None:
        from .local_file_store import LocalFileStore
        plugin_root = Path(__file__).resolve().parent
        self.root_dir = Path(root_dir).expanduser().resolve() if root_dir else (plugin_root / "local_store").resolve()
        self._store = LocalFileStore(root_dir=self.root_dir)

    def close(self) -> None:
        return None

    def health(self) -> dict[str, Any]:
        return self._store.ping().to_dict()

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
        return self._store.list_collections().to_dict()

    def create_collection(self, collection: str) -> dict[str, Any]:
        return self._store.create_collection(collection).to_dict()

    def delete_collection(self, collection: str) -> dict[str, Any]:
        clean = _local_safe_name(collection, "")
        if not clean:
            return _local_payload_error("collection is required.", status_code=400)
        path = self._store.collections_dir / clean
        if not path.exists():
            return _local_payload_error("Local collection does not exist.", {"collection": clean}, status_code=404)
        shutil.rmtree(path)
        return _local_payload_ok("Deleted local collection.",
                                 {"collection": clean, "storage_backend": "local_file_store"})

    def list_docs(self, *, collection: str, limit: int = 50, skip: int = 0, query: Optional[dict[str, Any]] = None) -> \
    dict[str, Any]:
        clean = _local_safe_name(collection, "documents")
        res = self._store.list_documents(clean, query=query, limit=100000, include_documents=True)
        if not res.success:
            return res.to_dict()

        docs = res.data.get("documents", [])
        safe_limit = max(1, min(int(limit or 50), 500))
        safe_skip = max(0, int(skip or 0))
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
        res = self._store.load_document(clean, document_id=clean_id)
        payload = res.to_dict()
        if res.success:
            payload["data"]["collection_name"] = clean
            payload["data"]["cache_hit"] = False
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
        clean = _local_safe_name(collection, "documents")
        clean_id = _local_clean_scalar(document_id)

        if clean_id:
            res = self._store.load_document(clean, document_id=clean_id)
            if not res.success:
                return res.to_dict()
            doc = res.data.get("document")
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

        listed = self.list_docs(collection=clean, query=query or {}, limit=100000, skip=0)
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

        safe_limit = max(1, min(int(limit or 50), 500))
        safe_skip = max(0, int(skip or 0))
        page = docs[safe_skip:safe_skip + safe_limit]

        data = dict(listed.get("data", {}))
        data.update({
            "documents": page,
            "results": page,
            "count": len(page),
            "limit": safe_limit,
            "skip": safe_skip,
            "document": page[0] if page else None
        })
        return _local_payload_ok("Queried local documents.", data)

    def count_docs(self, *, collection: str, query: Optional[dict[str, Any]] = None, document_id: str = "",
                   cache: bool = False) -> dict[str, Any]:
        clean = _local_safe_name(collection, "documents")
        clean_id = _local_clean_scalar(document_id)

        if clean_id:
            res = self._store.load_document(clean, document_id=clean_id)
            count = 1 if res.success else 0
        else:
            res = self._store.list_documents(clean, query=query, limit=100000, include_documents=False)
            count = res.data.get("count", 0) if res.success else 0

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
        res = self._store.save_document(clean, document, upsert=True)
        payload = res.to_dict()
        if res.success:
            doc_id = res.data.get("document_id")
            payload["data"]["inserted_id"] = doc_id
            payload["data"]["_id"] = doc_id
            payload["data"]["collection_name"] = clean
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
        clean = _local_safe_name(collection, "documents")
        clean_id = _local_clean_scalar(document_id)

        res = self._store.load_document(clean, document_id=clean_id, query=query)
        doc = res.data.get("document", {}) if res.success else {}

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

        save_res = self._store.save_document(clean, doc, document_id=doc.get("_id"), upsert=upsert)
        payload = save_res.to_dict()
        if save_res.success:
            payload["data"]["matched_count"] = 1
            payload["data"]["modified_count"] = 1
            payload["data"]["collection_name"] = clean
        return payload

    def delete_doc(self, *, collection: str, query: Optional[dict[str, Any]] = None, document_id: str = "") -> dict[
        str, Any]:
        clean = _local_safe_name(collection, "documents")
        clean_id = _local_clean_scalar(document_id)
        res = self._store.delete_document(clean, document_id=clean_id, query=query, delete_assets=True)
        payload = res.to_dict()
        if res.success:
            payload["data"]["deleted_ids"] = [clean_id] if clean_id else []
            payload["data"]["collection_name"] = clean
        return payload

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

        # [CRITICAL FIX]: Intercept binary envelopes!
        # If the value is a dict formatted as a zmongo image envelope, bypass
        # save_value json limits and use save_image native pointers instead.
        if isinstance(value, dict) and value.get("__type__") == "bytes" and "data" in value:
            try:
                b64_data = str(value["data"]).strip()
                if b64_data.startswith("data:") and "," in b64_data:
                    b64_data = b64_data.split(",", 1)[1]

                raw_bytes = base64.b64decode(b64_data, validate=False)
                filename = value.get("filename", "comfy_image.png")
                metadata = value.get("metadata", {})

                img_res = self._store.save_image(
                    clean,
                    raw_bytes,
                    filename=filename,
                    document_id=clean_id or None,
                    query=query,
                    field_path=field_path,
                    metadata=metadata,
                    upsert=upsert_if_missing
                )

                payload = img_res.to_dict()
                if img_res.success:
                    payload["data"]["collection_name"] = clean
                    payload["data"]["inserted_id"] = payload["data"].get("document_id")
                    payload["data"]["_id"] = payload["data"].get("document_id")
                return payload
            except Exception as exc:
                return _local_payload_error(f"Failed to intercept and save local image envelope: {exc}")

        # Standard save_value fallback
        res = self._store.save_value(
            clean,
            value,
            document_id=clean_id or None,
            query=query,
            field_path=field_path,
            upsert=upsert_if_missing,
            parse_json_strings=parse_json_strings
        )

        payload = res.to_dict()
        if res.success:
            payload["data"]["collection_name"] = clean
            payload["data"]["inserted_id"] = payload["data"].get("document_id")
            payload["data"]["_id"] = payload["data"].get("document_id")
        return payload

    def fetch_image_field(
            self,
            *,
            collection: str,
            document_id: str,
            field_path: str,
            master_key_hex: str = "",
    ) -> tuple[bytes, str]:
        """
        Load image bytes. Natively checks for local pointer references
        ({"local_path": ...}) created by save_image.
        """
        clean = _local_safe_name(collection, "images")
        clean_id = _local_clean_scalar(document_id)

        res = self._store.load_document(clean, document_id=clean_id)
        if not res.success:
            raise ValueError(f"Local document not found: {clean}/{clean_id}")

        doc = res.data.get("document", {})
        errors: list[str] = []

        for candidate in _image_field_candidates(field_path, "image_data"):
            try:
                value = _local_get_by_path(doc, candidate)

                # Route 1: Local file pointer resolution
                if isinstance(value, dict) and value.get("local_path"):
                    img_res = self._store.load_image(
                        clean,
                        document_id=clean_id,
                        field_path=candidate,
                        as_base64=False
                    )
                    if img_res.success and "bytes" in img_res.data:
                        return img_res.data["bytes"], f"local_file_store:{clean}/{clean_id}:{candidate}"
                    else:
                        errors.append(f"Found local_path but load_image failed: {img_res.message}")
                        continue

                # Route 2: Standard Base64 inline envelope resolution
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
        """Route a node-style request into the local file store API equivalent."""
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
            if method == "GET" and (path.endswith("/health") or path.endswith("/api/health")):
                return self.health()
            if method == "GET" and path.endswith("/api/whoami"):
                return self.whoami()
            if method == "GET" and path.endswith("/api/collections"):
                return self.list_collections()

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
                    return self.get_doc(collection=parts[0], document_id=parts[1],
                                        cache=str(params.get("cache", "false")).lower() == "true")

            if method == "POST" and path.endswith("/api/upload"):
                filename = _local_clean_scalar(body.get("filename")) or "uploaded_document"
                file_data = body.get("file_data") or ""
                text_value = body.get("text") or ""
                metadata = body.get("metadata") if isinstance(body.get("metadata"), dict) else {}
                document = body.get("document") if isinstance(body.get("document"), dict) else {}
                document.update({
                    "filename": filename,
                    "content_type": body.get("content_type") or mimetypes.guess_type(filename)[
                        0] or "application/octet-stream",
                    "file_data": file_data,
                    "size_bytes": len(base64.b64decode(file_data.encode("ascii"), validate=False)) if isinstance(
                        file_data, str) and file_data else 0,
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
    CATEGORY = "ZMongo/01 Collections"

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
    CATEGORY = "ZMongo/01 Collections"

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
    CATEGORY = "ZMongo/01 Collections"

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
    CATEGORY = "ZMongo/02 Docs"

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
    CATEGORY = "ZMongo/02 Docs"

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
    CATEGORY = "ZMongo/02 Docs"

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
    CATEGORY = "ZMongo/02 Docs"

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
    CATEGORY = "ZMongo/02 Docs"

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
    CATEGORY = "ZMongo/02 Docs"

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
    CATEGORY = "ZMongo/02 Docs"

    @staticmethod
    def _clean_scalar(value: Any) -> str:
        if isinstance(value, (list, tuple)):
            value = value[0] if value else ""

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

    def delete_doc(self, **kwargs):
        session = kwargs.get("session")
        collection_name = kwargs.get("collection_name", "")
        query_json = kwargs.get("query_json", "{}")
        document_id = kwargs.get("document_id", "")
        confirm_document_id = kwargs.get("confirm_document_id", "")

        cleaned_collection = self._clean_scalar(collection_name)
        cleaned_id = self._clean_scalar(document_id)
        confirmed_id = self._clean_scalar(confirm_document_id)

        token = _dirty_token("delete_doc", cleaned_collection, cleaned_id)

        if session is None:
            return (_json_text(_error_payload("No session provided.")), token)

        try:
            query = _parse_json_object(query_json, "query_json")

            if cleaned_id and cleaned_id != confirmed_id:
                payload = {
                    "success": False,
                    "message": "Document deletion by document_id requires matching confirm_document_id.",
                    "data": {
                        "collection_name": cleaned_collection,
                        "document_id": cleaned_id,
                        "confirm_document_id": confirmed_id,
                        "query_json": query_json,
                        "refresh": token,
                    },
                    "error": {"msg": "document_id and confirm_document_id do not match."},
                    "status_code": 400,
                }
                return (_json_text(payload), token)

            if not cleaned_id and not query:
                payload = {
                    "success": False,
                    "message": "Delete requires document_id or non-empty query_json.",
                    "data": {
                        "collection_name": cleaned_collection,
                        "raw_collection_name": collection_name,
                        "document_id": cleaned_id,
                        "raw_document_id": document_id,
                        "confirm_document_id": confirmed_id,
                        "raw_confirm_document_id": confirm_document_id,
                        "query_json": query_json,
                        "received_kwargs": sorted(str(key) for key in kwargs.keys()),
                        "refresh": token,
                        "checks": [
                            "Connect document_id directly from 99 Select Nth Item.",
                            "Connect confirm_document_id directly from the same 99 Select Nth Item output.",
                            "Confirmed backend route is POST /comfy-zmongo/api/doc/delete.",
                        ],
                    },
                    "error": {"msg": "query or document_id is required."},
                    "status_code": 400,
                }
                return (_json_text(payload), token)

            payload = session.delete_doc(
                collection=cleaned_collection,
                query=query,
                document_id=cleaned_id,
            )

            return (_json_text(payload), token)

        except Exception as exc:
            payload = {
                "success": False,
                "message": f"Delete Doc failed: {exc}",
                "data": {
                    "collection_name": cleaned_collection,
                    "raw_collection_name": collection_name,
                    "document_id": cleaned_id,
                    "raw_document_id": document_id,
                    "confirm_document_id": confirmed_id,
                    "raw_confirm_document_id": confirm_document_id,
                    "query_json": query_json,
                    "received_kwargs": sorted(str(key) for key in kwargs.keys()),
                    "refresh": token,
                },
                "error": {
                    "type": exc.__class__.__name__,
                    "msg": str(exc),
                },
                "status_code": 0,
            }
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
    CATEGORY = "ZMongo/02 Docs"

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
    CATEGORY = "ZMongo/02 Docs"

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
    # # 00 Auth
    # "ZMongoLocalFileStoreSessionNode": ZMongoLocalFileStoreSessionNode,
    # "ZMongoApiKeyOnlySessionNode": ZMongoApiKeyOnlySessionNode,
    # "ZMongoApiCloseSessionNode": ZMongoApiCloseSessionNode,

    # # 01 Service
    # "ZMongoApiHealthNode": ZMongoApiHealthNode,
    # "ZMongoApiWhoamiNode": ZMongoApiWhoamiNode,

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
    # # 00 Auth
    # "ZMongoLocalFileStoreSessionNode": "00 Local File Store Session",
    # "ZMongoApiKeyOnlySessionNode": "00 API Key Session (Simplified)",
    # "ZMongoApiCloseSessionNode": "00 Close API Session",

    # # 01 Service
    # "ZMongoApiHealthNode": "00 Health",
    # "ZMongoApiWhoamiNode": "00 Who Am I",

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