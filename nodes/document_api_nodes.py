from __future__ import annotations

import mimetypes
import os
import urllib.parse
from pathlib import Path
from typing import Any, Optional
from .generic_helpers import AlwaysDirtyMixin, SELECTABLE_RETURN_TYPES, SELECTABLE_RETURN_NAMES, \
    SELECTABLE_OUTPUT_IS_LIST, _session_request, _dirty_token, _json_text, _extract_data, _get_document_browser_root, \
    _resolve_document_file_path, ZMONGO_FILE_PATH, ZMONGO_FILENAME, ZMONGO_DOCUMENT_ID, _clean_scalar, \
    _coerce_file_path_link, _ensure_payload_dict, _as_comfy_list, _selectable_tail, _parse_json_object, _extract_doc_id, \
    _raise_if_failed, _extract_documents, _extract_doc_ids_from_documents, _extract_filenames_from_documents, \
    _coerce_document_id, _prefer_link_value, _extract_document, _extract_text, _dedupe_strings, _flatten_document_paths, \
    _indexed_list_text, _extract_field_paths, _coerce_field_path, _safe_get_by_path, _value_items, \
    _is_immutable_document_field_path, _blocked_immutable_field_payload, _parse_any_json, _parse_json_list, \
    _error_payload, ZMONGO_FIELD_PATH, _strip_index_prefix, _coerce_filename_link, _coerce_text_link, \
    _coerce_document_id_link, _document_summary, ZMONGO_TEXT, _read_file_as_base64, _list_document_files

DEFAULT_DOCUMENT_PREFIX = os.getenv("ZMONGO_DOCUMENT_API_PREFIX", "/documents").strip().rstrip("/") or "/documents"
DEFAULT_MAX_UPLOAD_BYTES = int(os.getenv("ZMONGO_DOCUMENT_NODE_MAX_UPLOAD_BYTES", str(64 * 1024 * 1024)))

try:
    import folder_paths  # ComfyUI runtime helper for input/output/temp directories
except Exception:  # Allows py_compile outside ComfyUI
    folder_paths = None

# -----------------------------------------------------------------------------
# Nodes
# -----------------------------------------------------------------------------

class ZMongoDocumentHealth(AlwaysDirtyMixin):
    @classmethod
    def INPUT_TYPES(cls):
        return {"required": {"session": ("ZMONGO_API_SESSION",), "document_prefix": ("STRING", {"default": DEFAULT_DOCUMENT_PREFIX})}, "optional": {"refresh_token": ("STRING", {"default": ""})}}

    RETURN_TYPES = ("STRING", "BOOLEAN", "STRING") + SELECTABLE_RETURN_TYPES
    RETURN_NAMES = ("json", "success", "refresh") + SELECTABLE_RETURN_NAMES
    OUTPUT_IS_LIST = (False, False, False) + SELECTABLE_OUTPUT_IS_LIST
    FUNCTION = "health"
    CATEGORY = "ZMongo/06 Documents"
    OUTPUT_NODE = True

    def health(self, session: Any, document_prefix: str, refresh_token: str = ""):
        payload = _session_request(session, "GET", document_prefix, "/health")
        refresh = _dirty_token("document_health", refresh_token)
        return (_json_text(payload), bool(payload.get("success")), refresh, *_selectable_tail([payload.get("message") or "health"]))


class ZMongoDocumentWhoAmI(AlwaysDirtyMixin):
    @classmethod
    def INPUT_TYPES(cls):
        return {"required": {"session": ("ZMONGO_API_SESSION",), "document_prefix": ("STRING", {"default": DEFAULT_DOCUMENT_PREFIX})}, "optional": {"refresh_token": ("STRING", {"default": ""})}}

    RETURN_TYPES = ("STRING", "STRING", "STRING", "BOOLEAN", "STRING") + SELECTABLE_RETURN_TYPES
    RETURN_NAMES = ("json", "username", "silo_db_name", "success", "refresh") + SELECTABLE_RETURN_NAMES
    OUTPUT_IS_LIST = (False, False, False, False, False) + SELECTABLE_OUTPUT_IS_LIST
    FUNCTION = "whoami"
    CATEGORY = "ZMongo/06 Documents"
    OUTPUT_NODE = True

    def whoami(self, session: Any, document_prefix: str, refresh_token: str = ""):
        payload = _session_request(session, "GET", document_prefix, "/api/whoami")
        data = _extract_data(payload)
        username = str(data.get("username") or "")
        silo_db_name = str(data.get("silo_db_name") or data.get("db_name") or "")
        return (_json_text(payload), username, silo_db_name, bool(payload.get("success")), _dirty_token("document_whoami", refresh_token), *_selectable_tail([username]))


class ZMongoDocumentFilePathBrowser(AlwaysDirtyMixin):
    @classmethod
    def INPUT_TYPES(cls):
        return {"required": {"root_mode": (["Documents", "Comfy Input"], {"default": "Documents"}), "selected_file": (_list_document_files(),), "manual_path": ("STRING", {"default": ""}), "custom_root": ("STRING", {"default": ""})}, "optional": {"refresh_token": ("STRING", {"default": ""})}}

    RETURN_TYPES = ("STRING", ZMONGO_FILE_PATH, "STRING", ZMONGO_FILENAME, "STRING", "INT", "BOOLEAN", "STRING", "STRING", "*", "*") + SELECTABLE_RETURN_TYPES
    RETURN_NAMES = ("file_path", "file_path_link", "filename", "filename_link", "content_type", "size_bytes", "exists", "browser_root", "refresh", "file_paths", "filenames") + SELECTABLE_RETURN_NAMES
    OUTPUT_IS_LIST = (False, False, False, False, False, False, False, False, False, True, True) + SELECTABLE_OUTPUT_IS_LIST
    FUNCTION = "resolve_file_path"
    CATEGORY = "ZMongo/06 Documents"
    OUTPUT_NODE = True

    def resolve_file_path(self, root_mode: str = "Documents", selected_file: str = "", manual_path: str = "", custom_root: str = "", refresh_token: str = ""):
        try:
            browser_root = _get_document_browser_root(root_mode=root_mode, custom_root=custom_root)
            resolved = _resolve_document_file_path(selected_file, manual_path, root_mode=root_mode, custom_root=custom_root)
            exists = resolved.exists() and resolved.is_file()
            size_bytes = resolved.stat().st_size if exists else 0
            content_type = mimetypes.guess_type(str(resolved))[0] or "application/octet-stream"
            filename = resolved.name if str(resolved) else ""
            file_path = str(resolved) if exists else ""
            file_paths = [file_path] if file_path else []
            filenames = [filename] if filename else []
            return (file_path, file_path, filename, filename, content_type, int(size_bytes), bool(exists), str(browser_root), _dirty_token("document_file_browser", refresh_token, root_mode, selected_file, manual_path, custom_root), _as_comfy_list(file_paths), _as_comfy_list(filenames), *_selectable_tail(file_paths))
        except Exception:
            try:
                browser_root = str(_get_document_browser_root(root_mode=root_mode, custom_root=custom_root))
            except Exception:
                browser_root = ""
            return ("", "", "", "", "application/octet-stream", 0, False, browser_root, _dirty_token("document_file_browser_error", refresh_token), [], [], *_selectable_tail([]))


class ZMongoDocumentUploadFile(AlwaysDirtyMixin):
    @classmethod
    def INPUT_TYPES(cls):
        return {"required": {"session": ("ZMONGO_API_SESSION",), "file_path": ("STRING", {"default": ""}), "case_id": ("STRING", {"default": ""}), "metadata_json": ("STRING", {"default": "{}", "multiline": True}), "document_prefix": ("STRING", {"default": DEFAULT_DOCUMENT_PREFIX})}, "optional": {"file_path_link": ("*",), "known_text": ("STRING", {"default": "", "multiline": True}), "refresh_token": ("STRING", {"default": ""})}}

    RETURN_TYPES = ("STRING", "STRING", ZMONGO_DOCUMENT_ID, "STRING", ZMONGO_FILENAME, "INT", "BOOLEAN", "STRING", "*", "*") + SELECTABLE_RETURN_TYPES
    RETURN_NAMES = ("json", "document_id", "document_id_link", "filename", "filename_link", "size_bytes", "success", "refresh", "document_ids", "filenames") + SELECTABLE_RETURN_NAMES
    OUTPUT_IS_LIST = (False, False, False, False, False, False, False, False, True, True) + SELECTABLE_OUTPUT_IS_LIST
    FUNCTION = "upload_file"
    CATEGORY = "ZMongo/06 Documents"
    OUTPUT_NODE = True

    def upload_file(self, session: Any, file_path: str, case_id: str, metadata_json: str, document_prefix: str, file_path_link: str = "", known_text: str = "", refresh_token: str = ""):
        refresh = _dirty_token("document_upload", refresh_token)

        primary_path = _clean_scalar(file_path)
        linked_path = _coerce_file_path_link(file_path_link)
        effective_file_path = linked_path or primary_path
        effective_path = Path(os.path.expanduser(effective_file_path or "")).resolve() if effective_file_path else None

        def nonfatal(payload: dict[str, Any], filename_hint: str = ""):
            name = filename_hint or (effective_path.name if effective_path is not None else "")
            return (
                _json_text(_ensure_payload_dict(payload)),
                "",
                "",
                name,
                name,
                0,
                False,
                refresh,
                _as_comfy_list([]),
                _as_comfy_list([name] if name else []),
                *_selectable_tail([]),
            )

        if effective_path is None or not effective_file_path:
            return nonfatal({
                "success": False,
                "message": "Upload skipped. No document file path was provided.",
                "data": {
                    "file_path": primary_path,
                    "file_path_link": _clean_scalar(file_path_link),
                    "effective_file_path": "",
                    "checks": [
                        "Connect 06 Document File Browser.file_path_link to 06 Upload Document File.file_path_link.",
                        "Select an actual file in the file browser node.",
                        "Do not connect browser_root, selected_file, indexed_items, or a directory path to file_path.",
                    ],
                    "refresh": refresh,
                },
                "error": {"type": "MissingFilePath", "msg": "No file path supplied."},
                "status_code": 400,
            })

        if effective_path.exists() and effective_path.is_dir():
            return nonfatal({
                "success": False,
                "message": f"Upload skipped. The selected path is a directory, not a document file: {effective_path}",
                "data": {
                    "file_path": primary_path,
                    "file_path_link": _clean_scalar(file_path_link),
                    "effective_file_path": str(effective_path),
                    "is_directory": True,
                    "checks": [
                        "Connect file_path_link from 06 Document File Browser, not browser_root.",
                        "In 06 Document File Browser, select a real PDF/TXT/DOCX/etc. file.",
                        "If using manual_path, enter the full path to a file, not a folder.",
                    ],
                    "refresh": refresh,
                },
                "error": {"type": "DirectorySelected", "msg": "Selected path is a directory."},
                "status_code": 400,
            }, effective_path.name)

        if not effective_path.exists() or not effective_path.is_file():
            return nonfatal({
                "success": False,
                "message": f"Upload skipped. Document file was not found: {effective_path}",
                "data": {
                    "file_path": primary_path,
                    "file_path_link": _clean_scalar(file_path_link),
                    "effective_file_path": str(effective_path),
                    "exists": effective_path.exists(),
                    "checks": [
                        "Confirm the file exists on the machine running ComfyUI, not merely on the browser client.",
                        "Put the document in /home/comfyuser/Documents or set ZMONGO_DOCUMENT_BROWSER_ROOT.",
                        "Reload the workflow if the file-browser dropdown has stale values.",
                    ],
                    "refresh": refresh,
                },
                "error": {"type": "FileNotFound", "msg": "Document file not found."},
                "status_code": 404,
            }, effective_path.name)

        try:
            filename, content_type, b64, size_bytes = _read_file_as_base64(str(effective_path))
            body = {
                "filename": filename,
                "content_type": content_type,
                "file_data": b64,
                "case_id": _clean_scalar(case_id) or None,
                "metadata": _parse_json_object(metadata_json, "metadata_json"),
                "text": known_text or "",
            }
            payload = _session_request(session, "POST", document_prefix, "/api/upload", json_body=body)
            data = _extract_data(payload)
            out_filename = str(data.get("filename") or filename)

            if not payload.get("success"):
                return (
                    _json_text(payload),
                    "",
                    "",
                    out_filename,
                    out_filename,
                    int(data.get("size_bytes") or size_bytes or 0),
                    False,
                    refresh,
                    _as_comfy_list([]),
                    _as_comfy_list([out_filename] if out_filename else []),
                    *_selectable_tail([]),
                )

            document_id = _extract_doc_id(payload)
            ids = [document_id] if document_id else []
            filenames = [out_filename] if out_filename else []
            return (
                _json_text(payload),
                document_id,
                document_id,
                out_filename,
                out_filename,
                int(data.get("size_bytes") or size_bytes or 0),
                bool(payload.get("success")),
                refresh,
                _as_comfy_list(ids),
                _as_comfy_list(filenames),
                *_selectable_tail(ids),
            )
        except Exception as exc:
            return nonfatal({
                "success": False,
                "message": f"Upload failed: {exc}",
                "data": {
                    "file_path": primary_path,
                    "file_path_link": _clean_scalar(file_path_link),
                    "effective_file_path": str(effective_path),
                    "refresh": refresh,
                },
                "error": {"type": exc.__class__.__name__, "msg": str(exc)},
                "status_code": 0,
            }, effective_path.name if effective_path is not None else "")


class ZMongoDocumentCreateText(AlwaysDirtyMixin):
    @classmethod
    def INPUT_TYPES(cls):
        return {"required": {"session": ("ZMONGO_API_SESSION",), "filename": ("STRING", {"default": "manual_document.txt"}), "text": ("STRING", {"default": "", "multiline": True}), "case_id": ("STRING", {"default": ""}), "metadata_json": ("STRING", {"default": "{}", "multiline": True}), "document_json": ("STRING", {"default": "{}", "multiline": True}), "document_prefix": ("STRING", {"default": DEFAULT_DOCUMENT_PREFIX})}, "optional": {"refresh_token": ("STRING", {"default": ""})}}

    RETURN_TYPES = ("STRING", "STRING", ZMONGO_DOCUMENT_ID, "STRING", ZMONGO_FILENAME, "INT", "BOOLEAN", "STRING", "*", "*") + SELECTABLE_RETURN_TYPES
    RETURN_NAMES = ("json", "document_id", "document_id_link", "filename", "filename_link", "text_length", "success", "refresh", "document_ids", "filenames") + SELECTABLE_RETURN_NAMES
    OUTPUT_IS_LIST = (False, False, False, False, False, False, False, False, True, True) + SELECTABLE_OUTPUT_IS_LIST
    FUNCTION = "create_text_document"
    CATEGORY = "ZMongo/06 Documents"
    OUTPUT_NODE = True

    def create_text_document(self, session: Any, filename: str, text: str, case_id: str, metadata_json: str, document_json: str, document_prefix: str, refresh_token: str = ""):
        body = {"filename": _clean_scalar(filename) or "manual_document.txt", "text": text or "", "case_id": _clean_scalar(case_id) or None, "metadata": _parse_json_object(metadata_json, "metadata_json"), "document": _parse_json_object(document_json, "document_json")}
        payload = _session_request(session, "POST", document_prefix, "/api/create", json_body=body)
        _raise_if_failed(payload, "Document create")
        data = _extract_data(payload)
        document_id = _extract_doc_id(payload)
        out_filename = str(data.get("filename") or body["filename"])
        ids = [document_id] if document_id else []
        filenames = [out_filename] if out_filename else []
        return (_json_text(payload), document_id, document_id, out_filename, out_filename, int(data.get("text_length") or len(text or "")), bool(payload.get("success")), _dirty_token("document_create", refresh_token), _as_comfy_list(ids), _as_comfy_list(filenames), *_selectable_tail(ids))


class ZMongoDocumentList(AlwaysDirtyMixin):
    @classmethod
    def INPUT_TYPES(cls):
        return {"required": {"session": ("ZMONGO_API_SESSION",), "query_json": ("STRING", {"default": "{}", "multiline": True}), "sort_json": ("STRING", {"default": '[["updated_at", -1]]', "multiline": True}), "limit": ("INT", {"default": 50, "min": 1, "max": 500}), "skip": ("INT", {"default": 0, "min": 0, "max": 1000000}), "include_file_data": ("BOOLEAN", {"default": False}), "document_prefix": ("STRING", {"default": DEFAULT_DOCUMENT_PREFIX})}, "optional": {"refresh_token": ("STRING", {"default": ""})}}

    RETURN_TYPES = ("STRING", "STRING", "STRING", "STRING", ZMONGO_DOCUMENT_ID, ZMONGO_FILENAME, "INT", "BOOLEAN", "STRING", "*", "*") + SELECTABLE_RETURN_TYPES
    RETURN_NAMES = ("json", "documents_json", "document_ids_json", "filenames_json", "first_document_id_link", "first_filename_link", "count", "success", "refresh", "document_ids", "filenames") + SELECTABLE_RETURN_NAMES
    OUTPUT_IS_LIST = (False, False, False, False, False, False, False, False, False, True, True) + SELECTABLE_OUTPUT_IS_LIST
    FUNCTION = "list_documents"
    CATEGORY = "ZMongo/06 Documents"
    OUTPUT_NODE = True

    def list_documents(self, session: Any, query_json: str, sort_json: str, limit: int, skip: int, include_file_data: bool, document_prefix: str, refresh_token: str = ""):
        body = {"query": _parse_json_object(query_json, "query_json"), "sort": _parse_json_list(sort_json, "sort_json"), "limit": int(limit), "skip": int(skip), "include_file_data": bool(include_file_data)}
        payload = _session_request(session, "POST", document_prefix, "/api/list", json_body=body)
        _raise_if_failed(payload, "Document list")
        docs = _extract_documents(payload)
        ids = _extract_doc_ids_from_documents(docs)
        filenames = _extract_filenames_from_documents(docs)
        return (_json_text(payload), _json_text(docs), _json_text(ids), _json_text(filenames), (ids[0] if ids else ""), (filenames[0] if filenames else ""), len(docs), bool(payload.get("success")), _dirty_token("document_list", refresh_token), _as_comfy_list(ids), _as_comfy_list(filenames), *_selectable_tail(ids))

class ZMongoDocumentGet(AlwaysDirtyMixin):
    @classmethod
    def INPUT_TYPES(cls):
        return {"required": {"session": ("ZMONGO_API_SESSION",), "document_id": ("STRING", {"default": ""}), "include_file_data": ("BOOLEAN", {"default": False}), "document_prefix": ("STRING", {"default": DEFAULT_DOCUMENT_PREFIX})}, "optional": {"document_id_link": ("*",), "refresh_token": ("STRING", {"default": ""})}}

    RETURN_TYPES = ("STRING", "STRING", "STRING", "STRING", "STRING", "BOOLEAN", "STRING", "*") + SELECTABLE_RETURN_TYPES
    RETURN_NAMES = ("json", "document_id", "document_json", "text", "summary", "success", "refresh", "document_ids") + SELECTABLE_RETURN_NAMES
    OUTPUT_IS_LIST = (False, False, False, False, False, False, False, True) + SELECTABLE_OUTPUT_IS_LIST
    FUNCTION = "get_document"
    CATEGORY = "ZMongo/06 Documents"
    OUTPUT_NODE = True

    def get_document(self, session: Any, document_id: str, include_file_data: bool, document_prefix: str, document_id_link: str = "", refresh_token: str = ""):
        clean_id = _coerce_document_id(_prefer_link_value(document_id, document_id_link))
        quoted = urllib.parse.quote(clean_id, safe="")
        payload = _session_request(session, "GET", document_prefix, f"/api/get/{quoted}", params={"include_file_data": "true" if include_file_data else "false"})
        _raise_if_failed(payload, "Document get")
        document = _extract_document(payload)
        resolved_id = _coerce_document_id(document) or clean_id
        ids = [resolved_id] if resolved_id else []
        return (_json_text(payload), resolved_id, _json_text(document), str(document.get("text") or ""), _document_summary(document), bool(payload.get("success")), _dirty_token("document_get", refresh_token), _as_comfy_list(ids), *_selectable_tail(ids))


class ZMongoDocumentGetText(AlwaysDirtyMixin):
    @classmethod
    def INPUT_TYPES(cls):
        return {"required": {"session": ("ZMONGO_API_SESSION",), "document_id": ("STRING", {"default": ""}), "document_prefix": ("STRING", {"default": DEFAULT_DOCUMENT_PREFIX})}, "optional": {"document_id_link": ("*",), "refresh_token": ("STRING", {"default": ""})}}

    RETURN_TYPES = ("STRING", "STRING", "INT", "BOOLEAN", "STRING", "*") + SELECTABLE_RETURN_TYPES
    RETURN_NAMES = ("json", "text", "text_length", "success", "refresh", "text_items") + SELECTABLE_RETURN_NAMES
    OUTPUT_IS_LIST = (False, False, False, False, False, True) + SELECTABLE_OUTPUT_IS_LIST
    FUNCTION = "get_text"
    CATEGORY = "ZMongo/06 Documents"
    OUTPUT_NODE = True

    def get_text(self, session: Any, document_id: str, document_prefix: str, document_id_link: str = "", refresh_token: str = ""):
        clean_id = _coerce_document_id(_prefer_link_value(document_id, document_id_link))
        quoted = urllib.parse.quote(clean_id, safe="")
        payload = _session_request(session, "GET", document_prefix, f"/api/text/{quoted}")
        _raise_if_failed(payload, "Document text get")
        text = _extract_text(payload)
        items = [text] if text else []
        return (_json_text(payload), text, len(text), bool(payload.get("success")), _dirty_token("document_text", refresh_token), _as_comfy_list(items), *_selectable_tail(items))


class ZMongoDocumentFieldPaths(AlwaysDirtyMixin):
    @classmethod
    def INPUT_TYPES(cls):
        return {
            "required": {
                "session": ("ZMONGO_API_SESSION",),
                "collection_name": ("STRING", {"default": "documents"}),
                "document_id": ("STRING", {"default": ""}),
                "document_prefix": ("STRING", {"default": DEFAULT_DOCUMENT_PREFIX}),
            },
            "optional": {
                "document_id_link": ("*",),
                "collection_name_link": ("*",),
                "refresh_token": ("STRING", {"default": ""}),
            },
        }

    RETURN_TYPES = ("STRING", "STRING", "STRING", "INT", "BOOLEAN", "STRING", "*") + SELECTABLE_RETURN_TYPES
    RETURN_NAMES = ("json", "field_paths_json", "indexed_field_paths", "count", "success", "refresh", "field_paths") + SELECTABLE_RETURN_NAMES
    OUTPUT_IS_LIST = (False, False, False, False, False, False, True) + SELECTABLE_OUTPUT_IS_LIST
    FUNCTION = "field_paths"
    CATEGORY = "ZMongo/06 Documents"
    OUTPUT_NODE = True

    def _local_field_paths_fallback(
        self,
        session: Any,
        *,
        collection_name: str,
        document_id: str,
        refresh: str,
        prior_payload: Optional[dict[str, Any]] = None,
    ):
        get_doc = getattr(session, "get_doc", None)
        if not callable(get_doc):
            payload = _ensure_payload_dict({
                "success": False,
                "message": "Document field paths failed. The /documents route failed and the session does not expose get_doc for local fallback.",
                "data": {
                    "collection_name": collection_name,
                    "document_id": document_id,
                    "prior_payload": prior_payload,
                    "refresh": refresh,
                    "checks": [
                        "For Local File Store, set collection_name to the local collection that contains the document.",
                        "Connect document_id_link from 06 Select Nth Document Item or a compatible selector.",
                        "If using the hosted backend, confirm document_prefix is /documents.",
                    ],
                },
                "error": {"msg": "No compatible local get_doc fallback."},
                "status_code": 404,
            })
            return (_json_text(payload), _json_text([]), _indexed_list_text([]), 0, False, refresh, _as_comfy_list([]), *_selectable_tail([]))

        local_payload = _ensure_payload_dict(
            get_doc(collection=collection_name, document_id=document_id, cache=False)
        )
        document = _extract_document(local_payload)
        if not document:
            payload = _ensure_payload_dict({
                "success": False,
                "message": "Document field paths failed. Local fallback could not load the document.",
                "data": {
                    "collection_name": collection_name,
                    "document_id": document_id,
                    "prior_payload": prior_payload,
                    "local_payload": local_payload,
                    "refresh": refresh,
                    "checks": [
                        "Confirm collection_name matches the Local File Store collection.",
                        "Confirm document_id exists in that local collection.",
                        "Use 03 List Docs or 06 List Documents to verify the document id.",
                    ],
                },
                "error": {"msg": "Document not found in local fallback."},
                "status_code": local_payload.get("status_code", 404),
            })
            return (_json_text(payload), _json_text([]), _indexed_list_text([]), 0, False, refresh, _as_comfy_list([]), *_selectable_tail([]))

        paths = _dedupe_strings(_flatten_document_paths(document))
        payload = _ensure_payload_dict({
            "success": True,
            "message": f"Found {len(paths)} field path(s) using local session fallback.",
            "data": {
                "collection_name": collection_name,
                "document_id": document_id,
                "field_paths": paths,
                "count": len(paths),
                "source": "local_session_get_doc_fallback",
                "prior_payload": prior_payload,
                "refresh": refresh,
            },
            "error": None,
            "status_code": 200,
        })
        return (_json_text(payload), _json_text(paths), _indexed_list_text(paths), len(paths), True, refresh, _as_comfy_list(paths), *_selectable_tail(paths))

    def field_paths(
        self,
        session: Any,
        collection_name: str,
        document_id: str,
        document_prefix: str,
        document_id_link: Any = "",
        collection_name_link: Any = "",
        refresh_token: str = "",
    ):
        clean_id = _coerce_document_id(_prefer_link_value(document_id, document_id_link))
        clean_collection = _clean_scalar(_prefer_link_value(collection_name, collection_name_link)) or "documents"
        refresh = _dirty_token("document_field_paths", refresh_token, clean_collection, clean_id)

        if not clean_id:
            payload = _ensure_payload_dict({
                "success": False,
                "message": "Document field paths skipped: document_id is empty.",
                "data": {
                    "collection_name": clean_collection,
                    "document_id": clean_id,
                    "refresh": refresh,
                    "checks": [
                        "Connect document_id_link from 06 Select Nth Document Item.",
                        "For Local File Store, select an actual document id, not a field path.",
                    ],
                },
                "error": {"msg": "missing_document_id"},
                "status_code": 400,
            })
            return (_json_text(payload), _json_text([]), _indexed_list_text([]), 0, False, refresh, _as_comfy_list([]), *_selectable_tail([]))

        quoted = urllib.parse.quote(clean_id, safe="")
        payload = _session_request(session, "GET", document_prefix, f"/api/field-paths/{quoted}")

        if payload.get("success"):
            paths = _extract_field_paths(payload)
            return (_json_text(payload), _json_text(paths), _indexed_list_text(paths), len(paths), True, refresh, _as_comfy_list(paths), *_selectable_tail(paths))

        # Local File Store and generic ZMongo sessions do not implement the
        # /documents field-path route.  Fall back to get_doc(collection, id) and
        # flatten the document locally.
        return self._local_field_paths_fallback(
            session,
            collection_name=clean_collection,
            document_id=clean_id,
            refresh=refresh,
            prior_payload=payload,
        )

class ZMongoDocumentGetValue(AlwaysDirtyMixin):
    @classmethod
    def INPUT_TYPES(cls):
        return {"required": {"session": ("ZMONGO_API_SESSION",), "document_id": ("STRING", {"default": ""}), "field_path": ("STRING", {"default": "text"}), "fallback": ("STRING", {"default": ""}), "document_prefix": ("STRING", {"default": DEFAULT_DOCUMENT_PREFIX})}, "optional": {"document_id_link": ("*",), "field_path_link": ("*",), "refresh_token": ("STRING", {"default": ""})}}

    RETURN_TYPES = ("STRING", "STRING", "BOOLEAN", "STRING", "STRING", "*") + SELECTABLE_RETURN_TYPES
    RETURN_NAMES = ("json", "value", "exists", "value_type", "refresh", "value_items") + SELECTABLE_RETURN_NAMES
    OUTPUT_IS_LIST = (False, False, False, False, False, True) + SELECTABLE_OUTPUT_IS_LIST
    FUNCTION = "get_value"
    CATEGORY = "ZMongo/06 Documents"
    OUTPUT_NODE = True

    def get_value(self, session: Any, document_id: str, field_path: str, fallback: str, document_prefix: str, document_id_link: str = "", field_path_link: str = "", refresh_token: str = ""):
        clean_id = _coerce_document_id(_prefer_link_value(document_id, document_id_link))
        clean_path = _coerce_field_path(_prefer_link_value(field_path, field_path_link))
        quoted = urllib.parse.quote(clean_id, safe="")
        payload = _session_request(session, "GET", document_prefix, f"/api/get/{quoted}")
        if not payload.get("success"):
            empty_tail = _selectable_tail([])
            return (_json_text(payload), fallback or "", False, "missing_document", _dirty_token("document_get_value_error", refresh_token), _as_comfy_list([]), *empty_tail)
        document = _extract_document(payload)
        marker = object()
        value = _safe_get_by_path(document, clean_path, marker)
        exists = value is not marker
        if not exists:
            value = fallback
        value_text = _json_text(value) if isinstance(value, (dict, list, tuple)) else str(value if value is not None else "")
        items = _value_items(value)
        return (_json_text(payload), value_text, bool(exists), type(value).__name__ if exists else "missing", _dirty_token("document_get_value", refresh_token), _as_comfy_list(items), *_selectable_tail(items))


class ZMongoDocumentSaveText(AlwaysDirtyMixin):
    @classmethod
    def INPUT_TYPES(cls):
        return {"required": {"session": ("ZMONGO_API_SESSION",), "document_id": ("STRING", {"default": ""}), "text": ("STRING", {"default": "", "multiline": True}), "document_prefix": ("STRING", {"default": DEFAULT_DOCUMENT_PREFIX})}, "optional": {"document_id_link": ("*",), "refresh_token": ("STRING", {"default": ""})}}

    RETURN_TYPES = ("STRING", "STRING", "INT", "BOOLEAN", "STRING", "*") + SELECTABLE_RETURN_TYPES
    RETURN_NAMES = ("json", "document_id", "text_length", "success", "refresh", "document_ids") + SELECTABLE_RETURN_NAMES
    OUTPUT_IS_LIST = (False, False, False, False, False, True) + SELECTABLE_OUTPUT_IS_LIST
    FUNCTION = "save_text"
    CATEGORY = "ZMongo/06 Documents"
    OUTPUT_NODE = True

    def save_text(self, session: Any, document_id: str, text: str, document_prefix: str, document_id_link: str = "", refresh_token: str = ""):
        clean_id = _coerce_document_id(_prefer_link_value(document_id, document_id_link))
        quoted = urllib.parse.quote(clean_id, safe="")
        payload = _session_request(session, "POST", document_prefix, f"/api/save-text/{quoted}", json_body={"text": text or ""})
        _raise_if_failed(payload, "Document save text")
        resolved_id = _extract_doc_id(payload) or clean_id
        ids = [resolved_id] if resolved_id else []
        return (_json_text(payload), resolved_id, int(_extract_data(payload).get("text_length") or len(text or "")), bool(payload.get("success")), _dirty_token("document_save_text", refresh_token), _as_comfy_list(ids), *_selectable_tail(ids))


class ZMongoDocumentSaveValue(AlwaysDirtyMixin):
    @classmethod
    def INPUT_TYPES(cls):
        return {"required": {"session": ("ZMONGO_API_SESSION",), "document_id": ("STRING", {"default": ""}), "field_path": ("STRING", {"default": "metadata.note"}), "value": ("STRING", {"default": "", "multiline": True}), "parse_json": ("BOOLEAN", {"default": True}), "normalize_for_storage": ("BOOLEAN", {"default": False}), "document_prefix": ("STRING", {"default": DEFAULT_DOCUMENT_PREFIX})}, "optional": {"document_id_link": ("*",), "field_path_link": ("*",), "refresh_token": ("STRING", {"default": ""})}}

    RETURN_TYPES = ("STRING", "STRING", "BOOLEAN", "STRING", "*") + SELECTABLE_RETURN_TYPES
    RETURN_NAMES = ("json", "document_id", "success", "refresh", "document_ids") + SELECTABLE_RETURN_NAMES
    OUTPUT_IS_LIST = (False, False, False, False, True) + SELECTABLE_OUTPUT_IS_LIST
    FUNCTION = "save_value"
    CATEGORY = "ZMongo/06 Documents"
    OUTPUT_NODE = True

    def save_value(self, session: Any, document_id: str, field_path: str, value: str, parse_json: bool, normalize_for_storage: bool, document_prefix: str, document_id_link: str = "", field_path_link: str = "", refresh_token: str = ""):
        clean_id = _coerce_document_id(_prefer_link_value(document_id, document_id_link))
        clean_path = _coerce_field_path(_prefer_link_value(field_path, field_path_link))
        refresh = _dirty_token("document_save_value", refresh_token, clean_id, clean_path)
        ids = [clean_id] if clean_id else []

        # MongoDB will reject attempts to update _id.  More importantly, these
        # fields are identity/ownership fields and should never be written by a
        # generic ComfyUI Save Value node.  Return a visible, non-throwing error
        # payload so the workflow continues and the user can choose a writable
        # field path from the selector.
        if _is_immutable_document_field_path(clean_path):
            payload = _blocked_immutable_field_payload(
                document_id=clean_id,
                field_path=clean_path,
                refresh=refresh,
            )
            return (_json_text(payload), clean_id, False, refresh, _as_comfy_list(ids), *_selectable_tail(ids))

        if not clean_path:
            payload = _ensure_payload_dict(
                {
                    "success": False,
                    "message": "Save skipped. field_path is required.",
                    "data": {"document_id": clean_id, "field_path": clean_path, "refresh": refresh},
                    "error": {"type": "MissingFieldPath", "msg": "field_path is required."},
                    "status_code": 400,
                }
            )
            return (_json_text(payload), clean_id, False, refresh, _as_comfy_list(ids), *_selectable_tail(ids))

        quoted = urllib.parse.quote(clean_id, safe="")
        body = {
            "field_path": clean_path,
            "value": _parse_any_json(value, parse_json=parse_json),
            "parse_json_strings": bool(parse_json),
            "normalize_for_storage": bool(normalize_for_storage),
        }
        payload = _session_request(session, "POST", document_prefix, f"/api/save-value/{quoted}", json_body=body)

        # Do not raise here.  Save Value is often used in demo workflows with
        # selectable fields; a failed write should be visible in the JSON output
        # but should not kill the entire ComfyUI graph.
        resolved_id = _extract_doc_id(payload) or clean_id
        ids = [resolved_id] if resolved_id else []
        return (_json_text(payload), resolved_id, bool(payload.get("success")), refresh, _as_comfy_list(ids), *_selectable_tail(ids))


class ZMongoDocumentUpdateMetadata(AlwaysDirtyMixin):
    @classmethod
    def INPUT_TYPES(cls):
        return {"required": {"session": ("ZMONGO_API_SESSION",), "document_id": ("STRING", {"default": ""}), "metadata_json": ("STRING", {"default": "{}", "multiline": True}), "case_id": ("STRING", {"default": ""}), "status": ("STRING", {"default": ""}), "title": ("STRING", {"default": ""}), "description": ("STRING", {"default": "", "multiline": True}), "tags_json": ("STRING", {"default": "[]", "multiline": True}), "document_prefix": ("STRING", {"default": DEFAULT_DOCUMENT_PREFIX})}, "optional": {"document_id_link": ("*",), "refresh_token": ("STRING", {"default": ""})}}

    RETURN_TYPES = ("STRING", "STRING", "BOOLEAN", "STRING", "*") + SELECTABLE_RETURN_TYPES
    RETURN_NAMES = ("json", "document_id", "success", "refresh", "document_ids") + SELECTABLE_RETURN_NAMES
    OUTPUT_IS_LIST = (False, False, False, False, True) + SELECTABLE_OUTPUT_IS_LIST
    FUNCTION = "update_metadata"
    CATEGORY = "ZMongo/06 Documents"
    OUTPUT_NODE = True

    def update_metadata(self, session: Any, document_id: str, metadata_json: str, case_id: str, status: str, title: str, description: str, tags_json: str, document_prefix: str, document_id_link: str = "", refresh_token: str = ""):
        clean_id = _coerce_document_id(_prefer_link_value(document_id, document_id_link))
        quoted = urllib.parse.quote(clean_id, safe="")
        body: dict[str, Any] = {"metadata": _parse_json_object(metadata_json, "metadata_json")}
        if _clean_scalar(case_id):
            body["case_id"] = _clean_scalar(case_id)
        if _clean_scalar(status):
            body["status"] = _clean_scalar(status)
        if _clean_scalar(title):
            body["title"] = _clean_scalar(title)
        if _clean_scalar(description):
            body["description"] = description
        tags = _parse_json_list(tags_json, "tags_json")
        if tags:
            body["tags"] = tags
        payload = _session_request(session, "POST", document_prefix, f"/api/metadata/{quoted}", json_body=body)
        _raise_if_failed(payload, "Document metadata update")
        resolved_id = _extract_doc_id(payload) or clean_id
        ids = [resolved_id] if resolved_id else []
        return (_json_text(payload), resolved_id, bool(payload.get("success")), _dirty_token("document_metadata", refresh_token), _as_comfy_list(ids), *_selectable_tail(ids))


class ZMongoDocumentExtractText(AlwaysDirtyMixin):
    @classmethod
    def INPUT_TYPES(cls):
        return {"required": {"session": ("ZMONGO_API_SESSION",), "document_id": ("STRING", {"default": ""}), "save": ("BOOLEAN", {"default": True}), "document_prefix": ("STRING", {"default": DEFAULT_DOCUMENT_PREFIX})}, "optional": {"document_id_link": ("*",), "refresh_token": ("STRING", {"default": ""})}}

    RETURN_TYPES = ("STRING", "STRING", "INT", "BOOLEAN", "STRING", "*") + SELECTABLE_RETURN_TYPES
    RETURN_NAMES = ("json", "text", "text_length", "success", "refresh", "text_items") + SELECTABLE_RETURN_NAMES
    OUTPUT_IS_LIST = (False, False, False, False, False, True) + SELECTABLE_OUTPUT_IS_LIST
    FUNCTION = "extract_text"
    CATEGORY = "ZMongo/06 Documents"
    OUTPUT_NODE = True

    def extract_text(self, session: Any, document_id: str, save: bool, document_prefix: str, document_id_link: str = "", refresh_token: str = ""):
        clean_id = _coerce_document_id(_prefer_link_value(document_id, document_id_link))
        refresh = _dirty_token("document_extract_text", refresh_token, clean_id)

        if not clean_id:
            payload = _ensure_payload_dict({
                "success": False,
                "message": "Document extract text skipped: document_id is empty.",
                "data": {"document_id": clean_id, "refresh": refresh},
                "error": {"msg": "missing_document_id"},
                "status_code": 400,
            })
            return (_json_text(payload), "", 0, False, refresh, _as_comfy_list([]), *_selectable_tail([]))

        quoted = urllib.parse.quote(clean_id, safe="")
        payload = _session_request(
            session,
            "POST",
            document_prefix,
            f"/api/extract-text/{quoted}",
            json_body={"save": bool(save)},
        )

        # Do not crash ComfyUI when the selected document is text-only or has no
        # stored file payload.  This is a normal state for documents created by
        # "06 Create Text Document".  Fall back to the document's stored text.
        if not payload.get("success"):
            fallback_payload = _session_request(session, "GET", document_prefix, f"/api/text/{quoted}")
            fallback_text = _extract_text(fallback_payload)

            if fallback_text:
                combined_payload = _ensure_payload_dict({
                    "success": True,
                    "message": "No file payload was available for extraction; returned existing stored text instead.",
                    "data": {
                        "document_id": clean_id,
                        "text": fallback_text,
                        "text_length": len(fallback_text),
                        "extract_payload": payload,
                        "fallback_payload": fallback_payload,
                        "source": "stored_text_fallback",
                        "refresh": refresh,
                    },
                    "error": None,
                    "status_code": 200,
                })
                items = [fallback_text]
                return (
                    _json_text(combined_payload),
                    fallback_text,
                    len(fallback_text),
                    True,
                    refresh,
                    _as_comfy_list(items),
                    *_selectable_tail(items),
                )

            # Nonfatal error: report the backend response, but let the workflow continue.
            nonfatal_payload = _ensure_payload_dict({
                "success": False,
                "message": payload.get("message") or "Document text extraction failed without stored text fallback.",
                "data": {
                    "document_id": clean_id,
                    "extract_payload": payload,
                    "fallback_payload": fallback_payload,
                    "source": "extract_failed_no_fallback_text",
                    "refresh": refresh,
                    "checks": [
                        "Use this node only for uploaded file-backed documents when you need PDF/file extraction.",
                        "For documents created with 06 Create Text Document, use 06 Get Document Text instead.",
                        "Confirm that the selected document_id came from 06 List Documents, not from 06 Document Field Paths.",
                    ],
                },
                "error": payload.get("error") or {"msg": payload.get("message") or "extract_failed"},
                "status_code": payload.get("status_code", 0),
            })
            return (_json_text(nonfatal_payload), "", 0, False, refresh, _as_comfy_list([]), *_selectable_tail([]))

        text = _extract_text(payload)
        items = [text] if text else []
        return (_json_text(payload), text, len(text), bool(payload.get("success")), refresh, _as_comfy_list(items), *_selectable_tail(items))


class ZMongoDocumentQueueOCR(AlwaysDirtyMixin):
    @classmethod
    def INPUT_TYPES(cls):
        return {"required": {"session": ("ZMONGO_API_SESSION",), "document_id": ("STRING", {"default": ""}), "priority": ("INT", {"default": 50, "min": 0, "max": 100}), "source": ("STRING", {"default": "comfyui_zmongo_document_node"}), "document_prefix": ("STRING", {"default": DEFAULT_DOCUMENT_PREFIX})}, "optional": {"document_id_link": ("*",), "refresh_token": ("STRING", {"default": ""})}}

    RETURN_TYPES = ("STRING", "STRING", "STRING", "BOOLEAN", "STRING", "*") + SELECTABLE_RETURN_TYPES
    RETURN_NAMES = ("json", "document_id", "status", "success", "refresh", "document_ids") + SELECTABLE_RETURN_NAMES
    OUTPUT_IS_LIST = (False, False, False, False, False, True) + SELECTABLE_OUTPUT_IS_LIST
    FUNCTION = "queue_ocr"
    CATEGORY = "ZMongo/06 Documents"
    OUTPUT_NODE = True

    def queue_ocr(self, session: Any, document_id: str, priority: int, source: str, document_prefix: str, document_id_link: str = "", refresh_token: str = ""):
        clean_id = _coerce_document_id(_prefer_link_value(document_id, document_id_link))
        quoted = urllib.parse.quote(clean_id, safe="")
        payload = _session_request(session, "POST", document_prefix, f"/api/ocr/queue/{quoted}", json_body={"priority": int(priority), "source": _clean_scalar(source) or "comfyui_zmongo_document_node"})
        _raise_if_failed(payload, "Document OCR queue")
        data = _extract_data(payload)
        resolved_id = _coerce_document_id(data.get("document_id")) or clean_id
        status = str(data.get("status") or _safe_get_by_path(data, "ocr.status", ""))
        ids = [resolved_id] if resolved_id else []
        return (_json_text(payload), resolved_id, status, bool(payload.get("success")), _dirty_token("document_queue_ocr", refresh_token), _as_comfy_list(ids), *_selectable_tail(ids))


class ZMongoDocumentOCRStatus(AlwaysDirtyMixin):
    @classmethod
    def INPUT_TYPES(cls):
        return {"required": {"session": ("ZMONGO_API_SESSION",), "document_id": ("STRING", {"default": ""}), "document_prefix": ("STRING", {"default": DEFAULT_DOCUMENT_PREFIX})}, "optional": {"document_id_link": ("*",), "refresh_token": ("STRING", {"default": ""})}}

    RETURN_TYPES = ("STRING", "STRING", "BOOLEAN", "STRING", "BOOLEAN", "STRING", "*") + SELECTABLE_RETURN_TYPES
    RETURN_NAMES = ("json", "status", "has_text", "last_error", "success", "refresh", "status_items") + SELECTABLE_RETURN_NAMES
    OUTPUT_IS_LIST = (False, False, False, False, False, False, True) + SELECTABLE_OUTPUT_IS_LIST
    FUNCTION = "ocr_status"
    CATEGORY = "ZMongo/06 Documents"
    OUTPUT_NODE = True

    def ocr_status(self, session: Any, document_id: str, document_prefix: str, document_id_link: str = "", refresh_token: str = ""):
        clean_id = _coerce_document_id(_prefer_link_value(document_id, document_id_link))
        quoted = urllib.parse.quote(clean_id, safe="")
        payload = _session_request(session, "GET", document_prefix, f"/api/ocr/status/{quoted}")
        _raise_if_failed(payload, "Document OCR status")
        data = _extract_data(payload)
        status = str(data.get("status") or "")
        last_error = str(data.get("last_error") or "")
        items = [status] if status else []
        return (_json_text(payload), status, bool(data.get("has_text")), last_error, bool(payload.get("success")), _dirty_token("document_ocr_status", refresh_token), _as_comfy_list(items), *_selectable_tail(items))


class ZMongoDocumentDelete(AlwaysDirtyMixin):
    @classmethod
    def INPUT_TYPES(cls):
        return {"required": {"session": ("ZMONGO_API_SESSION",), "document_id": ("STRING", {"default": ""}), "confirm_delete": ("BOOLEAN", {"default": False}), "document_prefix": ("STRING", {"default": DEFAULT_DOCUMENT_PREFIX})}, "optional": {"document_id_link": ("*",), "refresh_token": ("STRING", {"default": ""})}}

    RETURN_TYPES = ("STRING", "STRING", "BOOLEAN", "STRING", "*") + SELECTABLE_RETURN_TYPES
    RETURN_NAMES = ("json", "document_id", "success", "refresh", "document_ids") + SELECTABLE_RETURN_NAMES
    OUTPUT_IS_LIST = (False, False, False, False, True) + SELECTABLE_OUTPUT_IS_LIST
    FUNCTION = "delete_document"
    CATEGORY = "ZMongo/06 Documents"
    OUTPUT_NODE = True

    def delete_document(self, session: Any, document_id: str, confirm_delete: bool, document_prefix: str, document_id_link: str = "", refresh_token: str = ""):
        clean_id = _coerce_document_id(_prefer_link_value(document_id, document_id_link))
        if not confirm_delete:
            payload = _error_payload("Delete not executed. Set confirm_delete=true.", data={"document_id": clean_id}, status_code=400)
            ids = [clean_id] if clean_id else []
            return (_json_text(payload), clean_id, False, _dirty_token("document_delete_blocked", refresh_token), _as_comfy_list(ids), *_selectable_tail(ids))
        quoted = urllib.parse.quote(clean_id, safe="")
        payload = _session_request(session, "POST", document_prefix, f"/api/delete/{quoted}", json_body={})
        _raise_if_failed(payload, "Document delete")
        ids = [clean_id] if clean_id else []
        return (_json_text(payload), clean_id, bool(payload.get("success")), _dirty_token("document_delete", refresh_token), _as_comfy_list(ids), *_selectable_tail(ids))


class ZMongoDocumentSelectNthItem(AlwaysDirtyMixin):
    """
    Color-coded selector for document workflows.

    The selected item is returned as a generic string plus guarded semantic
    outputs. The guarded outputs prevent a selected field path like "_id" from
    being sent as a document id.
    """

    @classmethod
    def INPUT_TYPES(cls):
        return {
            "required": {
                "items_list": ("*",),
                "index": ("INT", {"default": 0, "min": 0, "max": 1000000}),
                "fallback": ("STRING", {"default": ""}),
                "semantic_hint": (
                    ["auto", "document_id", "field_path", "file_path", "filename", "text"],
                    {"default": "auto"},
                ),
            }
        }

    RETURN_TYPES = ("STRING", ZMONGO_DOCUMENT_ID, ZMONGO_FIELD_PATH, ZMONGO_FILE_PATH, ZMONGO_FILENAME, ZMONGO_TEXT, "STRING")
    RETURN_NAMES = ("item", "document_id_link", "field_path_link", "file_path_link", "filename_link", "text_link", "status")
    FUNCTION = "select_nth_item"
    CATEGORY = "ZMongo/06 Documents"
    INPUT_IS_LIST = True
    OUTPUT_NODE = True

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

    @staticmethod
    def _selected_as_string(value: Any) -> str:
        if isinstance(value, (dict, list, tuple)):
            return _json_text(value)
        return _strip_index_prefix(value)

    def _semantic_outputs(self, selected: str, semantic_hint: str = "auto") -> tuple[str, str, str, str, str]:
        # ComfyUI sends every input as a list when INPUT_IS_LIST=True.
        # Unwrap semantic_hint before using string methods so the selector can
        # run in both linked/list mode and normal widget mode.
        hint_value = self._unwrap_scalar(semantic_hint, "auto")
        hint = str(hint_value or "auto").strip().lower()

        selected = self._selected_as_string(self._unwrap_scalar(selected, selected))

        document_id = _coerce_document_id_link(selected)
        field_path = _coerce_field_path(selected)
        file_path = _coerce_file_path_link(selected)
        filename = _coerce_filename_link(selected)
        text = _coerce_text_link(selected)

        # Explicit hints produce only the matching semantic output.
        if hint == "document_id":
            return (document_id, "", "", "", "")
        if hint == "field_path":
            return ("", field_path, "", "", "")
        if hint == "file_path":
            return ("", "", file_path, "", "")
        if hint == "filename":
            return ("", "", "", filename, "")
        if hint == "text":
            return ("", "", "", "", selected)

        # Auto mode: never put a simple field-path key into document_id_link.
        if document_id:
            return (document_id, "", "", "", "")
        if file_path:
            return ("", "", file_path, filename, "")
        if field_path:
            return ("", field_path, "", filename, "")
        return ("", "", "", filename, text)

    def select_nth_item(self, items_list, index, fallback, semantic_hint="auto"):
        raw_items = self._normalize_items(items_list)
        fallback_value = str(self._unwrap_scalar(fallback, "") or "")
        index_value = self._unwrap_scalar(index, 0)

        try:
            safe_index = int(index_value or 0)
        except Exception:
            safe_index = 0

        cleaned = [self._selected_as_string(item).strip() for item in raw_items if self._selected_as_string(item).strip()]
        if not cleaned:
            selected = fallback_value
            document_id, field_path, file_path, filename, text = self._semantic_outputs(selected, semantic_hint)
            return (selected, document_id, field_path, file_path, filename, text, "Input list was empty.")

        selected_index = max(0, min(safe_index, len(cleaned) - 1))
        selected = cleaned[selected_index]
        document_id, field_path, file_path, filename, text = self._semantic_outputs(selected, semantic_hint)

        status = (
            f"Selected {selected_index + 1}/{len(cleaned)}: {selected} | "
            f"document_id_link={document_id or '<empty>'} | "
            f"field_path_link={field_path or '<empty>'} | "
            f"file_path_link={file_path or '<empty>'}"
        )
        return (selected, document_id, field_path, file_path, filename, text, status)

NODE_CLASS_MAPPINGS = {
    "ZMongoDocumentSelectNthItem": ZMongoDocumentSelectNthItem,
    "ZMongoDocumentFilePathBrowser": ZMongoDocumentFilePathBrowser,
    "ZMongoDocumentHealth": ZMongoDocumentHealth,
    "ZMongoDocumentWhoAmI": ZMongoDocumentWhoAmI,
    "ZMongoDocumentUploadFile": ZMongoDocumentUploadFile,
    "ZMongoDocumentCreateText": ZMongoDocumentCreateText,
    "ZMongoDocumentList": ZMongoDocumentList,
    "ZMongoDocumentGet": ZMongoDocumentGet,
    "ZMongoDocumentGetText": ZMongoDocumentGetText,
    "ZMongoDocumentFieldPaths": ZMongoDocumentFieldPaths,
    "ZMongoDocumentGetValue": ZMongoDocumentGetValue,
    "ZMongoDocumentSaveText": ZMongoDocumentSaveText,
    "ZMongoDocumentSaveValue": ZMongoDocumentSaveValue,
    "ZMongoDocumentUpdateMetadata": ZMongoDocumentUpdateMetadata,
    "ZMongoDocumentExtractText": ZMongoDocumentExtractText,
    "ZMongoDocumentQueueOCR": ZMongoDocumentQueueOCR,
    "ZMongoDocumentOCRStatus": ZMongoDocumentOCRStatus,
    "ZMongoDocumentDelete": ZMongoDocumentDelete,
}

NODE_DISPLAY_NAME_MAPPINGS = {
    "ZMongoDocumentSelectNthItem": "06 Select Nth Document Item",
    "ZMongoDocumentFilePathBrowser": "06 Document File Browser",
    "ZMongoDocumentHealth": "06 Document API Health",
    "ZMongoDocumentWhoAmI": "06 Document WhoAmI",
    "ZMongoDocumentUploadFile": "06 Upload Document File",
    "ZMongoDocumentCreateText": "06 Create Text Document",
    "ZMongoDocumentList": "06 List Documents",
    "ZMongoDocumentGet": "06 Get Document",
    "ZMongoDocumentGetText": "06 Get Document Text",
    "ZMongoDocumentFieldPaths": "06 Document Field Paths",
    "ZMongoDocumentGetValue": "06 Get Document Value",
    "ZMongoDocumentSaveText": "06 Save Document Text",
    "ZMongoDocumentSaveValue": "06 Save Document Value",
    "ZMongoDocumentUpdateMetadata": "06 Update Document Metadata",
    "ZMongoDocumentExtractText": "06 Extract Document Text",
    "ZMongoDocumentQueueOCR": "06 Queue Document OCR",
    "ZMongoDocumentOCRStatus": "06 Document OCR Status",
    "ZMongoDocumentDelete": "06 Delete Document",
}

__all__ = ["NODE_CLASS_MAPPINGS", "NODE_DISPLAY_NAME_MAPPINGS"]
