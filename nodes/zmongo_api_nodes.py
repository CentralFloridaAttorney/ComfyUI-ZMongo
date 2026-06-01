from __future__ import annotations
from __future__ import annotations

from .generic_helpers import AlwaysDirtyMixin, DEFAULT_BASE_URL, DEFAULT_COMFY_ZMONGO_PREFIX, DEFAULT_FLEET_PREFIX, \
    DEFAULT_COMFY_ZMONGO_FLEET_PREFIX, DEFAULT_TIMEOUT, _normalize_base_url, _clean_prefix, _json_text, _error_payload, \
    _success_payload, _indexed_list_text, _extract_collections, _as_comfy_list, _dirty_token, _parse_json_object, \
    _parse_json_list, _extract_doc_ids, _extract_count, _parse_any_json, _extract_document_from_payload, \
    ZMongoLocalFileStoreSessionNode, safe_get_by_path, _ensure_payload_dict

import json
from typing import Any
import requests
from .generic_helpers import AlwaysDirtyMixin, DEFAULT_BASE_URL, _json_text, _error_payload, _success_payload, ZMongoLocalFileStoreSessionNode

# -----------------------------------------------------------------------------
# HTTP client
# -----------------------------------------------------------------------------
#
# class ZMongoApiSession:
#     def __init__(self, *, base_url: str = DEFAULT_BASE_URL, zai_api_key: str = "", verify_tls: bool = True) -> None:
#         self.base_url = base_url
#         self.zai_api_key = zai_api_key.strip()
#         self.verify_tls = verify_tls
#         self.session = requests.Session()
#
#     def close(self) -> None:
#         self.session.close()
#
#     def _headers(self) -> dict[str, str]:
#         headers = {
#             "Accept": "application/json",
#             "Content-Type": "application/json",
#             "Authorization": f"Bearer {self.zai_api_key}",
#         }
#         return headers
#
#     def whoami(self) -> dict[str, Any]:
#         response = self.session.get(f"{self.base_url}/api/whoami", headers=self._headers(), verify=self.verify_tls)
#         try:
#             return response.json()
#         except Exception:
#             return {"success": False, "message": response.text}
#
# # -----------------------------------------------------------------------------
# # 00 Auth Nodes (Simplified API Key only)
# # -----------------------------------------------------------------------------
#
# class ZMongoApiKeyOnlySessionNode(AlwaysDirtyMixin):
#     CATEGORY = "ZMongo/00 Auth"
#     FUNCTION = "connect"
#     DISPLAY_ONLY = True
#
#     @classmethod
#     def INPUT_TYPES(cls):
#         return {"required": {"api_key": ("STRING", {"description": "Your ZMongo API key"})}}
#
#     RETURN_TYPES = ("ZMONGO_API_SESSION", "STRING", "STRING")
#     RETURN_NAMES = ("session", "json", "status")
#
#     def connect(self, api_key: str):
#         try:
#             session = ZMongoApiSession(base_url="https://businessprocessapplications.com/comfy-zmongo", zai_api_key=api_key)
#             payload = session.whoami()
#             status = payload.get("message", "API session created successfully.")
#             return session, _json_text(payload), status
#         except Exception as e:
#             payload = _error_payload(str(e))
#             return None, _json_text(payload), f"API session failed: {e}"
#
# class ZMongoApiCloseSessionNode(AlwaysDirtyMixin):
#     CATEGORY = "ZMongo/00 Auth"
#     FUNCTION = "close_session"
#
#     @classmethod
#     def INPUT_TYPES(cls):
#         return {"required": {"session": ("ZMONGO_API_SESSION",)}}
#
#     RETURN_TYPES = ("STRING",)
#     RETURN_NAMES = ("json",)
#
#     def close_session(self, session):
#         if session is None:
#             return (_json_text(_error_payload("No session provided.")),)
#         try:
#             session.close()
#             return (_json_text(_success_payload("Session closed.")),)
#         except Exception as exc:
#             return (_json_text(_error_payload(str(exc))),)




# -----------------------------------------------------------------------------
# 00 Auth nodes
# -----------------------------------------------------------------------------

# class ZMongoApiKeySessionNode(AlwaysDirtyMixin):
#     @classmethod
#     def INPUT_TYPES(cls):
#         return {
#             "required": {
#                 "base_url": ("STRING", {"default": DEFAULT_BASE_URL}),
#                 "zai_api_key": ("STRING", {"default": "", "multiline": False}),
#                 "username": ("STRING", {"default": ""}),
#                 "comfy_zmongo_prefix": ("STRING", {"default": DEFAULT_COMFY_ZMONGO_PREFIX}),
#                 "fleet_prefix": ("STRING", {"default": DEFAULT_FLEET_PREFIX}),
#                 "comfy_zmongo_fleet_prefix": ("STRING", {"default": DEFAULT_COMFY_ZMONGO_FLEET_PREFIX}),
#                 "timeout_seconds": ("INT", {"default": DEFAULT_TIMEOUT, "min": 1, "max": 300}),
#                 "verify_tls": ("BOOLEAN", {"default": True}),
#                 "test_whoami": ("BOOLEAN", {"default": True}),
#             }
#         }
#
#     RETURN_TYPES = ("ZMONGO_API_SESSION", "STRING", "STRING")
#     RETURN_NAMES = ("session", "json", "status")
#     FUNCTION = "connect"
#     CATEGORY = "ZMongo/00 Auth"
#
#     def connect(
#         self,
#         base_url: str,
#         zai_api_key: str,
#         username: str,
#         comfy_zmongo_prefix: str,
#         fleet_prefix: str,
#         comfy_zmongo_fleet_prefix: str,
#         timeout_seconds: int,
#         verify_tls: bool,
#         test_whoami: bool,
#     ):
#         try:
#             session = ZMongoApiSession(
#                 base_url=base_url,
#                 zai_api_key=zai_api_key,
#                 username=username,
#                 comfy_zmongo_prefix=comfy_zmongo_prefix,
#                 fleet_prefix=fleet_prefix,
#                 comfy_zmongo_fleet_prefix=comfy_zmongo_fleet_prefix,
#                 timeout=timeout_seconds,
#                 verify_tls=verify_tls,
#             )
#
#             if test_whoami:
#                 payload = session.whoami()
#                 status = payload.get("message") or "API session created."
#                 return (session, _json_text(payload), status)
#
#             payload = _success_payload(
#                 "API session created.",
#                 {
#                     "base_url": session.base_url,
#                     "username": session.username,
#                     "comfy_zmongo_prefix": session.comfy_zmongo_prefix,
#                     "fleet_prefix": session.fleet_prefix,
#                     "comfy_zmongo_fleet_prefix": session.comfy_zmongo_fleet_prefix,
#                 },
#             )
#             return (session, _json_text(payload), "API session created.")
#         except Exception as exc:
#             payload = _error_payload(str(exc))
#             return (None, _json_text(payload), f"API session failed: {exc}")


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
    # # 00 Auth
    # "ZMongoLocalFileStoreSessionNode": ZMongoLocalFileStoreSessionNode,
    # "ZMongoApiKeyOnlySessionNode": ZMongoApiKeyOnlySessionNode,
    # "ZMongoApiCloseSessionNode": ZMongoApiCloseSessionNode,

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
    # # 00 Auth
    # "ZMongoLocalFileStoreSessionNode": "00 Local File Store Session",
    # "ZMongoApiKeyOnlySessionNode": "00 API Key Session (Simplified)",
    # "ZMongoApiCloseSessionNode": "00 Close API Session",

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