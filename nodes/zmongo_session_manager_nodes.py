from __future__ import annotations

from typing import Any, Dict, Optional

from .session_client import (
    DEFAULT_BASE_URL,
    DEFAULT_TIMEOUT,
    ZTarotManagerSessionClient,
    extract_data,
    extract_field_paths_from_value,
    extract_primary_json_value,
    find_doc_by_key,
    json_dumps,
    parse_any_json,
    parse_json_maybe,
    parse_object_json,
    safe_get_by_path,
    summarize_for_text,
)

JSON_TYPE = "ZMONGO_JSON"
CLIENT_TYPE = "ZMONGO_MANAGER_SESSION_CLIENT"


def _success_from_payload(payload: Dict[str, Any]) -> bool:
    return bool(payload.get("success", False))


def _standard_output(
    payload: Dict[str, Any],
    data_value: Any = None,
    text_value: Optional[str] = None,
):
    primary = extract_primary_json_value(payload) if data_value is None else data_value
    text = summarize_for_text(primary) if text_value is None else text_value
    return (
        json_dumps(payload),
        json_dumps(primary),
        text,
        _success_from_payload(payload),
    )


def _exception_output(exc: Exception):
    payload = {
        "success": False,
        "message": str(exc),
        "status_code": 0,
        "error": {"msg": str(exc), "type": exc.__class__.__name__},
        "data": {},
    }
    return (
        json_dumps(payload),
        json_dumps({}),
        "",
        False,
    )


def _choose_json_source(text_value: str, json_input: Optional[str]) -> str:
    if json_input is not None:
        stripped = str(json_input).strip()
        if stripped != "":
            return stripped
    return text_value or ""


def _parse_any_source(text_value: str, json_input: Optional[str], default: Any = None) -> Any:
    source = _choose_json_source(text_value, json_input)
    return parse_any_json(source, default=default)


def _parse_object_source(
    text_value: str,
    json_input: Optional[str],
    default: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    source = _choose_json_source(text_value, json_input)
    return parse_object_json(source, default=default)


def _parse_value_source(text_value: str, json_input: Optional[str], parse_value_as_json: bool) -> Any:
    source = _choose_json_source(text_value, json_input)
    return parse_json_maybe(source, parse_value_as_json)


class ZMongoSessionManagerConnectNode:
    CATEGORY = "ZMongo/SessionManager"
    FUNCTION = "connect"
    RETURN_TYPES = (CLIENT_TYPE, JSON_TYPE, "STRING", "BOOLEAN")
    RETURN_NAMES = ("client", "result_json", "text_output", "success")

    @classmethod
    def INPUT_TYPES(cls):
        return {
            "required": {
                "base_url": ("STRING", {"default": DEFAULT_BASE_URL}),
                "username": ("STRING", {"default": ""}),
                "password": ("STRING", {"default": "", "multiline": False}),
                "use_env_credentials_when_blank": ("BOOLEAN", {"default": True}),
                "verify_tls": ("BOOLEAN", {"default": True}),
                "timeout_seconds": ("INT", {"default": DEFAULT_TIMEOUT, "min": 1, "max": 300}),
            }
        }

    def connect(
        self,
        base_url: str,
        username: str,
        password: str,
        use_env_credentials_when_blank: bool,
        verify_tls: bool,
        timeout_seconds: int,
    ):
        actual_username = None if use_env_credentials_when_blank and not username.strip() else username.strip()
        actual_password = None if use_env_credentials_when_blank and not password.strip() else password.strip()

        try:
            client = ZTarotManagerSessionClient(
                base_url=base_url,
                username=actual_username,
                password=actual_password,
                timeout=int(timeout_seconds),
                verify_tls=bool(verify_tls),
                refresh_session_each_request=True,
            )
            payload = client.login(force=True)
            return (
                client,
                json_dumps(payload),
                summarize_for_text(payload.get("data", {})),
                True,
            )
        except Exception as exc:
            payload = {
                "success": False,
                "status_code": 0,
                "message": f"Connect/login failed: {exc}",
                "error": {"msg": str(exc), "type": exc.__class__.__name__},
                "data": {},
            }
            return (None, json_dumps(payload), str(exc), False)


class ZMongoSessionManagerListCollectionsNode:
    CATEGORY = "ZMongo/SessionManager"
    FUNCTION = "list_collections"
    RETURN_TYPES = (JSON_TYPE, JSON_TYPE, "STRING", "BOOLEAN")
    RETURN_NAMES = ("result_json", "data_json", "text_output", "success")

    @classmethod
    def INPUT_TYPES(cls):
        return {"required": {"client": (CLIENT_TYPE,)}}

    def list_collections(self, client: ZTarotManagerSessionClient):
        try:
            payload = client.list_collections()
            return _standard_output(payload, extract_primary_json_value(payload))
        except Exception as exc:
            return _exception_output(exc)


class ZMongoSessionManagerListDocsNode:
    CATEGORY = "ZMongo/SessionManager"
    FUNCTION = "list_docs"
    RETURN_TYPES = (JSON_TYPE, JSON_TYPE, "STRING", "BOOLEAN")
    RETURN_NAMES = ("result_json", "data_json", "text_output", "success")

    @classmethod
    def INPUT_TYPES(cls):
        return {
            "required": {
                "client": (CLIENT_TYPE,),
                "collection_name": ("STRING", {"default": ""}),
                "limit": ("INT", {"default": 50, "min": 1, "max": 200}),
                "skip": ("INT", {"default": 0, "min": 0, "max": 1000000}),
            }
        }

    def list_docs(self, client: ZTarotManagerSessionClient, collection_name: str, limit: int, skip: int):
        try:
            payload = client.list_docs(collection_name=collection_name, limit=int(limit), skip=int(skip))
            return _standard_output(payload, extract_primary_json_value(payload))
        except Exception as exc:
            return _exception_output(exc)


class ZMongoSessionManagerGetDocNode:
    CATEGORY = "ZMongo/SessionManager"
    FUNCTION = "get_doc"
    RETURN_TYPES = (JSON_TYPE, JSON_TYPE, "STRING", "BOOLEAN")
    RETURN_NAMES = ("result_json", "data_json", "text_output", "success")

    @classmethod
    def INPUT_TYPES(cls):
        return {
            "required": {
                "client": (CLIENT_TYPE,),
                "collection_name": ("STRING", {"default": ""}),
                "document_id": ("STRING", {"default": ""}),
            }
        }

    def get_doc(self, client: ZTarotManagerSessionClient, collection_name: str, document_id: str):
        try:
            payload = client.get_doc(collection_name=collection_name, document_id=document_id)
            return _standard_output(payload, extract_primary_json_value(payload))
        except Exception as exc:
            return _exception_output(exc)


class ZMongoSessionManagerFindDocByKeyNode:
    CATEGORY = "ZMongo/SessionManager"
    FUNCTION = "find_doc_by_key"
    RETURN_TYPES = (JSON_TYPE, JSON_TYPE, "STRING", "BOOLEAN")
    RETURN_NAMES = ("result_json", "data_json", "text_output", "success")

    @classmethod
    def INPUT_TYPES(cls):
        return {
            "required": {
                "client": (CLIENT_TYPE,),
                "collection_name": ("STRING", {"default": ""}),
                "doc_key": ("STRING", {"default": ""}),
                "limit": ("INT", {"default": 200, "min": 1, "max": 200}),
            }
        }

    def find_doc_by_key(self, client: ZTarotManagerSessionClient, collection_name: str, doc_key: str, limit: int):
        try:
            docs_payload = client.list_docs(collection_name=collection_name, limit=int(limit), skip=0)
            document = find_doc_by_key(docs_payload, doc_key.strip()) or {}
            payload = {
                "success": bool(document),
                "message": "Found" if document else "Not found",
                "status_code": docs_payload.get("status_code", 200),
                "error": None if document else {"msg": "Document not found"},
                "data": document,
                "source_result": docs_payload,
            }
            return _standard_output(payload, document)
        except Exception as exc:
            return _exception_output(exc)


class ZMongoSessionManagerCreateCollectionNode:
    CATEGORY = "ZMongo/SessionManager"
    FUNCTION = "create_collection"
    RETURN_TYPES = (JSON_TYPE, JSON_TYPE, "STRING", "BOOLEAN")
    RETURN_NAMES = ("result_json", "data_json", "text_output", "success")

    @classmethod
    def INPUT_TYPES(cls):
        return {"required": {"client": (CLIENT_TYPE,), "collection_name": ("STRING", {"default": ""})}}

    def create_collection(self, client: ZTarotManagerSessionClient, collection_name: str):
        try:
            payload = client.create_collection(collection_name=collection_name)
            return _standard_output(payload)
        except Exception as exc:
            return _exception_output(exc)


class ZMongoSessionManagerDeleteCollectionNode:
    CATEGORY = "ZMongo/SessionManager"
    FUNCTION = "delete_collection"
    RETURN_TYPES = (JSON_TYPE, JSON_TYPE, "STRING", "BOOLEAN")
    RETURN_NAMES = ("result_json", "data_json", "text_output", "success")

    @classmethod
    def INPUT_TYPES(cls):
        return {"required": {"client": (CLIENT_TYPE,), "collection_name": ("STRING", {"default": ""})}}

    def delete_collection(self, client: ZTarotManagerSessionClient, collection_name: str):
        try:
            payload = client.delete_collection(collection_name=collection_name)
            return _standard_output(payload)
        except Exception as exc:
            return _exception_output(exc)


class ZMongoSessionManagerCreateDocNode:
    CATEGORY = "ZMongo/SessionManager"
    FUNCTION = "create_doc"
    RETURN_TYPES = (JSON_TYPE, JSON_TYPE, "STRING", "BOOLEAN")
    RETURN_NAMES = ("result_json", "data_json", "text_output", "success")

    @classmethod
    def INPUT_TYPES(cls):
        return {
            "required": {
                "client": (CLIENT_TYPE,),
                "collection_name": ("STRING", {"default": ""}),
                "document_json_text": ("STRING", {"default": "{}", "multiline": True}),
            },
            "optional": {
                "document_json_input": (JSON_TYPE,),
            },
        }

    def create_doc(self, client: ZTarotManagerSessionClient, collection_name: str, document_json_text: str, document_json_input: Optional[str] = None):
        try:
            document = _parse_object_source(document_json_text, document_json_input, default={})
            payload = client.create_doc(collection_name=collection_name, document=document)
            return _standard_output(payload)
        except Exception as exc:
            return _exception_output(exc)


class ZMongoSessionManagerUpdateFieldNode:
    CATEGORY = "ZMongo/SessionManager"
    FUNCTION = "update_field"
    RETURN_TYPES = (JSON_TYPE, JSON_TYPE, "STRING", "BOOLEAN")
    RETURN_NAMES = ("result_json", "data_json", "text_output", "success")

    @classmethod
    def INPUT_TYPES(cls):
        return {
            "required": {
                "client": (CLIENT_TYPE,),
                "collection_name": ("STRING", {"default": ""}),
                "document_id": ("STRING", {"default": ""}),
                "field_path": ("STRING", {"default": ""}),
                "value_json_text": ("STRING", {"default": "\"\"", "multiline": True}),
                "parse_value_as_json": ("BOOLEAN", {"default": True}),
            },
            "optional": {
                "value_json_input": (JSON_TYPE,),
            },
        }

    def update_field(self, client: ZTarotManagerSessionClient, collection_name: str, document_id: str, field_path: str, value_json_text: str, parse_value_as_json: bool, value_json_input: Optional[str] = None):
        try:
            value = _parse_value_source(value_json_text, value_json_input, parse_value_as_json)
            payload = client.update_field(collection_name=collection_name, document_id=document_id, field_path=field_path, value=value)
            return _standard_output(payload)
        except Exception as exc:
            return _exception_output(exc)


class ZMongoSessionManagerSaveValueByDocKeyNode:
    CATEGORY = "ZMongo/SessionManager"
    FUNCTION = "save_value_by_doc_key"
    RETURN_TYPES = (JSON_TYPE, JSON_TYPE, "STRING", "BOOLEAN")
    RETURN_NAMES = ("result_json", "data_json", "text_output", "success")

    @classmethod
    def INPUT_TYPES(cls):
        return {
            "required": {
                "client": (CLIENT_TYPE,),
                "collection_name": ("STRING", {"default": ""}),
                "doc_key": ("STRING", {"default": ""}),
                "field_path": ("STRING", {"default": ""}),
                "value_json_text": ("STRING", {"default": "\"\"", "multiline": True}),
                "parse_value_as_json": ("BOOLEAN", {"default": True}),
                "upsert_if_missing": ("BOOLEAN", {"default": True}),
                "read_limit": ("INT", {"default": 200, "min": 1, "max": 200}),
            },
            "optional": {
                "value_json_input": (JSON_TYPE,),
            },
        }

    def save_value_by_doc_key(self, client: ZTarotManagerSessionClient, collection_name: str, doc_key: str, field_path: str, value_json_text: str, parse_value_as_json: bool, upsert_if_missing: bool, read_limit: int, value_json_input: Optional[str] = None):
        try:
            value = _parse_value_source(value_json_text, value_json_input, parse_value_as_json)
            save_payload = client.save_value_by_doc_key(
                collection_name=collection_name,
                doc_key=doc_key.strip(),
                field_path=field_path,
                value=value,
                upsert_if_missing=bool(upsert_if_missing),
            )
            docs_payload = client.list_docs(collection_name=collection_name, limit=int(read_limit), skip=0)
            document = find_doc_by_key(docs_payload, doc_key.strip()) or {}
            payload = {
                "success": _success_from_payload(save_payload),
                "message": save_payload.get("message", "OK"),
                "status_code": save_payload.get("status_code", 200),
                "error": save_payload.get("error"),
                "data": document,
                "save_result": save_payload,
                "lookup_result": docs_payload,
            }
            return _standard_output(payload, document)
        except Exception as exc:
            return _exception_output(exc)


class ZMongoSessionManagerSaveValueByQueryNode:
    CATEGORY = "ZMongo/SessionManager"
    FUNCTION = "save_value_by_query"
    RETURN_TYPES = (JSON_TYPE, JSON_TYPE, "STRING", "BOOLEAN")
    RETURN_NAMES = ("result_json", "data_json", "text_output", "success")

    @classmethod
    def INPUT_TYPES(cls):
        return {
            "required": {
                "client": (CLIENT_TYPE,),
                "collection_name": ("STRING", {"default": ""}),
                "query_json_text": ("STRING", {"default": "{}", "multiline": True}),
                "field_path": ("STRING", {"default": ""}),
                "value_json_text": ("STRING", {"default": "\"\"", "multiline": True}),
                "parse_value_as_json": ("BOOLEAN", {"default": True}),
                "upsert_if_missing": ("BOOLEAN", {"default": True}),
            },
            "optional": {
                "query_json_input": (JSON_TYPE,),
                "value_json_input": (JSON_TYPE,),
            },
        }

    def save_value_by_query(self, client: ZTarotManagerSessionClient, collection_name: str, query_json_text: str, field_path: str, value_json_text: str, parse_value_as_json: bool, upsert_if_missing: bool, query_json_input: Optional[str] = None, value_json_input: Optional[str] = None):
        try:
            query = _parse_object_source(query_json_text, query_json_input, default={})
            value = _parse_value_source(value_json_text, value_json_input, parse_value_as_json)
            payload = client.save_value_by_query(collection_name=collection_name, query=query, field_path=field_path, value=value, upsert_if_missing=bool(upsert_if_missing))
            return _standard_output(payload)
        except Exception as exc:
            return _exception_output(exc)


class ZMongoSessionManagerDeleteDocNode:
    CATEGORY = "ZMongo/SessionManager"
    FUNCTION = "delete_doc"
    RETURN_TYPES = (JSON_TYPE, JSON_TYPE, "STRING", "BOOLEAN")
    RETURN_NAMES = ("result_json", "data_json", "text_output", "success")

    @classmethod
    def INPUT_TYPES(cls):
        return {
            "required": {
                "client": (CLIENT_TYPE,),
                "collection_name": ("STRING", {"default": ""}),
                "document_id": ("STRING", {"default": ""}),
            }
        }

    def delete_doc(self, client: ZTarotManagerSessionClient, collection_name: str, document_id: str):
        try:
            payload = client.delete_doc(collection_name=collection_name, document_id=document_id)
            return _standard_output(payload)
        except Exception as exc:
            return _exception_output(exc)


class ZMongoSessionManagerLogoutNode:
    CATEGORY = "ZMongo/SessionManager"
    FUNCTION = "logout"
    RETURN_TYPES = (JSON_TYPE, JSON_TYPE, "STRING", "BOOLEAN")
    RETURN_NAMES = ("result_json", "data_json", "text_output", "success")

    @classmethod
    def INPUT_TYPES(cls):
        return {"required": {"client": (CLIENT_TYPE,)}}

    def logout(self, client: ZTarotManagerSessionClient):
        try:
            payload = client.logout()
            return _standard_output(payload)
        except Exception as exc:
            return _exception_output(exc)


class ZMongoJsonExtractItemsNode:
    CATEGORY = "ZMongo/JSON"
    FUNCTION = "extract_items"
    RETURN_TYPES = (JSON_TYPE, JSON_TYPE, "STRING", "BOOLEAN")
    RETURN_NAMES = ("result_json", "data_json", "text_output", "success")

    @classmethod
    def INPUT_TYPES(cls):
        return {
            "required": {
                "json_text": ("STRING", {"default": "[]", "multiline": True}),
                "mode": (["auto", "data", "documents", "collections", "results", "items", "document", "value"],),
            },
            "optional": {
                "json_input": (JSON_TYPE,),
            },
        }

    def extract_items(self, json_text: str, mode: str, json_input: Optional[str] = None):
        try:
            parsed = _parse_any_source(json_text, json_input, default=[])
            if mode == "auto":
                selected = extract_primary_json_value(parsed)
            elif mode == "data":
                selected = extract_data(parsed) if isinstance(parsed, dict) and "data" in parsed else parsed
            elif mode in {"documents", "collections", "results", "items"}:
                source = parsed.get("data", parsed) if isinstance(parsed, dict) else {}
                selected = source.get(mode, []) if isinstance(source, dict) else []
            elif mode == "document":
                source = parsed.get("data", parsed) if isinstance(parsed, dict) else {}
                selected = source.get("document", {}) if isinstance(source, dict) else {}
            else:
                selected = parsed
            payload = {"success": True, "message": "Items extracted", "status_code": 200, "error": None, "data": selected}
            return _standard_output(payload, selected)
        except Exception as exc:
            return _exception_output(exc)


class ZMongoJsonSelectItemNode:
    CATEGORY = "ZMongo/JSON"
    FUNCTION = "select_item"
    RETURN_TYPES = (JSON_TYPE, JSON_TYPE, "STRING", "BOOLEAN")
    RETURN_NAMES = ("result_json", "data_json", "text_output", "success")

    @classmethod
    def INPUT_TYPES(cls):
        return {
            "required": {
                "json_text": ("STRING", {"default": "[]", "multiline": True}),
                "selection_mode": (["index", "_id", "doc_key", "key_value"],),
                "selection_value": ("STRING", {"default": "0"}),
                "key_name": ("STRING", {"default": ""}),
            },
            "optional": {
                "json_input": (JSON_TYPE,),
            },
        }

    def select_item(self, json_text: str, selection_mode: str, selection_value: str, key_name: str, json_input: Optional[str] = None):
        try:
            parsed = _parse_any_source(json_text, json_input, default=[])
            items = extract_primary_json_value(parsed)
            if isinstance(items, dict):
                items = [items]
            if not isinstance(items, list):
                raise ValueError("Input did not contain a selectable list")

            if selection_mode == "index":
                selected = items[int(selection_value)]
            elif selection_mode == "_id":
                selected = next((item for item in items if isinstance(item, dict) and str(item.get("_id", "")) == selection_value), {})
            elif selection_mode == "doc_key":
                selected = next((item for item in items if isinstance(item, dict) and str(item.get("doc_key", "")) == selection_value), {})
            else:
                if not key_name.strip():
                    raise ValueError("key_name is required for key_value mode")
                selected = next((item for item in items if isinstance(item, dict) and str(item.get(key_name.strip(), "")) == selection_value), {})

            payload = {
                "success": bool(selected or selection_mode == "index"),
                "message": "Item selected" if selected else "No matching item",
                "status_code": 200,
                "error": None if selected else {"msg": "No matching item"},
                "data": selected if selected is not None else {},
            }
            return _standard_output(payload, payload["data"])
        except Exception as exc:
            return _exception_output(exc)


class ZMongoJsonSelectPathNode:
    CATEGORY = "ZMongo/JSON"
    FUNCTION = "select_path"
    RETURN_TYPES = (JSON_TYPE, JSON_TYPE, "STRING", "BOOLEAN")
    RETURN_NAMES = ("result_json", "data_json", "text_output", "success")

    @classmethod
    def INPUT_TYPES(cls):
        return {
            "required": {
                "json_text": ("STRING", {"default": "{}", "multiline": True}),
                "field_path": ("STRING", {"default": ""}),
            },
            "optional": {
                "json_input": (JSON_TYPE,),
            },
        }

    def select_path(self, json_text: str, field_path: str, json_input: Optional[str] = None):
        try:
            parsed = _parse_any_source(json_text, json_input, default={})
            root = extract_primary_json_value(parsed)
            value = safe_get_by_path(root, field_path.strip())
            payload = {"success": True, "message": "Path selected", "status_code": 200, "error": None, "data": value}
            return _standard_output(payload, value)
        except Exception as exc:
            return _exception_output(exc)


class ZMongoJsonListPathsNode:
    CATEGORY = "ZMongo/JSON"
    FUNCTION = "list_paths"
    RETURN_TYPES = (JSON_TYPE, JSON_TYPE, "STRING", "BOOLEAN")
    RETURN_NAMES = ("result_json", "data_json", "text_output", "success")

    @classmethod
    def INPUT_TYPES(cls):
        return {
            "required": {"json_text": ("STRING", {"default": "{}", "multiline": True})},
            "optional": {"json_input": (JSON_TYPE,)},
        }

    def list_paths(self, json_text: str, json_input: Optional[str] = None):
        try:
            parsed = _parse_any_source(json_text, json_input, default={})
            paths = extract_field_paths_from_value(parsed)
            payload = {"success": True, "message": "Paths listed", "status_code": 200, "error": None, "data": paths}
            return _standard_output(payload, paths)
        except Exception as exc:
            return _exception_output(exc)


NODE_CLASS_MAPPINGS = {
    "ZMongoSessionManagerConnectNode": ZMongoSessionManagerConnectNode,
    "ZMongoSessionManagerListCollectionsNode": ZMongoSessionManagerListCollectionsNode,
    "ZMongoSessionManagerListDocsNode": ZMongoSessionManagerListDocsNode,
    "ZMongoSessionManagerGetDocNode": ZMongoSessionManagerGetDocNode,
    "ZMongoSessionManagerFindDocByKeyNode": ZMongoSessionManagerFindDocByKeyNode,
    "ZMongoSessionManagerCreateCollectionNode": ZMongoSessionManagerCreateCollectionNode,
    "ZMongoSessionManagerDeleteCollectionNode": ZMongoSessionManagerDeleteCollectionNode,
    "ZMongoSessionManagerCreateDocNode": ZMongoSessionManagerCreateDocNode,
    "ZMongoSessionManagerUpdateFieldNode": ZMongoSessionManagerUpdateFieldNode,
    "ZMongoSessionManagerSaveValueByDocKeyNode": ZMongoSessionManagerSaveValueByDocKeyNode,
    "ZMongoSessionManagerSaveValueByQueryNode": ZMongoSessionManagerSaveValueByQueryNode,
    "ZMongoSessionManagerDeleteDocNode": ZMongoSessionManagerDeleteDocNode,
    "ZMongoSessionManagerLogoutNode": ZMongoSessionManagerLogoutNode,
    "ZMongoJsonExtractItemsNode": ZMongoJsonExtractItemsNode,
    "ZMongoJsonSelectItemNode": ZMongoJsonSelectItemNode,
    "ZMongoJsonSelectPathNode": ZMongoJsonSelectPathNode,
    "ZMongoJsonListPathsNode": ZMongoJsonListPathsNode,
}

NODE_DISPLAY_NAME_MAPPINGS = {
    "ZMongoSessionManagerConnectNode": "ZMongo Session Connect",
    "ZMongoSessionManagerListCollectionsNode": "ZMongo List Collections",
    "ZMongoSessionManagerListDocsNode": "ZMongo List Docs",
    "ZMongoSessionManagerGetDocNode": "ZMongo Get Doc",
    "ZMongoSessionManagerFindDocByKeyNode": "ZMongo Find Doc By Key",
    "ZMongoSessionManagerCreateCollectionNode": "ZMongo Create Collection",
    "ZMongoSessionManagerDeleteCollectionNode": "ZMongo Delete Collection",
    "ZMongoSessionManagerCreateDocNode": "ZMongo Create Doc",
    "ZMongoSessionManagerUpdateFieldNode": "ZMongo Update Field",
    "ZMongoSessionManagerSaveValueByDocKeyNode": "ZMongo Save Value By Doc Key",
    "ZMongoSessionManagerSaveValueByQueryNode": "ZMongo Save Value By Query",
    "ZMongoSessionManagerDeleteDocNode": "ZMongo Delete Doc",
    "ZMongoSessionManagerLogoutNode": "ZMongo Logout",
    "ZMongoJsonExtractItemsNode": "ZMongo JSON Extract Items",
    "ZMongoJsonSelectItemNode": "ZMongo JSON Select Item",
    "ZMongoJsonSelectPathNode": "ZMongo JSON Select Path",
    "ZMongoJsonListPathsNode": "ZMongo JSON List Paths",
}
