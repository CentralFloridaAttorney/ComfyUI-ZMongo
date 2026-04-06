
import json
import logging
from typing import Any, Dict, List, Optional

from bson import ObjectId

from ..zmongo_toolbag.data_processor import DataProcessor
from ..zmongo_toolbag.safe_result import SafeResult
from ..zmongo_toolbag.zmongo import ZMongo

logger = logging.getLogger(__name__)


def _safe_json(value: Any) -> str:
    try:
        if isinstance(value, SafeResult):
            return value.to_json(indent=2)
        return DataProcessor.to_json(value, indent=2)
    except Exception as exc:
        return json.dumps({"error": f"Serialization failed: {exc}"}, indent=2)


def _parse_json_object(raw: str, field_name: str, default: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    text = str(raw or "").strip()
    if not text:
        return default or {}
    try:
        parsed = json.loads(text)
    except Exception as exc:
        raise ValueError(f"{field_name} is not valid JSON: {exc}") from exc
    if not isinstance(parsed, dict):
        raise ValueError(f"{field_name} must be a JSON object")
    return parsed


def _parse_value(raw: Any, *, parse_json: bool) -> Any:
    if not parse_json:
        return raw
    if isinstance(raw, str):
        stripped = raw.strip()
        if not stripped:
            return ""
        try:
            return json.loads(stripped)
        except Exception:
            return raw
    return raw


def _normalize_document_id(document_id: str) -> Any:
    raw = str(document_id or "").strip()
    if not raw:
        return ""
    return ObjectId(raw) if ObjectId.is_valid(raw) else raw


def _extract_document(result: SafeResult) -> Optional[Dict[str, Any]]:
    if not isinstance(result, SafeResult) or not result.success:
        return None

    for payload in (result.data, result.original()):
        if isinstance(payload, dict):
            doc = payload.get("document")
            if isinstance(doc, dict):
                return doc
            docs = payload.get("documents")
            if isinstance(docs, list) and docs and isinstance(docs[0], dict):
                return docs[0]
    return None


def _stringify_value(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, str):
        return value
    return _safe_json(value)


def _list_collections(zmongo: Optional[ZMongo]) -> List[str]:
    if zmongo is None:
        return []
    try:
        result = zmongo.list_collections()
        if not result.success or not isinstance(result.data, dict):
            return []
        items = result.data.get("collections", [])
        return [str(x) for x in items if str(x).strip()]
    except Exception:
        logger.exception("Failed listing collections")
        return []


def _list_fields(zmongo: Optional[ZMongo], collection_name: str) -> List[str]:
    if zmongo is None or not str(collection_name or "").strip():
        return []
    try:
        result = zmongo.find_one(collection_name, {})
        doc = _extract_document(result)
        if not isinstance(doc, dict):
            return []
        fields = DataProcessor.sorted_flattened_keys(doc)
        return [field for field in fields if field]
    except Exception:
        logger.exception("Failed listing fields")
        return []


class ZMongoStarterConnectNode:
    """
    Create a fresh ZMongo connection for starter workflows.

    This node is forced to re-execute whenever the graph runs, so downstream
    nodes always receive a refreshed connection object.
    """

    CATEGORY = "ZMongo/Starter"
    FUNCTION = "connect"
    RETURN_TYPES = ("ZMONGO_CONNECTION", "STRING", "STRING")
    RETURN_NAMES = ("zmongo", "database_name", "status_json")

    @classmethod
    def INPUT_TYPES(cls):
        return {
            "required": {
                "mongo_uri": ("STRING", {"default": "mongodb://127.0.0.1:27017"}),
                "database_name": ("STRING", {"default": "test"}),
                "default_collection_name": ("STRING", {"default": ""}),
                "cache_enabled": ("BOOLEAN", {"default": True}),
                "cache_ttl_seconds": ("INT", {"default": 5, "min": 0, "max": 86400}),
                "run_sync_timeout_seconds": ("FLOAT", {"default": 30.0, "min": 1.0, "max": 600.0}),
            }
        }

    @classmethod
    def IS_CHANGED(cls, **kwargs):
        """
        Force ComfyUI to treat this node as changed on every execution pass.
        """
        import time
        return time.time_ns()

    def connect(
        self,
        mongo_uri: str,
        database_name: str,
        default_collection_name: str,
        cache_enabled: bool,
        cache_ttl_seconds: int,
        run_sync_timeout_seconds: float,
    ):
        database_name = str(database_name or "").strip().replace(" ", "_") or "test"
        mongo_uri = str(mongo_uri or "").strip() or "mongodb://127.0.0.1:27017"
        default_collection_name = str(default_collection_name or "").strip()

        try:
            zmongo = ZMongo(
                uri=mongo_uri,
                db_name=database_name or "test",
                coll_name=default_collection_name or "documents",
                cache_enabled=cache_enabled,
                cache_ttl_seconds=cache_ttl_seconds,
                run_sync_timeout_seconds=run_sync_timeout_seconds,
            )

            # stamp friendly attrs for downstream nodes
            zmongo.db_name = database_name
            zmongo.database_name = database_name
            zmongo.mongo_uri = mongo_uri

            if default_collection_name:
                zmongo.coll_name = default_collection_name
                zmongo.collection_name = default_collection_name

            ping_res = zmongo.ping()
            payload = {
                "success": ping_res.success,
                "database_name": database_name,
                "mongo_uri": mongo_uri,
                "default_collection_name": default_collection_name,
                "ping": ping_res.data,
                "error": ping_res.error,
                "refreshed": True,
            }
            return zmongo, database_name, _safe_json(payload)

        except Exception as exc:
            failure = SafeResult.from_exception(exc, operation="connect")
            return None, database_name, failure.to_json(indent=2)


class ZMongoStarterCollectionNode:
    """
    Choose the collection by name or by index from the live database.
    """

    CATEGORY = "ZMongo/Starter"
    FUNCTION = "select_collection"
    RETURN_TYPES = ("ZMONGO_CONNECTION", "STRING", "INT", "STRING")
    RETURN_NAMES = ("zmongo", "collection_name", "collection_index", "collections_json")

    @classmethod
    def INPUT_TYPES(cls):
        return {
            "required": {
                "zmongo": ("ZMONGO_CONNECTION",),
                "collection_name": ("STRING", {"default": ""}),
                "collection_index": ("INT", {"default": -1, "min": -1}),
            }
        }

    def select_collection(self, zmongo: ZMongo, collection_name: str, collection_index: int):
        collections = _list_collections(zmongo)
        selected_name = str(collection_name or "").strip()
        selected_index = 0

        if collections:
            if collection_index >= 0:
                selected_index = max(0, min(int(collection_index), len(collections) - 1))
                selected_name = collections[selected_index]
            elif selected_name in collections:
                selected_index = collections.index(selected_name)
            elif not selected_name:
                selected_name = collections[0]
                selected_index = 0

        if zmongo is not None and selected_name:
            try:
                zmongo.coll_name = selected_name
                zmongo.collection_name = selected_name
            except Exception:
                logger.debug("Could not stamp collection name on zmongo", exc_info=True)

        return zmongo, selected_name, selected_index, _safe_json(collections)


class ZMongoStarterListRecordIdsNode:
    """
    Output all _id values for every record in the selected collection.
    """

    CATEGORY = "ZMongo/Starter"
    FUNCTION = "list_record_ids"
    RETURN_TYPES = ("STRING", "INT", "STRING")
    RETURN_NAMES = ("record_ids_json", "record_count", "numbered_record_ids_text")

    @classmethod
    def INPUT_TYPES(cls):
        return {
            "required": {
                "zmongo": ("ZMONGO_CONNECTION",),
                "collection_name": ("STRING", {"forceInput": True}),
            }
        }

    def list_record_ids(self, zmongo: ZMongo, collection_name: str):
        if zmongo is None:
            failure = SafeResult.fail("No ZMongo connection provided")
            return "[]", 0, failure.to_json(indent=2)

        try:
            collection_name = str(collection_name or "").strip()
            if not collection_name:
                raise ValueError("collection_name is required")

            result = zmongo.find_many(
                coll=collection_name,
                query={},
                limit=100000,
                cache=False,
                projection={"_id": 1},
            )

            if not result.success:
                return "[]", 0, result.to_json(indent=2)

            documents = []
            if isinstance(result.data, dict):
                documents = result.data.get("documents", []) or []

            record_ids = [
                str(doc.get("_id"))
                for doc in documents
                if isinstance(doc, dict) and doc.get("_id") is not None
            ]

            numbered_record_ids_text = (
                "\n".join(f"{idx}. {record_id}" for idx, record_id in enumerate(record_ids))
                if record_ids
                else "No record ids found."
            )

            return _safe_json(record_ids), len(record_ids), numbered_record_ids_text

        except Exception as exc:
            logger.exception("ZMongoStarterListRecordIdsNode failure")
            failure = SafeResult.from_exception(exc, operation="list_record_ids")
            return "[]", 0, failure.to_json(indent=2)

class ZMongoStarterFieldNode:
    """
    Choose the field path by name or by index from the first record in the collection.

    Priority:
    1. user-entered field_path
    2. field_index
    3. field path already stamped on the zmongo connection
    4. first available field in the collection
    """

    CATEGORY = "ZMongo/Starter"
    FUNCTION = "select_field"
    RETURN_TYPES = ("ZMONGO_CONNECTION", "STRING", "STRING", "INT", "STRING")
    RETURN_NAMES = ("zmongo", "collection_name", "field_path", "field_index", "fields_json")

    @classmethod
    def INPUT_TYPES(cls):
        return {
            "required": {
                "zmongo": ("ZMONGO_CONNECTION",),
                "collection_name": ("STRING", {"forceInput": True}),
                "field_path": ("STRING", {"default": ""}),
                "field_index": ("INT", {"default": -1, "min": -1}),
            }
        }

    def select_field(self, zmongo: ZMongo, collection_name: str, field_path: str, field_index: int):
        collection_name = str(collection_name or "").strip()
        fields = _list_fields(zmongo, collection_name)

        user_field_path = str(field_path or "").strip()
        connection_field_path = str(getattr(zmongo, "field_path", "") or "").strip()

        selected_path = ""
        selected_index = 0

        if fields:
            if user_field_path:
                selected_path = user_field_path
                selected_index = fields.index(user_field_path) if user_field_path in fields else 0
            elif field_index >= 0:
                selected_index = max(0, min(int(field_index), len(fields) - 1))
                selected_path = fields[selected_index]
            elif connection_field_path:
                selected_path = connection_field_path
                selected_index = fields.index(connection_field_path) if connection_field_path in fields else 0
            else:
                selected_path = fields[0]
                selected_index = 0
        else:
            selected_path = user_field_path or connection_field_path or ""
            selected_index = 0

        if zmongo is not None:
            try:
                setattr(zmongo, "field_path", selected_path)
            except Exception:
                pass

        return zmongo, collection_name, selected_path, selected_index, _safe_json(fields)


class ZMongoStarterGetValueNode:
    """
    Get one field value from a document selected by document_id or query_json.
    document_id wins when both are provided.
    """

    CATEGORY = "ZMongo/Starter"
    FUNCTION = "get_value"
    RETURN_TYPES = ("STRING", "STRING", "STRING", "STRING")
    RETURN_NAMES = ("value_text", "value_json", "record_json", "status_json")

    @classmethod
    def INPUT_TYPES(cls):
        return {
            "required": {
                "zmongo": ("ZMONGO_CONNECTION",),
                "collection_name": ("STRING", {"forceInput": True}),
                "field_path": ("STRING", {"forceInput": True}),
            },
            "optional": {
                "document_id": ("STRING", {"default": ""}),
                "query_json": ("STRING", {"default": "{}", "multiline": True}),
            },
        }

    def get_value(
        self,
        zmongo: ZMongo,
        collection_name: str,
        field_path: str,
        document_id: str = "",
        query_json: str = "{}",
    ):
        if zmongo is None:
            failure = SafeResult.fail("No ZMongo connection provided")
            return "", "null", "{}", failure.to_json(indent=2)

        try:
            collection_name = str(collection_name or "").strip()
            field_path = str(field_path or "").strip()
            if not collection_name:
                raise ValueError("collection_name is required")
            if not field_path:
                raise ValueError("field_path is required")

            normalized_id = _normalize_document_id(document_id)
            if normalized_id != "":
                query = {"_id": normalized_id}
            else:
                query = _parse_json_object(query_json, "query_json", default={})

            result = zmongo.find_one(coll=collection_name, query=query)
            doc = _extract_document(result)
            if doc is None:
                payload = result.to_dict()
                payload["collection_name"] = collection_name
                payload["field_path"] = field_path
                payload["query_used"] = DataProcessor.to_json_compatible(query)
                return "", "null", "{}", _safe_json(payload)

            value = DataProcessor.get_value(doc, field_path)
            payload = {
                "success": True,
                "collection_name": collection_name,
                "field_path": field_path,
                "query_used": DataProcessor.to_json_compatible(query),
                "record_id": str(doc.get("_id", "")),
                "path_exists": DataProcessor.path_exists(doc, field_path),
            }
            return _stringify_value(value), _safe_json(value), _safe_json(doc), _safe_json(payload)
        except Exception as exc:
            logger.exception("ZMongoStarterGetValueNode failure")
            failure = SafeResult.from_exception(exc, operation="get_value")
            return "", "null", "{}", failure.to_json(indent=2)


class ZMongoStarterSaveValueNode:
    """
    Save one value to a field.

    Behavior:
    - document_id wins over query_json
    - if record_index == -1, a new record is created
    - if field_path is blank and the value is an object, the object is applied to the document root
    """

    CATEGORY = "ZMongo/Starter"
    FUNCTION = "save_value"
    RETURN_TYPES = ("STRING", "BOOLEAN")
    RETURN_NAMES = ("result_json", "success")
    OUTPUT_NODE = True

    @classmethod
    def INPUT_TYPES(cls):
        return {
            "required": {
                "zmongo": ("ZMONGO_CONNECTION",),
                "collection_name": ("STRING", {"forceInput": True}),
                "field_path": ("STRING", {"default": ""}),
                "record_index": ("INT", {"default": 0, "min": -1}),
                "value_to_save": ("STRING", {"default": "", "multiline": True}),
                "parse_value_as_json": ("BOOLEAN", {"default": True}),
                "upsert_if_missing": ("BOOLEAN", {"default": True}),
            },
            "optional": {
                "document_id": ("STRING", {"default": ""}),
                "query_json": ("STRING", {"default": "{}", "multiline": True}),
            },
        }

    def save_value(
        self,
        zmongo: ZMongo,
        collection_name: str,
        field_path: str,
        record_index: int,
        value_to_save: str,
        parse_value_as_json: bool,
        upsert_if_missing: bool,
        document_id: str = "",
        query_json: str = "{}",
    ):
        if zmongo is None:
            failure = SafeResult.fail("No ZMongo connection provided")
            return failure.to_json(indent=2), False

        try:
            collection_name = str(collection_name or "").strip()
            field_path = str(field_path or "").strip()

            if not collection_name:
                raise ValueError("collection_name is required")

            parsed_value = _parse_value(value_to_save, parse_json=parse_value_as_json)

            # If record_index == -1, force creation of a new record by using no query.
            if int(record_index) == -1:
                query = {}
                effective_upsert = True
            else:
                normalized_id = _normalize_document_id(document_id)
                if normalized_id != "":
                    query = {"_id": normalized_id}
                else:
                    query = _parse_json_object(query_json, "query_json", default={})
                effective_upsert = upsert_if_missing

            result = zmongo.save_value(
                coll=collection_name,
                value=parsed_value,
                query=query,
                field_path=field_path or None,
                upsert=effective_upsert,
                parse_json_strings=False,
                normalize_for_storage=False,
            )

            payload = result.to_dict()
            payload["collection_name"] = collection_name
            payload["field_path"] = field_path
            payload["record_index"] = int(record_index)
            payload["created_new_record"] = int(record_index) == -1
            payload["query_used"] = DataProcessor.to_json_compatible(query)
            payload["saved_value"] = DataProcessor.to_json_compatible(parsed_value)

            return _safe_json(payload), bool(result.success)

        except Exception as exc:
            logger.exception("ZMongoStarterSaveValueNode failure")
            failure = SafeResult.from_exception(exc, operation="save_value")
            return failure.to_json(indent=2), False


class ZMongoStarterRecordSelectorNode:
    """
    Select a record _id from the active collection and also return the full record JSON.

    Priority:
    1. user-entered record_id_input
    2. record_index
    3. first available record _id
    """

    CATEGORY = "ZMongo/Starter"
    FUNCTION = "select_record"
    RETURN_TYPES = ("ZMONGO_CONNECTION", "STRING", "STRING", "INT", "STRING", "STRING")
    RETURN_NAMES = ("zmongo", "collection_name", "record_id", "record_index", "record_ids_json", "record_json")

    @classmethod
    def INPUT_TYPES(cls):
        return {
            "required": {
                "zmongo": ("ZMONGO_CONNECTION",),
                "collection_name": ("STRING", {"forceInput": True}),
                "record_index": ("INT", {"default": 0, "min": 0}),
                "record_id_input": ("STRING", {"default": ""}),
            }
        }

    def select_record(
        self,
        zmongo: ZMongo,
        collection_name: str,
        record_index: int,
        record_id_input: str,
    ):
        collection_name = str(collection_name or "").strip()
        user_record_id = str(record_id_input or "").strip()

        if zmongo is None or not collection_name:
            return None, collection_name, user_record_id, 0, "[]", "{}"

        record_ids = []
        selected_record_id = ""
        selected_index = 0
        record_json = "{}"

        try:
            result = zmongo.find_many(
                coll=collection_name,
                query={},
                limit=100000,
                cache=False,
                projection={"_id": 1},
            )

            if result.success and isinstance(result.data, dict):
                documents = result.data.get("documents", []) or []
                record_ids = [
                    str(doc.get("_id"))
                    for doc in documents
                    if isinstance(doc, dict) and doc.get("_id") is not None
                ]

            if record_ids:
                if user_record_id:
                    selected_record_id = user_record_id
                    selected_index = record_ids.index(user_record_id) if user_record_id in record_ids else 0
                else:
                    selected_index = max(0, min(int(record_index), len(record_ids) - 1))
                    selected_record_id = record_ids[selected_index]
            else:
                selected_record_id = user_record_id
                selected_index = 0

            if selected_record_id:
                query_id = ObjectId(selected_record_id) if ObjectId.is_valid(selected_record_id) else selected_record_id
                selected_result = zmongo.find_one(
                    coll=collection_name,
                    query={"_id": query_id},
                    cache=False,
                )

                if selected_result.success and isinstance(selected_result.data, dict):
                    document = selected_result.data.get("document")
                    if isinstance(document, dict):
                        record_json = _safe_json(document)

            if zmongo is not None:
                try:
                    setattr(zmongo, "record_id", selected_record_id)
                except Exception:
                    pass

            return (
                zmongo,
                collection_name,
                selected_record_id,
                selected_index,
                _safe_json(record_ids),
                record_json,
            )

        except Exception as exc:
            failure = SafeResult.from_exception(exc, operation="select_record")
            return (
                zmongo,
                collection_name,
                user_record_id,
                0,
                "[]",
                failure.to_json(indent=2),
            )

NODE_CLASS_MAPPINGS = {
    "ZMongoStarterConnectNode": ZMongoStarterConnectNode,
    "ZMongoStarterCollectionNode": ZMongoStarterCollectionNode,
    "ZMongoStarterFieldNode": ZMongoStarterFieldNode,
    "ZMongoStarterGetValueNode": ZMongoStarterGetValueNode,
    "ZMongoStarterSaveValueNode": ZMongoStarterSaveValueNode,
    "ZMongoStarterRecordSelectorNode": ZMongoStarterRecordSelectorNode,
    "ZMongoStarterListRecordIdsNode": ZMongoStarterListRecordIdsNode,
}

NODE_DISPLAY_NAME_MAPPINGS = {
    "ZMongoStarterConnectNode": "ZMongo Starter Connect",
    "ZMongoStarterCollectionNode": "ZMongo Starter Collection",
    "ZMongoStarterFieldNode": "ZMongo Starter Field",
    "ZMongoStarterGetValueNode": "ZMongo Starter Get Value",
    "ZMongoStarterSaveValueNode": "ZMongo Starter Save Value",
    "ZMongoStarterRecordSelectorNode": "ZMongo Starter Record Selector",
    "ZMongoStarterListRecordIdsNode": "ZMongo Starter List Record Ids",
}
