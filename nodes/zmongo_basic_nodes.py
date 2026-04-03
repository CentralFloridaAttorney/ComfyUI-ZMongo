import json
import logging
from typing import Any, Dict, List, Optional

from bson import ObjectId

from ..zmongo_toolbag.data_processor import DataProcessor
from ..zmongo_toolbag.safe_result import SafeResult
from ..zmongo_toolbag.zmongo import ZMongo

logger = logging.getLogger(__name__)


def _safe_json(obj: Any) -> str:
    try:
        if isinstance(obj, SafeResult):
            return obj.to_json(indent=2)
        return DataProcessor.to_json(obj, indent=2)
    except Exception as exc:
        return json.dumps({"error": f"Serialization failed: {exc}"}, indent=2)


def _parse_json_object(raw: str, field_name: str, default: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    text = (raw or "").strip()
    if not text:
        return default or {}

    try:
        parsed = json.loads(text)
    except Exception as exc:
        raise ValueError(f"{field_name} is not valid JSON: {exc}") from exc

    if not isinstance(parsed, dict):
        raise ValueError(f"{field_name} must be a JSON object")

    return parsed


def _parse_scalar_or_json(raw: Any, *, parse_json: bool) -> Any:
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


def _extract_document_from_result(result: SafeResult) -> Optional[Dict[str, Any]]:
    if not isinstance(result, SafeResult) or not result.success:
        return None

    data = result.data
    if isinstance(data, dict):
        document = data.get("document")
        if isinstance(document, dict):
            return document

        documents = data.get("documents")
        if isinstance(documents, list) and documents and isinstance(documents[0], dict):
            return documents[0]

    original = result.original()
    if isinstance(original, dict):
        document = original.get("document")
        if isinstance(document, dict):
            return document

        documents = original.get("documents")
        if isinstance(documents, list) and documents and isinstance(documents[0], dict):
            return documents[0]

    return None


def _normalize_document_id(document_id: str) -> Any:
    raw = str(document_id or "").strip()
    if not raw:
        return ""
    return ObjectId(raw) if ObjectId.is_valid(raw) else raw


# Global registry to manage multiple active database connections
# Keyed by "mongo_uri|database_name" to ensure instance isolation
ZMONGO_REGISTRY = {}


class ZMongoConnectNode:
    """
    Create a ZMongo connection object for downstream nodes and registers
    the instance for UI-side schema discovery.
    """

    CATEGORY = "ZMongo/Simple"
    FUNCTION = "connect"
    RETURN_TYPES = ("ZMONGO_CONNECTION", "STRING")
    RETURN_NAMES = ("zmongo", "status_json")

    @classmethod
    def INPUT_TYPES(cls):
        return {
            "required": {
                "mongo_uri": ("STRING", {"default": "mongodb://127.0.0.1:27017"}),
                "database_name": ("STRING", {"default": "test"}),
                "cache_enabled": ("BOOLEAN", {"default": True}),
                "cache_ttl_seconds": ("INT", {"default": 5, "min": 0, "max": 86400}),
                "run_sync_timeout_seconds": ("FLOAT", {"default": 30.0, "min": 1.0, "max": 600.0}),
            }
        }

    @classmethod
    def IS_CHANGED(
            cls,
            mongo_uri: str,
            database_name: str,
            cache_enabled: bool,
            cache_ttl_seconds: int,
            run_sync_timeout_seconds: float,
    ):
        return float("NaN")

    def connect(
            self,
            mongo_uri: str,
            database_name: str,
            cache_enabled: bool,
            cache_ttl_seconds: int,
            run_sync_timeout_seconds: float,
    ):
        database_name = str(database_name or "").strip()
        mongo_uri = str(mongo_uri or "").strip()

        if not mongo_uri:
            mongo_uri = "mongodb://127.0.0.1:27017"
        if not database_name:
            database_name = "test"

        instance_key = f"{mongo_uri}|{database_name}"

        try:
            zmongo = ZMongo(
                uri=mongo_uri,
                db_name=database_name,
                cache_enabled=cache_enabled,
                cache_ttl_seconds=cache_ttl_seconds,
                run_sync_timeout_seconds=run_sync_timeout_seconds,
            )

            # Stamp canonical connection metadata onto the object so every downstream
            # node can reliably discover the chosen database and connection settings.
            stamped_values = {
                "uri": mongo_uri,
                "mongo_uri": mongo_uri,
                "_uri": mongo_uri,
                "_mongo_uri": mongo_uri,
                "db_name": database_name,
                "database_name": database_name,
                "_db_name": database_name,
                "_database_name": database_name,
                "cache_enabled": bool(cache_enabled),
                "_cache_enabled": bool(cache_enabled),
                "cache_ttl_seconds": int(cache_ttl_seconds),
                "_cache_ttl_seconds": int(cache_ttl_seconds),
                "run_sync_timeout_seconds": float(run_sync_timeout_seconds),
                "_run_sync_timeout_seconds": float(run_sync_timeout_seconds),
            }

            for attr_name, attr_value in stamped_values.items():
                try:
                    setattr(zmongo, attr_name, attr_value)
                except Exception:
                    logger.debug("ZMongoConnectNode: could not set %s on returned zmongo", attr_name)

            ping_res = zmongo.ping()

            if ping_res.success:
                ZMONGO_REGISTRY[instance_key] = zmongo
                logger.info("ZMongo: Registered connection for %s", instance_key)

            status_payload = {
                "success": ping_res.success,
                "message": ping_res.message,
                "error": ping_res.error,
                "database": database_name,
                "db_name": database_name,
                "database_name": database_name,
                "mongo_uri": mongo_uri,
                "uri": mongo_uri,
                "ping": ping_res.data,
            }

            status_json = DataProcessor.to_json(status_payload, indent=2)
            return (zmongo, status_json)

        except Exception as exc:
            logger.exception("ZMongoConnectNode failure for %s", instance_key)
            error_payload = {
                "success": False,
                "error": str(exc),
                "operation": "connect",
                "database": database_name,
                "db_name": database_name,
                "database_name": database_name,
                "mongo_uri": mongo_uri,
                "uri": mongo_uri,
            }
            return (None, json.dumps(error_payload, indent=2))


class ZMongoListCollectionsNode:
    """List collections from an active ZMongo connection."""

    CATEGORY = "ZMongo/Simple"
    FUNCTION = "list_collections"
    RETURN_TYPES = ("STRING", "STRING", "INT")
    RETURN_NAMES = ("collections_json", "first_collection", "count")

    @classmethod
    def INPUT_TYPES(cls):
        return {"required": {"zmongo": ("ZMONGO_CONNECTION",)}}

    def list_collections(self, zmongo: ZMongo):
        if zmongo is None:
            return (_safe_json([]), "", 0)

        result = zmongo.list_collections()

        if not result.success:
            return (_safe_json(result.to_dict()), "", 0)

        data = result.data or {}
        collections = data.get("collections", []) if isinstance(data, dict) else []
        collections = [str(name) for name in collections if str(name).strip()]
        first = collections[0] if collections else ""
        return (_safe_json(collections), first, len(collections))


class ZMongoLoadRecordNode:
    """
    Load one record by document_id or query_json.

    If both are provided, document_id takes priority.
    """

    CATEGORY = "ZMongo/Simple"
    FUNCTION = "load_record"
    RETURN_TYPES = ("STRING", "STRING", "STRING")
    RETURN_NAMES = ("record_json", "record_id", "status_json")

    @classmethod
    def INPUT_TYPES(cls):
        return {
            "required": {
                "zmongo": ("ZMONGO_CONNECTION",),
                "collection_name": ("STRING", {"default": ""}),
            },
            "optional": {
                "document_id": ("STRING", {"default": ""}),
                "query_json": ("STRING", {"default": "{}", "multiline": True}),
            },
        }

    def load_record(
            self,
            zmongo: ZMongo,
            collection_name: str,
            document_id: str = "",
            query_json: str = "{}",
    ):
        if zmongo is None:
            failure = SafeResult.fail("No ZMongo connection provided")
            return ("{}", "", failure.to_json(indent=2))

        try:
            collection_name = str(collection_name or "").strip()
            if not collection_name:
                raise ValueError("collection_name is required")

            normalized_id = _normalize_document_id(document_id)
            if normalized_id != "":
                query = {"_id": normalized_id}
            else:
                query = _parse_json_object(query_json, "query_json", default={})

            result = zmongo.find_one(
                collection_name=collection_name,
                query=query,
            )
            document = _extract_document_from_result(result)

            if not result.success or document is None:
                status_payload = result.to_dict()
                status_payload["query_used"] = DataProcessor.to_json_compatible(query)
                status_payload["collection_name"] = collection_name
                return ("{}", "", _safe_json(status_payload))

            record_id = str(document.get("_id", ""))
            return (
                _safe_json(document),
                record_id,
                _safe_json(
                    {
                        "success": True,
                        "collection_name": collection_name,
                        "query_used": DataProcessor.to_json_compatible(query),
                        "record_id": record_id,
                    }
                ),
            )
        except Exception as exc:
            logger.exception("ZMongoLoadRecordNode failure")
            failure = SafeResult.from_exception(exc, operation="load_record")
            return ("{}", "", failure.to_json(indent=2))


class ZMongoPickFieldNode:
    """
    Read a value from a loaded record using canonical dot-path syntax.

    Added:
    - numbered_field_names_text: human-readable indexed list of flattened fields
    - field_count: number of available flattened fields
    """

    CATEGORY = "ZMongo/Simple"
    FUNCTION = "pick_field"
    RETURN_TYPES = ("STRING", "STRING", "STRING", "INT")
    RETURN_NAMES = (
        "field_value",
        "available_paths_json",
        "numbered_field_names_text",
        "field_count",
    )

    @classmethod
    def INPUT_TYPES(cls):
        return {
            "required": {
                "record_json": ("STRING", {"forceInput": True}),
                "field_path": ("STRING", {"default": "text"}),
            }
        }

    @staticmethod
    def _safe_json(value):
        try:
            return DataProcessor.to_json(value, indent=2)
        except Exception as exc:
            return json.dumps({"error": f"Serialization failed: {exc}"}, indent=2)

    @staticmethod
    def _stringify_value(value):
        if value is None:
            return ""
        if isinstance(value, str):
            return value
        try:
            return DataProcessor.to_json(value, indent=2)
        except Exception:
            return str(value)

    @staticmethod
    def _build_numbered_field_names_text(paths):
        if not paths:
            return "No flattened fields found."
        return "\n".join(f"{idx}. {path}" for idx, path in enumerate(paths))

    def pick_field(self, record_json, field_path):
        try:
            record = json.loads(record_json)
            if not isinstance(record, dict):
                raise ValueError("record_json must decode to a JSON object")

            flattened = DataProcessor.flatten_json(record)
            available_paths = sorted(str(k) for k in flattened.keys())
            numbered_field_names_text = self._build_numbered_field_names_text(available_paths)

            selected_path = str(field_path or "").strip()
            if not selected_path and available_paths:
                selected_path = available_paths[0]

            value = DataProcessor.get_value(record, selected_path)

            if value is None and available_paths and selected_path not in available_paths:
                selected_path = available_paths[0]
                value = DataProcessor.get_value(record, selected_path)

            field_value = self._stringify_value(value)

            return (
                field_value,
                self._safe_json(available_paths),
                numbered_field_names_text,
                len(available_paths),
            )

        except Exception as exc:
            logger.exception("ZMongoPickFieldNode failure")
            failure = SafeResult.from_exception(exc, operation="pick_field")
            return (
                failure.message,
                self._safe_json([]),
                "No flattened fields found.",
                0,
            )


class ZMongoQueryBuilderNode:
    """Build a simple MongoDB query object without hand-writing raw JSON."""

    OPERATORS = [
        "$eq",
        "$ne",
        "$gt",
        "$gte",
        "$lt",
        "$lte",
        "$in",
        "$nin",
        "$regex",
    ]

    CATEGORY = "ZMongo/Simple"
    FUNCTION = "build_query"
    RETURN_TYPES = ("STRING", "STRING")
    RETURN_NAMES = ("query_json", "summary")

    @classmethod
    def INPUT_TYPES(cls):
        return {
            "required": {
                "field_name": ("STRING", {"default": "_id"}),
                "operator": (cls.OPERATORS, {"default": "$eq"}),
                "value": ("STRING", {"default": "", "multiline": True}),
                "parse_value_as_json": ("BOOLEAN", {"default": True}),
            }
        }

    def build_query(
        self,
        field_name: str,
        operator: str,
        value: str,
        parse_value_as_json: bool,
    ):
        try:
            field_name = str(field_name or "").strip()
            if not field_name:
                raise ValueError("field_name is required")

            parsed_value = _parse_scalar_or_json(value, parse_json=parse_value_as_json)
            if operator == "$eq":
                query = {field_name: parsed_value}
            else:
                query = {field_name: {operator: parsed_value}}

            summary = f"{field_name} {operator} {parsed_value}"
            return (_safe_json(query), summary)
        except Exception as exc:
            logger.exception("ZMongoQueryBuilderNode failure")
            failure = SafeResult.from_exception(exc, operation="build_query")
            return (failure.to_json(indent=2), failure.message)


class ZMongoSaveValueNode:
    """Save one value back into MongoDB using a query and optional dot-path."""

    CATEGORY = "ZMongo/Simple"
    FUNCTION = "save_value"
    RETURN_TYPES = ("STRING", "BOOLEAN")
    RETURN_NAMES = ("result_json", "success")
    OUTPUT_NODE = True

    @classmethod
    def INPUT_TYPES(cls):
        return {
            "required": {
                "zmongo": ("ZMONGO_CONNECTION",),
                "collection_name": ("STRING", {"default": ""}),
                "value_to_save": ("STRING", {"default": "", "multiline": True}),
                "parse_value_as_json": ("BOOLEAN", {"default": True}),
                "upsert_if_missing": ("BOOLEAN", {"default": True}),
            },
            "optional": {
                "document_id": ("STRING", {"default": ""}),
                "query_json": ("STRING", {"default": "{}", "multiline": True}),
                "field_path": ("STRING", {"default": ""}),
            },
        }

    def save_value(
            self,
            zmongo: ZMongo,
            collection_name: str,
            value_to_save: str,
            parse_value_as_json: bool,
            upsert_if_missing: bool,
            document_id: str = "",
            query_json: str = "{}",
            field_path: str = "",
    ):
        if zmongo is None:
            failure = SafeResult.fail("No ZMongo connection provided")
            return (failure.to_json(indent=2), False)

        try:
            collection_name = str(collection_name or "").strip()
            if not collection_name:
                raise ValueError("collection_name is required")

            normalized_id = _normalize_document_id(document_id)
            if normalized_id != "":
                query = {"_id": normalized_id}
            else:
                query = _parse_json_object(query_json, "query_json", default={})

            parsed_value = _parse_scalar_or_json(value_to_save, parse_json=parse_value_as_json)

            result = zmongo.save_value(
                coll=collection_name,
                value=parsed_value,
                query=query,
                field_path=str(field_path or "").strip() or None,
                upsert=upsert_if_missing,
                parse_json_strings=False,
                normalize_for_storage=False,
            )

            payload = result.to_dict()
            payload["collection_name"] = collection_name
            payload["query_used"] = DataProcessor.to_json_compatible(query)
            payload["field_path"] = str(field_path or "").strip()
            return (_safe_json(payload), bool(result.success))

        except Exception as exc:
            logger.exception("ZMongoSaveValueNode failure")
            failure = SafeResult.from_exception(exc, operation="save_value")
            return (failure.to_json(indent=2), False)


NODE_CLASS_MAPPINGS = {
    "ZMongoConnectNode": ZMongoConnectNode,
    "ZMongoListCollectionsNode": ZMongoListCollectionsNode,
    "ZMongoLoadRecordNode": ZMongoLoadRecordNode,
    "ZMongoPickFieldNode": ZMongoPickFieldNode,
    "ZMongoQueryBuilderNode": ZMongoQueryBuilderNode,
    "ZMongoSaveValueNode": ZMongoSaveValueNode,
}

NODE_DISPLAY_NAME_MAPPINGS = {
    "ZMongoConnectNode": "ZMongo Connect",
    "ZMongoListCollectionsNode": "ZMongo List Collections",
    "ZMongoLoadRecordNode": "ZMongo Load Record",
    "ZMongoPickFieldNode": "ZMongo Pick Field",
    "ZMongoQueryBuilderNode": "ZMongo Query Builder",
    "ZMongoSaveValueNode": "ZMongo Save Value",
}
