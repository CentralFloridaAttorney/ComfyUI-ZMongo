import json
import logging
from typing import Any, Dict, List, Optional, Tuple

from .zmongo_toolbag.zmongo import ZMongo

logger = logging.getLogger(__name__)


class ZMongoWorkflowNode:
    """
    A general-purpose ComfyUI node for ZMongo operations.

    Inputs are JSON strings where appropriate so the user can drive the node
    from prompt text, upstream string nodes, or saved workflow values.
    """

    CATEGORY = "ZMongo"
    FUNCTION = "execute"
    RETURN_TYPES = ("STRING", "STRING", "BOOLEAN", "STRING", "INT")
    RETURN_NAMES = ("result_json", "data_json", "success", "message", "status_code")
    OUTPUT_NODE = False

    @classmethod
    def INPUT_TYPES(cls):
        return {
            "required": {
                "mongo_uri": ("STRING", {
                    "default": "mongodb://127.0.0.1:27017",
                    "multiline": False
                }),
                "database_name": ("STRING", {
                    "default": "test",
                    "multiline": False
                }),
                "collection_name": ("STRING", {
                    "default": "documents",
                    "multiline": False
                }),
                "operation": ([
                    "ping",
                    "list_collections",
                    "find_one",
                    "find_many",
                    "count_documents",
                    "insert_one",
                    "insert_many",
                    "update_one",
                    "update_many",
                    "delete_one",
                    "delete_many",
                    "insert_or_update",
                    "save_value",
                    "drop_database",
                    "clear_cache",
                ],),
                "query_json": ("STRING", {
                    "default": "{}",
                    "multiline": True
                }),
                "data_json": ("STRING", {
                    "default": "{}",
                    "multiline": True
                }),
                "field_path": ("STRING", {
                    "default": "",
                    "multiline": False
                }),
                "limit": ("INT", {
                    "default": 100,
                    "min": 0,
                    "max": 100000
                }),
                "sort_json": ("STRING", {
                    "default": "[]",
                    "multiline": True
                }),
                "cache": ("BOOLEAN", {
                    "default": False
                }),
                "upsert": ("BOOLEAN", {
                    "default": False
                }),
                "parse_json_strings": ("BOOLEAN", {
                    "default": True
                }),
                "normalize_for_storage": ("BOOLEAN", {
                    "default": False
                }),
            }
        }

    @staticmethod
    def _safe_json_loads(value: str, fallback: Any) -> Any:
        if value is None:
            return fallback
        if not isinstance(value, str):
            return value
        stripped = value.strip()
        if not stripped:
            return fallback
        try:
            return json.loads(stripped)
        except Exception:
            return fallback

    @staticmethod
    def _as_json(value: Any) -> str:
        try:
            return json.dumps(value, default=str, ensure_ascii=False, indent=2)
        except Exception:
            return json.dumps({"error": "Failed to serialize value"}, ensure_ascii=False)

    @staticmethod
    def _normalize_sort(sort_value: Any) -> Optional[List[Tuple[str, int]]]:
        """
        Accepts:
        - [["field", 1], ["other", -1]]
        - [{"field": "name", "direction": 1}]
        - {"name": 1}   -> becomes [("name", 1)]
        """
        if sort_value in (None, "", [], {}):
            return None

        if isinstance(sort_value, dict):
            result: List[Tuple[str, int]] = []
            for key, direction in sort_value.items():
                try:
                    result.append((str(key), int(direction)))
                except Exception:
                    result.append((str(key), 1))
            return result or None

        if isinstance(sort_value, list):
            normalized: List[Tuple[str, int]] = []
            for item in sort_value:
                if isinstance(item, (list, tuple)) and len(item) >= 2:
                    normalized.append((str(item[0]), int(item[1])))
                elif isinstance(item, dict) and "field" in item:
                    normalized.append((str(item["field"]), int(item.get("direction", 1))))
            return normalized or None

        return None

    def _build_outputs(self, result) -> Tuple[str, str, bool, str, int]:
        result_dict = result.to_dict()
        data_json = self._as_json(result_dict.get("data"))
        return (
            self._as_json(result_dict),
            data_json,
            bool(result_dict.get("success", False)),
            str(result_dict.get("message", "")),
            int(result_dict.get("status_code", 500)),
        )

    def execute(
        self,
        mongo_uri: str,
        database_name: str,
        collection_name: str,
        operation: str,
        query_json: str,
        data_json: str,
        field_path: str,
        limit: int,
        sort_json: str,
        cache: bool,
        upsert: bool,
        parse_json_strings: bool,
        normalize_for_storage: bool,
    ):
        zmongo = None
        try:
            query = self._safe_json_loads(query_json, {})
            data = self._safe_json_loads(data_json, {})
            sort_value = self._safe_json_loads(sort_json, [])
            sort = self._normalize_sort(sort_value)

            zmongo = ZMongo(
                uri=mongo_uri,
                db_name=database_name,
                cache_enabled=True,
                cache_ttl_seconds=60,
                run_sync_timeout_seconds=30,
            )

            if operation == "ping":
                result = zmongo.ping()

            elif operation == "list_collections":
                result = zmongo.list_collections()

            elif operation == "find_one":
                if not isinstance(query, dict):
                    raise ValueError("find_one requires query_json to be a JSON object")
                result = zmongo.find_one(
                    collection_name,
                    query,
                    cache=cache,
                )

            elif operation == "find_many":
                if not isinstance(query, dict):
                    raise ValueError("find_many requires query_json to be a JSON object")
                result = zmongo.find_many(
                    collection_name,
                    query=query,
                    sort=sort,
                    limit=limit,
                    cache=cache,
                )

            elif operation == "count_documents":
                if not isinstance(query, dict):
                    raise ValueError("count_documents requires query_json to be a JSON object")
                result = zmongo.count_documents(
                    collection_name,
                    query=query,
                    cache=cache,
                )

            elif operation == "insert_one":
                if not isinstance(data, dict):
                    raise ValueError("insert_one requires data_json to be a JSON object")
                result = zmongo.insert_one(collection_name, data)

            elif operation == "insert_many":
                if not isinstance(data, list):
                    raise ValueError("insert_many requires data_json to be a JSON array")
                result = zmongo.insert_many(collection_name, data)

            elif operation == "update_one":
                if not isinstance(query, dict):
                    raise ValueError("update_one requires query_json to be a JSON object")
                if not isinstance(data, dict):
                    raise ValueError("update_one requires data_json to be a JSON object")
                result = zmongo.update_one(
                    collection_name,
                    query,
                    data,
                    upsert=upsert,
                )

            elif operation == "update_many":
                if not isinstance(query, dict):
                    raise ValueError("update_many requires query_json to be a JSON object")
                if not isinstance(data, dict):
                    raise ValueError("update_many requires data_json to be a JSON object")
                result = zmongo.update_many(
                    collection_name,
                    query,
                    data,
                    upsert=upsert,
                )

            elif operation == "delete_one":
                if not isinstance(query, dict):
                    raise ValueError("delete_one requires query_json to be a JSON object")
                result = zmongo.delete_one(collection_name, query)

            elif operation == "delete_many":
                if not isinstance(query, dict):
                    raise ValueError("delete_many requires query_json to be a JSON object")
                result = zmongo.delete_many(collection_name, query)

            elif operation == "insert_or_update":
                if not isinstance(query, dict):
                    raise ValueError("insert_or_update requires query_json to be a JSON object")
                if not isinstance(data, dict):
                    raise ValueError("insert_or_update requires data_json to be a JSON object")
                result = zmongo.insert_or_update(
                    collection_name,
                    query,
                    data,
                )

            elif operation == "save_value":
                save_query = query if isinstance(query, dict) else {}
                save_value = data
                result = zmongo.save_value(
                    collection_name,
                    save_value,
                    query=save_query,
                    field_path=field_path or None,
                    upsert=upsert,
                    parse_json_strings=parse_json_strings,
                    normalize_for_storage=normalize_for_storage,
                )

            elif operation == "drop_database":
                result = zmongo.drop_database(database_name)

            elif operation == "clear_cache":
                zmongo.clear_cache(collection_name if collection_name else None)
                class _SimpleResult:
                    def to_dict(self):
                        return {
                            "success": True,
                            "data": {
                                "collection": collection_name,
                                "cache_cleared": True,
                            },
                            "message": "Cache cleared",
                            "error": None,
                            "status_code": 200,
                        }
                result = _SimpleResult()

            else:
                raise ValueError(f"Unsupported operation: {operation}")

            return self._build_outputs(result)

        except Exception as exc:
            error_payload = {
                "success": False,
                "data": None,
                "message": str(exc),
                "error": {
                    "error_type": exc.__class__.__name__,
                    "error": str(exc),
                    "operation": operation,
                },
                "status_code": 500,
            }
            return (
                self._as_json(error_payload),
                "null",
                False,
                str(exc),
                500,
            )
        finally:
            if zmongo is not None:
                try:
                    zmongo.close()
                except Exception:
                    logger.exception("Failed to close ZMongo in ZMongoWorkflowNode")


NODE_CLASS_MAPPINGS = {
    "ZMongoWorkflowNode": ZMongoWorkflowNode,
}

NODE_DISPLAY_NAME_MAPPINGS = {
    "ZMongoWorkflowNode": "ZMongo Workflow",
}