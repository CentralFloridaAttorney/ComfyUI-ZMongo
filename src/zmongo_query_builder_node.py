import json
import logging
from typing import Any, Dict, List, Optional, Set

from .zmongo_toolbag.data_processor import DataProcessor

logger = logging.getLogger(__name__)


class ZMongoQueryBuilderNode:
    """
    Easy query-maker node for ComfyUI.

    Purpose:
    - lets the user build Mongo queries without hand-writing JSON
    - populates a selectable field list from sample_data_json
    - accepts an optional runtime data_json input from an upstream node
    - falls back to default_value or default_query_json when query_value is blank

    Practical note:
    - the ComfyUI frontend can only populate the dropdown from widget text that
      exists in the editor, so `sample_data_json` is used for the dropdown UI
    - at execution time, if `data_json` is connected, that runtime value is used
      instead of `sample_data_json`
    """

    CATEGORY = "ZMongo"
    FUNCTION = "build_query"
    RETURN_TYPES = ("STRING", "STRING", "STRING", "STRING", "BOOLEAN")
    RETURN_NAMES = (
        "query_json",
        "selected_field",
        "effective_value_json",
        "field_options_json",
        "used_default",
    )

    @classmethod
    def INPUT_TYPES(cls):
        return {
            "required": {
                "sample_data_json": (
                    "STRING",
                    {
                        "default": "{}",
                        "multiline": True,
                    },
                ),
                "selected_field": (
                    "STRING",
                    {
                        "default": "",
                        "multiline": False,
                    },
                ),
                "operator": (
                    [
                        "eq",
                        "ne",
                        "gt",
                        "gte",
                        "lt",
                        "lte",
                        "in",
                        "nin",
                        "regex",
                        "exists",
                    ],
                ),
                "query_value": (
                    "STRING",
                    {
                        "default": "",
                        "multiline": False,
                    },
                ),
                "default_value": (
                    "STRING",
                    {
                        "default": "",
                        "multiline": False,
                    },
                ),
                "default_query_json": (
                    "STRING",
                    {
                        "default": "{}",
                        "multiline": True,
                    },
                ),
                "auto_exists_when_blank": (
                    "BOOLEAN",
                    {
                        "default": True,
                    },
                ),
            },
            "optional": {
                "data_json": (
                    "STRING",
                    {
                        "forceInput": True,
                    },
                ),
            },
        }

    @staticmethod
    def _safe_json_loads(value: Any, fallback: Any = None) -> Any:
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
    def _to_json(value: Any) -> str:
        try:
            return json.dumps(
                DataProcessor.to_json_compatible(value),
                ensure_ascii=False,
                indent=2,
            )
        except Exception as exc:
            return json.dumps({"error": str(exc)}, ensure_ascii=False, indent=2)

    @staticmethod
    def _parse_value_text(value: str) -> Any:
        if value is None:
            return None
        stripped = value.strip()
        if stripped == "":
            return None

        try:
            return json.loads(stripped)
        except Exception:
            lowered = stripped.lower()
            if lowered == "true":
                return True
            if lowered == "false":
                return False
            if lowered == "null":
                return None
            return stripped

    @staticmethod
    def _extract_query_source(payload: Any) -> Any:
        """
        Accepts:
        - raw document dict
        - list of docs
        - ZMongo find_one data_json: {"document": {...}, ...}
        - ZMongo find_many data_json: {"documents": [...], ...}
        - full SafeResult-shaped payloads: {"data": ...}
        """
        if isinstance(payload, dict):
            if "data" in payload and len(payload) <= 6:
                return ZMongoQueryBuilderNode._extract_query_source(payload["data"])
            if "document" in payload:
                return payload["document"]
            if "documents" in payload:
                return payload["documents"]
        return payload

    @staticmethod
    def _flatten_union_keys(data: Any) -> List[str]:
        keys: Set[str] = set()

        def collect(obj: Any):
            if isinstance(obj, dict):
                flat = DataProcessor.flatten_json(obj)
                keys.update(flat.keys())
                return

            if isinstance(obj, list):
                for item in obj[:50]:
                    if isinstance(item, dict):
                        flat = DataProcessor.flatten_json(item)
                        keys.update(flat.keys())

        collect(data)
        return sorted(keys)

    @staticmethod
    def _build_query(field: str, operator: str, value: Any, auto_exists_when_blank: bool) -> Dict[str, Any]:
        if not field:
            return {}

        if value is None or value == "":
            if operator == "exists" or auto_exists_when_blank:
                return {field: {"$exists": True}}
            return {}

        if operator == "eq":
            return {field: value}
        if operator == "ne":
            return {field: {"$ne": value}}
        if operator == "gt":
            return {field: {"$gt": value}}
        if operator == "gte":
            return {field: {"$gte": value}}
        if operator == "lt":
            return {field: {"$lt": value}}
        if operator == "lte":
            return {field: {"$lte": value}}
        if operator == "in":
            return {field: {"$in": value if isinstance(value, list) else [value]}}
        if operator == "nin":
            return {field: {"$nin": value if isinstance(value, list) else [value]}}
        if operator == "regex":
            return {field: {"$regex": str(value)}}
        if operator == "exists":
            return {field: {"$exists": bool(value) if isinstance(value, bool) else True}}

        return {field: value}

    def build_query(
        self,
        sample_data_json: str,
        selected_field: str,
        operator: str,
        query_value: str,
        default_value: str,
        default_query_json: str,
        auto_exists_when_blank: bool,
        data_json: Optional[str] = None,
    ):
        try:
            runtime_payload = self._safe_json_loads(data_json, None)
            sample_payload = self._safe_json_loads(sample_data_json, {})
            source_payload = runtime_payload if runtime_payload is not None else sample_payload

            extracted_source = self._extract_query_source(source_payload)
            field_options = self._flatten_union_keys(extracted_source)

            selected_field = (selected_field or "").strip()

            # Determine effective value
            used_default = False
            effective_value = self._parse_value_text(query_value)

            if effective_value is None and (query_value or "").strip() == "":
                default_parsed = self._parse_value_text(default_value)
                if default_parsed is not None or (default_value or "").strip() != "":
                    effective_value = default_parsed
                    used_default = True

            # If still no value, default_query_json may take over
            parsed_default_query = self._safe_json_loads(default_query_json, {})
            if not isinstance(parsed_default_query, dict):
                parsed_default_query = {}

            if not selected_field:
                final_query = parsed_default_query if parsed_default_query else {}
                return (
                    self._to_json(final_query),
                    "",
                    self._to_json(effective_value),
                    self._to_json(field_options),
                    used_default,
                )

            if effective_value is None and (query_value or "").strip() == "" and (default_value or "").strip() == "":
                if parsed_default_query:
                    final_query = parsed_default_query
                else:
                    final_query = self._build_query(
                        selected_field,
                        operator,
                        None,
                        auto_exists_when_blank,
                    )
            else:
                final_query = self._build_query(
                    selected_field,
                    operator,
                    effective_value,
                    auto_exists_when_blank,
                )

            return (
                self._to_json(final_query),
                selected_field,
                self._to_json(effective_value),
                self._to_json(field_options),
                used_default,
            )

        except Exception as exc:
            logger.exception("ZMongoQueryBuilderNode failed")
            error_payload = {"error": str(exc)}
            return (
                self._to_json(error_payload),
                "",
                "null",
                "[]",
                False,
            )


NODE_CLASS_MAPPINGS = {
    "ZMongoQueryBuilderNode": ZMongoQueryBuilderNode,
}

NODE_DISPLAY_NAME_MAPPINGS = {
    "ZMongoQueryBuilderNode": "ZMongo Query Builder",
}