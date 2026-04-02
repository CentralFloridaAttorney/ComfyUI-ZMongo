import json
import logging
import re
from typing import Any, Dict, List, Tuple

from .zmongo_toolbag.zmongo import ZMongo
from .zmongo_toolbag.data_processor import DataProcessor

logger = logging.getLogger(__name__)


class ZMongoTabularRecordViewNode:
    CATEGORY = "ZMongo"
    FUNCTION = "select_record"
    RETURN_TYPES = ("STRING", "STRING", "STRING", "INT", "STRING", "STRING")
    RETURN_NAMES = (
        "selected_record_json",
        "selected_record_id",
        "selected_field_name",
        "selected_record_index",
        "selected_field_value",
        "flattened_headings_json",
    )

    @staticmethod
    @staticmethod
    def _value_to_searchable_string(value: Any) -> str:
        if value is None:
            return ""
        if isinstance(value, str):
            return value
        if isinstance(value, (bool, int, float)):
            return str(value)
        try:
            return json.dumps(
                DataProcessor.to_json_compatible(value),
                ensure_ascii=False,
                sort_keys=True,
                default=str,
            )
        except Exception:
            return str(value)

    @staticmethod
    def _wildcard_to_regex(pattern: str) -> str:
        """
        Convert a simple wildcard pattern to regex.
        Supported:
          *  => any characters
          ?  => any single character
        """
        escaped = re.escape(pattern)
        escaped = escaped.replace(r"\*", ".*")
        escaped = escaped.replace(r"\?", ".")
        return f"^{escaped}$"

    @classmethod
    def _matches_search_text(cls, value: Any, search_text: str) -> bool:
        """
        Matching rules:
        - empty search_text => match everything
        - if search_text contains * or ?, use wildcard matching
        - otherwise do case-insensitive substring matching
        """
        haystack = cls._value_to_searchable_string(value)
        needle = str(search_text or "")

        if not needle:
            return True

        haystack_lower = haystack.lower()
        needle_lower = needle.lower()

        if "*" in needle or "?" in needle:
            regex = cls._wildcard_to_regex(needle_lower)
            return re.search(regex, haystack_lower) is not None

        return needle_lower in haystack_lower

    @classmethod
    def get_filtered_table_payload(
            cls,
            collection_name: str,
            search_text: str = "",
            flattened_field_name: str = "",
    ) -> Tuple[List[str], List[Dict[str, Any]], List[str]]:
        records = cls.get_all_records(collection_name)
        search_text = str(search_text or "").strip()
        flattened_field_name = str(flattened_field_name or "").strip()

        headings: List[str] = []
        seen = set()
        flat_records: List[Dict[str, Any]] = []
        record_ids: List[str] = []

        for record in records:
            flat = DataProcessor.flatten_json(record)
            normalized_flat = {
                str(k): DataProcessor.to_json_compatible(v)
                for k, v in flat.items()
            }

            if search_text:
                if flattened_field_name:
                    target_value = normalized_flat.get(flattened_field_name)
                    matched = cls._matches_search_text(target_value, search_text)
                else:
                    matched = any(
                        cls._matches_search_text(v, search_text)
                        for v in normalized_flat.values()
                    )

                if not matched:
                    continue

            flat_records.append(normalized_flat)

            record_id = str(record.get("_id", ""))
            record_ids.append(record_id)

            for key in normalized_flat.keys():
                if key not in seen:
                    seen.add(key)
                    headings.append(key)

        return headings, flat_records, record_ids

    @classmethod
    def _get_zmongo(cls) -> ZMongo:
        return ZMongo()

    @staticmethod
    def _clamp_index(value: int, max_len: int) -> int:
        if max_len <= 0:
            return 0
        return max(0, min(int(value), max_len - 1))

    @staticmethod
    def _stringify_value(value: Any) -> str:
        if value is None:
            return ""
        if isinstance(value, str):
            return value
        try:
            return json.dumps(
                DataProcessor.to_json_compatible(value),
                ensure_ascii=False,
                indent=2,
                default=str,
            )
        except Exception:
            return str(value)

    @classmethod
    def get_collection_names(cls) -> List[str]:
        zmongo = None
        try:
            zmongo = cls._get_zmongo()
            result = zmongo.run_sync(zmongo.list_collections_async)

            if not result or not getattr(result, "success", False):
                logger.warning(
                    "Could not load collection names: %s",
                    getattr(result, "error", None),
                )
                return ["<no_collections_found>"]

            data = result.data or {}
            if not isinstance(data, dict):
                return ["<no_collections_found>"]

            names = data.get("collections", [])
            if not isinstance(names, list):
                return ["<no_collections_found>"]

            names = [str(name) for name in names if str(name).strip()]
            return names or ["<no_collections_found>"]

        except Exception as exc:
            logger.exception("Error loading collection names: %s", exc)
            return ["<mongo_error>"]
        finally:
            if zmongo:
                try:
                    zmongo.close()
                except Exception:
                    pass

    @classmethod
    def get_all_records(cls, collection_name: str) -> List[Dict[str, Any]]:
        zmongo = None
        try:
            if not collection_name or collection_name.startswith("<"):
                return []

            zmongo = cls._get_zmongo()

            result = zmongo.find_many(
                collection_name,
                query={},
                sort=[("_id", 1)],
                limit=None,   # all records
            )

            if not result or not getattr(result, "success", False):
                logger.warning(
                    "Could not load records for '%s': %s",
                    collection_name,
                    getattr(result, "error", None),
                )
                return []

            data = result.original() if hasattr(result, "original") else result.data
            if not isinstance(data, list):
                return []

            return [record for record in data if isinstance(record, dict)]

        except Exception as exc:
            logger.exception("Error loading records for '%s': %s", collection_name, exc)
            return []
        finally:
            if zmongo:
                try:
                    zmongo.close()
                except Exception:
                    pass

    @classmethod
    def get_table_payload(cls, collection_name: str) -> Tuple[List[str], List[Dict[str, Any]], List[str]]:
        records = cls.get_all_records(collection_name)

        headings: List[str] = []
        seen = set()
        flat_records: List[Dict[str, Any]] = []
        record_ids: List[str] = []

        for record in records:
            flat = DataProcessor.flatten_json(record)
            normalized_flat = {
                str(k): DataProcessor.to_json_compatible(v)
                for k, v in flat.items()
            }
            flat_records.append(normalized_flat)

            record_id = str(record.get("_id", ""))
            record_ids.append(record_id)

            for key in normalized_flat.keys():
                if key not in seen:
                    seen.add(key)
                    headings.append(key)

        return headings, flat_records, record_ids

    @classmethod
    def INPUT_TYPES(cls) -> Dict[str, Any]:
        collections = cls.get_collection_names()
        default_collection = collections[0] if collections else "<no_collections_found>"

        return {
            "required": {
                "collection_name": (collections, {"default": default_collection}),
                "flattened_field_name": (
                    "STRING",
                    {
                        "default": "",
                        "multiline": False,
                    },
                ),
                "record_id": (
                    "STRING",
                    {
                        "default": "",
                        "multiline": False,
                    },
                ),
                "selected_record_index": (
                    "INT",
                    {
                        "default": 0,
                        "min": 0,
                        "max": 1000000,
                        "step": 1,
                        "display": "number",
                    },
                ),
            }
        }

    @classmethod
    def VALIDATE_INPUTS(cls, collection_name, flattened_field_name, record_id, selected_record_index):
        return True

    @classmethod
    def IS_CHANGED(cls, collection_name: str, flattened_field_name: str, record_id: str, selected_record_index: int):
        return f"{collection_name}|{flattened_field_name}|{record_id}|{selected_record_index}"

    def select_record(
        self,
        collection_name: str,
        flattened_field_name: str,
        record_id: str,
        selected_record_index: int,
    ):
        records = self.get_all_records(collection_name)
        headings, _, record_ids = self.get_table_payload(collection_name)

        if not records:
            return (
                "{}",
                "",
                str(flattened_field_name or ""),
                0,
                "",
                json.dumps([], indent=2),
            )

        selected_idx = None

        if record_id:
            record_id = str(record_id).strip()
            for idx, rid in enumerate(record_ids):
                if rid == record_id:
                    selected_idx = idx
                    break

        if selected_idx is None:
            selected_idx = self._clamp_index(selected_record_index, len(records))

        selected_record = records[selected_idx]
        selected_record_id = str(selected_record.get("_id", ""))

        # default record_id behavior: if blank, use record 0 / selected record
        if not record_id:
            record_id = selected_record_id

        selected_field_name = str(flattened_field_name or "").strip()
        selected_field_value = ""
        if selected_field_name:
            value = DataProcessor.get_value(selected_record, selected_field_name)
            selected_field_value = self._stringify_value(value)

        selected_record_json = json.dumps(
            DataProcessor.to_json_compatible(selected_record),
            ensure_ascii=False,
            indent=2,
            default=str,
        )

        return (
            selected_record_json,
            selected_record_id,
            selected_field_name,
            int(selected_idx),
            selected_field_value,
            json.dumps(headings, ensure_ascii=False, indent=2),
        )


NODE_CLASS_MAPPINGS = {
    "ZMongoTabularRecordViewNode": ZMongoTabularRecordViewNode,
}

NODE_DISPLAY_NAME_MAPPINGS = {
    "ZMongoTabularRecordViewNode": "📊 ZMongo Tabular Record View",
}