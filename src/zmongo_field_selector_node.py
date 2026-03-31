import json
import logging
from typing import Any, Dict, List

from .zmongo_toolbag.zmongo import ZMongo
from .zmongo_toolbag.data_processor import DataProcessor

logger = logging.getLogger(__name__)


class ZMongoFieldSelectorNode:
    CATEGORY = "ZMongo"
    FUNCTION = "select_field"
    RETURN_TYPES = ("STRING", "STRING", "INT", "STRING", "STRING")
    RETURN_NAMES = (
        "collection_name",
        "selected_field_name",
        "selected_field_index",
        "selected_field_value",
        "flattened_field_names_json",
    )

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
        if isinstance(value, (dict, list, tuple, bool, int, float)):
            try:
                return json.dumps(
                    DataProcessor.to_json_compatible(value),
                    ensure_ascii=False,
                    indent=2,
                    default=str,
                )
            except Exception:
                return str(value)
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
    def get_sample_document(cls, collection_name: str) -> Dict[str, Any]:
        zmongo = None
        try:
            if not collection_name or collection_name.startswith("<"):
                return {}

            zmongo = cls._get_zmongo()
            result = zmongo.find_one(collection_name, {})

            if not result or not getattr(result, "success", False):
                logger.warning(
                    "Could not load sample document for '%s': %s",
                    collection_name,
                    getattr(result, "error", None),
                )
                return {}

            doc = result.original() if hasattr(result, "original") else result.data
            return doc if isinstance(doc, dict) else {}

        except Exception as exc:
            logger.exception(
                "Error loading sample document for collection '%s': %s",
                collection_name,
                exc,
            )
            return {}
        finally:
            if zmongo:
                try:
                    zmongo.close()
                except Exception:
                    pass

    @classmethod
    def get_flattened_field_names(cls, collection_name: str) -> List[str]:
        try:
            doc = cls.get_sample_document(collection_name)
            if not doc:
                return []

            flattened = DataProcessor.flatten_json(doc)
            if not isinstance(flattened, dict):
                return []

            return sorted(str(k) for k in flattened.keys())
        except Exception as exc:
            logger.exception(
                "Error flattening fields for collection '%s': %s",
                collection_name,
                exc,
            )
            return []

    @classmethod
    def INPUT_TYPES(cls) -> Dict[str, Any]:
        collections = cls.get_collection_names()
        default_collection = collections[0] if collections else "<no_collections_found>"

        return {
            "required": {
                "collection_name": (collections, {"default": default_collection}),
                "field_name": (["<dynamic_field_name>"], {"default": "<dynamic_field_name>"}),
                "field_index": (
                    "INT",
                    {
                        "default": 0,
                        "min": 0,
                        "max": 100000,
                        "step": 1,
                        "display": "number",
                    },
                ),
            }
        }

    @classmethod
    def VALIDATE_INPUTS(cls, collection_name, field_name, field_index):
        return True

    @classmethod
    def IS_CHANGED(cls, collection_name: str, field_name: str, field_index: int):
        return f"{collection_name}|{field_name}|{field_index}"

    def select_field(self, collection_name: str, field_name: str, field_index: int):
        doc = self.get_sample_document(collection_name)
        field_names = self.get_flattened_field_names(collection_name)

        if not doc or not field_names:
            return (
                str(collection_name),
                "",
                0,
                "",
                json.dumps([], indent=2),
            )

        normalized_index = self._clamp_index(field_index, len(field_names))

        selected_field_name = field_name
        if (
            not selected_field_name
            or selected_field_name.startswith("<")
            or selected_field_name not in field_names
        ):
            selected_field_name = field_names[normalized_index]

        selected_index = field_names.index(selected_field_name)
        selected_value = DataProcessor.get_value(doc, selected_field_name)
        selected_value_str = self._stringify_value(selected_value)

        return (
            str(collection_name),
            str(selected_field_name),
            int(selected_index),
            selected_value_str,
            json.dumps(field_names, indent=2),
        )


NODE_CLASS_MAPPINGS = {
    "ZMongoFlattenedFieldDropdownNode": ZMongoFieldSelectorNode,
}

NODE_DISPLAY_NAME_MAPPINGS = {
    "ZMongoFlattenedFieldDropdownNode": "🧩 ZMongo Flattened Field Dropdown Node",
}