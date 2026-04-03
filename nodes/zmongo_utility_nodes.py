import json
import logging
from typing import Any, Dict, List, Tuple

from ..zmongo_toolbag.data_processor import DataProcessor
from ..zmongo_toolbag.safe_result import SafeResult

logger = logging.getLogger(__name__)


class ZMongoUtilityMixin:
    @staticmethod
    def _safe_json(value: Any) -> str:
        try:
            if isinstance(value, SafeResult):
                return value.to_json(indent=2)
            return DataProcessor.to_json(value, indent=2)
        except Exception as exc:
            return json.dumps({"error": f"Serialization failed: {exc}"}, indent=2)

    @staticmethod
    def _coerce_json_value(raw: Any) -> Any:
        if raw is None:
            return None
        if isinstance(raw, (dict, list, int, float, bool)):
            return raw
        text = str(raw).strip()
        if not text:
            return None
        try:
            return json.loads(text)
        except Exception:
            return raw

    @classmethod
    def _coerce_to_list(cls, raw: Any) -> List[Any]:
        parsed = cls._coerce_json_value(raw)
        if parsed is None:
            return []
        if isinstance(parsed, list):
            return parsed
        if isinstance(parsed, tuple):
            return list(parsed)
        return [parsed]

    @staticmethod
    def _clamp_index(index_value: int, item_count: int) -> int:
        if item_count <= 0:
            return 0
        return max(0, min(int(index_value), item_count - 1))

    @staticmethod
    def _stringify_value(value: Any) -> str:
        if value is None:
            return ""
        if isinstance(value, str):
            return value
        if isinstance(value, (dict, list, tuple, bool, int, float)):
            try:
                return DataProcessor.to_json(value, indent=2)
            except Exception:
                return str(value)
        return str(value)


class ListSelectorNode:
    """
    A ComfyUI node that provides a dropdown selection from a predefined list,
    with an optional integer input to override the selection by index.
    """

    def __init__(self):
        pass

    @classmethod
    def INPUT_TYPES(cls):
        # This is the list you provided
        cls.items = [
            "api_keys",
            "test_collection",
            "documents",
            "memoranda",
            "onehot_words",
            "guests",
            "ocr_jobs",
            "embedded_cases",
            "retriever_demo_kb",
            "test_vector_search",
            "causes_of_action",
            "users",
            "ocr_docs",
            "rooms",
            "case_metadata",
            "witnesses",
            "clues"
        ]

        return {
            "required": {
                # The dropdown menu
                "dropdown_selection": (cls.items, {"default": cls.items[0]}),
            },
            "optional": {
                # Optional integer input to select via index
                "index_input": ("INT", {"forceInput": True}),
            }
        }

    RETURN_TYPES = ("STRING", "INT")
    RETURN_NAMES = ("selected_item", "item_index")
    FUNCTION = "select_item"
    CATEGORY = "ZMongo/Utilities"

    def execute(self, dropdown_selection, index_input=None):
        # 1. Determine which index to use
        if index_input is not None:
            # Ensure the index is within the bounds of the list
            idx = max(0, min(index_input, len(self.items) - 1))
            selected_item = self.items[idx]
        else:
            # Use the dropdown selection and find its index
            selected_item = dropdown_selection
            idx = self.items.index(dropdown_selection)

        return (selected_item, idx)


class ZMongoFieldSelector:
    """
    Selects a field from a collection. The dropdown is populated dynamically
    via JavaScript calling a server route. Supports index-based selection
    for automated loops through document schemas.
    """
    CATEGORY = "ZMongo/Utilities"
    FUNCTION = "execute"
    RETURN_TYPES = ("ZMONGO_CONNECTION", "STRING", "INT")
    RETURN_NAMES = ("zmongo", "field_name", "index")

    @classmethod
    def INPUT_TYPES(cls):
        return {
            "required": {
                "zmongo": ("ZMONGO_CONNECTION",),
                "collection_name": ("STRING", {"forceInput": True}),
                "field_name": (["loading..."],),  # Populated dynamically by JS
            },
            "optional": {
                "index_input": ("INT", {"default": -1, "min": -1, "max": 9999}),
            }
        }

    @classmethod
    def VALIDATE_INPUTS(cls, **kwargs):
        """
        Bypasses strict 'Value not in list' validation.
        This is critical to allow the server to accept field names injected
        by the JavaScript extension that were not in the initial 'loading...' list.
        """
        return True

    def execute(self, zmongo, collection_name, field_name, index_input=-1):
        if zmongo is None:
            logger.error("ZMongoFieldSelector: No active connection.")
            return (None, field_name, 0)

        # 1. Fetch Schema Template
        # Uses the synchronous wrapper to ensure the document is retrieved
        # before the node continues execution.
        result = zmongo.find_one(collection_name, {})

        if not result.success:
            logger.warning(f"ZMongoFieldSelector: Failed to fetch from {collection_name}")
            return (zmongo, field_name, 0)

        # Extract document data from the SafeResult
        data = result.data or {}
        sample_doc = data.get("document") if isinstance(data, dict) else None

        if not sample_doc:
            return (zmongo, field_name, 0)

        # 2. Generate Flattened Keys
        # Converts nested objects (e.g. metadata.author) into dot-notation strings.
        fields = DataProcessor.sorted_flattened_keys(sample_doc)
        if "_id" in fields:
            fields.remove("_id")

        # 3. Handle Selection Logic
        # Priority: Automated Index Input > UI Dropdown Selection
        if index_input >= 0 and fields:
            # Clamp the index to prevent out-of-bounds errors in loops
            idx = max(0, min(index_input, len(fields) - 1))
            final_field = fields[idx]
        else:
            final_field = field_name
            # Determine the numeric index of the string selection for tracking
            try:
                idx = fields.index(field_name) if field_name in fields else 0
            except ValueError:
                idx = 0

        return (zmongo, final_field, idx)


class ZMongoCollectionSelector:
    """
    Selects a collection name from a predefined list for use with a ZMongo connection.
    Allows for manual dropdown selection or dynamic selection via an integer index.
    """

    CATEGORY = "ZMongo/Utilities"
    FUNCTION = "select_collection"
    # Returns the connection object (pass-through), the collection name, and the index
    RETURN_TYPES = ("ZMONGO_CONNECTION", "STRING", "INT")
    RETURN_NAMES = ("zmongo", "collection_name", "index")

    @classmethod
    def INPUT_TYPES(cls):
        # The list provided in your previous request
        cls.collection_list = [
            "api_keys", "test_collection", "documents", "memoranda",
            "onehot_words", "guests", "ocr_jobs", "embedded_cases",
            "retriever_demo_kb", "test_vector_search", "causes_of_action",
            "users", "ocr_docs", "rooms", "case_metadata", "witnesses", "clues"
        ]

        return {
            "required": {
                "zmongo": ("ZMONGO_CONNECTION",),
                "dropdown_selection": (cls.collection_list, {"default": "documents"}),
            },
            "optional": {
                "index_input": ("INT", {"forceInput": True, "default": 0}),
            }
        }

    def select_collection(self, zmongo, dropdown_selection, index_input=None):
        # 1. Handle Index Selection logic
        if index_input is not None:
            # Clamp the index to the bounds of the list to prevent errors
            idx = max(0, min(index_input, len(self.collection_list) - 1))
            selected_item = self.collection_list[idx]
        else:
            # Use the UI dropdown selection
            selected_item = dropdown_selection
            idx = self.collection_list.index(dropdown_selection)

        # 2. Return the data
        # We pass zmongo back out so you can chain this node to a "Query" or "Insert" node.
        return (zmongo, selected_item, idx)


class ZMongoProjectFieldListNode:
    """
    Project one field from each record in record_list_json into a JSON list.

    Main use:
    - extract all _id values from a selected collection range
    - build a loopable list of names, filepaths, statuses, etc.
    - create a numbered text preview for manual selection/debugging
    """

    CATEGORY = "ZMongo/Utilities"
    FUNCTION = "project_field_list"
    RETURN_TYPES = ("STRING", "STRING", "INT")
    RETURN_NAMES = ("value_list_json", "numbered_values_text", "count")

    @classmethod
    def INPUT_TYPES(cls) -> Dict[str, Any]:
        return {
            "required": {
                "record_list_json": ("STRING", {"default": "[]", "forceInput": True, "multiline": True}),
                "field_path": ("STRING", {"default": "_id"}),
            },
            "optional": {
                "skip_missing": ("BOOLEAN", {"default": True}),
                "unique_only": ("BOOLEAN", {"default": False}),
            },
        }

    @staticmethod
    def _safe_json(value: Any) -> str:
        try:
            if isinstance(value, SafeResult):
                return value.to_json(indent=2)
            return DataProcessor.to_json(value, indent=2)
        except Exception as exc:
            return json.dumps({"error": f"Serialization failed: {exc}"}, indent=2)

    @staticmethod
    def _stringify_value(value: Any) -> str:
        if value is None:
            return ""
        if isinstance(value, str):
            return value
        try:
            return DataProcessor.to_json(value, indent=2)
        except Exception:
            return str(value)

    @classmethod
    def _build_numbered_text(cls, values: List[Any]) -> str:
        if not values:
            return "No values found."
        return "\n".join(f"{idx}. {cls._stringify_value(value)}" for idx, value in enumerate(values))

    def project_field_list(
        self,
        record_list_json: str,
        field_path: str,
        skip_missing: bool = True,
        unique_only: bool = False,
    ):
        try:
            parsed = json.loads(record_list_json or "[]")
            if not isinstance(parsed, list):
                raise ValueError("record_list_json must decode to a JSON list")

            normalized_field_path = str(field_path or "").strip()
            if not normalized_field_path:
                raise ValueError("field_path is required")

            values: List[Any] = []
            seen = set()

            for item in parsed:
                if not isinstance(item, dict):
                    if skip_missing:
                        continue
                    value = None
                else:
                    value = DataProcessor.get_value(item, normalized_field_path)

                if value is None and skip_missing:
                    continue

                if unique_only:
                    marker = json.dumps(
                        DataProcessor.to_json_compatible(value),
                        sort_keys=True,
                        default=str,
                    )
                    if marker in seen:
                        continue
                    seen.add(marker)

                values.append(value)

            return (
                self._safe_json(values),
                self._build_numbered_text(values),
                len(values),
            )

        except Exception as exc:
            logger.exception("ZMongoProjectFieldListNode failure")
            failure = SafeResult.from_exception(exc, operation="project_field_list")
            return (
                self._safe_json([]),
                failure.message,
                0,
            )


class ZMongoListItemSelectorNode(ZMongoUtilityMixin):
    """
    Reusable label/value picker for:
    - collection lists
    - field-name lists
    - record/object lists

    Backward compatibility:
    The first 4 outputs remain compatible with the older node:
    1. selected_item
    2. selected_index
    3. item_count
    4. items_json

    New capabilities:
    - If items_input is a list of dicts, you can choose:
      - display_field: what the user sees
      - value_field: what gets returned
    - If value_field is blank and the item is a dict, `_id` is preferred.
    - If display_field is blank and the item is a dict, a readable label is chosen.
    """

    CATEGORY = "ZMongo/Utilities"
    FUNCTION = "select_item"
    RETURN_TYPES = ("STRING", "INT", "INT", "STRING", "STRING", "STRING")
    RETURN_NAMES = (
        "selected_item",
        "selected_index",
        "item_count",
        "items_json",
        "selected_label",
        "selected_item_json",
    )

    @classmethod
    def INPUT_TYPES(cls) -> Dict[str, Any]:
        return {
            "required": {
                "items_input": ("STRING", {"default": "[]", "forceInput": True, "multiline": True}),
                "item_index": ("INT", {"default": 0, "min": 0, "max": 1000000, "step": 1}),
            },
            "optional": {
                "display_field": ("STRING", {"default": ""}),
                "value_field": ("STRING", {"default": ""}),
            },
        }

    @classmethod
    def IS_CHANGED(
        cls,
        items_input: str,
        item_index: int,
        display_field: str = "",
        value_field: str = "",
    ):
        return f"{items_input}|{item_index}|{display_field}|{value_field}"

    @staticmethod
    def _preferred_label_fields() -> List[str]:
        return [
            "name",
            "title",
            "label",
            "filename",
            "original_name",
            "text",
            "content",
            "_id",
        ]

    @staticmethod
    def _preferred_value_fields() -> List[str]:
        return [
            "_id",
            "id",
            "system_id",
            "name",
            "title",
            "filename",
            "filepath",
        ]

    def _resolve_display_value(self, item: Any, display_field: str) -> str:
        if isinstance(item, dict):
            requested = str(display_field or "").strip()
            if requested:
                value = DataProcessor.get_value(item, requested)
                if value is not None:
                    return self._stringify_value(value)

            for field_name in self._preferred_label_fields():
                value = DataProcessor.get_value(item, field_name)
                if value is not None and str(value).strip():
                    return self._stringify_value(value)

            return self._safe_json(item)

        return self._stringify_value(item)

    def _resolve_return_value(self, item: Any, value_field: str, fallback_label: str) -> str:
        if isinstance(item, dict):
            requested = str(value_field or "").strip()
            if requested:
                value = DataProcessor.get_value(item, requested)
                if value is not None:
                    return self._stringify_value(value)

            for field_name in self._preferred_value_fields():
                value = DataProcessor.get_value(item, field_name)
                if value is not None and str(value).strip():
                    return self._stringify_value(value)

            return fallback_label

        return self._stringify_value(item)

    def select_item(
        self,
        items_input: str,
        item_index: int,
        display_field: str = "",
        value_field: str = "",
    ):
        try:
            items = self._coerce_to_list(items_input)
            if not items:
                return ("", 0, 0, self._safe_json([]), "", "{}")

            normalized_index = self._clamp_index(item_index, len(items))
            selected_item = items[normalized_index]

            selected_label = self._resolve_display_value(selected_item, display_field)
            selected_value = self._resolve_return_value(selected_item, value_field, selected_label)

            if isinstance(selected_item, str):
                selected_item_json = self._safe_json(selected_item)
            else:
                selected_item_json = self._safe_json(selected_item)

            return (
                selected_value,
                normalized_index,
                len(items),
                self._safe_json(items),
                selected_label,
                selected_item_json,
            )

        except Exception as exc:
            logger.exception("ZMongoListItemSelectorNode failure")
            failure = SafeResult.from_exception(exc, operation="select_item")
            return ("", 0, 0, self._safe_json([]), failure.message, "{}")


class ZMongoFirstNonEmptyStringNode(ZMongoUtilityMixin):
    """
    Return the first non-empty string among several candidates.

    Useful for:
    - fallback collection names
    - fallback query strings
    - fallback selected field names
    """

    CATEGORY = "ZMongo/Utilities"
    FUNCTION = "pick_first"
    RETURN_TYPES = ("STRING", "INT")
    RETURN_NAMES = ("selected_value", "selected_slot")

    @classmethod
    def INPUT_TYPES(cls) -> Dict[str, Any]:
        return {
            "required": {
                "value_1": ("STRING", {"default": "", "forceInput": True}),
                "value_2": ("STRING", {"default": "", "forceInput": True}),
                "value_3": ("STRING", {"default": "", "forceInput": True}),
                "value_4": ("STRING", {"default": "", "forceInput": True}),
            }
        }

    def pick_first(self, value_1: str, value_2: str, value_3: str, value_4: str):
        values = [value_1, value_2, value_3, value_4]
        for idx, value in enumerate(values, start=1):
            if str(value or "").strip():
                return (str(value), idx)
        return ("", 0)


class ZMongoJsonPathValueNode(ZMongoUtilityMixin):
    """
    Extract a value from JSON using DataProcessor dot-path access.

    Useful for:
    - query state payloads
    - node status payloads
    - picking nested values without writing JS
    """

    CATEGORY = "ZMongo/Utilities"
    FUNCTION = "get_json_path_value"
    RETURN_TYPES = ("STRING", "STRING", "BOOLEAN")
    RETURN_NAMES = ("value_text", "normalized_path", "path_exists")

    @classmethod
    def INPUT_TYPES(cls) -> Dict[str, Any]:
        return {
            "required": {
                "json_input": ("STRING", {"default": "{}", "forceInput": True, "multiline": True}),
                "field_path": ("STRING", {"default": ""}),
            }
        }

    def get_json_path_value(self, json_input: str, field_path: str):
        parsed = self._coerce_json_value(json_input)
        normalized_path = str(field_path or "").strip()

        if parsed is None:
            return ("", normalized_path, False)

        if not normalized_path:
            return (self._stringify_value(parsed), "", True)

        exists = DataProcessor.path_exists(parsed, normalized_path)
        value = DataProcessor.get_value(parsed, normalized_path)
        return (
            self._stringify_value(value),
            normalized_path,
            bool(exists),
        )


class ZMongoStringToJsonNode(ZMongoUtilityMixin):
    """
    Normalize arbitrary text into JSON text when possible.

    Useful for:
    - converting text widgets into stable JSON payloads
    - validating JSON before query/save nodes
    """

    CATEGORY = "ZMongo/Utilities"
    FUNCTION = "normalize_json"
    RETURN_TYPES = ("STRING", "BOOLEAN", "STRING")
    RETURN_NAMES = ("normalized_json", "is_valid_json", "summary")

    @classmethod
    def INPUT_TYPES(cls) -> Dict[str, Any]:
        return {
            "required": {
                "raw_input": ("STRING", {"default": "", "forceInput": True, "multiline": True}),
                "wrap_non_json_as_string": ("BOOLEAN", {"default": True}),
            }
        }

    def normalize_json(self, raw_input: str, wrap_non_json_as_string: bool):
        text = str(raw_input or "").strip()
        if not text:
            return ("null", True, "Empty input normalized to null")

        try:
            parsed = json.loads(text)
            return (self._safe_json(parsed), True, f"Valid JSON: {type(parsed).__name__}")
        except Exception:
            if wrap_non_json_as_string:
                return (
                    self._safe_json(text),
                    False,
                    "Input was not valid JSON; wrapped as JSON string",
                )
            return (
                text,
                False,
                "Input was not valid JSON",
            )


class ZMongoStringDefaultNode(ZMongoUtilityMixin):
    """
    Return primary_value if non-empty, else default_value.

    Useful for:
    - providing collection defaults
    - default field paths
    - optional text/query inputs
    """

    CATEGORY = "ZMongo/Utilities"
    FUNCTION = "apply_default"
    RETURN_TYPES = ("STRING", "BOOLEAN")
    RETURN_NAMES = ("resolved_value", "used_default")

    @classmethod
    def INPUT_TYPES(cls) -> Dict[str, Any]:
        return {
            "required": {
                "primary_value": ("STRING", {"default": "", "forceInput": True}),
                "default_value": ("STRING", {"default": ""}),
            }
        }

    def apply_default(self, primary_value: str, default_value: str):
        if str(primary_value or "").strip():
            return (str(primary_value), False)
        return (str(default_value or ""), True)


NODE_CLASS_MAPPINGS = {
    "ZMongoListItemSelectorNode": ZMongoListItemSelectorNode,
    "ZMongoFirstNonEmptyStringNode": ZMongoFirstNonEmptyStringNode,
    "ZMongoJsonPathValueNode": ZMongoJsonPathValueNode,
    "ZMongoStringToJsonNode": ZMongoStringToJsonNode,
    "ZMongoStringDefaultNode": ZMongoStringDefaultNode,
    "ListSelectorNode": ListSelectorNode,
    "ZMongoCollectionSelector": ZMongoCollectionSelector,
    "ZMongoFieldSelector": ZMongoFieldSelector,
    "ZMongoProjectFieldListNode": ZMongoProjectFieldListNode,
}

NODE_DISPLAY_NAME_MAPPINGS = {
    "ZMongoListItemSelectorNode": "ZMongo List Item Selector",
    "ZMongoFirstNonEmptyStringNode": "ZMongo First Non-Empty String",
    "ZMongoJsonPathValueNode": "ZMongo JSON Path Value",
    "ZMongoStringToJsonNode": "ZMongo String To JSON",
    "ZMongoStringDefaultNode": "ZMongo String Default",
    "ListSelectorNode": "List Selection Dropdown",
    "ZMongoCollectionSelector": "ZMongo Collection Selector",
    "ZMongoFieldSelector": "ZMongo Field Selector (Dynamic)",
    "ZMongoProjectFieldListNode": "ZMongo Project Field List",
}
