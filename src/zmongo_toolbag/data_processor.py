import json
import re
import logging
import datetime
import html
from typing import Any, Dict, List, Union, Optional
from bson import ObjectId

logger = logging.getLogger(__name__)


class DataProcessor:
    """Centralized data processing and normalization utilities."""

    # ---------------------------------------------------------------------
    # Entity & Index Helpers
    # ---------------------------------------------------------------------
    @staticmethod
    def get_entity_name(_data_row: List[Any]) -> tuple:
        """
        Determines the entity name based on the last non-NaN index.
        """
        last_index = DataProcessor.get_index_last_non_nan(_data_row)
        # An Entity's last_name is the sum of all strings in its name
        # An Entity's first_name is "Entity"
        entity_name = DataProcessor.get_string(_data_row, 0, last_index)
        return 'Entity', entity_name

    @staticmethod
    def get_index_last_non_excluded(_name_row: List[Any], excluded_parts: Optional[set] = None) -> Any:
        """
        Finds the last index in a row that is not in the excluded list.
        """
        excluded_parts = excluded_parts or set()
        last_value_index = DataProcessor.get_index_last_non_nan(_name_row)

        # Iterating backwards from the last non-nan value
        for i in range(last_value_index, -1, -1):
            try:
                this_value = _name_row[i]
                if this_value not in excluded_parts:
                    return i
            except Exception as e:
                logger.error(f"Error in get_index_last_non_excluded: {e}")
        return None

    @staticmethod
    def get_index_last_non_nan(data: List[Any]) -> int:
        """Helper to find the last index that isn't null/NaN."""
        for i in range(len(data) - 1, -1, -1):
            if data[i] is not None and str(data[i]).lower() != 'nan':
                return i
        return 0

    @staticmethod
    def get_string(data: List[Any], start: int, end: int) -> str:
        """Helper to join list elements into a single string."""
        parts = data[start:end + 1]
        return " ".join([str(p) for p in parts if p is not None])

    # ---------------------------------------------------------------------
    # BSON Normalization
    # ---------------------------------------------------------------------
    @staticmethod
    def normalize_objectid(obj: Any) -> Any:
        if isinstance(obj, ObjectId):
            return str(obj)
        if isinstance(obj, datetime.datetime):
            return obj.isoformat()
        if isinstance(obj, dict):
            return {k: DataProcessor.normalize_objectid(v) for k, v in obj.items()}
        if isinstance(obj, (list, tuple, set)):
            return [DataProcessor.normalize_objectid(v) for v in obj]
        return obj

    @staticmethod
    def to_json_compatible(data: Any) -> Any:
        return DataProcessor.normalize_objectid(data)

    # ---------------------------------------------------------------------
    # JSON Serialization
    # ---------------------------------------------------------------------
    @staticmethod
    def to_json(data: Any, indent: Optional[int] = None) -> str:
        def _default(o):
            if isinstance(o, ObjectId):
                return str(o)
            if isinstance(o, datetime.datetime):
                return o.isoformat()
            return str(o)
        try:
            return json.dumps(
                DataProcessor.normalize_objectid(data), indent=indent,
                default=_default, ensure_ascii=False
            )
        except Exception as e:
            logger.error(f"Error serializing to JSON: {e}")
            return json.dumps({"error": str(e)})

    # ---------------------------------------------------------------------
    # Flattening
    # ---------------------------------------------------------------------
    @staticmethod
    def flatten_json(data: Any, parent_key: str = "", sep: str = ".") -> Dict[str, Any]:
        flat: Dict[str, Any] = {}
        if isinstance(data, dict):
            for k, v in data.items():
                new_key = f"{parent_key}{sep}{k}" if parent_key else k
                flat.update(DataProcessor.flatten_json(v, new_key, sep))
        elif isinstance(data, list):
            for i, v in enumerate(data):
                new_key = f"{parent_key}{sep}{i}" if parent_key else str(i)
                flat.update(DataProcessor.flatten_json(v, new_key, sep))
        else:
            if parent_key:
                flat[parent_key] = data
        return flat

    @staticmethod
    def flatten_dict(data: Dict[str, Any]) -> Dict[str, Any]:
        """
        FIX: DynamicRecordController expects flatten_dict(), so we map it to flatten_json()
        """
        return DataProcessor.flatten_json(data)

    # ---------------------------------------------------------------------
    # Value get/set
    # ---------------------------------------------------------------------
    @staticmethod
    def get_value(data: Union[Dict[str, Any], List[Any]], key: str) -> Any:
        keys = key.split(".")
        value = data
        for k in keys:
            if isinstance(value, dict):
                value = value.get(k)
            elif isinstance(value, list) and k.isdigit():
                idx = int(k)
                value = value[idx] if 0 <= idx < len(value) else None
            else:
                return None
            if value is None:
                return None
        return value

    @staticmethod
    def set_value(data: Union[Dict[str, Any], List[Any]], key: str, val: Any) -> bool:
        if not isinstance(data, (dict, list)) or not key:
            return False
        parts = key.split(".")
        target = data
        for k in parts[:-1]:
            if isinstance(target, dict):
                target = target.setdefault(k, {})
            elif isinstance(target, list) and k.isdigit():
                idx = int(k)
                if 0 <= idx < len(target):
                    target = target[idx]
                else:
                    return False
            else:
                return False
        last = parts[-1]
        if isinstance(target, dict):
            target[last] = val
            return True
        elif isinstance(target, list) and last.isdigit():
            idx = int(last)
            if 0 <= idx < len(target):
                target[idx] = val
                return True
        return False

    # ---------------------------------------------------------------------
    # Text & HTML Helpers
    # ---------------------------------------------------------------------
    @staticmethod
    def clean_output_text(text: str) -> str:
        if not isinstance(text, str):
            raise ValueError("Input must be a string")
        t = text.strip()
        if t.startswith("```"):
            t = re.sub(r"^```[a-zA-Z]*\n?", "", t)
            if t.endswith("```"):
                t = t[:-3]
        return t.replace("\\n", "\n").strip()

    @staticmethod
    def convert_text_to_html(data: Union[str, Dict[str, Any]]) -> str:
        # Determine text source safely
        if isinstance(data, str):
            text = data
        elif isinstance(data, dict):
            if "output_text" not in data:
                raise ValueError("Dictionary must contain 'output_text' key for HTML conversion.")
            text = data.get("output_text", "")
        else:
            raise ValueError(f"Invalid input type: {type(data)}. Expected str or dict.")

        if not isinstance(text, str):
            raise ValueError("Invalid text for HTML conversion")

        decoded = html.unescape(text)
        decoded = re.sub(r">\s+<", "> <", decoded)
        return decoded

    # ---------------------------------------------------------------------
    # Object Conversion
    # ---------------------------------------------------------------------
    @staticmethod
    def convert_object_to_json(obj: Any, _seen: Optional[set] = None) -> Any:
        import numpy as np
        import pandas as pd
        from collections import deque

        if _seen is None:
            _seen = set()
        obj_id = id(obj)
        if obj_id in _seen:
            # FIX: Use key '__circular_reference__' to match test expectation
            return {"__circular_reference__": str(type(obj).__name__)}
        _seen.add(obj_id)

        if obj is None or isinstance(obj, (bool, int, float, str)):
            return obj
        if isinstance(obj, bytes):
            try:
                return obj.decode()
            except Exception:
                return str(obj)
        if isinstance(obj, datetime.datetime):
            return obj.isoformat()
        if isinstance(obj, ObjectId):
            return str(obj)
        if isinstance(obj, np.ndarray):
            return [DataProcessor.convert_object_to_json(i, _seen.copy()) for i in obj.tolist()]
        if isinstance(obj, pd.DataFrame):
            return [DataProcessor.convert_object_to_json(r, _seen.copy()) for r in obj.to_dict(orient="records")]
        if isinstance(obj, pd.Series):
            return {k: DataProcessor.convert_object_to_json(v, _seen.copy()) for k, v in obj.items()}
        if isinstance(obj, (list, tuple, set, deque)):
            return [DataProcessor.convert_object_to_json(i, _seen.copy()) for i in list(obj)]
        if isinstance(obj, dict):
            return {k: DataProcessor.convert_object_to_json(v, _seen.copy()) for k, v in obj.items()}
        if hasattr(obj, "__dict__"):
            attrs = {k: v for k, v in vars(obj).items() if not k.startswith("_")}
            return DataProcessor.convert_object_to_json(attrs, _seen.copy())
        return str(obj)