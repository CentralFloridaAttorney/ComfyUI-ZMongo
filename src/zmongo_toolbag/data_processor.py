import datetime
import html
import json
import logging
import re
from collections import deque
from typing import Any, Dict, List, Optional, Sequence, Tuple, Union

try:
    from bson import ObjectId
except ImportError:  # pragma: no cover - bson should normally be available
    from bson.objectid import ObjectId  # type: ignore

logger = logging.getLogger(__name__)


class DataProcessor:
    """Centralized data processing, normalization, flattening, and text helpers.

    This class preserves the legacy helper methods from the prior implementation
    while standardizing everything under the new ``DataProcessor`` name.
    """

    # ------------------------------------------------------------------
    # Entity & index helpers (legacy compatibility)
    # ------------------------------------------------------------------
    @staticmethod
    def get_entity_name(data_row: Sequence[Any]) -> Tuple[str, str]:
        """Build a fallback entity name from the populated values in a row."""
        last_index = DataProcessor.get_index_last_non_nan(data_row)
        entity_name = DataProcessor.get_string(data_row, 0, last_index)
        return "Entity", entity_name

    @staticmethod
    def get_index_last_non_excluded(
        name_row: Sequence[Any],
        excluded_parts: Optional[set] = None,
    ) -> Optional[int]:
        """Return the last non-empty index whose value is not excluded."""
        excluded_parts = excluded_parts or set()
        last_value_index = DataProcessor.get_index_last_non_nan(name_row)

        for i in range(last_value_index, -1, -1):
            try:
                value = name_row[i]
                if value not in excluded_parts:
                    return i
            except Exception as exc:  # pragma: no cover - defensive logging
                logger.error("Error in get_index_last_non_excluded: %s", exc)
        return None

    @staticmethod
    def get_index_last_non_nan(data: Sequence[Any]) -> int:
        """Return the last index whose value is not None and not NaN-like."""
        for i in range(len(data) - 1, -1, -1):
            value = data[i]
            if value is not None and str(value).lower() != "nan":
                return i
        return 0

    @staticmethod
    def get_string(data: Sequence[Any], start: int, end: int) -> str:
        """Join a slice of values into a single space-delimited string."""
        if not data:
            return ""
        end = min(end, len(data) - 1)
        start = max(start, 0)
        if end < start:
            return ""
        parts = data[start : end + 1]
        return " ".join(str(part) for part in parts if part is not None)

    # ------------------------------------------------------------------
    # BSON / datetime normalization
    # ------------------------------------------------------------------
    @staticmethod
    def normalize_objectid(obj: Any) -> Any:
        """Recursively convert ObjectId and datetime values to JSON-safe strings."""
        if isinstance(obj, ObjectId):
            return str(obj)
        if isinstance(obj, (datetime.datetime, datetime.date)):
            return obj.isoformat()
        if isinstance(obj, dict):
            return {k: DataProcessor.normalize_objectid(v) for k, v in obj.items()}
        if isinstance(obj, (list, tuple, set)):
            return [DataProcessor.normalize_objectid(v) for v in obj]
        return obj

    @staticmethod
    def to_json_compatible(data: Any) -> Any:
        """Backward-compatible alias for recursive normalization."""
        return DataProcessor.normalize_objectid(data)

    # ------------------------------------------------------------------
    # JSON serialization
    # ------------------------------------------------------------------
    @staticmethod
    def to_json(data: Any, indent: Optional[int] = None) -> str:
        """Serialize arbitrary data to JSON, coercing unsupported values."""

        def _default(obj: Any) -> str:
            if isinstance(obj, ObjectId):
                return str(obj)
            if isinstance(obj, (datetime.datetime, datetime.date)):
                return obj.isoformat()
            return str(obj)

        try:
            normalized = DataProcessor.normalize_objectid(data)
            return json.dumps(
                normalized,
                indent=indent,
                default=_default,
                ensure_ascii=False,
            )
        except Exception as exc:
            logger.error("Error serializing to JSON: %s", exc)
            return json.dumps({"error": str(exc)})

    # ------------------------------------------------------------------
    # Flattening helpers
    # ------------------------------------------------------------------
    @staticmethod
    def flatten_json(data: Any, parent_key: str = "", sep: str = ".") -> Dict[str, Any]:
        """Flatten nested dict/list structures using dot-path keys."""
        flat: Dict[str, Any] = {}

        if isinstance(data, dict):
            for key, value in data.items():
                new_key = f"{parent_key}{sep}{key}" if parent_key else str(key)
                flat.update(DataProcessor.flatten_json(value, new_key, sep))
            return flat

        if isinstance(data, list):
            for index, value in enumerate(data):
                new_key = f"{parent_key}{sep}{index}" if parent_key else str(index)
                flat.update(DataProcessor.flatten_json(value, new_key, sep))
            return flat

        if parent_key:
            flat[parent_key] = data
        return flat

    @staticmethod
    def flatten_dict(data: Dict[str, Any], sep: str = ".") -> Dict[str, Any]:
        """Legacy wrapper expected by older callers."""
        return DataProcessor.flatten_json(data, sep=sep)

    # ------------------------------------------------------------------
    # Path-based value access
    # ------------------------------------------------------------------
    @staticmethod
    def get_value(data: Union[Dict[str, Any], List[Any]], key: str) -> Any:
        """Fetch a nested value using a dot-path key."""
        if not key:
            return data

        value: Any = data
        for part in key.split("."):
            if isinstance(value, dict):
                value = value.get(part)
            elif isinstance(value, list) and part.isdigit():
                index = int(part)
                value = value[index] if 0 <= index < len(value) else None
            else:
                return None

            if value is None:
                return None

        return value

    @staticmethod
    def set_value(data: Union[Dict[str, Any], List[Any]], key: str, val: Any) -> bool:
        """Set a nested value using a dot-path key.

        Missing intermediate dict nodes are created automatically.
        Existing lists are supported when numeric indexes are used.
        """
        if not isinstance(data, (dict, list)) or not key:
            return False

        parts = key.split(".")
        target: Any = data

        for part in parts[:-1]:
            if isinstance(target, dict):
                next_target = target.get(part)
                if next_target is None:
                    next_target = {}
                    target[part] = next_target
                target = next_target
            elif isinstance(target, list) and part.isdigit():
                index = int(part)
                if not (0 <= index < len(target)):
                    return False
                target = target[index]
            else:
                return False

        last = parts[-1]
        if isinstance(target, dict):
            target[last] = val
            return True
        if isinstance(target, list) and last.isdigit():
            index = int(last)
            if 0 <= index < len(target):
                target[index] = val
                return True
        return False

    # ------------------------------------------------------------------
    # Text / HTML helpers
    # ------------------------------------------------------------------
    @staticmethod
    def clean_output_text(text: str) -> str:
        """Strip Markdown code fences and unescape literal newlines."""
        if not isinstance(text, str):
            raise ValueError("Input must be a string")

        cleaned = text.strip()
        if cleaned.startswith("```"):
            cleaned = re.sub(r"^```[a-zA-Z0-9_-]*\n?", "", cleaned)
            if cleaned.endswith("```"):
                cleaned = cleaned[:-3]

        return cleaned.replace("\\n", "\n").strip()

    @staticmethod
    def convert_text_to_html(data: Union[str, Dict[str, Any]]) -> str:
        """Convert escaped HTML text to rendered HTML text."""
        if isinstance(data, str):
            text = data
        elif isinstance(data, dict):
            if "output_text" not in data:
                raise ValueError(
                    "Dictionary must contain 'output_text' key for HTML conversion."
                )
            text = data.get("output_text", "")
        else:
            raise ValueError(
                f"Invalid input type: {type(data)}. Expected str or dict."
            )

        if not isinstance(text, str):
            raise ValueError("Invalid text for HTML conversion")

        decoded = html.unescape(text)
        decoded = re.sub(r">\s+<", "> <", decoded)
        return decoded

    # ------------------------------------------------------------------
    # Object conversion helpers
    # ------------------------------------------------------------------
    @staticmethod
    def convert_object_to_json(obj: Any, _seen: Optional[set] = None) -> Any:
        """Recursively coerce arbitrary Python objects into JSON-safe values."""
        import numpy as np
        import pandas as pd

        if _seen is None:
            _seen = set()

        obj_id = id(obj)
        if obj_id in _seen:
            return {"__circular_reference__": type(obj).__name__}
        _seen.add(obj_id)

        if obj is None or isinstance(obj, (bool, int, float, str)):
            return obj
        if isinstance(obj, bytes):
            try:
                return obj.decode()
            except Exception:
                return str(obj)
        if isinstance(obj, (datetime.datetime, datetime.date)):
            return obj.isoformat()
        if isinstance(obj, ObjectId):
            return str(obj)
        if isinstance(obj, np.ndarray):
            return [
                DataProcessor.convert_object_to_json(item, _seen.copy())
                for item in obj.tolist()
            ]
        if isinstance(obj, pd.DataFrame):
            return [
                DataProcessor.convert_object_to_json(record, _seen.copy())
                for record in obj.to_dict(orient="records")
            ]
        if isinstance(obj, pd.Series):
            return {
                key: DataProcessor.convert_object_to_json(value, _seen.copy())
                for key, value in obj.items()
            }
        if isinstance(obj, (list, tuple, set, deque)):
            return [
                DataProcessor.convert_object_to_json(item, _seen.copy())
                for item in list(obj)
            ]
        if isinstance(obj, dict):
            return {
                key: DataProcessor.convert_object_to_json(value, _seen.copy())
                for key, value in obj.items()
            }
        if hasattr(obj, "__dict__"):
            attrs = {
                key: value
                for key, value in vars(obj).items()
                if not key.startswith("_")
            }
            return DataProcessor.convert_object_to_json(attrs, _seen.copy())
        return str(obj)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    print("--- 1. BSON & JSON normalization ---")
    mock_data = {
        "_id": ObjectId("65918f0678e24c0001f3e5b1"),
        "timestamp": datetime.datetime(2024, 1, 1, 12, 0, 0),
        "tags": {"active", "verified"},
        "meta": {"author": "User123"},
    }
    print(DataProcessor.normalize_objectid(mock_data))
    print(DataProcessor.to_json(mock_data, indent=2))
    print()

    print("--- 2. Flattening ---")
    nested = {
        "user": {"id": 1, "profile": {"name": "Alice", "role": "Admin"}},
        "items": ["laptop", "mouse"],
    }
    print(DataProcessor.flatten_dict(nested))
    print()

    print("--- 3. Path-based get/set ---")
    print(DataProcessor.get_value(nested, "user.profile.name"))
    print(DataProcessor.get_value(nested, "items.1"))
    DataProcessor.set_value(nested, "user.profile.role", "Superuser")
    DataProcessor.set_value(nested, "items.0", "macbook")
    print(nested)
    print()

    print("--- 4. Text / HTML helpers ---")
    raw_text = "```python\nprint('Hello World')\n```"
    print(repr(DataProcessor.clean_output_text(raw_text)))
    html_payload = {"output_text": "&lt;p&gt;Hello &amp; Welcome&lt;/p&gt;"}
    print(DataProcessor.convert_text_to_html(html_payload))
    print()

    print("--- 5. Legacy helpers ---")
    row = ["Acme", "Holdings", None, "nan"]
    print(DataProcessor.get_entity_name(row))
    print(DataProcessor.get_index_last_non_excluded(row, {None, "nan"}))
    print()

    print("--- 6. Complex object conversion & circular refs ---")

    class User:
        def __init__(self, name: str) -> None:
            self.name = name
            self.friend = None

    alice = User("Alice")
    bob = User("Bob")
    alice.friend = bob
    bob.friend = alice

    print(DataProcessor.convert_object_to_json(alice))