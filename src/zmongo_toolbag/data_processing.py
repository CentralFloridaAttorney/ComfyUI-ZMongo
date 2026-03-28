import json
import re
import logging
import html
from typing import Any, Dict, List, Union, Optional
from bson.objectid import ObjectId

logger = logging.getLogger(__name__)
from datetime import datetime

class DataProcessor:
    """Centralized data processing and normalization utilities."""

    # ---------------------------------------------------------------------
    # BSON Normalization
    # ---------------------------------------------------------------------
    @staticmethod
    def normalize_objectid(obj: Any) -> Any:
        if isinstance(obj, ObjectId):
            return str(obj)
        if isinstance(obj, datetime):
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


if __name__ == "__main__":
    import datetime
    from bson.objectid import ObjectId

    # Setup basic logging to see errors if they occur
    logging.basicConfig(level=logging.INFO)
    dp = DataProcessor()

    print("--- 1. BSON & JSON Normalization ---")
    mock_data = {
        "_id": ObjectId("65918f0678e24c0001f3e5b1"),
        "timestamp": datetime.datetime(2024, 1, 1, 12, 0, 0),
        "tags": {"active", "verified"},  # Set (non-serializable by default)
        "meta": {"author": "User123"}
    }
    normalized = dp.normalize_objectid(mock_data)
    print(f"Normalized Data: {normalized}")
    print(f"JSON String:\n{dp.to_json(mock_data, indent=2)}")
    print("\n")

    print("--- 2. Flattening ---")
    nested = {
        "user": {"id": 1, "profile": {"name": "Alice", "role": "Admin"}},
        "items": ["laptop", "mouse"]
    }
    flattened = dp.flatten_dict(nested)
    print(f"Flattened: {flattened}")
    print("\n")

    print("--- 3. Value Get/Set (Path Access) ---")
    # Getting a deep value
    val = dp.get_value(nested, "user.profile.name")
    list_val = dp.get_value(nested, "items.1")
    print(f"Get 'user.profile.name': {val}")
    print(f"Get 'items.1': {list_val}")

    # Setting a deep value
    dp.set_value(nested, "user.profile.role", "Superuser")
    dp.set_value(nested, "items.0", "macbook")
    print(f"Modified dict: {nested}")
    print("\n")

    print("--- 4. Text & HTML Helpers ---")
    raw_text = "```python\nprint('Hello World')\n```"
    cleaned = dp.clean_output_text(raw_text)
    print(f"Cleaned Markdown: {repr(cleaned)}")

    html_payload = {"output_text": "&lt;p&gt;Hello &amp; Welcome&lt;/p&gt;"}
    converted_html = dp.convert_text_to_html(html_payload)
    print(f"HTML Converted: {converted_html}")
    print("\n")

    print("--- 5. Complex Object Conversion & Circular Refs ---")


    # Create a class to test __dict__ conversion
    class User:
        def __init__(self, name):
            self.name = name
            self.friend = None


    alice = User("Alice")
    bob = User("Bob")

    # Create circular reference: Alice -> Bob -> Alice
    alice.friend = bob
    bob.friend = alice

    # Convert complex object
    converted_obj = dp.convert_object_to_json(alice)
    print(f"Object with circular reference: {converted_obj}")