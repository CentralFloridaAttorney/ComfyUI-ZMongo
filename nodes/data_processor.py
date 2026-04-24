import base64
import datetime
import html
import json
import logging
import math
import re
import uuid
from collections import deque
from decimal import Decimal
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
        """Legacy alias retained for backward compatibility."""
        return DataProcessor.to_json_compatible(obj)

    @staticmethod
    def to_json_compatible(
            data: Any,
            _seen: Optional[set] = None,
            *,
            max_depth: int = 50,
            _depth: int = 0,
    ) -> Any:
        """
        Recursively convert arbitrary Python objects into JSON-compatible data.

        Updated for ZAI Fleet:
        - Enhanced binary handling for GridFS-ready image transport.
        - Preserves structure for numpy/pandas and Pydantic v1/v2 models.
        - Enforces depth limits and circular reference protection.

        Conversion rules:
        - None / bool / int / str -> unchanged
        - float NaN / inf -> string representation ("NaN", "Infinity")
        - ObjectId / UUID -> str
        - datetime/date/time -> isoformat()
        - Decimal -> int if integral, else float, fallback str
        - bytes / bytearray / memoryview -> UTF-8 string if decodable,
          otherwise a structured base64 dictionary with metadata.
        - dict -> recursively converted dict with string keys
        - list / tuple / set / frozenset / deque -> recursively converted list
        - Exception -> structured error dict with class name and message
        - numpy scalars/arrays -> Python scalar/list
        - pandas DataFrame/Series -> records/dict
        - objects with model_dump()/dict()/__dict__ -> converted recursively
        - circular references -> {"__circular_reference__": type_name}
        - unknown objects -> str(obj)
        """
        import math
        import uuid
        import datetime
        import re
        import base64
        import json
        from decimal import Decimal
        from collections import deque
        from bson import ObjectId

        if _seen is None:
            _seen = set()

        if _depth > max_depth:
            return {"__truncated__": f"max_depth_exceeded:{max_depth}"}

        # 1. Fast-path safe primitives
        if data is None or isinstance(data, (bool, int, str)):
            return data

        # 2. Floats (JSON-standard compliance)
        if isinstance(data, float):
            if math.isnan(data):
                return "NaN"
            if math.isinf(data):
                return "Infinity" if data > 0 else "-Infinity"
            return data

        # 3. Common Database & Logic Scalars
        if isinstance(data, ObjectId):
            return str(data)

        if isinstance(data, uuid.UUID):
            return str(data)

        if isinstance(data, Decimal):
            try:
                if data == data.to_integral_value():
                    return int(data)
                return float(data)
            except Exception:
                return str(data)

        if isinstance(data, (datetime.datetime, datetime.date, datetime.time)):
            try:
                return data.isoformat()
            except Exception:
                return str(data)

        if isinstance(data, re.Pattern):
            return data.pattern

        # 4. Binary Payloads (GridFS & Image Support)
        if isinstance(data, (bytes, bytearray, memoryview)):
            raw = bytes(data)
            try:
                # Attempt to treat as text (e.g., encoded logs or small JSON fragments)
                return raw.decode("utf-8")
            except Exception:
                # Treat as raw binary (Images, Models, Pickles)
                # Providing the size helps the FleetManager handle GridFS shunting
                return {
                    "__type__": "bytes",
                    "encoding": "base64",
                    "size_bytes": len(raw),
                    "data": base64.b64encode(raw).decode("ascii"),
                }

        # 5. Exceptions (Structured for UI Debugging)
        if isinstance(data, BaseException):
            return {
                "__type__": data.__class__.__name__,
                "message": str(data),
                "args": DataProcessor.to_json_compatible(
                    list(getattr(data, "args", [])),
                    _seen=_seen.copy(),
                    max_depth=max_depth,
                    _depth=_depth + 1,
                ),
            }

        # 6. Container Types & Cycle Tracking
        # Avoid circular references on any object that might contain itself
        needs_cycle_tracking = isinstance(
            data, (dict, list, tuple, set, frozenset, deque)
        ) or hasattr(data, "__dict__") or hasattr(data, "model_dump") or hasattr(data, "dict")

        if needs_cycle_tracking:
            obj_id = id(data)
            if obj_id in _seen:
                return {"__circular_reference__": type(data).__name__}
            _seen.add(obj_id)

        # Mapping types
        if isinstance(data, dict):
            converted = {}
            for key, value in data.items():
                # JSON object keys must be strings; recursively convert complex keys
                if isinstance(key, (str, int, float, bool)) or key is None:
                    safe_key = str(key)
                else:
                    safe_key = str(
                        DataProcessor.to_json_compatible(
                            key,
                            _seen=_seen.copy(),
                            max_depth=max_depth,
                            _depth=_depth + 1,
                        )
                    )

                converted[safe_key] = DataProcessor.to_json_compatible(
                    value,
                    _seen=_seen.copy(),
                    max_depth=max_depth,
                    _depth=_depth + 1,
                )
            return converted

        # Sequence / set-like types
        if isinstance(data, (list, tuple, set, frozenset, deque)):
            iterable = list(data)
            # Ensure deterministic output for sets by sorting where possible
            if isinstance(data, (set, frozenset)):
                try:
                    iterable = sorted(iterable, key=lambda x: repr(x))
                except Exception:
                    pass

            return [
                DataProcessor.to_json_compatible(
                    item,
                    _seen=_seen.copy(),
                    max_depth=max_depth,
                    _depth=_depth + 1,
                )
                for item in iterable
            ]

        # 7. Specialized Ecosystem Support (Numpy, Pandas, Pydantic)
        # Numpy
        try:
            import numpy as np
            if isinstance(data, np.generic):
                return DataProcessor.to_json_compatible(data.item(), _seen=_seen.copy(), _depth=_depth + 1)
            if isinstance(data, np.ndarray):
                return DataProcessor.to_json_compatible(data.tolist(), _seen=_seen.copy(), _depth=_depth + 1)
        except ImportError:
            pass

        # Pandas
        try:
            import pandas as pd
            if isinstance(data, pd.DataFrame):
                return DataProcessor.to_json_compatible(data.to_dict(orient="records"), _seen=_seen.copy(),
                                                        _depth=_depth + 1)
            if isinstance(data, pd.Series):
                return DataProcessor.to_json_compatible(data.to_dict(), _seen=_seen.copy(), _depth=_depth + 1)
        except ImportError:
            pass

        # Pydantic v2 (model_dump)
        if hasattr(data, "model_dump") and callable(data.model_dump):
            try:
                return DataProcessor.to_json_compatible(data.model_dump(), _seen=_seen.copy(), _depth=_depth + 1)
            except Exception:
                pass

        # Pydantic v1 / Generic .dict()
        if hasattr(data, "dict") and callable(data.dict):
            try:
                return DataProcessor.to_json_compatible(data.dict(), _seen=_seen.copy(), _depth=_depth + 1)
            except Exception:
                pass

        # 8. Generic Class Instances (via __dict__)
        if hasattr(data, "__dict__"):
            try:
                attrs = {k: v for k, v in vars(data).items() if not k.startswith("_")}
                return DataProcessor.to_json_compatible(attrs, _seen=_seen.copy(), _depth=_depth + 1)
            except Exception:
                pass

        # 9. Final Fallback
        try:
            return str(data)
        except Exception:
            return f"<unserializable {type(data).__name__}>"

    # ------------------------------------------------------------------
    # JSON serialization
    # ------------------------------------------------------------------
    @staticmethod
    def to_json(data: Any, indent: Optional[int] = None) -> str:
        """
        Serialize arbitrary data to a JSON string.

        This refactored version uses to_json_compatible to handle complex
        types like Fleet image bytes, Numpy arrays, and Pydantic models
        before performing the final string serialization.
        """
        try:
            # First, use the "Smart Translator" to make the object JSON-safe.
            # This handles your ComfyUI images, ObjectIds, and Decimals correctly.
            safe_data = DataProcessor.to_json_compatible(data)

            # Now, perform the standard string dump.
            # We don't need a complex 'default' here because safe_data
            # is already composed of primitives (str, int, float, list, dict).
            return json.dumps(
                safe_data,
                indent=indent,
                ensure_ascii=False,
                sort_keys=False  # Preserve order for Fleet UI consistency
            )
        except Exception as exc:
            logger.error("Error serializing to JSON string: %s", exc)
            # Return a JSON-formatted error string so the receiver can still parse it
            return json.dumps({
                "success": False,
                "error": "Serialization failed",
                "details": str(exc)
            })

    # ------------------------------------------------------------------
    # Flattening helpers
    # ------------------------------------------------------------------

    @staticmethod
    def flatten_json(data: Any, parent_key: str = "", sep: str = ".") -> Dict[str, Any]:
        """
        Flatten nested structures.
        Updated to treat Fleet binary envelopes as atomic units to prevent metadata corruption.
        """
        flat: Dict[str, Any] = {}

        # ATOMIC CHECK: If this is a Fleet binary envelope, do not flatten its internals.
        if isinstance(data, dict) and data.get("__type__") == "bytes":
            flat[parent_key] = data
            return flat

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
        """
        Strip Markdown code fences.
        Improved to handle large Base64 payloads common in ZAI Fleet/ComfyUI responses.
        """
        if not isinstance(text, str):
            return str(text)

        cleaned = text.strip()
        # Robustly handle various markdown fence styles (```, ```base64, ```json)
        if "```" in cleaned:
            cleaned = re.sub(r"```[a-zA-Z0-9_-]*\n?", "", cleaned)
            cleaned = cleaned.replace("```", "")

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
        """
        Legacy wrapper for object conversion.
        Refactored to use the binary-safe to_json_compatible logic.
        """
        return DataProcessor.to_json_compatible(obj, _seen=_seen)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    print("--- 1. Fleet Binary/Image Handling ---")
    # Simulate a raw PNG header from ComfyUI
    mock_image = b"\x89PNG\r\n\x1a\n\x00\x00\x00\rIHDR"
    safe_img = DataProcessor.to_json_compatible(mock_image)
    print(f"Binary Envelope Metadata: { {k: v for k, v in safe_img.items() if k != 'data'} }")
    print(f"Serialized Fleet JSON: {DataProcessor.to_json(mock_image)[:100]}...")
    print()

    print("--- 2. Atomic Flattening Test ---")
    # Test that flattening doesn't break the image envelope
    nested_fleet_data = {
        "task_id": str(uuid.uuid4()),
        "agent": "Remote-Comfy-01",
        "payload": {
            "render": mock_image,
            "metadata": {"seed": 42}
        }
    }
    # Pre-process to compatible format
    compatible_data = DataProcessor.to_json_compatible(nested_fleet_data)
    flattened = DataProcessor.flatten_dict(compatible_data)

    print("Flattened Fleet Keys:")
    for k, v in flattened.items():
        if isinstance(v, dict) and v.get("__type__") == "bytes":
            print(f"  [ATOMIC] {k}: Binary Envelope (Size: {v.get('size_bytes')} bytes)")
        else:
            print(f"  {k}: {v}")
    print()

    print("--- 3. Large Payload Code Fence Cleaning ---")
    # Simulate agent output wrapping a base64 image in markdown
    b64_payload = "```base64\niVBORw0KGgoAAAANSUhEUgAA...[REDACTED]...```"
    cleaned = DataProcessor.clean_output_text(b64_payload)
    print(f"Cleaned String Start: {cleaned[:30]}...")
    print(f"Fence Successfully Removed: {'```' not in cleaned}")
    print()

    print("--- 4. Legacy Wrapper & Circular Reference Safety ---")


    class User:
        def __init__(self, name):
            self.name = name
            self.image_buffer = b"\xff\xd8\xff"  # Mock JPEG
            self.self_ref = self


    test_user = User("Florida_Agent_01")
    # Legacy function now uses the new logic internally
    result = DataProcessor.convert_object_to_json(test_user)
    print(f"Legacy Result Keys: {list(result.keys())}")
    print(f"Circular Ref Handled: {result.get('self_ref')}")