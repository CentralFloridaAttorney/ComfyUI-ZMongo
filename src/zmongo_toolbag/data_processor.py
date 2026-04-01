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
    """Centralized data processing, normalization, flattening, and text helpers."""

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
        """Recursively convert arbitrary Python objects into JSON-compatible data."""
        if _seen is None:
            _seen = set()

        if _depth > max_depth:
            return {"__truncated__": f"max_depth_exceeded:{max_depth}"}

        if data is None or isinstance(data, (bool, int, str)):
            return data

        if isinstance(data, float):
            if math.isnan(data):
                return "NaN"
            if math.isinf(data):
                return "Infinity" if data > 0 else "-Infinity"
            return data

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

        if isinstance(data, (bytes, bytearray, memoryview)):
            raw = bytes(data)
            try:
                return raw.decode("utf-8")
            except Exception:
                return {
                    "__type__": "bytes",
                    "encoding": "base64",
                    "data": base64.b64encode(raw).decode("ascii"),
                }

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

        needs_cycle_tracking = isinstance(
            data,
            (dict, list, tuple, set, frozenset, deque),
        ) or hasattr(data, "__dict__") or hasattr(data, "model_dump") or hasattr(data, "dict")

        if needs_cycle_tracking:
            obj_id = id(data)
            if obj_id in _seen:
                return {"__circular_reference__": type(data).__name__}
            _seen.add(obj_id)

        if isinstance(data, dict):
            converted: Dict[str, Any] = {}
            for key, value in data.items():
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

        if isinstance(data, (list, tuple, set, frozenset, deque)):
            iterable = list(data)
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

        try:
            import numpy as np  # type: ignore

            if isinstance(data, np.generic):
                return DataProcessor.to_json_compatible(
                    data.item(),
                    _seen=_seen.copy(),
                    max_depth=max_depth,
                    _depth=_depth + 1,
                )

            if isinstance(data, np.ndarray):
                return DataProcessor.to_json_compatible(
                    data.tolist(),
                    _seen=_seen.copy(),
                    max_depth=max_depth,
                    _depth=_depth + 1,
                )
        except Exception:
            pass

        try:
            import pandas as pd  # type: ignore

            if isinstance(data, pd.DataFrame):
                return DataProcessor.to_json_compatible(
                    data.to_dict(orient="records"),
                    _seen=_seen.copy(),
                    max_depth=max_depth,
                    _depth=_depth + 1,
                )

            if isinstance(data, pd.Series):
                return DataProcessor.to_json_compatible(
                    data.to_dict(),
                    _seen=_seen.copy(),
                    max_depth=max_depth,
                    _depth=_depth + 1,
                )
        except Exception:
            pass

        if hasattr(data, "model_dump") and callable(getattr(data, "model_dump")):
            try:
                dumped = data.model_dump()
                return DataProcessor.to_json_compatible(
                    dumped,
                    _seen=_seen.copy(),
                    max_depth=max_depth,
                    _depth=_depth + 1,
                )
            except Exception:
                pass

        if hasattr(data, "dict") and callable(getattr(data, "dict")):
            try:
                dumped = data.dict()
                return DataProcessor.to_json_compatible(
                    dumped,
                    _seen=_seen.copy(),
                    max_depth=max_depth,
                    _depth=_depth + 1,
                )
            except Exception:
                pass

        if hasattr(data, "__dict__"):
            try:
                attrs = {
                    key: value
                    for key, value in vars(data).items()
                    if not key.startswith("_")
                }
                return DataProcessor.to_json_compatible(
                    attrs,
                    _seen=_seen.copy(),
                    max_depth=max_depth,
                    _depth=_depth + 1,
                )
            except Exception:
                pass

        try:
            return str(data)
        except Exception:
            return f"<unserializable {type(data).__name__}>"

    # ------------------------------------------------------------------
    # JSON serialization
    # ------------------------------------------------------------------
    @staticmethod
    def to_json(data: Any, indent: Optional[int] = None) -> str:
        """Serialize arbitrary data to JSON, coercing unsupported values."""
        try:
            normalized = DataProcessor.to_json_compatible(data)
            return json.dumps(normalized, indent=indent, ensure_ascii=False)
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
        """Set a nested value using a dot-path key."""
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
                raise ValueError("Dictionary must contain 'output_text' key for HTML conversion.")
            text = data.get("output_text", "")
        else:
            raise ValueError(f"Invalid input type: {type(data)}. Expected str or dict.")

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
        return DataProcessor.to_json_compatible(obj, _seen=_seen)
