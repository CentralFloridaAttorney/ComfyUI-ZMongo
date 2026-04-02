import base64
import datetime as _dt
import html
import json
import logging
import math
import re
import uuid
from collections import deque
from decimal import Decimal
from typing import Any, Dict, Iterable, List, MutableMapping, MutableSequence, Optional, Sequence, Tuple, Union

try:
    from bson import ObjectId
except ImportError:  # pragma: no cover
    from bson.objectid import ObjectId  # type: ignore

logger = logging.getLogger(__name__)

JsonLike = Union[Dict[str, Any], List[Any]]


class DataProcessor:
    """
    Canonical normalization and dot-path utility layer for ZMongo.

    Design goals:
    - Keep the class framework-agnostic.
    - Provide a stable dot-path model for ComfyUI node adapters.
    - Make all serialization BSON-safe and JSON-compatible.
    - Favor fail-safe behavior for workflow stability.
    - Preserve backward compatibility with existing callers where practical.
    """

    DEFAULT_SEPARATOR = "."
    DEFAULT_MAX_DEPTH = 50

    # ------------------------------------------------------------------
    # Legacy compatibility helpers
    # ------------------------------------------------------------------
    @staticmethod
    def get_entity_name(data_row: Sequence[Any]) -> Tuple[str, str]:
        last_index = DataProcessor.get_index_last_non_nan(data_row)
        entity_name = DataProcessor.get_string(data_row, 0, last_index)
        return "Entity", entity_name

    @staticmethod
    def get_index_last_non_excluded(
        name_row: Sequence[Any],
        excluded_parts: Optional[set] = None,
    ) -> Optional[int]:
        excluded_parts = excluded_parts or set()
        last_value_index = DataProcessor.get_index_last_non_nan(name_row)

        for i in range(last_value_index, -1, -1):
            try:
                value = name_row[i]
                if value not in excluded_parts:
                    return i
            except Exception as exc:  # pragma: no cover
                logger.error("Error in get_index_last_non_excluded: %s", exc)
        return None

    @staticmethod
    def get_index_last_non_nan(data: Sequence[Any]) -> int:
        for i in range(len(data) - 1, -1, -1):
            value = data[i]
            if value is not None and str(value).lower() != "nan":
                return i
        return 0

    @staticmethod
    def get_string(data: Sequence[Any], start: int, end: int) -> str:
        if not data:
            return ""
        start = max(0, int(start))
        end = min(int(end), len(data) - 1)
        if end < start:
            return ""
        return " ".join(str(part) for part in data[start : end + 1] if part is not None)

    # ------------------------------------------------------------------
    # Path helpers
    # ------------------------------------------------------------------
    @staticmethod
    def split_path(path: Any, sep: str = DEFAULT_SEPARATOR) -> List[str]:
        if path is None:
            return []
        text = str(path).strip()
        if not text:
            return []
        return [part for part in text.split(sep) if part != ""]

    @staticmethod
    def join_path(parts: Iterable[Any], sep: str = DEFAULT_SEPARATOR) -> str:
        return sep.join(str(part) for part in parts if str(part) != "")

    @staticmethod
    def is_list_index(part: Any) -> bool:
        return isinstance(part, str) and part.isdigit()

    @staticmethod
    def normalize_objectid(obj: Any) -> Any:
        return DataProcessor.to_json_compatible(obj)

    # ------------------------------------------------------------------
    # JSON/BSON-safe normalization
    # ------------------------------------------------------------------
    @staticmethod
    def to_json_compatible(
        data: Any,
        _seen: Optional[set] = None,
        *,
        max_depth: int = DEFAULT_MAX_DEPTH,
        _depth: int = 0,
    ) -> Any:
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
                return int(data) if data == data.to_integral_value() else float(data)
            except Exception:
                return str(data)

        if isinstance(data, (_dt.datetime, _dt.date, _dt.time)):
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
                safe_key = str(key)
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
                    iterable = sorted(iterable, key=repr)
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
                return DataProcessor.to_json_compatible(
                    data.model_dump(),
                    _seen=_seen.copy(),
                    max_depth=max_depth,
                    _depth=_depth + 1,
                )
            except Exception:
                pass

        if hasattr(data, "dict") and callable(getattr(data, "dict")):
            try:
                return DataProcessor.to_json_compatible(
                    data.dict(),
                    _seen=_seen.copy(),
                    max_depth=max_depth,
                    _depth=_depth + 1,
                )
            except Exception:
                pass

        if hasattr(data, "__dict__"):
            try:
                attrs = {k: v for k, v in vars(data).items() if not k.startswith("_")}
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

    @staticmethod
    def to_json(data: Any, indent: Optional[int] = None, *, sort_keys: bool = False) -> str:
        try:
            normalized = DataProcessor.to_json_compatible(data)
            return json.dumps(normalized, indent=indent, ensure_ascii=False, sort_keys=sort_keys)
        except Exception as exc:
            logger.error("Error serializing to JSON: %s", exc)
            return json.dumps({"error": str(exc)}, ensure_ascii=False)

    @staticmethod
    def from_json(raw: Any, default: Optional[Any] = None) -> Any:
        if raw is None:
            return default
        if not isinstance(raw, str):
            return raw
        text = raw.strip()
        if not text:
            return default
        try:
            return json.loads(text)
        except Exception:
            return default

    # ------------------------------------------------------------------
    # Flattening and unflattening
    # ------------------------------------------------------------------
    @staticmethod
    def flatten_json(data: Any, parent_key: str = "", sep: str = DEFAULT_SEPARATOR) -> Dict[str, Any]:
        flat: Dict[str, Any] = {}

        if isinstance(data, dict):
            if parent_key and not data:
                flat[parent_key] = {}
                return flat
            for key, value in data.items():
                new_key = f"{parent_key}{sep}{key}" if parent_key else str(key)
                flat.update(DataProcessor.flatten_json(value, new_key, sep))
            return flat

        if isinstance(data, list):
            if parent_key and not data:
                flat[parent_key] = []
                return flat
            for index, value in enumerate(data):
                new_key = f"{parent_key}{sep}{index}" if parent_key else str(index)
                flat.update(DataProcessor.flatten_json(value, new_key, sep))
            return flat

        if parent_key:
            flat[parent_key] = data
        elif not isinstance(data, (dict, list)):
            flat[""] = data
        return flat

    @staticmethod
    def flatten_dict(data: Dict[str, Any], sep: str = DEFAULT_SEPARATOR) -> Dict[str, Any]:
        return DataProcessor.flatten_json(data, sep=sep)

    @staticmethod
    def unflatten_dict(flat_data: Dict[str, Any], sep: str = DEFAULT_SEPARATOR) -> Dict[str, Any]:
        root: Dict[str, Any] = {}
        for path, value in (flat_data or {}).items():
            if not path:
                continue
            DataProcessor.set_value(root, path, value, sep=sep, create_missing=True, allow_list_growth=False)
        return root

    @staticmethod
    def sorted_flattened_keys(data: Any, sep: str = DEFAULT_SEPARATOR) -> List[str]:
        return sorted(str(k) for k in DataProcessor.flatten_json(data, sep=sep).keys())

    # ------------------------------------------------------------------
    # Dot-path access and mutation
    # ------------------------------------------------------------------
    @staticmethod
    def get_value(
        data: Union[Dict[str, Any], List[Any]],
        key: str,
        *,
        default: Any = None,
        sep: str = DEFAULT_SEPARATOR,
    ) -> Any:
        parts = DataProcessor.split_path(key, sep=sep)
        if not parts:
            return data

        value: Any = data
        for part in parts:
            if isinstance(value, dict):
                value = value.get(part, default)
            elif isinstance(value, list) and DataProcessor.is_list_index(part):
                index = int(part)
                value = value[index] if 0 <= index < len(value) else default
            else:
                return default

            if value is default:
                return default
        return value

    @staticmethod
    def path_exists(
        data: Union[Dict[str, Any], List[Any]],
        key: str,
        *,
        sep: str = DEFAULT_SEPARATOR,
    ) -> bool:
        sentinel = object()
        return DataProcessor.get_value(data, key, default=sentinel, sep=sep) is not sentinel

    @staticmethod
    def set_value(
        data: Union[MutableMapping[str, Any], MutableSequence[Any]],
        key: str,
        val: Any,
        *,
        sep: str = DEFAULT_SEPARATOR,
        create_missing: bool = True,
        allow_list_growth: bool = False,
    ) -> bool:
        if not isinstance(data, (dict, list)):
            return False

        parts = DataProcessor.split_path(key, sep=sep)
        if not parts:
            return False

        target: Any = data

        for idx, part in enumerate(parts[:-1]):
            next_part = parts[idx + 1]

            if isinstance(target, dict):
                if part not in target or target[part] is None:
                    if not create_missing:
                        return False
                    target[part] = [] if DataProcessor.is_list_index(next_part) else {}
                target = target[part]
                continue

            if isinstance(target, list) and DataProcessor.is_list_index(part):
                list_index = int(part)
                if list_index < 0:
                    return False
                if list_index >= len(target):
                    if not allow_list_growth:
                        return False
                    while len(target) <= list_index:
                        target.append([] if DataProcessor.is_list_index(next_part) else {})
                if target[list_index] is None:
                    target[list_index] = [] if DataProcessor.is_list_index(next_part) else {}
                target = target[list_index]
                continue

            return False

        last = parts[-1]

        if isinstance(target, dict):
            target[last] = val
            return True

        if isinstance(target, list) and DataProcessor.is_list_index(last):
            list_index = int(last)
            if list_index < 0:
                return False
            if list_index >= len(target):
                if not allow_list_growth:
                    return False
                while len(target) <= list_index:
                    target.append(None)
            target[list_index] = val
            return True

        return False

    @staticmethod
    def delete_value(
        data: Union[MutableMapping[str, Any], MutableSequence[Any]],
        key: str,
        *,
        sep: str = DEFAULT_SEPARATOR,
    ) -> bool:
        parts = DataProcessor.split_path(key, sep=sep)
        if not parts or not isinstance(data, (dict, list)):
            return False

        parent_path = DataProcessor.join_path(parts[:-1], sep=sep)
        last = parts[-1]
        parent = data if not parent_path else DataProcessor.get_value(data, parent_path, default=None, sep=sep)

        if isinstance(parent, dict):
            if last in parent:
                del parent[last]
                return True
            return False

        if isinstance(parent, list) and DataProcessor.is_list_index(last):
            index = int(last)
            if 0 <= index < len(parent):
                del parent[index]
                return True
        return False

    @staticmethod
    def copy_with_value(
        data: JsonLike,
        key: str,
        val: Any,
        *,
        sep: str = DEFAULT_SEPARATOR,
        create_missing: bool = True,
        allow_list_growth: bool = False,
    ) -> JsonLike:
        clone = DataProcessor.deep_copy_jsonish(data)
        DataProcessor.set_value(
            clone,
            key,
            val,
            sep=sep,
            create_missing=create_missing,
            allow_list_growth=allow_list_growth,
        )
        return clone

    @staticmethod
    def deep_copy_jsonish(data: Any) -> Any:
        return json.loads(DataProcessor.to_json(data))

    # ------------------------------------------------------------------
    # Text / HTML helpers
    # ------------------------------------------------------------------
    @staticmethod
    def clean_output_text(text: str) -> str:
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

    @staticmethod
    def safe_preview_text(value: Any, max_length: int = 200) -> str:
        if value is None:
            return ""
        if isinstance(value, str):
            text = value
        elif isinstance(value, (dict, list)):
            text = DataProcessor.to_json(value)
        else:
            text = str(value)
        text = text.replace("\n", " ").strip()
        return text if len(text) <= max_length else text[:max_length] + "..."

    # ------------------------------------------------------------------
    # Object conversion helpers
    # ------------------------------------------------------------------
    @staticmethod
    def convert_object_to_json(obj: Any, _seen: Optional[set] = None) -> Any:
        return DataProcessor.to_json_compatible(obj, _seen=_seen)
