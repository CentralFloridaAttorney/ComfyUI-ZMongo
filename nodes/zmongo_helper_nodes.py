from __future__ import annotations

import json
import re
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

try:
    from .generic_helpers import (
        AlwaysDirtyMixin,
        _as_comfy_list,
        _dirty_token,
        _ensure_payload_dict,
        _indexed_list_text,
        _json_text,
        safe_get_by_path,
    )
except Exception:  # pragma: no cover - allows syntax checking outside ComfyUI package
    class AlwaysDirtyMixin:
        @classmethod
        def IS_CHANGED(cls, *args, **kwargs):
            return float('nan')

    def _as_comfy_list(value):
        return value if isinstance(value, list) else ([] if value is None else [value])

    def _dirty_token(*parts):
        return "::".join(str(p) for p in parts if p is not None)

    def _ensure_payload_dict(value):
        return value if isinstance(value, dict) else {"success": False, "message": str(value), "data": value}

    def _indexed_list_text(items):
        return json.dumps([f"{i}: {x}" for i, x in enumerate(items)], indent=2)

    def _json_text(value):
        try:
            return json.dumps(value, indent=2, ensure_ascii=False, default=str)
        except Exception:
            return str(value)

    def safe_get_by_path(data, path, default=None):
        return _get_by_dot_path(data, path, default)

try:  # Newer ComfyUI-ZMongo helper API.
    from .generic_helpers import _session_get_doc as _GENERIC_SESSION_GET_DOC  # type: ignore
except Exception:  # pragma: no cover
    _GENERIC_SESSION_GET_DOC = None

try:
    from .generic_helpers import _session_api_request as _GENERIC_SESSION_API_REQUEST  # type: ignore
except Exception:  # pragma: no cover
    _GENERIC_SESSION_API_REQUEST = None


# -----------------------------------------------------------------------------
# Shared helpers
# -----------------------------------------------------------------------------

def _unwrap_scalar(value: Any, default: Any = None) -> Any:
    if isinstance(value, list):
        if not value:
            return default
        return value[0]
    return value if value is not None else default


def _normalize_items(value: Any) -> list[Any]:
    if value is None:
        return []
    if isinstance(value, tuple):
        value = list(value)
    if isinstance(value, list):
        if len(value) == 1 and isinstance(value[0], (list, tuple)):
            return list(value[0])
        return value
    return [value]


def _item_to_string(value: Any) -> str:
    if isinstance(value, (dict, list, tuple)):
        return _json_text(value)
    return str(value if value is not None else "")


def _clean_items(raw_items: Any) -> list[str]:
    return [
        _item_to_string(item).strip()
        for item in _normalize_items(raw_items)
        if _item_to_string(item).strip()
    ]


def _coerce_bool(value: Any, default: bool = False) -> bool:
    value = _unwrap_scalar(value, value)
    if isinstance(value, bool):
        return value
    if value is None:
        return default
    if isinstance(value, (int, float)):
        return bool(value)
    if isinstance(value, str):
        text = value.strip().lower()
        if text in {"true", "1", "yes", "y", "on"}:
            return True
        if text in {"false", "0", "no", "n", "off", ""}:
            return default if text == "" else False
    return bool(value)


def _coerce_int(value: Any, default: int = 0) -> int:
    value = _unwrap_scalar(value, value)
    if value is None:
        return default
    if isinstance(value, str) and not value.strip():
        return default
    try:
        return int(float(value))
    except Exception:
        return default


def _timestamp(format_name: str = "iso_utc") -> str:
    fmt = str(format_name or "iso_utc").strip().lower()
    if fmt == "unix":
        return str(time.time())
    if fmt == "local_iso":
        return datetime.now().isoformat(timespec="seconds")
    return datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def _safe_state_name(name: str, fallback: str = "default_loop") -> str:
    clean = re.sub(r"[^a-zA-Z0-9_.-]+", "_", str(name or "").strip()).strip("._")
    return clean or fallback


def _state_dir() -> Path:
    root = Path(__file__).resolve().parent
    out = root / "local_store" / "loop_state"
    out.mkdir(parents=True, exist_ok=True)
    return out


def _state_path(name: str) -> Path:
    return _state_dir() / f"{_safe_state_name(name)}.json"


def _load_state(name: str) -> dict[str, Any]:
    path = _state_path(name)
    if not path.exists():
        return {}
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def _save_state(name: str, state: dict[str, Any]) -> None:
    path = _state_path(name)
    path.write_text(_json_text(state), encoding="utf-8")


def _parse_json_object(text: Any, default: Optional[dict[str, Any]] = None) -> dict[str, Any]:
    if isinstance(text, dict):
        return text
    if text is None:
        return default or {}
    raw = str(text).strip()
    if not raw:
        return default or {}
    try:
        value = json.loads(raw)
        return value if isinstance(value, dict) else (default or {})
    except Exception:
        return default or {}


def _get_by_dot_path(data: Any, path: str, default: Any = None) -> Any:
    if not path:
        return data
    current = data
    for raw_part in str(path).split("."):
        part = raw_part.strip()
        if not part:
            continue
        if isinstance(current, dict):
            if part in current:
                current = current[part]
                continue
            return default
        if isinstance(current, list):
            try:
                index = int(part)
            except Exception:
                return default
            if 0 <= index < len(current):
                current = current[index]
                continue
            return default
        return default
    return current


def _extract_document_from_response(payload: Any) -> dict[str, Any]:
    if not isinstance(payload, dict):
        return {}

    data = payload.get("data")
    if isinstance(data, dict):
        for key in ("document", "doc", "result"):
            value = data.get(key)
            if isinstance(value, dict):
                return value

        docs = data.get("documents") or data.get("docs") or data.get("results") or data.get("items")
        if isinstance(docs, list) and docs and isinstance(docs[0], dict):
            return docs[0]

        if any(key in data for key in ("_id", "document_id", "id", "document_text", "text")):
            return data

    for key in ("document", "doc", "result"):
        value = payload.get(key)
        if isinstance(value, dict):
            return value

    docs = payload.get("documents") or payload.get("docs") or payload.get("results") or payload.get("items")
    if isinstance(docs, list) and docs and isinstance(docs[0], dict):
        return docs[0]

    if any(key in payload for key in ("_id", "document_id", "id", "document_text", "text")):
        return payload

    return {}


def _session_get_document(session: Any, collection_name: str, document_id: str) -> tuple[dict[str, Any], dict[str, Any]]:
    """
    Load a document through the real ComfyUI-ZMongo session contract.

    This deliberately does NOT call session.get_value().  ZMongoApiSession does
    not expose get_value; read the full document through get_doc/API routes and
    resolve flattened paths locally.
    """
    collection = str(collection_name or "").strip()
    clean_id = str(document_id or "").strip()

    if not session:
        payload = _ensure_payload_dict({
            "success": False,
            "message": "No session was provided.",
            "data": {"collection_name": collection, "document_id": clean_id},
            "error": {"msg": "missing_session"},
            "status_code": 400,
        })
        return {}, payload

    attempts: list[dict[str, Any]] = []

    # 1. Preferred generic helper used by existing document nodes.
    if callable(_GENERIC_SESSION_GET_DOC):
        try:
            payload = _ensure_payload_dict(_GENERIC_SESSION_GET_DOC(session, collection, clean_id, cache=False))
            document = _extract_document_from_response(payload)
            if document:
                return document, payload
            attempts.append({"method": "_session_get_doc", "payload": payload})
        except Exception as exc:
            attempts.append({"method": "_session_get_doc", "error": str(exc), "type": exc.__class__.__name__})

    # 2. ZMongoApiSession / local session direct methods, if present.
    get_doc = getattr(session, "get_doc", None)
    if callable(get_doc):
        call_variants = [
            dict(collection=collection, document_id=clean_id, cache=False),
            dict(collection_name=collection, document_id=clean_id, cache=False),
            dict(coll=collection, document_id=clean_id, cache=False),
            dict(collection=collection, id=clean_id, cache=False),
            dict(collection=collection, document_id=clean_id),
            dict(collection_name=collection, document_id=clean_id),
            dict(coll=collection, doc_id=clean_id),
        ]
        for kwargs in call_variants:
            try:
                payload = _ensure_payload_dict(get_doc(**kwargs))
                document = _extract_document_from_response(payload)
                if document:
                    return document, payload
                attempts.append({"method": "get_doc", "kwargs": kwargs, "payload": payload})
            except TypeError:
                continue
            except Exception as exc:
                attempts.append({"method": "get_doc", "kwargs": kwargs, "error": str(exc), "type": exc.__class__.__name__})

    # 3. API route fallback.  The backend exposes GET /api/doc/<coll>/<doc_id>.
    if callable(_GENERIC_SESSION_API_REQUEST):
        try:
            import urllib.parse
            q_coll = urllib.parse.quote(collection, safe="")
            q_id = urllib.parse.quote(clean_id, safe="")
            payload = _ensure_payload_dict(_GENERIC_SESSION_API_REQUEST(session, "GET", f"/api/doc/{q_coll}/{q_id}"))
            document = _extract_document_from_response(payload)
            if document:
                return document, payload
            attempts.append({"method": "_session_api_request.GET /api/doc", "payload": payload})
        except Exception as exc:
            attempts.append({"method": "_session_api_request.GET /api/doc", "error": str(exc), "type": exc.__class__.__name__})

    # 4. Query fallback.
    query_docs = getattr(session, "query_docs", None)
    if callable(query_docs):
        for query in ({"_id": clean_id}, {"document_id": clean_id}, {"id": clean_id}):
            call_variants = [
                dict(collection=collection, query=query, many=False, limit=1, skip=0, cache=False),
                dict(collection_name=collection, query=query, many=False, limit=1, skip=0, cache=False),
                dict(coll=collection, query=query, many=False, limit=1, skip=0, cache=False),
            ]
            for kwargs in call_variants:
                try:
                    payload = _ensure_payload_dict(query_docs(**kwargs))
                    document = _extract_document_from_response(payload)
                    if document:
                        return document, payload
                    attempts.append({"method": "query_docs", "kwargs": kwargs, "payload": payload})
                except TypeError:
                    continue
                except Exception as exc:
                    attempts.append({"method": "query_docs", "kwargs": kwargs, "error": str(exc), "type": exc.__class__.__name__})

    payload = _ensure_payload_dict({
        "success": False,
        "message": "Could not load document from session.",
        "data": {
            "collection_name": collection,
            "document_id": clean_id,
            "attempts": attempts[-10:],
        },
        "error": {"msg": "document_not_found"},
        "status_code": 404,
    })
    return {}, payload



# -----------------------------------------------------------------------------
# 99 Helper nodes
# -----------------------------------------------------------------------------

class ZMongoApiSelectNthItemNode(AlwaysDirtyMixin):
    """Generic item selector for any ComfyUI list output."""

    @classmethod
    def INPUT_TYPES(cls):
        return {
            "required": {
                "items_list": ("*",),
                "index": ("INT", {"default": 0, "min": -1000000, "max": 1000000}),
                "fallback": ("STRING", {"default": ""}),
                "selection": (
                    "STRING",
                    {
                        "default": "",
                        "tooltip": "Examples: 0, 2-7, 7-2, 0,2,5, 0,2-5,9, *, all, first, last, -1. Empty uses index.",
                    },
                ),
                "mode": (["auto", "single", "range", "series", "all"], {"default": "auto"}),
                "include_end": ("BOOLEAN", {"default": True}),
                "dedupe": ("BOOLEAN", {"default": True}),
            }
        }

    RETURN_TYPES = ("STRING", "STRING", "*", "STRING", "STRING", "INT")
    RETURN_NAMES = ("item", "status", "selected_items", "selected_json", "indexed_selected_items", "count")
    OUTPUT_IS_LIST = (False, False, True, False, False, False)
    FUNCTION = "select_nth_item"
    CATEGORY = "ZMongo/99 Helpers"
    INPUT_IS_LIST = True

    @staticmethod
    def _normalize_index(index: Any, count: int) -> Optional[int]:
        if count <= 0:
            return None
        raw = _coerce_int(index, 0)
        if raw < 0:
            raw = count + raw
        return max(0, min(raw, count - 1))

    @classmethod
    def _parse_selection_indexes(
        cls,
        *,
        selection: str,
        index: Any,
        mode: str,
        count: int,
        include_end: bool,
    ) -> tuple[list[int], list[str]]:
        warnings: list[str] = []
        if count <= 0:
            return [], warnings

        clean_mode = str(mode or "auto").strip().lower()
        expression = str(selection or "").strip().lower()

        if clean_mode == "all" or expression in {"*", "all"}:
            return list(range(count)), warnings

        if clean_mode in {"single", "range"} and not expression:
            selected = cls._normalize_index(index, count)
            return ([selected] if selected is not None else []), warnings

        if not expression:
            selected = cls._normalize_index(index, count)
            return ([selected] if selected is not None else []), warnings

        aliases = {"first": "0", "last": "-1"}
        expression = aliases.get(expression, expression)
        indexes: list[int] = []

        for part in [p.strip() for p in expression.split(",") if p.strip()]:
            part = aliases.get(part, part)
            if part in {"*", "all"}:
                indexes.extend(range(count))
                continue

            if "-" in part[1:]:
                split_at = part.find("-", 1)
                left = part[:split_at].strip()
                right = part[split_at + 1 :].strip()
                try:
                    start = cls._normalize_index(int(left), count)
                    end = cls._normalize_index(int(right), count)
                except Exception:
                    warnings.append(f"Ignored invalid range {part!r}.")
                    continue
                if start is None or end is None:
                    continue
                step = 1 if start <= end else -1
                final = end + step if include_end else end
                indexes.extend(range(start, final, step))
                continue

            try:
                one = cls._normalize_index(int(part), count)
            except Exception:
                warnings.append(f"Ignored invalid index {part!r}.")
                continue
            if one is not None:
                indexes.append(one)

        return indexes, warnings

    @staticmethod
    def _dedupe_indexes(indexes: list[int]) -> list[int]:
        seen: set[int] = set()
        output: list[int] = []
        for value in indexes:
            if value in seen:
                continue
            seen.add(value)
            output.append(value)
        return output

    def select_nth_item(
        self,
        items_list,
        index,
        fallback,
        selection="",
        mode="auto",
        include_end=True,
        dedupe=True,
    ):
        cleaned = _clean_items(items_list)
        fallback_value = str(_unwrap_scalar(fallback, "") or "")
        index_value = _unwrap_scalar(index, 0)
        selection_value = str(_unwrap_scalar(selection, "") or "")
        mode_value = str(_unwrap_scalar(mode, "auto") or "auto")
        include_end_value = _coerce_bool(_unwrap_scalar(include_end, True), True)
        dedupe_value = _coerce_bool(_unwrap_scalar(dedupe, True), True)

        if not cleaned:
            selected_items = [fallback_value] if fallback_value else []
            return (
                fallback_value,
                "Input list was empty; returned fallback." if fallback_value else "Input list was empty.",
                _as_comfy_list(selected_items),
                _json_text(selected_items),
                _indexed_list_text(selected_items),
                len(selected_items),
            )

        indexes, warnings = self._parse_selection_indexes(
            selection=selection_value,
            index=index_value,
            mode=mode_value,
            count=len(cleaned),
            include_end=include_end_value,
        )
        if dedupe_value:
            indexes = self._dedupe_indexes(indexes)

        selected_items = [cleaned[i] for i in indexes if 0 <= i < len(cleaned)]
        if not selected_items and fallback_value:
            selected_items = [fallback_value]

        first = selected_items[0] if selected_items else ""
        status = f"Selected {len(selected_items)} item(s) from {len(cleaned)} total."
        if indexes:
            status += f" First index={indexes[0]}."
        if warnings:
            status += " " + " ".join(warnings)

        return (
            first,
            status,
            _as_comfy_list(selected_items),
            _json_text(selected_items),
            _indexed_list_text(selected_items),
            len(selected_items),
        )


class ZMongoRecordLoopManagerNode(AlwaysDirtyMixin):
    """Self-advancing loop manager for document ids or arbitrary records."""

    @classmethod
    def INPUT_TYPES(cls):
        return {
            "required": {
                "items_list": ("*",),
                "index": ("INT", {"default": 0, "min": 0, "max": 1000000}),
                "wrap": ("BOOLEAN", {"default": False}),
                "advance_on_execute": ("BOOLEAN", {"default": True}),
                "state_name": ("STRING", {"default": "default_record_loop"}),
                "reset_state": ("BOOLEAN", {"default": False}),
                "timestamp_format": (["iso_utc", "unix", "local_iso"], {"default": "iso_utc"}),
            },
            "optional": {
                "refresh_token": ("STRING", {"default": ""}),
            },
        }

    RETURN_TYPES = ("STRING", "STRING", "BOOLEAN", "INT", "INT", "INT", "STRING", "*", "STRING", "STRING", "INT", "STRING", "STRING", "STRING")
    RETURN_NAMES = ("item", "document_id_link", "done", "index", "item_count", "remaining_count", "reviewed_timestamp", "selected_items", "state_json", "status", "next_index", "visible_index", "visible_next_index", "progress_text")
    OUTPUT_IS_LIST = (False, False, False, False, False, False, False, True, False, False, False, False, False, False)
    FUNCTION = "loop_record"
    CATEGORY = "ZMongo/99 Helpers"
    INPUT_IS_LIST = True
    OUTPUT_NODE = True

    def loop_record(
        self,
        items_list,
        index,
        wrap,
        advance_on_execute,
        state_name,
        reset_state,
        timestamp_format,
        refresh_token="",
    ):
        cleaned = _clean_items(items_list)
        count = len(cleaned)
        widget_index = max(0, _coerce_int(_unwrap_scalar(index, 0), 0))
        wrap_value = _coerce_bool(_unwrap_scalar(wrap, False), False)
        advance_value = _coerce_bool(_unwrap_scalar(advance_on_execute, True), True)
        reset_value = _coerce_bool(_unwrap_scalar(reset_state, False), False)
        state_base = str(_unwrap_scalar(state_name, "default_record_loop") or "default_record_loop")
        state_key = _safe_state_name(state_base, "default_record_loop")
        ts = _timestamp(str(_unwrap_scalar(timestamp_format, "iso_utc") or "iso_utc"))
        refresh_value = str(_unwrap_scalar(refresh_token, "") or "")

        prior = {} if reset_value else _load_state(state_key)
        persisted_index = _coerce_int(prior.get("next_index"), widget_index)
        current_index = widget_index if reset_value or not prior else max(0, persisted_index)

        if count <= 0:
            state = {
                "done": True,
                "state_name": state_key,
                "index": current_index,
                "next_index": current_index,
                "item_count": 0,
                "timestamp": ts,
                "refresh": _dirty_token("record_loop_empty", refresh_value, state_key),
            }
            if advance_value or reset_value:
                _save_state(state_key, state)
            return ("", "", True, current_index, 0, 0, ts, _as_comfy_list([]), _json_text(state), "Record loop is empty.", current_index, "0 / 0", "0 / 0", "No items to process.")

        done = current_index >= count
        effective_index = current_index
        if done and wrap_value:
            effective_index = current_index % count
            done = False

        if done:
            item = ""
            next_index = current_index
            status = f"Loop complete: index={current_index}, item_count={count}. Reset state to start again."
        else:
            item = cleaned[effective_index]
            next_index = effective_index + 1
            if wrap_value and next_index >= count:
                next_index = 0
            status = f"Selected record {effective_index + 1}/{count}; next_index={next_index}."

        remaining = max(0, count - next_index) if not wrap_value else count
        state = {
            "done": bool(done),
            "state_name": state_key,
            "index": int(effective_index if not done else current_index),
            "current_index": int(current_index),
            "next_index": int(next_index),
            "item_count": int(count),
            "item": item,
            "timestamp": ts,
            "updated_at_unix": time.time(),
            "refresh": _dirty_token("record_loop", refresh_value, state_key, current_index, count),
        }
        if advance_value or reset_value:
            _save_state(state_key, state)
            if not done:
                status += " Persisted next_index for the next run."

        visible_current = int(effective_index if not done else current_index)
        visible_next = int(next_index)
        progress = f"item_index={visible_current}; next_index={visible_next}; item={visible_current + 1 if not done else count}/{count}; done={bool(done)}; item_value={item}"
        selected_items = [item] if item else []
        return (
            item,
            item,
            bool(done),
            visible_current,
            int(count),
            int(remaining),
            ts,
            _as_comfy_list(selected_items),
            _json_text(state),
            status,
            visible_next,
            f"{visible_current} / {max(count - 1, 0)}",
            f"{visible_next} / {max(count - 1, 0)}",
            progress,
        )



class ZMongoApiJsonPickNode(AlwaysDirtyMixin):
    @classmethod
    def INPUT_TYPES(cls):
        return {
            "required": {
                "json_text": ("STRING", {"default": "{}", "multiline": True}),
                "path": ("STRING", {"default": "data"}),
                "fallback": ("STRING", {"default": ""}),
            }
        }

    RETURN_TYPES = ("STRING",)
    RETURN_NAMES = ("value",)
    FUNCTION = "json_pick"
    CATEGORY = "ZMongo/99 Helpers"

    def json_pick(self, json_text: str, path: str, fallback: str):
        try:
            data = json.loads(json_text or "{}")
            current: Any = data
            for part in (path or "").split("."):
                if not part:
                    continue
                if isinstance(current, dict):
                    current = current[part]
                elif isinstance(current, list):
                    current = current[int(part)]
                else:
                    return (fallback or "",)
            if isinstance(current, (dict, list)):
                return (_json_text(current),)
            return ("" if current is None else str(current),)
        except Exception:
            return (fallback or "",)


# -----------------------------------------------------------------------------
# ComfyUI mappings
# -----------------------------------------------------------------------------

NODE_CLASS_MAPPINGS = {
    "ZMongoApiSelectNthItemNode": ZMongoApiSelectNthItemNode,
    "ZMongoRecordLoopManagerNode": ZMongoRecordLoopManagerNode,
    "ZMongoApiJsonPickNode": ZMongoApiJsonPickNode,
}

NODE_DISPLAY_NAME_MAPPINGS = {
    "ZMongoApiSelectNthItemNode": "99 Select Nth Item",
    "ZMongoRecordLoopManagerNode": "99 Record Loop Manager",
    "ZMongoApiJsonPickNode": "99 JSON Pick",
}

__all__ = ["NODE_CLASS_MAPPINGS", "NODE_DISPLAY_NAME_MAPPINGS"]