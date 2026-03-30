import json
import logging
import random
import threading
from typing import Any, Dict, List, Optional

import requests
from bson import json_util
from bson.objectid import ObjectId
from pymongo import MongoClient
from pymongo.errors import DuplicateKeyError
from .zmongo_toolbag.safe_result import SafeResult
from .zmongo_toolbag.zmongo import ZMongo
from .zmongo_toolbag.data_processor import DataProcessor
from .zmongo_toolbag.zembedder import ZEmbedder

logger = logging.getLogger(__name__)


_CONFIG_LOCK = threading.Lock()
_ZMONGO_SINGLETON: Optional[ZMongo] = None
_CURRENT_URI: str = "mongodb://127.0.0.1:27017"
_CURRENT_DB: str = "test"


def _safe_json(obj: Any) -> str:
    try:
        if isinstance(obj, SafeResult):
            return DataProcessor.to_json(obj.to_dict(), indent=2)
        return DataProcessor.to_json(obj, indent=2)
    except Exception:
        try:
            return json.dumps(
                json.loads(json_util.dumps(obj)),
                indent=2,
                ensure_ascii=False,
                default=str,
            )
        except Exception as exc:
            return json.dumps({"error": f"Serialization failed: {exc}"}, indent=2)


def _coerce_records(data_json: str) -> SafeResult:
    if not data_json or not data_json.strip():
        return SafeResult.fail("No data provided")

    try:
        data = json.loads(data_json)
    except Exception as exc:
        return SafeResult.fail(f"Invalid JSON: {exc}")

    if isinstance(data, list):
        return SafeResult.ok(data)
    if isinstance(data, dict):
        return SafeResult.ok([data])
    return SafeResult.fail("JSON payload must be an object or an array of objects")


def _build_summary_from_hits(hits: List[Dict[str, Any]]) -> str:
    if not hits:
        return "No matching documents found."

    lines: List[str] = []
    for index, hit in enumerate(hits, start=1):
        doc = hit.get("document") or {}
        score = hit.get("retrieval_score", 0.0)
        doc_id = doc.get("_id", "")
        title = (
            doc.get("title")
            or doc.get("name")
            or doc.get("username")
            or doc.get("email")
            or doc.get("text")
            or doc.get("content")
            or ""
        )
        if isinstance(title, str):
            title = title.strip().replace("\n", " ")
            if len(title) > 100:
                title = title[:100] + "..."
        else:
            title = str(title)
        lines.append(f"{index}. score={float(score):.4f} | _id={doc_id} | {title}")
    return "\n".join(lines)


def _get_zmongo(uri: Optional[str] = None, db_name: Optional[str] = None) -> ZMongo:
    global _ZMONGO_SINGLETON, _CURRENT_URI, _CURRENT_DB

    wanted_uri = uri or _CURRENT_URI
    wanted_db = db_name or _CURRENT_DB

    with _CONFIG_LOCK:
        needs_new = (
            _ZMONGO_SINGLETON is None
            or _CURRENT_URI != wanted_uri
            or _CURRENT_DB != wanted_db
        )
        if needs_new:
            old_instance = _ZMONGO_SINGLETON
            _CURRENT_URI = wanted_uri
            _CURRENT_DB = wanted_db
            _ZMONGO_SINGLETON = ZMongo(uri=wanted_uri, db_name=wanted_db)
            if old_instance is not None:
                try:
                    old_instance.close()
                except Exception:
                    logger.debug("Failed to close prior ZMongo instance", exc_info=True)
        return _ZMONGO_SINGLETON


class ZMongoAPIMixin:
    """Shared API/local access helpers for ZMongo nodes."""

    API_COMMON_INPUTS = {
        "zmongo_api_url": ("STRING", {"default": ""}),
        "zmongo_api_key": ("STRING", {"default": "", "multiline": False}),
        "use_api": ("BOOLEAN", {"default": False}),
        "api_timeout_sec": ("INT", {"default": 10, "min": 1, "max": 120}),
    }

    @staticmethod
    def _parse_json_object(raw: str, field_name: str) -> Dict[str, Any]:
        text = (raw or "").strip()
        if not text:
            return {}
        try:
            parsed = json.loads(text)
        except Exception as exc:
            raise ValueError(f"{field_name} is not valid JSON: {exc}") from exc
        if not isinstance(parsed, dict):
            raise ValueError(f"{field_name} must be a JSON object")
        return parsed

    @staticmethod
    def _normalize_sort(sort_field: str, sort_direction: str):
        direction = 1 if sort_direction == "ascending" else -1
        field = (sort_field or "").strip() or "_id"
        return [(field, direction)]

    @staticmethod
    def _build_auth_headers(api_key: str) -> Dict[str, str]:
        headers = {"Content-Type": "application/json"}
        if api_key:
            headers["Authorization"] = f"Bearer {api_key}"
        return headers

    def _post_api(self, api_url: str, api_key: str, path: str, payload: Dict[str, Any], timeout_sec: int) -> Dict[str, Any]:
        if not api_url or not api_url.strip():
            raise ValueError("zmongo_api_url is required when use_api=True")

        url = api_url.rstrip("/") + path
        headers = self._build_auth_headers(api_key)

        response = requests.post(url, json=payload, headers=headers, timeout=int(timeout_sec))
        if response.status_code != 200:
            raise RuntimeError(f"API request failed: {response.status_code} {response.text}")

        data = response.json()
        if not data.get("success", False):
            raise RuntimeError(f"API error: {data}")
        return data

    def _fetch_record_by_index_api(
        self,
        api_url: str,
        api_key: str,
        database_name: str,
        collection_name: str,
        select_index: int,
        sort_field: str,
        sort_direction: str,
        mongo_filter: Dict[str, Any],
        projection: Dict[str, Any],
        timeout_sec: int,
    ) -> Dict[str, Any]:
        payload = {
            "database_name": database_name,
            "collection_name": collection_name,
            "index": int(select_index),
            "sort": {
                "field": sort_field or "_id",
                "direction": sort_direction,
            },
            "filter": mongo_filter or {},
            "projection": projection or {},
        }
        return self._post_api(
            api_url=api_url,
            api_key=api_key,
            path="/record/by_index",
            payload=payload,
            timeout_sec=timeout_sec,
        )

    def _fetch_records_range_api(
        self,
        api_url: str,
        api_key: str,
        database_name: str,
        collection_name: str,
        start_index: int,
        end_index: int,
        step: int,
        sort_field: str,
        sort_direction: str,
        mongo_filter: Dict[str, Any],
        projection: Dict[str, Any],
        timeout_sec: int,
    ) -> Dict[str, Any]:
        payload = {
            "database_name": database_name,
            "collection_name": collection_name,
            "start_index": int(start_index),
            "end_index": int(end_index),
            "step": int(step),
            "sort": {
                "field": sort_field or "_id",
                "direction": sort_direction,
            },
            "filter": mongo_filter or {},
            "projection": projection or {},
        }
        return self._post_api(
            api_url=api_url,
            api_key=api_key,
            path="/records/range",
            payload=payload,
            timeout_sec=timeout_sec,
        )

    def _fetch_collection_preview_api(
        self,
        api_url: str,
        api_key: str,
        database_name: str,
        collection_name: str,
        limit: int,
        select_index: int,
        timeout_sec: int,
    ) -> Dict[str, Any]:
        payload = {
            "database_name": database_name,
            "collection_name": collection_name,
            "limit": int(limit),
            "select_index": int(select_index),
        }
        return self._post_api(
            api_url=api_url,
            api_key=api_key,
            path="/collection/preview",
            payload=payload,
            timeout_sec=timeout_sec,
        )

class ZMongoPromptDemoNode(ZMongoAPIMixin):
    """
    Standalone demo node for real-database prompt retrieval.

    Purpose:
    - Connect to a real MongoDB collection
    - Fetch one record by 1-based index using a stable sort
    - Extract prompt text from a selected field path with fallbacks
    - Return prompt text plus useful demo metadata

    Intended use:
    - prompt_text -> CLIPTextEncode (positive)
    - summary / selected_record_json -> PreviewAny
    """

    @classmethod
    def INPUT_TYPES(cls):
        return {
            "required": {
                "mongo_uri": ("STRING", {"default": "mongodb://127.0.0.1:27017"}),
                "database_name": ("STRING", {"default": "test"}),
                "collection_name": ("STRING", {"default": "ocr_docs"}),
                "select_index": ("INT", {"default": 1, "min": 1, "max": 100000000}),
                "sort_field": ("STRING", {"default": "_id"}),
                "sort_direction": (["ascending", "descending"],),
                "field_path": ("STRING", {"default": "text"}),
                "fallback_field_paths": ("STRING", {
                    "default": "text\ndoc_text\ndocument.text\nprompt",
                    "multiline": True,
                }),
                "filter_json": ("STRING", {"default": "{}", "multiline": True}),
                "projection_json": ("STRING", {"default": "{}", "multiline": True}),
                "strip_prompt": ("BOOLEAN", {"default": True}),
                "max_prompt_length": ("INT", {"default": 2000, "min": 0, "max": 100000}),
                "refresh_nonce": ("INT", {"default": 0, "min": 0, "max": 999999999}),
            }
        }

    RETURN_TYPES = (
        "STRING",  # prompt_text
        "STRING",  # selected_record_json
        "STRING",  # summary
        "INT",     # selected_index_out
        "INT",     # total_count
        "STRING",  # record_id
    )
    RETURN_NAMES = (
        "prompt_text",
        "selected_record_json",
        "summary",
        "selected_index_out",
        "total_count",
        "record_id",
    )
    FUNCTION = "fetch_demo_prompt"
    CATEGORY = "ZMongo/Demo"

    @staticmethod
    def _parse_json_object(raw: str, field_name: str) -> Dict[str, Any]:
        text = (raw or "").strip()
        if not text:
            return {}
        try:
            parsed = json.loads(text)
        except Exception as exc:
            raise ValueError(f"{field_name} is not valid JSON: {exc}") from exc
        if not isinstance(parsed, dict):
            raise ValueError(f"{field_name} must be a JSON object")
        return parsed

    @staticmethod
    def _normalize_sort(sort_field: str, sort_direction: str):
        direction = 1 if sort_direction == "ascending" else -1
        field = (sort_field or "").strip() or "_id"
        return [(field, direction)]

    @staticmethod
    def _candidate_paths(primary: str, fallback_blob: str) -> List[str]:
        paths: List[str] = []
        if primary and primary.strip():
            paths.append(primary.strip())
        for line in (fallback_blob or "").splitlines():
            value = line.strip()
            if value and value not in paths:
                paths.append(value)
        return paths

    @staticmethod
    def _extract_prompt_from_record(
        record: Dict[str, Any],
        paths: List[str],
        strip_prompt: bool,
        max_prompt_length: int,
    ) -> (str, str):
        for path in paths:
            try:
                value = DataProcessor.get_value(record, path)
            except Exception:
                value = None

            if value is None:
                continue

            if isinstance(value, (dict, list)):
                text = _safe_json(value)
            else:
                text = str(value)

            if strip_prompt:
                text = text.strip()

            if max_prompt_length > 0 and len(text) > max_prompt_length:
                text = text[:max_prompt_length]

            return text, path

        return "", ""

    @staticmethod
    def _record_id(record: Dict[str, Any]) -> str:
        value = record.get("_id")
        return "" if value is None else str(value)

    @staticmethod
    def _short_preview(text: str, length: int = 180) -> str:
        preview = (text or "").replace("\n", " ").strip()
        if len(preview) > length:
            return preview[:length] + "..."
        return preview

    def fetch_demo_prompt(
        self,
        mongo_uri,
        database_name,
        collection_name,
        select_index,
        sort_field,
        sort_direction,
        field_path,
        fallback_field_paths,
        filter_json,
        projection_json,
        strip_prompt,
        max_prompt_length,
        refresh_nonce,
    ):
        _ = refresh_nonce
        client = None
        try:
            client = MongoClient(mongo_uri, serverSelectionTimeoutMS=5000)
            client.admin.command("ping")

            collection = client[database_name][collection_name]
            mongo_filter = self._parse_json_object(filter_json, "filter_json")
            projection = self._parse_json_object(projection_json, "projection_json")
            sort_spec = self._normalize_sort(sort_field, sort_direction)

            total_count = collection.count_documents(mongo_filter)
            if total_count == 0:
                empty_payload = {
                    "status": "empty",
                    "database_name": database_name,
                    "collection_name": collection_name,
                    "filter_json": mongo_filter,
                }
                return (
                    "",
                    json.dumps(empty_payload, indent=2),
                    f"No records found in {database_name}.{collection_name}",
                    0,
                    0,
                    "",
                )

            bounded_index = max(1, min(int(select_index), int(total_count)))

            cursor = collection.find(mongo_filter, projection or None).sort(sort_spec).skip(bounded_index - 1).limit(1)
            selected_record = next(cursor, None)

            if selected_record is None:
                error_payload = {
                    "error": "Selected record could not be retrieved",
                    "database_name": database_name,
                    "collection_name": collection_name,
                    "selected_index": bounded_index,
                }
                return (
                    "",
                    json.dumps(error_payload, indent=2),
                    "Selected record could not be retrieved.",
                    int(bounded_index),
                    int(total_count),
                    "",
                )

            paths = self._candidate_paths(field_path, fallback_field_paths)
            prompt_text, used_path = self._extract_prompt_from_record(
                record=selected_record,
                paths=paths,
                strip_prompt=strip_prompt,
                max_prompt_length=max_prompt_length,
            )

            record_copy = dict(selected_record)
            record_copy["_selected_index"] = int(bounded_index)
            record_copy["_database_name"] = str(database_name)
            record_copy["_collection_name"] = str(collection_name)
            record_copy["_field_path_used"] = used_path

            record_id = self._record_id(record_copy)

            if not prompt_text:
                summary = (
                    f"Demo prompt fetch succeeded, but no prompt text was found.\n"
                    f"Collection: {database_name}.{collection_name}\n"
                    f"Selected index: {bounded_index} / {total_count}\n"
                    f"Record _id: {record_id}\n"
                    f"Tried paths: {', '.join(paths) if paths else '(none)'}"
                )
                return (
                    "",
                    _safe_json(record_copy),
                    summary,
                    int(bounded_index),
                    int(total_count),
                    record_id,
                )

            summary = (
                f"Real DB prompt demo\n"
                f"Collection: {database_name}.{collection_name}\n"
                f"Selected index: {bounded_index} / {total_count}\n"
                f"Record _id: {record_id}\n"
                f"Prompt path used: {used_path or '(none)'}\n"
                f"Prompt preview: {self._short_preview(prompt_text)}"
            )

            return (
                prompt_text,
                _safe_json(record_copy),
                summary,
                int(bounded_index),
                int(total_count),
                record_id,
            )

        except Exception as exc:
            error_payload = {
                "error": str(exc),
                "mongo_uri": mongo_uri,
                "database_name": database_name,
                "collection_name": collection_name,
            }
            return (
                "",
                json.dumps(error_payload, indent=2),
                f"ZMongoPromptDemoNode error: {exc}",
                0,
                0,
                "",
            )
        finally:
            if client is not None:
                try:
                    client.close()
                except Exception:
                    pass


class ZMongoLoopControllerNode:
    """
    Pure loop/index controller node.

    This node does not touch MongoDB.
    It only computes which record index should be used on this pass
    and exposes a loop counter plus status flags.
    """

    @classmethod
    def INPUT_TYPES(cls):
        return {
            "required": {
                "start_index": ("INT", {"default": 1, "min": 1, "max": 100000000}),
                "end_index": ("INT", {"default": 10, "min": 1, "max": 100000000}),
                "step": ("INT", {"default": 1, "min": 1, "max": 100000}),
                "loop_count": ("INT", {"default": 0, "min": 0, "max": 100000000}),
                "loop_mode": (["stop_at_end", "wrap"],),
                "reset_nonce": ("INT", {"default": 0, "min": 0, "max": 999999999}),
                "max_iterations": ("INT", {"default": 1000, "min": 1, "max": 100000000}),
            }
        }

    RETURN_TYPES = (
        "INT",     # record_index
        "INT",     # next_loop_count
        "BOOLEAN", # has_next
        "BOOLEAN", # is_finished
        "STRING",  # summary
        "STRING",  # loop_state_json
    )
    RETURN_NAMES = (
        "record_index",
        "next_loop_count",
        "has_next",
        "is_finished",
        "summary",
        "loop_state_json",
    )
    FUNCTION = "control_loop"
    CATEGORY = "ZMongo/Looping"

    def control_loop(
        self,
        start_index,
        end_index,
        step,
        loop_count,
        loop_mode,
        reset_nonce,
        max_iterations,
    ):
        _ = reset_nonce

        safe_start = max(1, int(start_index))
        safe_end = max(1, int(end_index))
        safe_step = max(1, int(step))
        safe_loop_count = max(0, int(loop_count))
        safe_max_iterations = max(1, int(max_iterations))

        forward = safe_start <= safe_end

        if forward:
            sequence = list(range(safe_start, safe_end + 1, safe_step))
        else:
            sequence = list(range(safe_start, safe_end - 1, -safe_step))

        if not sequence:
            state = {
                "start_index": safe_start,
                "end_index": safe_end,
                "step": safe_step,
                "loop_count": safe_loop_count,
                "record_index": 0,
                "has_next": False,
                "is_finished": True,
                "loop_mode": loop_mode,
                "max_iterations": safe_max_iterations,
                "sequence_length": 0,
            }
            return (
                0,
                safe_loop_count,
                False,
                True,
                "Loop controller: empty sequence.",
                _safe_json(state),
            )

        seq_len = len(sequence)

        if safe_loop_count >= safe_max_iterations:
            record_index = sequence[-1]
            state = {
                "start_index": safe_start,
                "end_index": safe_end,
                "step": safe_step,
                "loop_count": safe_loop_count,
                "record_index": record_index,
                "has_next": False,
                "is_finished": True,
                "loop_mode": loop_mode,
                "max_iterations": safe_max_iterations,
                "sequence_length": seq_len,
            }
            return (
                int(record_index),
                int(safe_loop_count),
                False,
                True,
                f"Loop controller reached max_iterations={safe_max_iterations}.",
                _safe_json(state),
            )

        if loop_mode == "wrap":
            seq_pos = safe_loop_count % seq_len
            record_index = sequence[seq_pos]
            has_next = True
            is_finished = False
            next_loop_count = safe_loop_count + 1
        else:
            seq_pos = min(safe_loop_count, seq_len - 1)
            record_index = sequence[seq_pos]
            has_next = safe_loop_count < (seq_len - 1)
            is_finished = not has_next
            next_loop_count = safe_loop_count + 1 if has_next else safe_loop_count

        state = {
            "start_index": safe_start,
            "end_index": safe_end,
            "step": safe_step,
            "loop_count": safe_loop_count,
            "record_index": int(record_index),
            "sequence_position": int(seq_pos),
            "sequence_length": int(seq_len),
            "has_next": bool(has_next),
            "is_finished": bool(is_finished),
            "loop_mode": str(loop_mode),
            "max_iterations": int(safe_max_iterations),
        }

        summary = (
            f"Loop count: {safe_loop_count}\n"
            f"Record index: {record_index}\n"
            f"Range: {safe_start} -> {safe_end} (step {safe_step})\n"
            f"Mode: {loop_mode}\n"
            f"Has next: {has_next}\n"
            f"Finished: {is_finished}"
        )

        return (
            int(record_index),
            int(next_loop_count),
            bool(has_next),
            bool(is_finished),
            summary,
            _safe_json(state),
        )


class ZMongoRecordLoopNode:
    """
    Iterates records from MongoDB with support for:
    - single / range / all / batch / random_single / random_batch
    - filter_json and projection_json
    - stable sorting
    - prompt extraction via field_path + fallback paths
    - skipping empty prompts
    - deterministic shuffle/random via seed
    """

    @classmethod
    def INPUT_TYPES(cls):
        return {
            "required": {
                "mongo_uri": ("STRING", {"default": "mongodb://127.0.0.1:27017"}),
                "database_name": ("STRING", {"default": "test"}),
                "collection_name": ("STRING", {"default": "ocr_docs"}),

                "mode": ([
                    "single",
                    "range",
                    "all",
                    "batch",
                    "random_single",
                    "random_batch",
                ],),

                "start_index": ("INT", {"default": 1, "min": 1, "max": 100000000}),
                "end_index": ("INT", {"default": 10, "min": 1, "max": 100000000}),
                "step": ("INT", {"default": 1, "min": 1, "max": 100000}),
                "batch_size": ("INT", {"default": 1, "min": 1, "max": 1024}),

                "sort_field": ("STRING", {"default": "_id"}),
                "sort_direction": (["ascending", "descending"],),

                "field_path": ("STRING", {"default": "text"}),
                "fallback_field_paths": ("STRING", {
                    "default": "text\ndoc_text\ndocument.text\nprompt",
                    "multiline": True,
                }),

                "filter_json": ("STRING", {"default": "{}", "multiline": True}),
                "projection_json": ("STRING", {"default": "{}", "multiline": True}),

                "shuffle": ("BOOLEAN", {"default": False}),
                "seed": ("INT", {"default": 0, "min": 0, "max": 2147483647}),
                "deduplicate_by_id": ("BOOLEAN", {"default": True}),
                "skip_empty_prompts": ("BOOLEAN", {"default": True}),
                "strip_prompt": ("BOOLEAN", {"default": True}),
                "max_prompt_length": ("INT", {"default": 2000, "min": 0, "max": 100000}),

                "empty_prompt_policy": (["skip", "emit_empty", "use_placeholder"],),
                "placeholder_prompt": ("STRING", {"default": "empty prompt"}),

                "loop_policy": (["stop_at_end", "wrap", "ping_pong"],),
                "emit_mode": ([
                    "record_json",
                    "prompt_text",
                    "record_and_prompt",
                    "list_of_records",
                    "list_of_prompts",
                ],),

                "refresh_nonce": ("INT", {"default": 0, "min": 0, "max": 999999999}),
            }
        }

    RETURN_TYPES = (
        "STRING",  # record_data
        "STRING",  # prompt_data
        "INT",     # current_index
        "INT",     # total_count
        "INT",     # emitted_count
        "STRING",  # summary
        "STRING",  # loop_state_json
    )
    RETURN_NAMES = (
        "record_data",
        "selected_field_text",
        "current_index",
        "total_count",
        "emitted_count",
        "summary",
        "loop_state_json",
    )
    FUNCTION = "iterate_records"
    CATEGORY = "ZMongo/Looping"

    @staticmethod
    def _parse_json_object(raw: str, field_name: str) -> Dict[str, Any]:
        text = (raw or "").strip()
        if not text:
            return {}
        try:
            parsed = json.loads(text)
        except Exception as exc:
            raise ValueError(f"{field_name} is not valid JSON: {exc}") from exc
        if not isinstance(parsed, dict):
            raise ValueError(f"{field_name} must be a JSON object")
        return parsed

    @staticmethod
    def _fallback_paths(field_path: str, fallback_field_paths: str) -> List[str]:
        paths: List[str] = []
        if field_path and field_path.strip():
            paths.append(field_path.strip())
        for line in (fallback_field_paths or "").splitlines():
            value = line.strip()
            if value and value not in paths:
                paths.append(value)
        return paths

    @staticmethod
    def _normalize_sort(sort_field: str, sort_direction: str):
        direction = 1 if sort_direction == "ascending" else -1
        field = (sort_field or "").strip() or "_id"
        return [(field, direction)]

    @staticmethod
    def _extract_prompt(
        record: Dict[str, Any],
        paths: List[str],
        strip_prompt: bool,
        max_prompt_length: int,
    ) -> (str, str):
        for path in paths:
            try:
                value = DataProcessor.get_value(record, path)
            except Exception:
                value = None

            if value is None:
                continue

            if isinstance(value, (dict, list)):
                try:
                    text = _safe_json(value)
                except Exception:
                    text = str(value)
            else:
                text = str(value)

            if strip_prompt:
                text = text.strip()

            if max_prompt_length > 0 and len(text) > max_prompt_length:
                text = text[:max_prompt_length]

            return text, path

        return "", ""

    @staticmethod
    def _apply_empty_policy(
        prompt: str,
        empty_prompt_policy: str,
        placeholder_prompt: str,
    ) -> str:
        if prompt:
            return prompt
        if empty_prompt_policy == "emit_empty":
            return ""
        if empty_prompt_policy == "use_placeholder":
            return placeholder_prompt or "empty prompt"
        return ""

    @staticmethod
    def _doc_id_string(doc: Dict[str, Any]) -> str:
        value = doc.get("_id")
        return "" if value is None else str(value)

    def _load_records(
        self,
        collection,
        mongo_filter: Dict[str, Any],
        projection: Dict[str, Any],
        sort_spec,
    ) -> List[Dict[str, Any]]:
        cursor = collection.find(mongo_filter, projection or None).sort(sort_spec)
        return list(cursor)

    def _select_records(
        self,
        records: List[Dict[str, Any]],
        mode: str,
        start_index: int,
        end_index: int,
        step: int,
        batch_size: int,
        shuffle: bool,
        seed: int,
        deduplicate_by_id: bool,
    ) -> (List[Dict[str, Any]], int):
        if not records:
            return [], 0

        working = list(records)

        if deduplicate_by_id:
            seen = set()
            deduped = []
            for record in working:
                sid = self._doc_id_string(record)
                key = sid or id(record)
                if key in seen:
                    continue
                seen.add(key)
                deduped.append(record)
            working = deduped

        rng = random.Random(seed)

        if shuffle:
            rng.shuffle(working)

        total = len(working)

        bounded_start = max(1, min(int(start_index), total))
        bounded_end = max(1, min(int(end_index), total))
        safe_step = max(1, int(step))
        safe_batch_size = max(1, int(batch_size))

        if mode == "single":
            selected = [working[bounded_start - 1]]
            return selected, bounded_start

        if mode == "random_single":
            idx = rng.randint(1, total)
            selected = [working[idx - 1]]
            return selected, idx

        if mode == "all":
            return working, 1 if working else 0

        if mode == "range":
            if bounded_start <= bounded_end:
                indices = list(range(bounded_start, bounded_end + 1, safe_step))
            else:
                indices = list(range(bounded_start, bounded_end - 1, -safe_step))
            selected = [working[i - 1] for i in indices]
            return selected, indices[0] if indices else 0

        if mode == "batch":
            if bounded_start <= bounded_end:
                indices = list(range(bounded_start, bounded_end + 1, safe_step))
            else:
                indices = list(range(bounded_start, bounded_end - 1, -safe_step))
            indices = indices[:safe_batch_size]
            selected = [working[i - 1] for i in indices]
            return selected, indices[0] if indices else 0

        if mode == "random_batch":
            population = list(range(1, total + 1))
            rng.shuffle(population)
            indices = sorted(population[:safe_batch_size])
            selected = [working[i - 1] for i in indices]
            return selected, indices[0] if indices else 0

        return [], 0

    def iterate_records(
        self,
        mongo_uri,
        database_name,
        collection_name,
        mode,
        start_index,
        end_index,
        step,
        batch_size,
        sort_field,
        sort_direction,
        field_path,
        fallback_field_paths,
        filter_json,
        projection_json,
        shuffle,
        seed,
        deduplicate_by_id,
        skip_empty_prompts,
        strip_prompt,
        max_prompt_length,
        empty_prompt_policy,
        placeholder_prompt,
        loop_policy,
        emit_mode,
        refresh_nonce,
    ):
        _ = refresh_nonce
        client = None
        try:
            client = MongoClient(mongo_uri, serverSelectionTimeoutMS=5000)
            client.admin.command("ping")
            collection = client[database_name][collection_name]

            total_count = collection.count_documents({})

            mongo_filter = self._parse_json_object(filter_json, "filter_json")
            projection = self._parse_json_object(projection_json, "projection_json")
            sort_spec = self._normalize_sort(sort_field, sort_direction)
            paths = self._fallback_paths(field_path, fallback_field_paths)

            records = self._load_records(collection, mongo_filter, projection, sort_spec)

            selected_records, current_index = self._select_records(
                records=records,
                mode=mode,
                start_index=start_index,
                end_index=end_index,
                step=step,
                batch_size=batch_size,
                shuffle=shuffle,
                seed=seed,
                deduplicate_by_id=deduplicate_by_id,
            )

            if not selected_records:
                loop_state = {
                    "database_name": str(database_name),
                    "collection_name": str(collection_name),
                    "total_count": int(total_count),
                    "filtered_count": int(len(records)),
                    "start_index": int(start_index),
                    "end_index": int(end_index),
                    "step": int(step),
                    "current_index": 0,
                    "batch_size": int(batch_size),
                    "mode": str(mode),
                    "loop_policy": str(loop_policy),
                    "sort_field": str(sort_field or "_id"),
                    "sort_direction": str(sort_direction),
                    "field_path_used": "",
                    "record_id": "",
                    "has_next": False,
                    "has_previous": False,
                    "wrapped": False,
                    "direction": "forward",
                }
                return (
                    "[]",
                    "",
                    0,
                    int(total_count),
                    0,
                    f"No records matched in {database_name}.{collection_name}",
                    _safe_json(loop_state),
                )

            prompt_items: List[Dict[str, Any]] = []
            kept_records: List[Dict[str, Any]] = []

            for offset, record in enumerate(selected_records):
                prompt_text, used_path = self._extract_prompt(
                    record=record,
                    paths=paths,
                    strip_prompt=strip_prompt,
                    max_prompt_length=max_prompt_length,
                )
                prompt_text = self._apply_empty_policy(
                    prompt=prompt_text,
                    empty_prompt_policy=empty_prompt_policy,
                    placeholder_prompt=placeholder_prompt,
                )

                if skip_empty_prompts and not prompt_text:
                    continue

                record_copy = dict(record)
                record_copy["_loop_index"] = int(current_index + offset)
                record_copy["_database_name"] = str(database_name)
                record_copy["_collection_name"] = str(collection_name)
                record_copy["_field_path_used"] = used_path

                kept_records.append(record_copy)
                prompt_items.append({
                    "index": int(current_index + offset),
                    "record_id": self._doc_id_string(record_copy),
                    "field_path_used": used_path,
                    "prompt": prompt_text,
                })

            if not kept_records:
                loop_state = {
                    "database_name": str(database_name),
                    "collection_name": str(collection_name),
                    "total_count": int(total_count),
                    "filtered_count": int(len(records)),
                    "start_index": int(start_index),
                    "end_index": int(end_index),
                    "step": int(step),
                    "current_index": int(current_index),
                    "batch_size": int(batch_size),
                    "mode": str(mode),
                    "loop_policy": str(loop_policy),
                    "sort_field": str(sort_field or "_id"),
                    "sort_direction": str(sort_direction),
                    "field_path_used": "",
                    "record_id": "",
                    "has_next": False,
                    "has_previous": current_index > 1,
                    "wrapped": False,
                    "direction": "forward",
                }
                return (
                    "[]",
                    "",
                    int(current_index),
                    int(total_count),
                    0,
                    "All selected records were skipped due to empty prompts.",
                    _safe_json(loop_state),
                )

            first_record = kept_records[0]
            first_prompt = prompt_items[0]["prompt"]
            first_path = prompt_items[0]["field_path_used"]
            filtered_count = len(records)

            loop_state = {
                "database_name": str(database_name),
                "collection_name": str(collection_name),
                "total_count": int(total_count),
                "filtered_count": int(filtered_count),
                "start_index": int(start_index),
                "end_index": int(end_index),
                "step": int(step),
                "current_index": int(prompt_items[0]["index"]),
                "current_zero_based": max(0, int(prompt_items[0]["index"]) - 1),
                "batch_size": int(batch_size),
                "batch_number": 1,
                "has_next": int(prompt_items[-1]["index"]) < int(filtered_count),
                "has_previous": int(prompt_items[0]["index"]) > 1,
                "wrapped": False,
                "direction": "forward",
                "sort_field": str(sort_field or "_id"),
                "sort_direction": str(sort_direction),
                "mode": str(mode),
                "loop_policy": str(loop_policy),
                "field_path_used": first_path,
                "record_id": self._doc_id_string(first_record),
            }

            prompt_preview = first_prompt.replace("\n", " ")
            if len(prompt_preview) > 180:
                prompt_preview = prompt_preview[:180] + "..."

            summary = (
                f"Mode: {mode}\n"
                f"Collection: {database_name}.{collection_name}\n"
                f"Filtered records: {filtered_count} / {total_count}\n"
                f"Current index: {prompt_items[0]['index']}\n"
                f"Record _id: {self._doc_id_string(first_record)}\n"
                f"Prompt path used: {first_path or '(none)'}\n"
                f"Prompt preview: {prompt_preview}"
            )

            if emit_mode == "record_json":
                record_data = _safe_json(first_record)
                selected_field_data = first_prompt
                emitted_count = 1
            elif emit_mode == "prompt_text":
                record_data = _safe_json(first_record)
                selected_field_data = first_prompt
                emitted_count = 1
            elif emit_mode == "record_and_prompt":
                record_data = _safe_json(first_record)
                selected_field_data = first_prompt
                emitted_count = 1
            elif emit_mode == "list_of_records":
                record_data = _safe_json(kept_records)
                selected_field_data = _safe_json(prompt_items)
                emitted_count = len(kept_records)
            elif emit_mode == "list_of_prompts":
                record_data = _safe_json(kept_records)
                selected_field_data = _safe_json([item["prompt"] for item in prompt_items])
                emitted_count = len(prompt_items)
            else:
                record_data = _safe_json(first_record)
                selected_field_data = first_prompt
                emitted_count = 1

            return (
                record_data,
                selected_field_data,
                int(prompt_items[0]["index"]),
                int(total_count),
                int(emitted_count),
                summary,
                _safe_json(loop_state),
            )

        except Exception as exc:
            error_payload = {
                "error": str(exc),
                "mongo_uri": mongo_uri,
                "database_name": database_name,
                "collection_name": collection_name,
            }
            return (
                json.dumps(error_payload, indent=2),
                "",
                0,
                0,
                0,
                f"ZMongoRecordLoopNode error: {exc}",
                json.dumps(error_payload, indent=2),
            )
        finally:
            if client is not None:
                try:
                    client.close()
                except Exception:
                    pass


class ZRetrieverNode:
    """Semantic search node that avoids running ad-hoc event loops inside ComfyUI."""

    @classmethod
    def INPUT_TYPES(cls):
        return {
            "required": {
                "query_text": ("STRING", {"multiline": True}),
                "collection": ("STRING", {"default": "ocr_docs"}),
                "similarity_threshold": ("FLOAT", {"default": 0.7, "min": 0.0, "max": 1.0}),
                "n_results": ("INT", {"default": 3, "min": 1, "max": 50}),
            }
        }

    RETURN_TYPES = ("STRING", "STRING")
    RETURN_NAMES = ("top_result_json", "summary")
    FUNCTION = "retrieve"
    CATEGORY = "ZMongo/AI"

    def retrieve(self, query_text, collection, similarity_threshold, n_results):
        # 1. Guard against empty/whitespace queries
        if not query_text or not query_text.strip():
            return ("{}", "Search skipped: Query text is empty.")

        if ZEmbedder is None:
            return ("{}", "Search failed: ZEmbedder is not importable.")

        try:
            zmongo = _get_zmongo()
            # Ensure dimensionality matches your BGE-M3 config
            embedder = ZEmbedder(output_dimensionality=768)

            result = zmongo.run_sync(
                embedder.find_similar_documents,
                query_text=query_text,
                target_collection=collection,
                n_results=n_results,
            )
            if not isinstance(result, SafeResult):
                result = SafeResult.fail(f"Unexpected search result type: {type(result).__name__}")

            if not result.success:
                return ("{}", f"Search failed: {result.error}")

            raw_hits = (result.data or {}).get("results", [])
            hits = [
                hit for hit in raw_hits
                if float(hit.get("retrieval_score", 0.0)) >= float(similarity_threshold)
            ]

            if not hits:
                return ("{}", "No matching documents found above the similarity threshold.")

            return _safe_json(hits[0].get("document", {})), _build_summary_from_hits(hits)
        except Exception as exc:
            logger.exception("ZRetrieverNode failure")
            return ("{}", f"Search failed: {exc}")


class ZMongoDatabaseBrowserNode:
    """Synchronous browser node for listing records from a collection."""

    @classmethod
    def INPUT_TYPES(cls):
        return {
            "required": {
                "mongo_uri": ("STRING", {"default": "mongodb://127.0.0.1:27017", "multiline": False}),
                "database_name": ("STRING", {"default": "test", "multiline": False}),
                "collection_name": ("STRING", {"default": "ocr_docs", "multiline": False}),
                "limit": ("INT", {"default": 50, "min": 1, "max": 1000, "step": 1}),
                "select_index": ("INT", {"default": 1, "min": 1, "max": 100000, "step": 1}),
                "refresh_nonce": ("INT", {"default": 1, "min": 0, "max": 999999999, "step": 1}),
            }
        }

    RETURN_TYPES = ("STRING", "STRING", "INT", "STRING", "STRING", "INT")
    RETURN_NAMES = (
        "selected_record_json",
        "record_list_summary",
        "total_count",
        "db_name_out",
        "coll_name_out",
        "selected_index_out",
    )
    FUNCTION = "browse_database"
    CATEGORY = "ZMongo/Database"

    @staticmethod
    def _make_summary(records: List[dict]) -> str:
        if not records:
            return "No records found."

        lines = []
        for idx, record in enumerate(records, start=1):
            record_id = record.get("_id", "")
            title = (
                record.get("title")
                or record.get("name")
                or record.get("username")
                or record.get("email")
                or record.get("text")
                or record.get("content")
                or ""
            )
            if isinstance(title, str):
                title = title.strip().replace("\n", " ")
                if len(title) > 100:
                    title = title[:100] + "..."
            else:
                title = str(title)
            if not title:
                title = f"keys={list(record.keys())[:6]}"
            lines.append(f"{idx}. _id={record_id} | {title}")
        return "\n".join(lines)

    def browse_database(self, mongo_uri, database_name, collection_name, limit, select_index, refresh_nonce):
        _ = refresh_nonce
        client = None
        try:
            client = MongoClient(mongo_uri, serverSelectionTimeoutMS=5000)
            client.admin.command("ping")
            collection = client[database_name][collection_name]

            total_count = collection.count_documents({})

            if total_count == 0:
                return (
                    json.dumps(
                        {
                            "status": "empty",
                            "message": f"No records found in {database_name}.{collection_name}",
                        },
                        indent=2,
                    ),
                    "No records found.",
                    0,
                    str(database_name),
                    str(collection_name),
                    0,
                )

            # Clamp the requested index to a valid 1-based record index.
            bounded_index = max(1, min(int(select_index), int(total_count)))

            # Use a stable sort so index N always maps to the same record ordering.
            sort_spec = [("_id", 1)]

            # Fetch the selected record directly by index.
            selected_cursor = collection.find({}).sort(sort_spec).skip(bounded_index - 1).limit(1)
            selected_record = next(selected_cursor, None)

            if selected_record is None:
                return (
                    json.dumps(
                        {
                            "error": "Selected record could not be retrieved",
                            "selected_index": bounded_index,
                        },
                        indent=2,
                    ),
                    "Database browse error: Selected record could not be retrieved.",
                    int(total_count),
                    str(database_name),
                    str(collection_name),
                    int(bounded_index),
                )

            # Fetch a preview list for the browser summary.
            summary_limit = max(1, min(int(limit), int(total_count)))
            records = list(collection.find({}).sort(sort_spec).limit(summary_limit))

            summary_lines = []
            for idx, record in enumerate(records, start=1):
                record_id = record.get("_id", "")
                title = (
                        record.get("title")
                        or record.get("name")
                        or record.get("username")
                        or record.get("email")
                        or record.get("text")
                        or record.get("content")
                        or ""
                )
                if isinstance(title, str):
                    title = title.strip().replace("\n", " ")
                    if len(title) > 100:
                        title = title[:100] + "..."
                else:
                    title = str(title)

                if not title:
                    title = f"keys={list(record.keys())[:6]}"

                marker = ">> " if idx == bounded_index else "   "
                summary_lines.append(f"{marker}{idx}. _id={record_id} | {title}")

            record_list_summary = "\n".join(summary_lines) if summary_lines else "No records found."

            # Include the selected index in the returned payload so downstream nodes
            # can verify they are using the correct selected record.
            selected_payload = dict(selected_record)
            selected_payload["_selected_index"] = bounded_index
            selected_payload["_database_name"] = str(database_name)
            selected_payload["_collection_name"] = str(collection_name)

            return (
                _safe_json(selected_payload),
                record_list_summary,
                int(total_count),
                str(database_name),
                str(collection_name),
                int(bounded_index),
            )

        except Exception as exc:
            error_payload = {
                "error": str(exc),
                "mongo_uri": mongo_uri,
                "database_name": database_name,
                "collection_name": collection_name,
            }
            return (
                json.dumps(error_payload, indent=2),
                f"Database browse error: {exc}",
                0,
                str(database_name),
                str(collection_name),
                int(select_index),
            )
        finally:
            if client is not None:
                try:
                    client.close()
                except Exception:
                    pass

class ZMongoRecordSplitter:
    """Converts JSON arrays into a list output usable by downstream ComfyUI nodes."""

    @classmethod
    def INPUT_TYPES(cls):
        return {"required": {"records_json": ("STRING", {"multiline": True})}}

    RETURN_TYPES = ("STRING", "INT")
    RETURN_NAMES = ("record_json_list", "count")
    OUTPUT_IS_LIST = (True, False)
    FUNCTION = "split"
    CATEGORY = "ZMongo/Database"

    def split(self, records_json):
        parsed = _coerce_records(records_json)
        if not parsed.success:
            return ([json.dumps(parsed.to_dict())], 0)

        output = [DataProcessor.to_json(record) for record in parsed.data or []]
        return (output, len(output))


class ZMongoFieldSelector:
    """
    Dot-path field extractor with frontend-assisted dropdown population.

    How it works:
    - record_json comes from an upstream node
    - on execution, this node flattens the JSON into path-like keys
    - it returns those paths both as an output string and as UI metadata
    - frontend JS silently updates the field_path dropdown choices
    """

    @classmethod
    def INPUT_TYPES(cls):
        return {
            "required": {
                "record_json": ("STRING", {"forceInput": True}),
                "field_path": (["text"], {"default": "text"}),
            }
        }

    RETURN_TYPES = ("STRING", "STRING", "STRING")
    RETURN_NAMES = ("field_value", "available_paths", "selected_path")
    FUNCTION = "select_field"
    CATEGORY = "ZMongo/Database"

    @staticmethod
    def _normalize_available_paths(record: Dict[str, Any]) -> List[str]:
        try:
            flattened = DataProcessor.flatten_dict(record)
            if isinstance(flattened, dict) and flattened:
                paths = sorted(str(k) for k in flattened.keys())
            else:
                paths = []
        except Exception:
            paths = []

        if not paths and isinstance(record, dict):
            paths = sorted(str(k) for k in record.keys())

        if not paths:
            paths = ["text"]

        return paths

    @staticmethod
    def _serialize_value(value: Any) -> str:
        if value is None:
            return ""
        if isinstance(value, (dict, list)):
            return _safe_json(value)
        return str(value)

    def select_field(self, record_json, field_path):
        try:
            record = json.loads(record_json)
            if not isinstance(record, dict):
                raise ValueError("record_json must decode to a JSON object")

            available_paths_list = self._normalize_available_paths(record)
            available_paths_text = "\n".join(available_paths_list)

            selected_path = field_path if field_path in available_paths_list else available_paths_list[0]

            try:
                value = DataProcessor.get_value(record, selected_path)
            except Exception:
                value = None

            field_value = self._serialize_value(value)

            return {
                "ui": {
                    # Important: send the plain list, not [list]
                    "field_choices": available_paths_list,
                    "selected_path": selected_path,
                    "available_paths": available_paths_text,
                },
                "result": (
                    field_value,
                    available_paths_text,
                    selected_path,
                ),
            }

        except Exception as exc:
            logger.exception("ZMongoFieldSelector failure")
            error_text = f"Error: {exc}"
            return {
                "ui": {
                    "field_choices": ["text"],
                    "selected_path": "text",
                    "available_paths": "",
                },
                "result": (
                    error_text,
                    "",
                    "text",
                ),
            }

class ZMongoOperationsNode:
    """Insert node using ZMongo.run_sync and SafeResult throughout."""

    @classmethod
    def INPUT_TYPES(cls):
        return {
            "required": {
                "collection": ("STRING", {"default": "legal_codex"}),
                "data_json": ("STRING", {"multiline": True}),
                "operation_type": (["Add Unique (No Update)", "Standard Insert"],),
            }
        }

    RETURN_TYPES = ("STRING", "INT")
    RETURN_NAMES = ("result_json", "count")
    FUNCTION = "execute_op"
    CATEGORY = "ZMongo/Operations"

    def _insert_unique_no_update(self, zmongo: ZMongo, collection: str, records: List[Dict[str, Any]]) -> SafeResult:
        inserted_ids: List[Any] = []
        skipped = 0
        errors: List[str] = []

        for record in records:
            if not isinstance(record, dict):
                errors.append("Record is not an object")
                continue

            if "_id" in record:
                exists = zmongo.run_sync(zmongo.find_one, collection, {"_id": record.get("_id")})
                if isinstance(exists, SafeResult) and exists.success and exists.data is not None:
                    skipped += 1
                    continue
                if isinstance(exists, SafeResult) and not exists.success:
                    errors.append(str(exists.error))
                    continue

            result = zmongo.run_sync(zmongo.insert_one, collection, record)
            if isinstance(result, SafeResult) and result.success:
                inserted_ids.append((result.data or {}).get("inserted_id"))
            elif isinstance(result, SafeResult):
                if "duplicate" in str(result.error).lower():
                    skipped += 1
                else:
                    errors.append(str(result.error))
            else:
                errors.append(f"Unexpected insert result type: {type(result).__name__}")

        payload = {
            "inserted_count": len(inserted_ids),
            "inserted_ids": inserted_ids,
            "skipped_count": skipped,
            "error_count": len(errors),
            "errors": errors,
        }
        return SafeResult.ok(payload) if not errors else SafeResult.fail("Some records failed", data=payload)

    def execute_op(self, collection, data_json, operation_type):
        parsed = _coerce_records(data_json)
        if not parsed.success:
            return (_safe_json(parsed), 0)

        try:
            zmongo = _get_zmongo()
            records = parsed.data or []

            if operation_type == "Add Unique (No Update)":
                result = self._insert_unique_no_update(zmongo, collection, records)
                count = ((result.data or {}).get("inserted_count", 0) if isinstance(result.data, dict) else 0)
                return (_safe_json(result), int(count))

            result = zmongo.run_sync(zmongo.insert_many, collection, records)
            if not isinstance(result, SafeResult):
                result = SafeResult.fail(f"Unexpected insert result type: {type(result).__name__}")

            inserted_ids = (result.data or {}).get("inserted_ids", []) if isinstance(result.data, dict) else []
            return (_safe_json(result), len(inserted_ids))
        except DuplicateKeyError as exc:
            failure = SafeResult.fail(str(exc))
            return (_safe_json(failure), 0)
        except Exception as exc:
            logger.exception("ZMongoOperationsNode failure")
            failure = SafeResult.fail(str(exc))
            return (_safe_json(failure), 0)


class ZMongoConfigNode:
    """Configures the shared ZMongo instance used by the other nodes."""

    @classmethod
    def INPUT_TYPES(cls):
        return {
            "required": {
                "mongo_uri": ("STRING", {"default": "mongodb://127.0.0.1:27017"}),
                "db_name": ("STRING", {"default": "test"}),
            }
        }

    RETURN_TYPES = ("STRING",)
    RETURN_NAMES = ("status",)
    FUNCTION = "configure"
    CATEGORY = "ZMongo/Config"

    def configure(self, mongo_uri, db_name):
        try:
            zmongo = _get_zmongo(uri=mongo_uri, db_name=db_name)
            ping = zmongo.run_sync(zmongo.sync_timestamp)
            if isinstance(ping, SafeResult) and ping.success:
                return (f"ZMongo connected to {db_name}",)
            error_text = ping.error if isinstance(ping, SafeResult) else f"Unexpected result: {type(ping).__name__}"
            return (f"ZMongo config failed: {error_text}",)
        except Exception as exc:
            logger.exception("ZMongoConfigNode failure")
            return (f"ZMongo config failed: {exc}",)


class ZMongoTextFetcher:
    """Fetches the `text` field from a document using ZMongo.run_sync."""

    @classmethod
    def INPUT_TYPES(cls):
        return {
            "required": {
                "collection_name": ("STRING", {"default": "test"}),
                "document_id": ("STRING", {"default": ""}),
            },
        }

    RETURN_TYPES = ("STRING",)
    RETURN_NAMES = ("text_value",)
    FUNCTION = "fetch_text"
    CATEGORY = "ZMongo"

    def fetch_text(self, collection_name, document_id):
        if not document_id or not document_id.strip():
            return ("No ID Provided",)

        try:
            query_id = ObjectId(document_id) if ObjectId.is_valid(document_id) else document_id
            zmongo = _get_zmongo()
            result = zmongo.run_sync(zmongo.find_one, collection_name, {"_id": query_id})
            if not isinstance(result, SafeResult):
                result = SafeResult.fail(f"Unexpected fetch result type: {type(result).__name__}")

            if not result.success:
                return (f"Error: {result.error}",)
            if not result.data:
                return ("Document not found",)

            text_value = result.data.get("text")
            if text_value is None:
                return ("Field 'text' not found",)
            if isinstance(text_value, (dict, list)):
                return (_safe_json(text_value),)
            return (str(text_value),)
        except Exception as exc:
            logger.exception("ZMongoTextFetcher failure")
            return (f"Error: {exc}",)


NODE_CLASS_MAPPINGS = {
    "ZMongoConfigNode": ZMongoConfigNode,
    "ZMongoTextFetcher": ZMongoTextFetcher,
    "ZMongoOperationsNode": ZMongoOperationsNode,
    "ZMongoRecordSplitter": ZMongoRecordSplitter,
    "ZMongoFieldSelector": ZMongoFieldSelector,
    "ZRetrieverNode": ZRetrieverNode,
    "ZMongoDatabaseBrowserNode": ZMongoDatabaseBrowserNode,
}

NODE_DISPLAY_NAME_MAPPINGS = {
    "ZMongoConfigNode": "ZMongo Config",
    "ZMongoTextFetcher": "ZMongo Text Fetcher",
    "ZMongoOperationsNode": "ZMongo Operations",
    "ZMongoRecordSplitter": "ZMongo Record Splitter",
    "ZMongoFieldSelector": "ZMongo Field Selector",
    "ZRetrieverNode": "ZMongo Retriever",
    "ZMongoDatabaseBrowserNode": "ZMongo Database Browser",
}
