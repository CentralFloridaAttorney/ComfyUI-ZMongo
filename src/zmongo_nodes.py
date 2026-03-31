import json
import logging
import random
import threading
from typing import Any, Dict, List, Optional

import requests
from aiohttp import web
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

from .zmongo_toolbag.data_processor import DataProcessor

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


def _register_zmongo_field_selector_routes(prompt_server_instance):
    routes = prompt_server_instance.routes

    @routes.get("/zmongo/flattened_fields")
    async def zmongo_get_flattened_fields(request):
        try:
            collection_name = request.rel_url.query.get("collection_name", "").strip()
            fields = ZMongoFlattenedFieldDropdownNode.get_flattened_field_names(collection_name)
            return web.json_response(
                {
                    "success": True,
                    "collection_name": collection_name,
                    "fields": fields,
                }
            )
        except Exception as exc:
            return web.json_response(
                {
                    "success": False,
                    "error": str(exc),
                    "fields": [],
                },
                status=500,
            )
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


class ZMongoFlattenedFieldSelector:
    """
    ComfyUI node that:
      1. Loads MongoDB collection names into a dropdown
      2. Loads one sample document from the selected collection
      3. Flattens that document with DataProcessor.flatten_json(...)
      4. Uses an INT input as the selected flattened field index
      5. Outputs the selected field name and related metadata
    """

    CATEGORY = "ZMongo"
    RETURN_TYPES = ("STRING", "STRING", "INT", "STRING")
    RETURN_NAMES = (
        "collection_name",
        "selected_field_name",
        "selected_field_index",
        "flattened_field_names_json",
    )
    FUNCTION = "select_field"

    @classmethod
    def _get_zmongo(cls) -> ZMongo:
        return ZMongo()

    @classmethod
    def _safe_collection_names(cls) -> List[str]:
        zmongo = None
        try:
            zmongo = cls._get_zmongo()

            # Prefer sync access through run_sync because current ZMongo.list_collections()
            # is async and not a normal sync wrapper.
            result = zmongo.run_sync(zmongo.list_collections_async)

            if not result or not getattr(result, "success", False):
                logger.warning("Failed to load Mongo collections: %s", getattr(result, "error", None))
                return ["<no_collections_found>"]

            data = result.data or {}
            collections = data.get("collections", []) if isinstance(data, dict) else []

            if not collections:
                return ["<no_collections_found>"]

            return [str(name) for name in collections]

        except Exception as exc:
            logger.exception("Error retrieving collection names: %s", exc)
            return ["<mongo_error>"]
        finally:
            if zmongo:
                try:
                    zmongo.close()
                except Exception:
                    pass

    @classmethod
    def INPUT_TYPES(cls) -> Dict[str, Any]:
        collections = cls._safe_collection_names()

        return {
            "required": {
                "collection_name": (collections,),
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
    def IS_CHANGED(cls, collection_name: str, field_index: int):
        # Causes reevaluation when either changes.
        return f"{collection_name}:{field_index}"

    def _get_flattened_field_names(self, collection_name: str) -> List[str]:
        zmongo = None
        try:
            if not collection_name or collection_name.startswith("<"):
                return []

            zmongo = self._get_zmongo()

            # Pull one sample document from the collection.
            result = zmongo.find_one(collection_name, {})

            if not result or not getattr(result, "success", False):
                logger.warning(
                    "find_one failed for collection '%s': %s",
                    collection_name,
                    getattr(result, "error", None),
                )
                return []

            doc = result.original() if hasattr(result, "original") else result.data
            if not isinstance(doc, dict) or not doc:
                return []

            flattened = DataProcessor.flatten_json(doc)
            if not isinstance(flattened, dict):
                return []

            return list(flattened.keys())

        except Exception as exc:
            logger.exception("Error flattening fields for '%s': %s", collection_name, exc)
            return []
        finally:
            if zmongo:
                try:
                    zmongo.close()
                except Exception:
                    pass

    def select_field(self, collection_name: str, field_index: int):
        field_names = self._get_flattened_field_names(collection_name)

        if not field_names:
            return (
                str(collection_name),
                "",
                0,
                json.dumps([], indent=2),
            )

        normalized_index = max(0, min(int(field_index), len(field_names) - 1))
        selected_field_name = field_names[normalized_index]

        return (
            str(collection_name),
            str(selected_field_name),
            normalized_index,
            json.dumps(field_names, indent=2),
        )


class ZMongoSaveValueNode:
    """
    Saves a passed value from prior nodes into MongoDB using a flattened
    dot-path field key.

    Supports:
    - source_record_json as one object
    - source_record_json as a list of objects
    - choosing a record from the list with source_record_index
    - explicit query override
    """

    @classmethod
    def INPUT_TYPES(cls):
        return {
            "required": {
                "mongo_uri": ("STRING", {"default": "mongodb://127.0.0.1:27017"}),
                "database_name": ("STRING", {"default": "test"}),

                "source_record_json": ("STRING", {"forceInput": True}),
                "source_record_index": ("INT", {"default": 1, "min": 1, "max": 1000000}),
                "value_to_save": ("STRING", {"forceInput": True}),

                "target_collection": ("STRING", {"default": "comfy"}),
                "target_field_path": ("STRING", {"default": "responses.output.text"}),

                "parse_value_as_json": ("BOOLEAN", {"default": False}),
                "upsert_if_missing": ("BOOLEAN", {"default": False}),

                "explicit_query_json": ("STRING", {"default": "", "multiline": True}),
            }
        }

    RETURN_TYPES = (
        "STRING",  # result_json
        "STRING",  # target_query_json
        "STRING",  # saved_value_json
        "STRING",  # target_collection_out
        "STRING",  # target_field_path_out
    )
    RETURN_NAMES = (
        "result_json",
        "target_query_json",
        "saved_value_json",
        "target_collection_out",
        "target_field_path_out",
    )
    FUNCTION = "save_value"
    CATEGORY = "ZMongo/Operations"

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
    def _parse_source_record(raw: str, source_record_index: int) -> Dict[str, Any]:
        text = (raw or "").strip()
        if not text:
            raise ValueError("source_record_json is empty")

        try:
            parsed = json.loads(text)
        except Exception as exc:
            raise ValueError(f"source_record_json is not valid JSON: {exc}") from exc

        if isinstance(parsed, dict):
            return parsed

        if isinstance(parsed, list):
            if not parsed:
                raise ValueError("source_record_json list is empty")

            bounded_index = max(1, min(int(source_record_index), len(parsed)))
            selected = parsed[bounded_index - 1]

            if not isinstance(selected, dict):
                raise ValueError("Selected item from source_record_json list is not a JSON object")

            return selected

        raise ValueError("source_record_json must be a JSON object or a JSON list of objects")

    @staticmethod
    def _parse_value(raw_value: Any, parse_value_as_json: bool) -> Any:
        if raw_value is None:
            return None

        if not parse_value_as_json:
            return raw_value

        if isinstance(raw_value, str):
            stripped = raw_value.strip()
            if not stripped:
                return ""
            try:
                return json.loads(stripped)
            except Exception:
                return raw_value

        return raw_value

    @staticmethod
    def _extract_target_query(
        source_record: Dict[str, Any],
        explicit_query_json: str,
    ) -> Dict[str, Any]:
        explicit_query = ZMongoSaveValueNode._parse_json_object(
            explicit_query_json, "explicit_query_json"
        )
        if explicit_query:
            return explicit_query

        if isinstance(source_record, dict) and source_record.get("_id") is not None:
            return {"_id": source_record["_id"]}

        raise ValueError(
            "No target query could be determined. Provide source_record_json with _id "
            "or set explicit_query_json."
        )

    @staticmethod
    def _extract_target_collection(
        source_record: Dict[str, Any],
        target_collection: str,
    ) -> str:
        if target_collection and target_collection.strip():
            return target_collection.strip()

        inferred = ""
        if isinstance(source_record, dict):
            inferred = str(source_record.get("_collection_name") or "").strip()

        if inferred:
            return inferred

        raise ValueError(
            "No target collection provided. Set target_collection or pass source_record_json "
            "containing _collection_name."
        )

    def save_value(
        self,
        mongo_uri,
        database_name,
        source_record_json,
        source_record_index,
        value_to_save,
        target_collection,
        target_field_path,
        parse_value_as_json,
        upsert_if_missing,
        explicit_query_json,
    ):
        try:
            source_record = self._parse_source_record(source_record_json, source_record_index)
            resolved_collection = self._extract_target_collection(source_record, target_collection)
            target_query = self._extract_target_query(source_record, explicit_query_json)

            if not target_field_path or not str(target_field_path).strip():
                raise ValueError("target_field_path is required")

            resolved_field_path = str(target_field_path).strip()
            parsed_value = self._parse_value(value_to_save, parse_value_as_json)

            update_doc = {
                resolved_field_path: parsed_value
            }

            zmongo = _get_zmongo(uri=mongo_uri, db_name=database_name)
            result = zmongo.run_sync(
                zmongo.update_one,
                resolved_collection,
                target_query,
                update_doc,
                upsert_if_missing,
            )

            if not isinstance(result, SafeResult):
                result = SafeResult.fail(
                    f"Unexpected update result type: {type(result).__name__}"
                )

            result_payload = {
                "success": result.success,
                "message": result.message,
                "error": result.error,
                "data": result.data,
                "target_collection": resolved_collection,
                "target_field_path": resolved_field_path,
                "target_query": target_query,
                "source_record_index_used": int(source_record_index),
            }

            return (
                _safe_json(result_payload),
                _safe_json(target_query),
                _safe_json(parsed_value),
                str(resolved_collection),
                str(resolved_field_path),
            )

        except Exception as exc:
            error_payload = {
                "success": False,
                "error": str(exc),
                "target_collection": target_collection,
                "target_field_path": target_field_path,
            }
            return (
                json.dumps(error_payload, indent=2),
                "{}",
                _safe_json(value_to_save),
                str(target_collection or ""),
                str(target_field_path or ""),
            )

class ZMongoSaveBatchTextNode:
    """
    Saves a batch of text items to file(s) in the ComfyUI output directory.

    Intended use:
    - true ComfyUI list input
    - JSON list string input
    - one file per item
    - one combined file
    - append or overwrite behavior
    - stable indexed filenames for batch exports
    """

    INPUT_IS_LIST = True

    @classmethod
    def INPUT_TYPES(cls):
        return {
            "required": {
                "batch_content": ("STRING", {"forceInput": True}),
                "base_filename": ("STRING", {"default": "zmongo_batch"}),
                "starting_index": ("INT", {"default": 1, "min": 0, "max": 1000000}),
                "file_type": (["txt", "json", "md"],),
                "save_behavior": ([
                    "write new content",
                    "append to existing file",
                ],),
                "save_layout": ([
                    "separate file per item",
                    "one combined file",
                ],),
                "reuse_exact_filenames": ("BOOLEAN", {"default": False}),
                "skip_empty_items": ("BOOLEAN", {"default": True}),
                "trim_whitespace": ("BOOLEAN", {"default": True}),
                "join_separator": ("STRING", {"default": "\n"}),
            }
        }

    RETURN_TYPES = ("STRING", "INT", "STRING")
    RETURN_NAMES = (
        "saved_file_path_or_paths",
        "saved_file_count",
        "summary",
    )
    FUNCTION = "save_batch"
    CATEGORY = "ZMongo/Output"

    @staticmethod
    def _unwrap_scalar(value, default=None):
        """
        ComfyUI may pass scalar inputs as one-item lists when INPUT_IS_LIST = True.
        """
        if isinstance(value, list):
            if not value:
                return default
            return value[0]
        return value

    @classmethod
    def _unwrap_and_cast_int(cls, value, default=0) -> int:
        value = cls._unwrap_scalar(value, default)
        if value is None:
            return int(default)
        return int(value)

    @classmethod
    def _unwrap_and_cast_bool(cls, value, default=False) -> bool:
        value = cls._unwrap_scalar(value, default)
        if isinstance(value, bool):
            return value
        return bool(value)

    @classmethod
    def _unwrap_and_cast_str(cls, value, default="") -> str:
        value = cls._unwrap_scalar(value, default)
        if value is None:
            return str(default)
        return str(value)

    @staticmethod
    def _coerce_to_items(batch_content):
        """
        Accepts:
        - true ComfyUI list input
        - JSON list string
        - single plain string
        """
        if isinstance(batch_content, list):
            if len(batch_content) == 1 and isinstance(batch_content[0], str):
                raw = batch_content[0].strip()
                if raw:
                    try:
                        parsed = json.loads(raw)
                        if isinstance(parsed, list):
                            return parsed
                    except Exception:
                        pass
            return batch_content

        if batch_content is None:
            return [""]

        raw = str(batch_content)
        stripped = raw.strip()
        if not stripped:
            return [""]

        try:
            parsed = json.loads(stripped)
            if isinstance(parsed, list):
                return parsed
            return [raw]
        except Exception:
            return [raw]

    @staticmethod
    def _item_to_text(item, file_type: str, trim_whitespace: bool) -> str:
        if item is None:
            text = ""
        elif isinstance(item, str):
            text = item
        elif file_type == "json":
            text = json.dumps(item, indent=2, ensure_ascii=False)
        elif isinstance(item, (dict, list)):
            text = json.dumps(item, indent=2, ensure_ascii=False)
        else:
            text = str(item)

        if trim_whitespace and isinstance(text, str):
            text = text.strip()

        return text

    @staticmethod
    def _ensure_parent_dir(path: str):
        import os
        parent = os.path.dirname(path)
        if parent:
            os.makedirs(parent, exist_ok=True)

    @staticmethod
    def _make_single_filename(base_filename: str, file_type: str) -> str:
        return f"{base_filename}.{file_type}"

    @staticmethod
    def _make_indexed_filename(base_filename: str, index: int, file_type: str) -> str:
        return f"{base_filename}_{int(index):04d}.{file_type}"

    @staticmethod
    def _make_unique_path(path: str, file_type: str) -> str:
        import os

        if not os.path.exists(path):
            return path

        base = path[: -(len(file_type) + 1)]
        counter = 1
        while True:
            candidate = f"{base}_{counter}.{file_type}"
            if not os.path.exists(candidate):
                return candidate
            counter += 1

    def save_batch(
        self,
        batch_content,
        base_filename,
        starting_index,
        file_type,
        save_behavior,
        save_layout,
        reuse_exact_filenames,
        skip_empty_items,
        trim_whitespace,
        join_separator,
    ):
        try:
            import os
            import folder_paths

            # Normalize scalar inputs that may arrive as [value]
            base_filename = self._unwrap_and_cast_str(base_filename, "zmongo_batch")
            starting_index = self._unwrap_and_cast_int(starting_index, 1)
            file_type = self._unwrap_and_cast_str(file_type, "txt")
            save_behavior = self._unwrap_and_cast_str(save_behavior, "write new content")
            save_layout = self._unwrap_and_cast_str(save_layout, "separate file per item")
            reuse_exact_filenames = self._unwrap_and_cast_bool(reuse_exact_filenames, False)
            skip_empty_items = self._unwrap_and_cast_bool(skip_empty_items, True)
            trim_whitespace = self._unwrap_and_cast_bool(trim_whitespace, True)
            join_separator = self._unwrap_and_cast_str(join_separator, "\n")

            output_dir = folder_paths.get_output_directory()
            raw_items = self._coerce_to_items(batch_content)

            normalized_items = []
            for item in raw_items:
                text_value = self._item_to_text(item, file_type, trim_whitespace)
                if skip_empty_items and not text_value:
                    continue
                normalized_items.append(text_value)

            if not normalized_items:
                return ("[]", 0, "No non-empty items to save.")

            append_mode = (save_behavior == "append to existing file")
            file_open_mode = "a" if append_mode else "w"

            saved_paths = []

            if save_layout == "one combined file":
                filename = self._make_single_filename(base_filename, file_type)
                full_path = os.path.join(output_dir, filename)

                if not reuse_exact_filenames and not append_mode:
                    full_path = self._make_unique_path(full_path, file_type)

                self._ensure_parent_dir(full_path)

                with open(full_path, file_open_mode, encoding="utf-8") as f:
                    f.write(join_separator.join(normalized_items))

                saved_paths.append(full_path)

            else:
                for offset, item_text in enumerate(normalized_items):
                    item_index = int(starting_index) + offset
                    filename = self._make_indexed_filename(base_filename, item_index, file_type)
                    full_path = os.path.join(output_dir, filename)

                    if not reuse_exact_filenames and not append_mode:
                        full_path = self._make_unique_path(full_path, file_type)

                    self._ensure_parent_dir(full_path)

                    with open(full_path, file_open_mode, encoding="utf-8") as f:
                        f.write(item_text)

                    saved_paths.append(full_path)

            if len(saved_paths) == 1:
                saved_output = saved_paths[0]
            else:
                saved_output = json.dumps(saved_paths, indent=2, ensure_ascii=False)

            summary = (
                f"Saved {len(saved_paths)} file(s)\n"
                f"Base filename: {base_filename}\n"
                f"File type: {file_type}\n"
                f"Save behavior: {save_behavior}\n"
                f"Save layout: {save_layout}"
            )

            return (saved_output, len(saved_paths), summary)

        except Exception as exc:
            return (f"Error saving batch file(s): {exc}", 0, f"Error: {exc}")


class ZMongoDataPassThroughNode:
    """
    Normalizes ZMongo output into formats that ComfyUI core nodes can consume.

    Main purposes:
    - Pass through one text value
    - Convert JSON list text into ComfyUI list output
    - Convert JSON dict/list items into JSON strings
    - Optionally split plain text by lines
    - Provide both a single preview value and a list output for batch/list mapping
    """

    @classmethod
    def INPUT_TYPES(cls):
        return {
            "required": {
                "input_data": ("STRING", {"forceInput": True}),
                "input_interpretation": ([
                    "auto detect",
                    "treat as plain text",
                    "treat as json",
                    "treat as newline list",
                ],),
                "output_mode": ([
                    "single item only",
                    "list output only",
                    "single item and list output",
                ],),
                "item_conversion": ([
                    "keep strings as-is",
                    "convert everything to text",
                    "convert dicts and lists to json text",
                ],),
                "skip_empty_items": ("BOOLEAN", {"default": True}),
                "trim_whitespace": ("BOOLEAN", {"default": True}),
            }
        }

    RETURN_TYPES = (
        "STRING",  # single_item
        "STRING",  # list_items
        "INT",     # item_count
        "STRING",  # detected_input_type
        "STRING",  # normalized_json
    )
    RETURN_NAMES = (
        "single_item",
        "list_items",
        "item_count",
        "detected_input_type",
        "normalized_json",
    )
    OUTPUT_IS_LIST = (False, True, False, False, False)
    FUNCTION = "pass_through"
    CATEGORY = "ZMongo/Utility"

    @staticmethod
    def _normalize_item(item: Any, item_conversion: str, trim_whitespace: bool) -> str:
        if item is None:
            text = ""
        elif item_conversion == "keep strings as-is" and isinstance(item, str):
            text = item
        elif item_conversion == "convert dicts and lists to json text" and isinstance(item, (dict, list)):
            text = _safe_json(item)
        elif isinstance(item, str):
            text = item
        elif isinstance(item, (dict, list)):
            text = _safe_json(item)
        else:
            text = str(item)

        if trim_whitespace and isinstance(text, str):
            text = text.strip()

        return text

    @staticmethod
    def _split_newline_items(raw_text: str) -> List[str]:
        return raw_text.splitlines()

    @staticmethod
    def _coerce_items(input_data: str, input_interpretation: str) -> tuple[List[Any], str]:
        raw_text = "" if input_data is None else str(input_data)
        stripped = raw_text.strip()

        if input_interpretation == "treat as plain text":
            return [raw_text], "plain_text"

        if input_interpretation == "treat as newline list":
            return ZMongoDataPassThroughNode._split_newline_items(raw_text), "newline_list"

        if input_interpretation == "treat as json":
            if not stripped:
                return [], "json_empty"
            parsed = json.loads(stripped)
            if isinstance(parsed, list):
                return parsed, "json_list"
            return [parsed], "json_object"

        # auto detect
        if not stripped:
            return [], "empty"

        try:
            parsed = json.loads(stripped)
            if isinstance(parsed, list):
                return parsed, "json_list"
            if isinstance(parsed, dict):
                return [parsed], "json_object"
        except Exception:
            pass

        if "\n" in raw_text:
            return ZMongoDataPassThroughNode._split_newline_items(raw_text), "newline_list"

        return [raw_text], "plain_text"

    def pass_through(
        self,
        input_data,
        input_interpretation,
        output_mode,
        item_conversion,
        skip_empty_items,
        trim_whitespace,
    ):
        try:
            raw_items, detected_input_type = self._coerce_items(
                input_data=input_data,
                input_interpretation=input_interpretation,
            )

            normalized_items: List[str] = []
            for item in raw_items:
                text = self._normalize_item(
                    item=item,
                    item_conversion=item_conversion,
                    trim_whitespace=trim_whitespace,
                )
                if skip_empty_items and not text:
                    continue
                normalized_items.append(text)

            item_count = len(normalized_items)
            normalized_json = _safe_json(normalized_items)

            if item_count == 0:
                single_item = ""
                list_items = []
            else:
                single_item = normalized_items[0]

                if output_mode == "single item only":
                    list_items = []
                else:
                    # Important: return a flat list of strings here
                    list_items = normalized_items

            return (
                single_item,
                list_items,
                int(item_count),
                detected_input_type,
                normalized_json,
            )

        except Exception as exc:
            error_text = f"ZMongoDataPassThroughNode error: {exc}"
            return (
                error_text,
                [],
                0,
                "error",
                _safe_json({"error": str(exc)}),
            )


class ZMongoSaveTextNode:
    """
    Saves text or JSON text to file(s) in the ComfyUI output directory.

    User-oriented behavior:
    - Save one value to one file
    - Save a JSON list to separate files
    - Save a true ComfyUI list to separate files
    - Save many items into one combined file
    - Overwrite exact filenames, append to files, or auto-create unique names
    """

    @classmethod
    def INPUT_TYPES(cls):
        return {
            "required": {
                "content_to_save": ("STRING", {"forceInput": True}),
                "base_filename": ("STRING", {"default": "zmongo_output"}),
                "starting_index": ("INT", {"default": 1, "min": 0, "max": 1000000}),
                "file_type": (["txt", "json", "md"],),
                "save_behavior": ([
                    "write new content",
                    "append to existing file",
                ],),
                "save_layout": ([
                    "one combined file",
                    "separate file per item",
                ],),
                "reuse_exact_filenames": ("BOOLEAN", {"default": False}),
            }
        }

    RETURN_TYPES = ("STRING", "INT")
    RETURN_NAMES = ("saved_file_path_or_paths", "saved_file_count")
    FUNCTION = "save_text"
    CATEGORY = "ZMongo/Output"
    INPUT_IS_LIST = True

    @staticmethod
    def _unwrap_scalar(value, default=None):
        """
        ComfyUI may pass scalar inputs as one-item lists when INPUT_IS_LIST = True.
        """
        if isinstance(value, list):
            if not value:
                return default
            return value[0]
        return value

    @classmethod
    def _unwrap_and_cast_int(cls, value, default=0) -> int:
        value = cls._unwrap_scalar(value, default)
        if value is None:
            return int(default)
        return int(value)

    @classmethod
    def _unwrap_and_cast_bool(cls, value, default=False) -> bool:
        value = cls._unwrap_scalar(value, default)
        if isinstance(value, bool):
            return value
        return bool(value)

    @classmethod
    def _unwrap_and_cast_str(cls, value, default="") -> str:
        value = cls._unwrap_scalar(value, default)
        if value is None:
            return str(default)
        return str(value)

    @staticmethod
    def _coerce_to_items(content_to_save):
        """
        Accepts:
        - plain string
        - JSON list string
        - real ComfyUI list input
        """
        if isinstance(content_to_save, list):
            if len(content_to_save) == 1 and isinstance(content_to_save[0], str):
                raw = content_to_save[0]
                stripped = raw.strip()
                if stripped:
                    try:
                        parsed = json.loads(stripped)
                        if isinstance(parsed, list):
                            return parsed
                    except Exception:
                        pass
            return content_to_save

        if content_to_save is None:
            return [""]

        raw = str(content_to_save)
        stripped = raw.strip()
        if not stripped:
            return [""]

        try:
            parsed = json.loads(stripped)
            if isinstance(parsed, list):
                return parsed
            return [raw]
        except Exception:
            return [raw]

    @staticmethod
    def _item_to_text(item, file_type: str) -> str:
        if item is None:
            return ""
        if isinstance(item, str):
            return item
        if file_type == "json":
            return json.dumps(item, indent=2, ensure_ascii=False)
        if isinstance(item, (dict, list)):
            return json.dumps(item, indent=2, ensure_ascii=False)
        return str(item)

    @staticmethod
    def _ensure_parent_dir(path: str):
        import os
        parent = os.path.dirname(path)
        if parent:
            os.makedirs(parent, exist_ok=True)

    @staticmethod
    def _make_single_filename(base_filename: str, file_type: str) -> str:
        return f"{base_filename}.{file_type}"

    @staticmethod
    def _make_indexed_filename(base_filename: str, index: int, file_type: str) -> str:
        return f"{base_filename}_{int(index):04d}.{file_type}"

    @staticmethod
    def _make_unique_path(path: str, file_type: str) -> str:
        import os

        if not os.path.exists(path):
            return path

        base = path[: -(len(file_type) + 1)]
        counter = 1
        while True:
            candidate = f"{base}_{counter}.{file_type}"
            if not os.path.exists(candidate):
                return candidate
            counter += 1

    def save_text(
        self,
        content_to_save,
        base_filename,
        starting_index,
        file_type,
        save_behavior,
        save_layout,
        reuse_exact_filenames,
    ):
        try:
            import os
            import folder_paths

            # Normalize scalar inputs that may arrive as [value]
            base_filename = self._unwrap_and_cast_str(base_filename, "zmongo_output")
            starting_index = self._unwrap_and_cast_int(starting_index, 1)
            file_type = self._unwrap_and_cast_str(file_type, "txt")
            save_behavior = self._unwrap_and_cast_str(save_behavior, "write new content")
            save_layout = self._unwrap_and_cast_str(save_layout, "one combined file")
            reuse_exact_filenames = self._unwrap_and_cast_bool(reuse_exact_filenames, False)

            output_dir = folder_paths.get_output_directory()
            items = self._coerce_to_items(content_to_save)

            append_mode = (save_behavior == "append to existing file")
            file_open_mode = "a" if append_mode else "w"

            saved_paths = []

            if save_layout == "one combined file":
                filename = self._make_single_filename(base_filename, file_type)
                full_path = os.path.join(output_dir, filename)

                if not reuse_exact_filenames and not append_mode:
                    full_path = self._make_unique_path(full_path, file_type)

                self._ensure_parent_dir(full_path)

                with open(full_path, file_open_mode, encoding="utf-8") as f:
                    for i, item in enumerate(items):
                        text_value = self._item_to_text(item, file_type)
                        f.write(text_value)
                        if i < len(items) - 1:
                            f.write("\n")

                saved_paths.append(full_path)

            else:
                for offset, item in enumerate(items):
                    item_index = int(starting_index) + offset
                    filename = self._make_indexed_filename(base_filename, item_index, file_type)
                    full_path = os.path.join(output_dir, filename)

                    if not reuse_exact_filenames and not append_mode:
                        full_path = self._make_unique_path(full_path, file_type)

                    self._ensure_parent_dir(full_path)

                    text_value = self._item_to_text(item, file_type)

                    with open(full_path, file_open_mode, encoding="utf-8") as f:
                        f.write(text_value)

                    saved_paths.append(full_path)

            if len(saved_paths) == 1:
                return (saved_paths[0], len(saved_paths))

            return (json.dumps(saved_paths, indent=2, ensure_ascii=False), len(saved_paths))

        except Exception as exc:
            return (f"Error saving file(s): {exc}", 0)


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
    User-friendly record selection node for ZMongo.

    Main purposes:
    - Get one record
    - Get a range of records
    - Get all records
    - Get a batch from a range
    - Get random records
    - Return one field value, one record, or JSON lists for saving/export
    """

    @classmethod
    def INPUT_TYPES(cls):
        return {
            "required": {
                "mongo_uri": ("STRING", {"default": "mongodb://127.0.0.1:27017"}),
                "database_name": ("STRING", {"default": "test"}),
                "collection_name": ("STRING", {"default": "ocr_docs"}),

                "selection_mode": ([
                    "one record",
                    "record range",
                    "all records",
                    "first batch from range",
                    "one random record",
                    "random batch",
                ],),

                "record_start": ("INT", {"default": 1, "min": 1, "max": 100000000}),
                "record_end": ("INT", {"default": 10, "min": 1, "max": 100000000}),
                "record_step": ("INT", {"default": 1, "min": 1, "max": 100000}),
                "records_per_batch": ("INT", {"default": 1, "min": 1, "max": 1024}),

                "sort_by_field": ("STRING", {"default": "_id"}),
                "sort_order": (["ascending", "descending"],),

                "primary_field_name": ("STRING", {"default": "text"}),
                "fallback_field_names": ("STRING", {
                    "default": "text\ndoc_text\ndocument.text\nprompt",
                    "multiline": True,
                }),

                "filter_query_json": ("STRING", {"default": "{}", "multiline": True}),
                "fields_to_include_json": ("STRING", {"default": "{}", "multiline": True}),

                "randomize_order": ("BOOLEAN", {"default": False}),
                "random_seed": ("INT", {"default": 0, "min": 0, "max": 2147483647}),
                "remove_duplicate_ids": ("BOOLEAN", {"default": True}),
                "skip_records_with_empty_field": ("BOOLEAN", {"default": True}),
                "trim_whitespace": ("BOOLEAN", {"default": True}),
                "maximum_field_length": ("INT", {"default": 2000, "min": 0, "max": 100000}),

                "empty_value_behavior": ([
                    "skip record",
                    "return empty string",
                    "use placeholder text",
                ],),
                "placeholder_text": ("STRING", {"default": "empty prompt"}),

                "range_end_behavior": ([
                    "stop at end",
                    "wrap to beginning",
                    "bounce back",
                ],),

                "output_format": ([
                    "one field value",
                    "one record as json",
                    "one record and field value",
                    "record list as json",
                    "field value list as json",
                ],),

                "refresh_nonce": ("INT", {"default": 0, "min": 0, "max": 999999999}),
            }
        }

    RETURN_TYPES = (
        "STRING",  # selected_record_json_or_list
        "STRING",  # selected_field_value_or_list
        "INT",     # current_record_index
        "INT",     # total_record_count
        "INT",     # returned_item_count
        "STRING",  # summary
        "STRING",  # selection_state_json
    )
    RETURN_NAMES = (
        "selected_record_json_or_list",
        "selected_field_value_or_list",
        "current_record_index",
        "total_record_count",
        "returned_item_count",
        "summary",
        "selection_state_json",
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
    def _fallback_paths(primary_field_name: str, fallback_field_names: str) -> List[str]:
        paths: List[str] = []
        if primary_field_name and primary_field_name.strip():
            paths.append(primary_field_name.strip())
        for line in (fallback_field_names or "").splitlines():
            value = line.strip()
            if value and value not in paths:
                paths.append(value)
        return paths

    @staticmethod
    def _normalize_sort(sort_by_field: str, sort_order: str):
        direction = 1 if sort_order == "ascending" else -1
        field = (sort_by_field or "").strip() or "_id"
        return [(field, direction)]

    @staticmethod
    def _extract_field_value(
        record: Dict[str, Any],
        paths: List[str],
        trim_whitespace: bool,
        maximum_field_length: int,
    ) -> tuple[str, str]:
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

            if trim_whitespace:
                text = text.strip()

            if maximum_field_length > 0 and len(text) > maximum_field_length:
                text = text[:maximum_field_length]

            return text, path

        return "", ""

    @staticmethod
    def _apply_empty_value_behavior(
        field_value: str,
        empty_value_behavior: str,
        placeholder_text: str,
    ) -> str:
        if field_value:
            return field_value
        if empty_value_behavior == "return empty string":
            return ""
        if empty_value_behavior == "use placeholder text":
            return placeholder_text or "empty prompt"
        return ""

    @staticmethod
    def _doc_id_string(doc: Dict[str, Any]) -> str:
        value = doc.get("_id")
        return "" if value is None else str(value)

    def _load_records(
        self,
        collection,
        filter_query: Dict[str, Any],
        fields_to_include: Dict[str, Any],
        sort_spec,
    ) -> List[Dict[str, Any]]:
        cursor = collection.find(filter_query, fields_to_include or None).sort(sort_spec)
        return list(cursor)

    def _select_records(
        self,
        records: List[Dict[str, Any]],
        selection_mode: str,
        record_start: int,
        record_end: int,
        record_step: int,
        records_per_batch: int,
        randomize_order: bool,
        random_seed: int,
        remove_duplicate_ids: bool,
    ) -> tuple[List[Dict[str, Any]], int]:
        if not records:
            return [], 0

        working = list(records)

        if remove_duplicate_ids:
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

        rng = random.Random(random_seed)

        if randomize_order:
            rng.shuffle(working)

        total = len(working)

        bounded_start = max(1, min(int(record_start), total))
        bounded_end = max(1, min(int(record_end), total))
        safe_step = max(1, int(record_step))
        safe_batch_size = max(1, int(records_per_batch))

        if selection_mode == "one record":
            selected = [working[bounded_start - 1]]
            return selected, bounded_start

        if selection_mode == "one random record":
            idx = rng.randint(1, total)
            selected = [working[idx - 1]]
            return selected, idx

        if selection_mode == "all records":
            return working, 1 if working else 0

        if selection_mode == "record range":
            if bounded_start <= bounded_end:
                indices = list(range(bounded_start, bounded_end + 1, safe_step))
            else:
                indices = list(range(bounded_start, bounded_end - 1, -safe_step))
            selected = [working[i - 1] for i in indices]
            return selected, indices[0] if indices else 0

        if selection_mode == "first batch from range":
            if bounded_start <= bounded_end:
                indices = list(range(bounded_start, bounded_end + 1, safe_step))
            else:
                indices = list(range(bounded_start, bounded_end - 1, -safe_step))
            indices = indices[:safe_batch_size]
            selected = [working[i - 1] for i in indices]
            return selected, indices[0] if indices else 0

        if selection_mode == "random batch":
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
        selection_mode,
        record_start,
        record_end,
        record_step,
        records_per_batch,
        sort_by_field,
        sort_order,
        primary_field_name,
        fallback_field_names,
        filter_query_json,
        fields_to_include_json,
        randomize_order,
        random_seed,
        remove_duplicate_ids,
        skip_records_with_empty_field,
        trim_whitespace,
        maximum_field_length,
        empty_value_behavior,
        placeholder_text,
        range_end_behavior,
        output_format,
        refresh_nonce,
    ):
        _ = refresh_nonce
        client = None
        try:
            client = MongoClient(mongo_uri, serverSelectionTimeoutMS=5000)
            client.admin.command("ping")
            collection = client[database_name][collection_name]

            total_record_count = collection.count_documents({})

            filter_query = self._parse_json_object(filter_query_json, "filter_query_json")
            fields_to_include = self._parse_json_object(fields_to_include_json, "fields_to_include_json")
            sort_spec = self._normalize_sort(sort_by_field, sort_order)
            paths = self._fallback_paths(primary_field_name, fallback_field_names)

            records = self._load_records(collection, filter_query, fields_to_include, sort_spec)

            selected_records, current_record_index = self._select_records(
                records=records,
                selection_mode=selection_mode,
                record_start=record_start,
                record_end=record_end,
                record_step=record_step,
                records_per_batch=records_per_batch,
                randomize_order=randomize_order,
                random_seed=random_seed,
                remove_duplicate_ids=remove_duplicate_ids,
            )

            if not selected_records:
                selection_state = {
                    "database_name": str(database_name),
                    "collection_name": str(collection_name),
                    "total_record_count": int(total_record_count),
                    "filtered_record_count": int(len(records)),
                    "record_start": int(record_start),
                    "record_end": int(record_end),
                    "record_step": int(record_step),
                    "current_record_index": 0,
                    "records_per_batch": int(records_per_batch),
                    "selection_mode": str(selection_mode),
                    "range_end_behavior": str(range_end_behavior),
                    "sort_by_field": str(sort_by_field or "_id"),
                    "sort_order": str(sort_order),
                    "field_name_used": "",
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
                    int(total_record_count),
                    0,
                    f"No records matched in {database_name}.{collection_name}",
                    _safe_json(selection_state),
                )

            field_items: List[Dict[str, Any]] = []
            kept_records: List[Dict[str, Any]] = []

            for offset, record in enumerate(selected_records):
                field_value, used_path = self._extract_field_value(
                    record=record,
                    paths=paths,
                    trim_whitespace=trim_whitespace,
                    maximum_field_length=maximum_field_length,
                )
                field_value = self._apply_empty_value_behavior(
                    field_value=field_value,
                    empty_value_behavior=empty_value_behavior,
                    placeholder_text=placeholder_text,
                )

                if skip_records_with_empty_field and not field_value:
                    continue

                record_copy = dict(record)
                record_copy["_loop_index"] = int(current_record_index + offset)
                record_copy["_database_name"] = str(database_name)
                record_copy["_collection_name"] = str(collection_name)
                record_copy["_field_path_used"] = used_path

                kept_records.append(record_copy)
                field_items.append({
                    "index": int(current_record_index + offset),
                    "record_id": self._doc_id_string(record_copy),
                    "field_path_used": used_path,
                    "field_value": field_value,
                })

            if not kept_records:
                selection_state = {
                    "database_name": str(database_name),
                    "collection_name": str(collection_name),
                    "total_record_count": int(total_record_count),
                    "filtered_record_count": int(len(records)),
                    "record_start": int(record_start),
                    "record_end": int(record_end),
                    "record_step": int(record_step),
                    "current_record_index": int(current_record_index),
                    "records_per_batch": int(records_per_batch),
                    "selection_mode": str(selection_mode),
                    "range_end_behavior": str(range_end_behavior),
                    "sort_by_field": str(sort_by_field or "_id"),
                    "sort_order": str(sort_order),
                    "field_name_used": "",
                    "record_id": "",
                    "has_next": False,
                    "has_previous": current_record_index > 1,
                    "wrapped": False,
                    "direction": "forward",
                }
                return (
                    "[]",
                    "",
                    int(current_record_index),
                    int(total_record_count),
                    0,
                    "All selected records were skipped because the chosen field was empty.",
                    _safe_json(selection_state),
                )

            first_record = kept_records[0]
            first_field_value = field_items[0]["field_value"]
            first_path = field_items[0]["field_path_used"]
            filtered_record_count = len(records)

            selection_state = {
                "database_name": str(database_name),
                "collection_name": str(collection_name),
                "total_record_count": int(total_record_count),
                "filtered_record_count": int(filtered_record_count),
                "record_start": int(record_start),
                "record_end": int(record_end),
                "record_step": int(record_step),
                "current_record_index": int(field_items[0]["index"]),
                "current_zero_based": max(0, int(field_items[0]["index"]) - 1),
                "records_per_batch": int(records_per_batch),
                "batch_number": 1,
                "has_next": int(field_items[-1]["index"]) < int(filtered_record_count),
                "has_previous": int(field_items[0]["index"]) > 1,
                "wrapped": False,
                "direction": "forward",
                "sort_by_field": str(sort_by_field or "_id"),
                "sort_order": str(sort_order),
                "selection_mode": str(selection_mode),
                "range_end_behavior": str(range_end_behavior),
                "field_name_used": first_path,
                "record_id": self._doc_id_string(first_record),
            }

            field_preview = first_field_value.replace("\n", " ")
            if len(field_preview) > 180:
                field_preview = field_preview[:180] + "..."

            summary = (
                f"Selection mode: {selection_mode}\n"
                f"Collection: {database_name}.{collection_name}\n"
                f"Filtered records: {filtered_record_count} / {total_record_count}\n"
                f"Current record index: {field_items[0]['index']}\n"
                f"Record _id: {self._doc_id_string(first_record)}\n"
                f"Field used: {first_path or '(none)'}\n"
                f"Value preview: {field_preview}"
            )

            if output_format == "one record as json":
                selected_record_output = _safe_json(first_record)
                selected_field_output = first_field_value
                returned_item_count = 1
            elif output_format == "one field value":
                selected_record_output = _safe_json(first_record)
                selected_field_output = first_field_value
                returned_item_count = 1
            elif output_format == "one record and field value":
                selected_record_output = _safe_json(first_record)
                selected_field_output = first_field_value
                returned_item_count = 1
            elif output_format == "record list as json":
                selected_record_output = _safe_json(kept_records)
                selected_field_output = _safe_json(field_items)
                returned_item_count = len(kept_records)
            elif output_format == "field value list as json":
                selected_record_output = _safe_json(kept_records)
                selected_field_output = _safe_json([item["field_value"] for item in field_items])
                returned_item_count = len(field_items)
            else:
                selected_record_output = _safe_json(first_record)
                selected_field_output = first_field_value
                returned_item_count = 1

            return (
                selected_record_output,
                selected_field_output,
                int(field_items[0]["index"]),
                int(total_record_count),
                int(returned_item_count),
                summary,
                _safe_json(selection_state),
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

            result = zmongo.run_sync(zmongo.insert_one_async, collection, record)
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
    "ZMongoRecordLoopNode": ZMongoRecordLoopNode,
    "ZMongoSaveTextNode": ZMongoSaveTextNode,
    "ZMongoDataPassThroughNode": ZMongoDataPassThroughNode,
    "ZMongoSaveBatchTextNode": ZMongoSaveBatchTextNode,
    "ZMongoSaveValueNode": ZMongoSaveValueNode,
    "ZMongoFlattenedFieldSelector": ZMongoFlattenedFieldSelector,
}

NODE_DISPLAY_NAME_MAPPINGS = {
    "ZMongoConfigNode": "ZMongo Config",
    "ZMongoTextFetcher": "ZMongo Text Fetcher",
    "ZMongoOperationsNode": "ZMongo Operations",
    "ZMongoRecordSplitter": "ZMongo Record Splitter",
    "ZMongoFieldSelector": "ZMongo Field Selector",
    "ZRetrieverNode": "ZMongo Retriever",
    "ZMongoDatabaseBrowserNode": "ZMongo Database Browser",
    "ZMongoRecordLoopNode": "ZMongo Record Loop",
    "ZMongoSaveTextNode": "ZMongo Save Text Node",
    "ZMongoDataPassThroughNode": "ZMongo Data Pass Through Node",
    "ZMongoSaveBatchTextNode": "ZMongo Save Batch Text Node",
    "ZMongoSaveValueNode": "ZMongo Save Value Node",
    "ZMongoFlattenedFieldSelector": "ZMongo Flattened Field Selector",
}