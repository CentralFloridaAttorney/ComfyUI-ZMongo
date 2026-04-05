import json
import logging
from typing import Any, Dict, List, Optional, Tuple

from bson import ObjectId

from ..zmongo_toolbag.data_processor import DataProcessor
from ..zmongo_toolbag.safe_result import SafeResult
from ..zmongo_toolbag.zmongo import ZMongo

logger = logging.getLogger(__name__)


def _safe_json(obj: Any) -> str:
    try:
        if isinstance(obj, SafeResult):
            return obj.to_json(indent=2)
        return DataProcessor.to_json(obj, indent=2)
    except Exception as exc:
        return json.dumps({"error": f"Serialization failed: {exc}"}, indent=2)


def _parse_json_object(raw: str, field_name: str, default: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    text = (raw or "").strip()
    if not text:
        return default or {}

    try:
        parsed = json.loads(text)
    except Exception as exc:
        raise ValueError(f"{field_name} is not valid JSON: {exc}") from exc

    if not isinstance(parsed, dict):
        raise ValueError(f"{field_name} must be a JSON object")

    return parsed


def _normalize_document_id(document_id: str) -> Any:
    raw = str(document_id or "").strip()
    if not raw:
        return ""
    return ObjectId(raw) if ObjectId.is_valid(raw) else raw


def _extract_document_from_result(result: SafeResult) -> Optional[Dict[str, Any]]:
    if not isinstance(result, SafeResult) or not result.success:
        return None

    if isinstance(result.data, dict):
        document = result.data.get("document")
        if isinstance(document, dict):
            return document

    original = result.original()
    if isinstance(original, dict):
        document = original.get("document")
        if isinstance(document, dict):
            return document

    return None


def _extract_documents_from_result(result: SafeResult) -> List[Dict[str, Any]]:
    if not isinstance(result, SafeResult) or not result.success:
        return []

    if isinstance(result.data, dict):
        documents = result.data.get("documents")
        if isinstance(documents, list):
            return [doc for doc in documents if isinstance(doc, dict)]

    original = result.original()
    if isinstance(original, dict):
        documents = original.get("documents")
        if isinstance(documents, list):
            return [doc for doc in documents if isinstance(doc, dict)]

    return []


def _stringify_value(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, str):
        return value
    try:
        return DataProcessor.to_json(value, indent=2)
    except Exception:
        return str(value)


def _clamp_index(index_value: int, item_count: int) -> int:
    if item_count <= 0:
        return 0
    return max(0, min(int(index_value), item_count - 1))


def _preview_text(value: Any, max_len: int = 140) -> str:
    text = _stringify_value(value).replace("\n", " ").strip()
    if len(text) > max_len:
        return text[:max_len] + "..."
    return text


def _normalize_sort(sort_field: str, sort_direction: str) -> List[Tuple[str, int]]:
    field = str(sort_field or "").strip() or "_id"
    direction = 1 if str(sort_direction or "").strip().lower() == "ascending" else -1
    return [(field, direction)]


def _build_summary_lines(records: List[Dict[str, Any]], base_offset: int = 0) -> str:
    if not records:
        return "No records found."

    lines: List[str] = []
    for idx, record in enumerate(records, start=1):
        record_num = base_offset + idx
        record_id = str(record.get("_id", ""))
        title = (
            record.get("title")
            or record.get("name")
            or record.get("username")
            or record.get("email")
            or record.get("text")
            or record.get("content")
            or f"keys={list(record.keys())[:6]}"
        )
        lines.append(f"{record_num}. _id={record_id} | {_preview_text(title, max_len=100)}")
    return "\n".join(lines)


class ZMongoRecordRangeSelectorNode:
    """
    Retrieve a range of records from a collection using a stable sort.

    Main use:
    - browse a collection
    - feed record lists into downstream utilities
    - provide summaries for users selecting records by index
    """

    CATEGORY = "ZMongo/Workflow"
    FUNCTION = "select_range"
    RETURN_TYPES = ("STRING", "INT", "INT", "STRING", "STRING")
    RETURN_NAMES = (
        "record_list_json",
        "returned_count",
        "total_count",
        "record_summary",
        "status_json",
    )

    @classmethod
    def INPUT_TYPES(cls) -> Dict[str, Any]:
        return {
            "required": {
                "zmongo": ("ZMONGO_CONNECTION",),
                "collection_name": ("STRING", {"default": ""}),
                "start_index": ("INT", {"default": 0, "min": 0, "max": 1000000, "step": 1}),
                "limit": ("INT", {"default": 25, "min": 1, "max": 1000, "step": 1}),
                "sort_field": ("STRING", {"default": "_id"}),
                "sort_direction": (["ascending", "descending"], {"default": "ascending"}),
            },
            "optional": {
                "query_json": ("STRING", {"default": "{}", "multiline": True}),
            },
        }

    @classmethod
    def IS_CHANGED(
        cls,
        zmongo: Any,
        collection_name: str,
        start_index: int,
        limit: int,
        sort_field: str,
        sort_direction: str,
        query_json: str = "{}",
    ):
        return f"{id(zmongo)}|{collection_name}|{start_index}|{limit}|{sort_field}|{sort_direction}|{query_json}"

    def select_range(
        self,
        zmongo: ZMongo,
        collection_name: str,
        start_index: int,
        limit: int,
        sort_field: str,
        sort_direction: str,
        query_json: str = "{}",
    ):
        if zmongo is None:
            failure = SafeResult.fail("No ZMongo connection provided")
            return ("[]", 0, 0, "No records found.", failure.to_json(indent=2))

        try:
            collection_name = str(collection_name or "").strip()
            if not collection_name:
                raise ValueError("collection_name is required")

            query = _parse_json_object(query_json, "query_json", default={})
            sort_spec = _normalize_sort(sort_field, sort_direction)
            start_index = max(0, int(start_index))
            limit = max(1, int(limit))

            total_res = zmongo.count_documents(collection_name, query)
            total_count = 0
            if total_res.success and isinstance(total_res.data, dict):
                total_count = int(total_res.data.get("count", 0))

            result = zmongo.find_many(
                collection_name,
                query=query,
                sort=sort_spec,
                limit=start_index + limit,
                cache=False,
            )

            docs = _extract_documents_from_result(result)
            selected_docs = docs[start_index : start_index + limit]

            status_payload = {
                "success": result.success,
                "collection_name": collection_name,
                "query_used": DataProcessor.to_json_compatible(query),
                "sort": sort_spec,
                "start_index": start_index,
                "limit": limit,
                "returned_count": len(selected_docs),
                "total_count": total_count,
                "error": result.error,
                "message": result.message,
            }

            return (
                _safe_json(selected_docs),
                len(selected_docs),
                total_count,
                _build_summary_lines(selected_docs, base_offset=start_index),
                _safe_json(status_payload),
            )
        except Exception as exc:
            logger.exception("ZMongoRecordRangeSelectorNode failure")
            failure = SafeResult.from_exception(exc, operation="select_range")
            return ("[]", 0, 0, "No records found.", failure.to_json(indent=2))


class ZMongoRecordBrowserNode:
    """
    Browse and select a single record by record_index from a collection.

    Main use:
    - user-friendly browsing
    - stable selected_record_json / selected_record_id outputs
    - summary text for previews
    """

    CATEGORY = "ZMongo/Workflow"
    FUNCTION = "browse_record"
    RETURN_TYPES = ("STRING", "STRING", "INT", "INT", "STRING", "STRING")
    RETURN_NAMES = (
        "selected_record_json",
        "selected_record_id",
        "selected_index",
        "total_count",
        "record_summary",
        "status_json",
    )

    @classmethod
    def INPUT_TYPES(cls) -> Dict[str, Any]:
        return {
            "required": {
                "zmongo": ("ZMONGO_CONNECTION",),
                "collection_name": ("STRING", {"default": ""}),
                "record_index": ("INT", {"default": 0, "min": 0, "max": 1000000, "step": 1}),
                "sort_field": ("STRING", {"default": "_id"}),
                "sort_direction": (["ascending", "descending"], {"default": "ascending"}),
            },
            "optional": {
                "query_json": ("STRING", {"default": "{}", "multiline": True}),
                "summary_window": ("INT", {"default": 10, "min": 1, "max": 100, "step": 1}),
            },
        }

    @classmethod
    def IS_CHANGED(
        cls,
        zmongo: Any,
        collection_name: str,
        record_index: int,
        sort_field: str,
        sort_direction: str,
        query_json: str = "{}",
        summary_window: int = 10,
    ):
        return (
            f"{id(zmongo)}|{collection_name}|{record_index}|"
            f"{sort_field}|{sort_direction}|{query_json}|{summary_window}"
        )

    def browse_record(
        self,
        zmongo: ZMongo,
        collection_name: str,
        record_index: int,
        sort_field: str,
        sort_direction: str,
        query_json: str = "{}",
        summary_window: int = 10,
    ):
        if zmongo is None:
            failure = SafeResult.fail("No ZMongo connection provided")
            return ("{}", "", 0, 0, "No records found.", failure.to_json(indent=2))

        try:
            collection_name = str(collection_name or "").strip()
            if not collection_name:
                raise ValueError("collection_name is required")

            query = _parse_json_object(query_json, "query_json", default={})
            sort_spec = _normalize_sort(sort_field, sort_direction)

            total_res = zmongo.count_documents(collection_name, query)
            total_count = 0
            if total_res.success and isinstance(total_res.data, dict):
                total_count = int(total_res.data.get("count", 0))

            if total_count <= 0:
                status_payload = {
                    "success": True,
                    "collection_name": collection_name,
                    "query_used": DataProcessor.to_json_compatible(query),
                    "sort": sort_spec,
                    "record_index": 0,
                    "total_count": 0,
                    "message": "No records found",
                }
                return ("{}", "", 0, 0, "No records found.", _safe_json(status_payload))

            selected_index = _clamp_index(record_index, total_count)

            range_limit = max(1, int(summary_window))
            preview_start = max(0, selected_index)
            preview_res = zmongo.find_many(
                collection_name,
                query=query,
                sort=sort_spec,
                limit=preview_start + range_limit,
                cache=False,
            )
            preview_docs = _extract_documents_from_result(preview_res)
            window_docs = preview_docs[preview_start : preview_start + range_limit]

            record_res = zmongo.find_many(
                collection_name,
                query=query,
                sort=sort_spec,
                limit=selected_index + 1,
                cache=False,
            )
            docs = _extract_documents_from_result(record_res)
            if selected_index >= len(docs):
                return ("{}", "", 0, total_count, "No records found.", _safe_json(record_res.to_dict()))

            selected_record = docs[selected_index]
            selected_record_id = str(selected_record.get("_id", ""))
            summary = _build_summary_lines(window_docs, base_offset=preview_start)

            status_payload = {
                "success": True,
                "collection_name": collection_name,
                "query_used": DataProcessor.to_json_compatible(query),
                "sort": sort_spec,
                "record_index": selected_index,
                "total_count": total_count,
                "selected_record_id": selected_record_id,
            }

            return (
                _safe_json(selected_record),
                selected_record_id,
                selected_index,
                total_count,
                summary,
                _safe_json(status_payload),
            )
        except Exception as exc:
            logger.exception("ZMongoRecordBrowserNode failure")
            failure = SafeResult.from_exception(exc, operation="browse_record")
            return ("{}", "", 0, 0, "No records found.", failure.to_json(indent=2))


class ZMongoLoadRecordByIdNode:
    """
    Convenience node to load one record directly by _id.

    This is useful when a workflow already knows the document id and wants
    a cleaner path than passing query JSON around.
    """

    CATEGORY = "ZMongo/Workflow"
    FUNCTION = "load_by_id"
    RETURN_TYPES = ("STRING", "STRING", "STRING")
    RETURN_NAMES = ("record_json", "record_id", "status_json")

    @classmethod
    def INPUT_TYPES(cls) -> Dict[str, Any]:
        return {
            "required": {
                "zmongo": ("ZMONGO_CONNECTION",),
                "collection_name": ("STRING", {"default": ""}),
                "document_id": ("STRING", {"default": ""}),
            }
        }

    def load_by_id(self, zmongo: ZMongo, collection_name: str, document_id: str):
        if zmongo is None:
            failure = SafeResult.fail("No ZMongo connection provided")
            return ("{}", "", failure.to_json(indent=2))

        try:
            collection_name = str(collection_name or "").strip()
            if not collection_name:
                raise ValueError("collection_name is required")

            normalized_id = _normalize_document_id(document_id)
            if normalized_id == "":
                raise ValueError("document_id is required")

            result = zmongo.find_one(collection_name, {"_id": normalized_id}, cache=False)
            document = _extract_document_from_result(result)

            if not result.success or document is None:
                status_payload = result.to_dict()
                status_payload["collection_name"] = collection_name
                status_payload["document_id"] = document_id
                return ("{}", "", _safe_json(status_payload))

            record_id = str(document.get("_id", ""))
            status_payload = {
                "success": True,
                "collection_name": collection_name,
                "document_id": record_id,
            }
            return (_safe_json(document), record_id, _safe_json(status_payload))
        except Exception as exc:
            logger.exception("ZMongoLoadRecordByIdNode failure")
            failure = SafeResult.from_exception(exc, operation="load_by_id")
            return ("{}", "", failure.to_json(indent=2))


NODE_CLASS_MAPPINGS = {
    "ZMongoRecordRangeSelectorNode": ZMongoRecordRangeSelectorNode,
    "ZMongoRecordBrowserNode": ZMongoRecordBrowserNode,
    "ZMongoLoadRecordByIdNode": ZMongoLoadRecordByIdNode,
}

NODE_DISPLAY_NAME_MAPPINGS = {
    "ZMongoRecordRangeSelectorNode": "ZMongo Record Range Selector",
    "ZMongoRecordBrowserNode": "ZMongo Record Browser",
    "ZMongoLoadRecordByIdNode": "ZMongo Load Record By ID",
}