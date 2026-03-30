import json
import logging
import threading
from typing import Any, Dict, List, Optional

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
    """Dynamic dot-path field extractor with path list output."""

    @classmethod
    def INPUT_TYPES(cls):
        return {
            "required": {
                "record_json": ("STRING", {"forceInput": True}),
                "field_path": ("STRING", {"default": "metadata.case_name"}),
            }
        }

    RETURN_TYPES = ("STRING", "STRING")
    RETURN_NAMES = ("field_value", "available_paths")
    FUNCTION = "select_field"
    CATEGORY = "ZMongo/Database"

    def select_field(self, record_json, field_path):
        try:
            record = json.loads(record_json)
            value = DataProcessor.get_value(record, field_path)
            flattened = DataProcessor.flatten_dict(record)
            available_paths = "\n".join(sorted(flattened.keys()))
            if value is None:
                return ("", available_paths)
            if isinstance(value, (dict, list)):
                return (_safe_json(value), available_paths)
            return (str(value), available_paths)
        except Exception as exc:
            logger.exception("ZMongoFieldSelector failure")
            return (f"Error: {exc}", "")


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
