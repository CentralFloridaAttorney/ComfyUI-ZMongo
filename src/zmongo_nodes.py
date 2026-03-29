import asyncio
import json
import logging
from typing import Any, List

from bson import json_util
from bson.objectid import ObjectId
from pymongo import MongoClient

from zmongo_toolbag.data_processing import DataProcessor
from zmongo_toolbag.zmongo import ZMongo
from zmongo_manager import ZMongoManager
from zmongo_toolbag.zembedder import ZEmbedder

logger = logging.getLogger(__name__)


class ZRetrieverNode:
    """LangChain-compatible semantic search using embeddings."""

    @classmethod
    def INPUT_TYPES(cls):
        return {
            "required": {
                "query_text": ("STRING", {"multiline": True}),
                "collection": ("STRING", {"default": "legal_codex"}),
                "similarity_threshold": ("FLOAT", {"default": 0.7, "min": 0.0, "max": 1.0}),
                "n_results": ("INT", {"default": 3, "min": 1, "max": 10}),
            }
        }

    RETURN_TYPES = ("STRING", "STRING")
    RETURN_NAMES = ("top_result_json", "summary")
    FUNCTION = "retrieve"
    CATEGORY = "ZMongo/AI"

    def retrieve(self, query_text, collection, similarity_threshold, n_results):
        try:
            manager = ZMongoManager.get_instance()
            embedder = ZEmbedder(output_dimensionality=768)

            res = manager.client.run_sync(
                embedder.find_similar_documents,
                query_text=query_text,
                target_collection=collection,
                n_results=n_results
            )

            if not res.success:
                return ("{}", f"Search failed: {res.error}")

            hits = [h for h in res.data["results"] if h["retrieval_score"] >= similarity_threshold]
            summary = "\n".join(
                [f"Score: {h['retrieval_score']:.4f} | ID: {h['document'].get('_id')}" for h in hits]
            )

            top_hit = DataProcessor.to_json(hits[0]["document"]) if hits else "{}"
            return top_hit, summary
        except Exception as e:
            logger.exception("ZRetrieverNode failure")
            return ("{}", f"Search failed: {e}")


class ZMongoDatabaseBrowserNode:
    """
    ComfyUI node to browse a MongoDB collection and output:
      1. selected_record_json
      2. record_list_summary
      3. total_count
      4. db_name_out
      5. coll_name_out
      6. selected_index_out

    Matches the workflow signature used by your database browser JSON.
    """

    @classmethod
    def INPUT_TYPES(cls):
        return {
            "required": {
                "mongo_uri": (
                    "STRING",
                    {
                        "default": "mongodb://127.0.0.1:27017",
                        "multiline": False,
                    },
                ),
                "database_name": (
                    "STRING",
                    {
                        "default": "test",
                        "multiline": False,
                    },
                ),
                "collection_name": (
                    "STRING",
                    {
                        "default": "documents",
                        "multiline": False,
                    },
                ),
                "limit": (
                    "INT",
                    {
                        "default": 50,
                        "min": 1,
                        "max": 1000,
                        "step": 1,
                    },
                ),
                "select_index": (
                    "INT",
                    {
                        "default": 1,
                        "min": 1,
                        "max": 100000,
                        "step": 1,
                    },
                ),
                "refresh_nonce": (
                    "INT",
                    {
                        "default": 1,
                        "min": 0,
                        "max": 999999999,
                        "step": 1,
                    },
                ),
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
    def _safe_json(obj: Any) -> str:
        """
        Serialize Mongo/BSON-safe JSON for PreviewAny and downstream text nodes.
        """
        try:
            return json.dumps(
                json.loads(json_util.dumps(obj)),
                indent=2,
                ensure_ascii=False,
                default=str,
            )
        except Exception:
            try:
                return json_util.dumps(obj, indent=2)
            except Exception as exc:
                return json.dumps({"error": f"Serialization failed: {exc}"}, indent=2)

    @staticmethod
    def _make_summary(records: List[dict]) -> str:
        """
        Build a compact human-readable list of records.
        """
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
                keys = list(record.keys())[:6]
                title = f"keys={keys}"

            lines.append(f"{idx}. _id={record_id} | {title}")

        return "\n".join(lines)

    def browse_database(
        self,
        mongo_uri: str,
        database_name: str,
        collection_name: str,
        limit: int,
        select_index: int,
        refresh_nonce: int,
    ):
        """
        refresh_nonce is intentionally unused except to force ComfyUI reevaluation.
        """
        _ = refresh_nonce

        client = None
        try:
            client = MongoClient(mongo_uri, serverSelectionTimeoutMS=5000)
            db = client[database_name]
            collection = db[collection_name]

            # Force connection test
            client.admin.command("ping")

            total_count = collection.count_documents({})

            cursor = collection.find({}).limit(limit)
            records = list(cursor)

            if not records:
                return (
                    json.dumps(
                        {
                            "status": "empty",
                            "message": f"No records found in {database_name}.{collection_name}",
                        },
                        indent=2,
                    ),
                    "No records found.",
                    int(total_count),
                    str(database_name),
                    str(collection_name),
                    int(0),
                )

            # Workflow uses 1-based indexing in the widget sample.
            bounded_index = max(1, min(select_index, len(records)))
            selected_record = records[bounded_index - 1]

            selected_record_json = self._safe_json(selected_record)
            record_list_summary = self._make_summary(records)

            return (
                selected_record_json,
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
    """Converts JSON arrays into individual processable strings for batching."""

    @classmethod
    def INPUT_TYPES(cls):
        return {"required": {"records_json": ("STRING", {"multiline": True})}}

    RETURN_TYPES = ("STRING", "INT")
    RETURN_NAMES = ("record_json_list", "count")
    OUTPUT_IS_LIST = (True, False)
    FUNCTION = "split"
    CATEGORY = "ZMongo/Database"

    def split(self, records_json):
        try:
            data = json.loads(records_json)
            records = data if isinstance(data, list) else [data]
            output = [DataProcessor.to_json(r) for r in records]
            return (output, len(output))
        except Exception as e:
            logger.exception("ZMongoRecordSplitter failure")
            return ([json.dumps({"error": str(e)})], 0)


class ZMongoFieldSelector:
    """Dynamic dot-notation field extractor with metadata for UI dropdowns."""

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
            val = DataProcessor.get_value(record, field_path)
            flattened = DataProcessor.flatten_dict(record)
            available_paths = "\n".join(sorted(flattened.keys()))
            return str(val), available_paths
        except Exception as e:
            logger.exception("ZMongoFieldSelector failure")
            return (f"Error: {e}", "")


class ZMongoOperationsNode:
    """Handles data ingestion and maintenance tasks."""

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

    def execute_op(self, collection, data_json, operation_type):
        try:
            manager = ZMongoManager.get_instance()
            data = json.loads(data_json)
            records = data if isinstance(data, list) else [data]

            if operation_type == "Add Unique (No Update)":
                res = manager.client.bulk_write(collection, records)
                return (DataProcessor.to_json(res.data), res.data.get("inserted_count", 0))

            res = manager.client.insert_many(collection, records)
            return DataProcessor.to_json(res.data), len(res.data.get("inserted_ids", []))
        except Exception as e:
            logger.exception("ZMongoOperationsNode failure")
            return (json.dumps({"error": str(e)}), 0)


class ZMongoConfigNode:
    """Manages the ZMongoManager singleton and connection state."""

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
            ZMongoManager.reinitialize(uri=mongo_uri, db_name=db_name)
            return (f"ZMongo connected to {db_name}",)
        except Exception as e:
            logger.exception("ZMongoConfigNode failure")
            return (f"ZMongo config failed: {e}",)


class ZMongoTextFetcher:
    def __init__(self):
        self.zmongo = ZMongo()

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
        if not document_id.strip():
            return ("No ID Provided",)

        try:
            q_id = ObjectId(document_id) if ObjectId.is_valid(document_id) else document_id
        except Exception:
            q_id = document_id

        async def _query():
            return await self.zmongo.find_one(collection_name, {"_id": q_id})

        loop = asyncio.new_event_loop()
        try:
            asyncio.set_event_loop(loop)
            res = loop.run_until_complete(_query())
        finally:
            loop.close()
            asyncio.set_event_loop(None)

        if res.success and res.data:
            return (str(res.data.get("text", "Field 'text' not found")),)
        return (f"Error: {res.error}",)

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
