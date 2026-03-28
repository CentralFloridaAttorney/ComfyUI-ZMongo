import json
from typing import Any, List

from bson import json_util
from pymongo import MongoClient


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


NODE_CLASS_MAPPINGS = {
    "ZMongoDatabaseBrowserNode": ZMongoDatabaseBrowserNode,
}

NODE_DISPLAY_NAME_MAPPINGS = {
    "ZMongoDatabaseBrowserNode": "ZMongo Database Browser",
}