import json

from zmongo_manager import ZMongoManager
from zmongo_toolbag.data_processing import DataProcessor


class ZMongoOperationsNode:
    """Handles unique data ingestion and maintenance tasks."""

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
        manager = ZMongoManager.get_instance()
        data = json.loads(data_json)
        records = data if isinstance(data, list) else [data]

        if operation_type == "Add Unique (No Update)":
            # Implements 'check-before-insert' via async bulk_write
            res = manager.client.run_sync(manager.client.bulk_write_async, collection, records)
            return (DataProcessor.to_json(res.data), res.data.get("inserted_count", 0))

        res = manager.client.insert_many(collection, records)
        return (DataProcessor.to_json(res.data), len(res.data.get("inserted_ids", [])))