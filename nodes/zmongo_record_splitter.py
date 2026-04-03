import json
from .zmongo_toolbag.data_processor import DataProcessor


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
            # Handles concatenated objects and arrays
            data = json.loads(records_json)
            records = data if isinstance(data, list) else [data]
            output = [DataProcessor.to_json(r) for r in records]
            return (output, len(output))
        except Exception as e:
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
        record = json.loads(record_json)
        # Extract nested values using DataProcessor dot-notation
        val = DataProcessor.get_value(record, field_path)

        # Output paths to allow JS extension to build dropdowns
        flattened = DataProcessor.flatten_dict(record)
        available_paths = "\n".join(sorted(flattened.keys()))

        return (str(val), available_paths)