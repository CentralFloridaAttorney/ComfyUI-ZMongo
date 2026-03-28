import asyncio

from bson.objectid import ObjectId

from zmongo_toolbag.zmongo import ZMongo


class ZMongoTextFetcher:
    def __init__(self):
        # Initializes the ZMongo instance from your toolbag
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

        # Convert string ID to BSON ObjectId
        try:
            q_id = ObjectId(document_id) if ObjectId.is_valid(document_id) else document_id
        except:
            q_id = document_id

        async def _query():
            # Use the existing find_one helper
            return await self.zmongo.find_one(collection_name, {"_id": q_id})

        # ComfyUI is synchronous; we run the async query in a temporary loop
        loop = asyncio.new_event_loop()
        res = loop.run_until_complete(_query())
        loop.close()

        if res.success and res.data:
            # Output the 'text' field as requested
            return (str(res.data.get("text", "Field 'text' not found")),)
        return (f"Error: {res.error}",)

NODE_CLASS_MAPPINGS = {"ZMongoTextFetcher": ZMongoTextFetcher}
NODE_DISPLAY_NAME_MAPPINGS = {"ZMongoTextFetcher": "ZMongo Text Fetcher"}