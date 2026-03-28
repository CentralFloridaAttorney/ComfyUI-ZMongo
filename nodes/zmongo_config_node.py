from src.utils.zmongo_manager import ZMongoManager


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
        # Reinitializes the singleton with new parameters
        ZMongoManager.reinitialize(uri=mongo_uri, db_name=db_name)
        return (f"ZMongo connected to {db_name}",)