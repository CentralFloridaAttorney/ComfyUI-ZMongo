# ComfyUI-ZMongo Nodes — Internet-based
# Fleet nodes removed, API submenu flattened
# Categories: 00 Auth, 01 Service, 02 Collections, 03 Docs, 04 Images, 99 Helpers

from session_client import ZMongoApiSession, _flatten_path_keys, _dirty_token, _json_text, safe_get_by_path
from typing import Any, Dict, List, Optional
import json

# -------------------------------
# 00 Auth
# -------------------------------
class ZMongoApiKeySessionNode:
    display_name = "00 API Key Session"
    category = "00 Auth"

    @classmethod
    def INPUT_TYPES(cls):
        return {"required": {"base_url": ("STRING", {"default": "https://businessprocessapplications.com"}),
                             "zai_api_key": ("STRING", {"default": ""}),
                             "username": ("STRING", {"default": ""}),
                             "timeout": ("INT", {"default": 30})}}

    RETURN_TYPES = ("ZMONGO_API_SESSION", "STRING")
    RETURN_NAMES = ("session", "json")
    FUNCTION = "connect"

    def connect(self, base_url: str, zai_api_key: str, username: str, timeout: int):
        try:
            session = ZMongoApiSession(base_url=base_url, zai_api_key=zai_api_key, username=username, timeout=timeout)
            payload = session.whoami()
            return (session, _json_text(payload))
        except Exception as exc:
            return (None, _json_text({"success": False, "message": str(exc)}))

class ZMongoApiCloseSessionNode:
    display_name = "00 Close API Session"
    category = "00 Auth"

    @classmethod
    def INPUT_TYPES(cls):
        return {"required": {"session": ("ZMONGO_API_SESSION",)}}

    RETURN_TYPES = ("STRING",)
    RETURN_NAMES = ("json",)
    FUNCTION = "close_session"

    def close_session(self, session):
        if session is None:
            return (_json_text({"success": False, "message": "No session provided"}),)
        session.close()
        return (_json_text({"success": True, "message": "Session closed"}),)

# -------------------------------
# 01 Service
# -------------------------------
class ZMongoApiHealthNode:
    display_name = "01 Health"
    category = "01 Service"

    @classmethod
    def INPUT_TYPES(cls):
        return {"required": {"session": ("ZMONGO_API_SESSION",)}}

    RETURN_TYPES = ("STRING", "BOOLEAN")
    RETURN_NAMES = ("json", "success")
    FUNCTION = "health"

    def health(self, session):
        if session is None:
            return (_json_text({"success": False, "message": "No session"}), False)
        payload = session.health()
        return (_json_text(payload), bool(payload.get("success")))

class ZMongoApiWhoamiNode:
    display_name = "01 Who Am I"
    category = "01 Service"

    @classmethod
    def INPUT_TYPES(cls):
        return {"required": {"session": ("ZMONGO_API_SESSION",)}}

    RETURN_TYPES = ("STRING", "STRING", "STRING", "BOOLEAN")
    RETURN_NAMES = ("json", "username", "db_name", "success")
    FUNCTION = "whoami"

    def whoami(self, session):
        if session is None:
            return (_json_text({"success": False, "message": "No session"}), "", "", False)
        payload = session.whoami()
        data = payload.get("data", {}) if isinstance(payload, dict) else {}
        username = str(data.get("username") or "")
        db_name = str(data.get("silo_db_name") or data.get("db_name") or "")
        return (_json_text(payload), username, db_name, bool(payload.get("success")))

# -------------------------------
# 02 Collections
# -------------------------------
class ZMongoApiListCollectionsNode:
    display_name = "02 List Collections"
    category = "02 Collections"

    @classmethod
    def INPUT_TYPES(cls):
        return {"required": {"session": ("ZMONGO_API_SESSION",)}}

    RETURN_TYPES = ("STRING", "*", "STRING")
    RETURN_NAMES = ("json", "collections", "indexed")
    OUTPUT_IS_LIST = (False, True, False)
    FUNCTION = "list_collections"

    def list_collections(self, session):
        if session is None:
            return (_json_text({"success": False, "message": "No session"}), [], "")
        payload = session.list_collections()
        collections = payload.get("data", {}).get("collections", []) if payload.get("success") else []
        return (_json_text(payload), collections, "\n".join(str(c) for c in collections))

class ZMongoApiCreateCollectionNode:
    display_name = "02 Create Collection"
    category = "02 Collections"

    @classmethod
    def INPUT_TYPES(cls):
        return {"required": {"session": ("ZMONGO_API_SESSION",), "collection_name": ("STRING", {"default": ""})}}

    RETURN_TYPES = ("STRING", "STRING")
    RETURN_NAMES = ("json", "refresh")
    FUNCTION = "create_collection"

    def create_collection(self, session, collection_name: str):
        token = _dirty_token("create_collection", collection_name)
        if session is None:
            return (_json_text({"success": False, "message": "No session"}), token)
        payload = session.create_collection(collection_name)
        return (_json_text(payload), token)

class ZMongoApiDeleteCollectionNode:
    display_name = "02 Delete Collection"
    category = "02 Collections"

    @classmethod
    def INPUT_TYPES(cls):
        return {"required": {"session": ("ZMONGO_API_SESSION",), "collection_name": ("STRING", {"default": ""})}}

    RETURN_TYPES = ("STRING", "STRING")
    RETURN_NAMES = ("json", "refresh")
    FUNCTION = "delete_collection"

    def delete_collection(self, session, collection_name: str):
        token = _dirty_token("delete_collection", collection_name)
        if session is None:
            return (_json_text({"success": False, "message": "No session"}), token)
        payload = session.delete_collection(collection_name)
        return (_json_text(payload), token)

# -------------------------------
# 03 Docs
# -------------------------------
class ZMongoApiListDocsNode:
    display_name = "03 List Docs"
    category = "03 Docs"

    @classmethod
    def INPUT_TYPES(cls):
        return {"required": {"session": ("ZMONGO_API_SESSION",), "collection_name": ("STRING", {"default": ""})}}

    RETURN_TYPES = ("STRING", "*", "STRING")
    RETURN_NAMES = ("json", "ids", "indexed")
    OUTPUT_IS_LIST = (False, True, False)
    FUNCTION = "list_docs"

    def list_docs(self, session, collection_name: str):
        if session is None:
            return (_json_text({"success": False, "message": "No session"}), [], "")
        payload = session.list_docs(collection_name)
        ids = [str(doc.get("_id")) for doc in payload.get("data", []) if isinstance(doc, dict)]
        return (_json_text(payload), ids, "\n".join(ids))

# -------------------------------
# 04 Images
# -------------------------------
class ZMongoDisplayImageNode:
    display_name = "04 Display Image"
    category = "04 Images"

    @classmethod
    def INPUT_TYPES(cls):
        return {"required": {"session": ("ZMONGO_API_SESSION",),
                             "collection_name": ("STRING", {"default": ""}),
                             "document_id": ("STRING", {"default": ""}),
                             "field_path": ("STRING", {"default": "image_data"})}}

    RETURN_TYPES = ("IMAGE", "STRING", "STRING")
    RETURN_NAMES = ("image", "status", "json")
    FUNCTION = "display_image"

    def display_image(self, session, collection_name: str, document_id: str, field_path: str):
        image_bytes, status, payload = session.fetch_image_field(collection_name, document_id, field_path)
        img = Image.open(io.BytesIO(image_bytes))
        tensor = torch.from_numpy(np.array(img)[None, ...] / 255.0).float()
        return tensor, status, payload

class ZMongoApiEasySaveImageNode:
    display_name = "04 Easy Save Image"
    category = "04 Images"

    @classmethod
    def INPUT_TYPES(cls):
        return {"required": {"session": ("ZMONGO_API_SESSION",),
                             "image_bytes": ("BYTES",),
                             "collection_name": ("STRING", {"default": "images"}),
                             "document_id": ("STRING", {"default": ""}),
                             "field_path": ("STRING", {"default": "image_data"}),
                             "filename": ("STRING", {"default": "comfy_image.png"})}}

    RETURN_TYPES = ("STRING", "STRING", "STRING", "STRING", "BOOLEAN")
    RETURN_NAMES = ("json", "document_id", "field_path", "refresh", "created_new_document")
    FUNCTION = "save_image"

    def save_image(self, session, image_bytes: bytes, collection_name: str, document_id: str, field_path: str, filename: str):
        payload = session.upload_image_to_field(collection_name, image_bytes, filename, document_id, field_path)
        new_id = payload.get("data", {}).get("_id", "")
        return (_json_text(payload), new_id, field_path, _dirty_token("save_image", collection_name, new_id), document_id == "")

# -------------------------------
# 99 Helpers
# -------------------------------
class ZMongoApiSelectNthItemNode:
    display_name = "99 Select Nth Item"
    category = "99 Helpers"

    @classmethod
    def INPUT_TYPES(cls):
        return {"required": {"items": ("*",), "index": ("INT", {"default": 0})}}

    RETURN_TYPES = ("STRING",)
    RETURN_NAMES = ("item",)
    FUNCTION = "select_nth_item"

    def select_nth_item(self, items, index):
        items_list = list(items)
        if not items_list:
            return ("",)
        idx = max(0, min(index, len(items_list)-1))
        return (str(items_list[idx]),)

class ZMongoApiJsonPickNode:
    display_name = "99 JSON Pick"
    category = "99 Helpers"

    @classmethod
    def INPUT_TYPES(cls):
        return {"required": {"json_text": ("STRING", {"default": "{}"}), "path": ("STRING", {"default": ""})}}

    RETURN_TYPES = ("STRING",)
    RETURN_NAMES = ("value",)
    FUNCTION = "json_pick"

    def json_pick(self, json_text: str, path: str):
        try:
            data = json.loads(json_text)
        except Exception:
            return ("",)
        val = safe_get_by_path(data, path)
        return (str(val),)

class ZMongoApiMetadataFlattenedPathsNode:
    display_name = "04 Metadata Flattened Paths"
    category = "04 Images"

    @classmethod
    def INPUT_TYPES(cls):
        return {"required": {"session": ("ZMONGO_API_SESSION",),
                             "collection_name": ("STRING", {"default": ""}),
                             "document_id": ("STRING", {"default": ""}),
                             "metadata_field_path": ("STRING", {"default": ""})}}

    RETURN_TYPES = ("STRING", "*", "STRING", "INT")
    RETURN_NAMES = ("json", "paths", "indexed", "count")
    OUTPUT_IS_LIST = (False, True, False)
    FUNCTION = "metadata_flattened_paths"

    def metadata_flattened_paths(self, session, collection_name: str, document_id: str, metadata_field_path: str):
        payload = session.get_doc(collection_name, document_id)
        document = payload.get("data", {}).get("document", {})
        flat_keys = _flatten_path_keys(document)
        paths = sorted(flat_keys.keys())
        return (_json_text(payload), paths, "\n".join(paths), len(paths))


# -------------------------------
# Node class mappings
# -------------------------------

__all__ = [
    "ZMongoApiKeySessionNode",
    "ZMongoApiCloseSessionNode",
    "ZMongoApiHealthNode",
    "ZMongoApiWhoamiNode",
    "ZMongoApiListCollectionsNode",
    "ZMongoApiCreateCollectionNode",
    "ZMongoApiDeleteCollectionNode",
    "ZMongoApiListDocsNode",
    "ZMongoApiDisplayImageNode",
    "ZMongoApiEasySaveImageNode",
    "ZMongoApiSelectNthItemNode",
    "ZMongoApiJsonPickNode",
    "ZMongoApiMetadataFlattenedPathsNode",
]