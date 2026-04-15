from __future__ import annotations

import base64
import io
from typing import Any, Dict

import numpy as np
import torch
from PIL import Image, ImageOps

from .session_client import (
    ZTarotManagerSessionClient,
    find_doc_by_key,
    json_dumps,
    safe_get_by_path,
)

JSON_TYPE = "ZMONGO_JSON"
CLIENT_TYPE = "ZMONGO_MANAGER_SESSION_CLIENT"


def _empty_outputs(message: str):
    image = torch.zeros((1, 64, 64, 3), dtype=torch.float32)
    mask = torch.zeros((1, 64, 64), dtype=torch.float32)
    payload = {
        "success": False,
        "message": message,
        "status_code": 0,
        "error": {"msg": message},
        "data": {},
    }
    return image, mask, json_dumps(payload), "", False


def _require_client(client: ZTarotManagerSessionClient) -> ZTarotManagerSessionClient:
    if client is None:
        raise RuntimeError(
            "ZMongo session client is missing. The upstream 'ZMongo Session Connect' node failed."
        )
    return client


def _looks_like_bytes_envelope(value: Any) -> bool:
    return isinstance(value, dict) and value.get("__type__") == "bytes" and "data" in value


def _extract_bytes_envelope(value: Any) -> Dict[str, Any]:
    """
    Accept all image-storage shapes that have shown up so far:

    1. Direct bytes envelope:
       field_path -> {"__type__": "bytes", ...}

    2. Image record:
       field_path -> {"image": {"__type__": "bytes", ...}, ...}

    3. Result wrapper accidentally saved instead of data_json:
       field_path -> {"success": true, "data": {"__type__": "bytes", ...}, ...}

    4. Nested record containing a wrapped image:
       field_path -> {"image": {"success": true, "data": {"__type__": "bytes", ...}}}
    """
    if _looks_like_bytes_envelope(value):
        return value

    if isinstance(value, dict):
        direct_data = value.get("data")
        if _looks_like_bytes_envelope(direct_data):
            return direct_data

        image_value = value.get("image")
        if _looks_like_bytes_envelope(image_value):
            return image_value

        if isinstance(image_value, dict):
            image_data = image_value.get("data")
            if _looks_like_bytes_envelope(image_data):
                return image_data

    raise ValueError(
        "Selected field does not contain a recognized ZMongo image payload. "
        "Expected a bytes envelope, an image record, or a saved result wrapper with data.__type__='bytes'."
    )


def _decode_image_tensors(envelope: Dict[str, Any]):
    encoded = envelope.get("data")
    if not encoded:
        raise ValueError("Bytes envelope is missing base64 image data.")

    raw = base64.b64decode(encoded)
    pil = Image.open(io.BytesIO(raw))
    pil = ImageOps.exif_transpose(pil)

    if "A" in pil.getbands():
        rgba = pil.convert("RGBA")
        rgb = rgba.convert("RGB")
        alpha = np.array(rgba.getchannel("A")).astype(np.float32) / 255.0
        mask = 1.0 - alpha
    else:
        rgb = pil.convert("RGB")
        mask = np.zeros((rgb.height, rgb.width), dtype=np.float32)

    image_np = np.array(rgb).astype(np.float32) / 255.0
    image_tensor = torch.from_numpy(image_np)[None,]
    mask_tensor = torch.from_numpy(mask)[None,]
    return image_tensor, mask_tensor, rgb.width, rgb.height


class ZMongoLoadImageNode:
    CATEGORY = "ZMongo/Image"
    FUNCTION = "load_image"
    RETURN_TYPES = ("IMAGE", "MASK", JSON_TYPE, "STRING", "BOOLEAN")
    RETURN_NAMES = ("image", "mask", "result_json", "text_output", "success")

    @classmethod
    def INPUT_TYPES(cls):
        return {
            "required": {
                "client": (CLIENT_TYPE,),
                "collection_name": ("STRING", {"default": ""}),
                "lookup_mode": (["doc_key", "document_id"],),
                "lookup_value": ("STRING", {"default": ""}),
                "field_path": ("STRING", {"default": "images.0.image"}),
                "read_limit": ("INT", {"default": 200, "min": 1, "max": 200}),
            }
        }

    def load_image(
        self,
        client: ZTarotManagerSessionClient,
        collection_name: str,
        lookup_mode: str,
        lookup_value: str,
        field_path: str,
        read_limit: int,
    ):
        try:
            client = _require_client(client)
            lookup_value = (lookup_value or "").strip()
            if not collection_name.strip():
                raise ValueError("collection_name is required.")
            if not lookup_value:
                raise ValueError("lookup_value is required.")
            if not field_path.strip():
                raise ValueError("field_path is required.")

            document: Dict[str, Any]
            source_payload: Dict[str, Any]

            if lookup_mode == "doc_key":
                source_payload = client.list_docs(
                    collection_name=collection_name,
                    limit=int(read_limit),
                    skip=0,
                )
                document = find_doc_by_key(source_payload, lookup_value) or {}
                if not document:
                    raise ValueError(
                        f"No document found in collection {collection_name!r} with doc_key={lookup_value!r}."
                    )
            else:
                source_payload = client.get_doc(
                    collection_name=collection_name,
                    document_id=lookup_value,
                )
                data = source_payload.get("data") or {}
                if isinstance(data, dict) and isinstance(data.get("document"), dict):
                    document = data["document"]
                elif isinstance(data, dict):
                    document = data
                else:
                    document = {}
                if not document:
                    raise ValueError(
                        f"No document found in collection {collection_name!r} with _id={lookup_value!r}."
                    )

            selected_value = safe_get_by_path(document, field_path.strip())
            envelope = _extract_bytes_envelope(selected_value)
            image_tensor, mask_tensor, width, height = _decode_image_tensors(envelope)

            payload = {
                "success": True,
                "message": "Image loaded from ZMongo",
                "status_code": 200,
                "error": None,
                "data": {
                    "collection": collection_name,
                    "lookup_mode": lookup_mode,
                    "lookup_value": lookup_value,
                    "field_path": field_path,
                    "document_id": str(document.get("_id", "")),
                    "filename": envelope.get("filename", "image.png"),
                    "mime_type": envelope.get("mime_type", "image/png"),
                    "width": width,
                    "height": height,
                    "size_bytes": envelope.get("size_bytes"),
                    "sha256": envelope.get("sha256"),
                },
                "source_result": source_payload,
            }

            text_output = (
                f'{payload["data"]["filename"]} | {width}x{height} | '
                f'doc={payload["data"]["document_id"]} | field={field_path}'
            )
            return image_tensor, mask_tensor, json_dumps(payload), text_output, True

        except Exception as exc:
            return _empty_outputs(str(exc))


NODE_CLASS_MAPPINGS = {
    "ZMongoLoadImageNode": ZMongoLoadImageNode,
}

NODE_DISPLAY_NAME_MAPPINGS = {
    "ZMongoLoadImageNode": "ZMongo Load Image",
}
