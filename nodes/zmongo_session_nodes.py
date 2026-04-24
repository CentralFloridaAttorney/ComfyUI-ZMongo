from __future__ import annotations

import base64
import binascii
import io
import json
from pathlib import Path
from typing import Any

import numpy as np
import torch
from PIL import Image
from dotenv import load_dotenv

from .data_processor import DataProcessor
from .session_client import ZTarotManagerSessionClient, safe_get_by_path


ENV_PATH1 = Path.home() / ".resources" / ".env"
load_dotenv(ENV_PATH1)
ENV_PATH2 = Path.home() / ".resources" / ".secrets"
load_dotenv(ENV_PATH2)

def _indexed_values(values: list[Any]) -> list[str]:
    return [f"{index}: {value}" for index, value in enumerate(values)]


def _indexed_list_text(values: list[Any]) -> str:
    return json.dumps(_indexed_values(values), indent=2, ensure_ascii=False)

def _flattened_key_paths(document: dict[str, Any]) -> list[str]:
    if not isinstance(document, dict):
        return []

    compatible = DataProcessor.to_json_compatible(document)
    flattened = DataProcessor.flatten_json(compatible)
    return sorted(str(key) for key in flattened.keys())


def _filter_key_paths(
    keys: list[str],
    contains_text: str = "",
    image_only: bool = False,
) -> list[str]:
    filtered = list(keys)

    contains_text = (contains_text or "").strip().lower()
    if contains_text:
        filtered = [key for key in filtered if contains_text in key.lower()]

    if image_only:
        image_markers = (
            "image",
            "img",
            "thumbnail",
            "preview",
            "picture",
            "photo",
            "poster",
            "frame",
            "base64",
            "content",
            "data",
            "blob",
            "bytes",
        )
        filtered = [
            key for key in filtered
            if any(marker in key.lower() for marker in image_markers)
        ]

    return filtered


def _json_text(value: Any) -> str:
    return json.dumps(value, indent=2, ensure_ascii=False, default=str)


def _string_list_text(values: list[Any]) -> str:
    return json.dumps([str(v) for v in values], indent=2, ensure_ascii=False)


def _comfyui_list(values: list[Any] | tuple[Any, ...] | None) -> list[Any]:
    if values is None:
        return []
    if isinstance(values, list):
        return values
    if isinstance(values, tuple):
        return list(values)
    return [values]


def _error_payload(message: str) -> dict[str, Any]:
    return {
        "success": False,
        "message": message,
        "data": {},
        "error": None,
    }


def _extract_collection_names(payload: dict[str, Any]) -> list[str]:
    data = payload.get("data", {}) if isinstance(payload, dict) else {}
    collections = data.get("collections", []) if isinstance(data, dict) else []
    return [str(item) for item in collections]


def _extract_doc_ids(payload: dict[str, Any]) -> list[str]:
    data = payload.get("data", {}) if isinstance(payload, dict) else {}
    documents = data.get("documents", []) if isinstance(data, dict) else []
    result: list[str] = []
    for item in documents:
        if isinstance(item, dict) and "_id" in item:
            result.append(str(item["_id"]))
    return result


def _pil_to_comfy_image(image: Image.Image) -> torch.Tensor:
    image = image.convert("RGB")
    np_image = np.asarray(image).astype(np.float32) / 255.0
    return torch.from_numpy(np_image)[None,]


def _comfy_image_to_png_bytes(image: torch.Tensor) -> bytes:
    if image is None:
        raise ValueError("No image provided.")

    if not isinstance(image, torch.Tensor):
        raise TypeError(f"Expected torch.Tensor image, got {type(image).__name__}.")

    tensor = image.detach().cpu()

    if tensor.ndim == 4:
        if tensor.shape[0] < 1:
            raise ValueError("Image batch is empty.")
        tensor = tensor[0]

    if tensor.ndim != 3:
        raise ValueError(f"Expected image tensor with 3 dims, got shape {tuple(tensor.shape)}.")

    tensor = tensor.clamp(0.0, 1.0).numpy()
    np_image = (tensor * 255.0).round().astype(np.uint8)

    pil_image = Image.fromarray(np_image, mode="RGB")
    buffer = io.BytesIO()
    pil_image.save(buffer, format="PNG")
    return buffer.getvalue()


def _split_comfy_image_batch(images: torch.Tensor) -> list[torch.Tensor]:
    if images is None:
        raise ValueError("No image tensor provided.")

    if not isinstance(images, torch.Tensor):
        raise TypeError(f"Expected torch.Tensor, got {type(images).__name__}.")

    tensor = images.detach().cpu()

    if tensor.ndim == 3:
        return [tensor]

    if tensor.ndim != 4:
        raise ValueError(f"Expected IMAGE tensor with 3 or 4 dims, got shape {tuple(tensor.shape)}.")

    if tensor.shape[0] < 1:
        raise ValueError("Image batch is empty.")

    return [tensor[i] for i in range(tensor.shape[0])]


def _decode_image_bytes_from_value(value: Any) -> bytes:
    if value is None:
        raise ValueError("Image field value is empty.")

    if isinstance(value, (bytes, bytearray)):
        return bytes(value)

    if isinstance(value, dict):
        if value.get("__type__") == "bytes":
            raw = value.get("data", "")
            if not raw:
                raise ValueError("Bytes wrapper did not contain data.")
            return base64.b64decode(raw)

        for key in ("data", "base64", "content"):
            raw = value.get(key)
            if isinstance(raw, str) and raw.strip():
                stripped = raw.strip()
                if stripped.startswith("data:") and "," in stripped:
                    stripped = stripped.split(",", 1)[1]
                try:
                    return base64.b64decode(stripped)
                except (binascii.Error, ValueError):
                    pass

    if isinstance(value, str):
        stripped = value.strip()
        if not stripped:
            raise ValueError("Image field string is empty.")

        if stripped.startswith("data:") and "," in stripped:
            stripped = stripped.split(",", 1)[1]

        try:
            return base64.b64decode(stripped)
        except (binascii.Error, ValueError) as exc:
            raise ValueError("Image field was a string but not valid base64 image data.") from exc

    raise ValueError(f"Unsupported image field type: {type(value).__name__}")


def _extract_document_from_payload(payload: dict[str, Any]) -> dict[str, Any]:
    data = payload.get("data", {})
    if isinstance(data, dict):
        document = data.get("document")
        if isinstance(document, dict):
            return document
        if "_id" in data:
            return data
    return {}


def _get_next_container_index(document: dict[str, Any], array_field_path: str) -> int:
    if not array_field_path.strip():
        return 0

    try:
        value = safe_get_by_path(document, array_field_path)
    except Exception:
        return 0

    if value is None:
        return 0

    if isinstance(value, list):
        return len(value)

    if isinstance(value, dict):
        numeric_keys: list[int] = []
        for key in value.keys():
            key_str = str(key).strip()
            if key_str.isdigit():
                numeric_keys.append(int(key_str))

        if not numeric_keys:
            return 0

        return max(numeric_keys) + 1

    raise ValueError(
        f"Field {array_field_path!r} exists but is neither a list nor a dict. "
        f"Found type: {type(value).__name__}."
    )


class ZTarotLoginNode:
    @classmethod
    def INPUT_TYPES(cls):
        return {
            "required": {
                "base_url": ("STRING", {"default": "https://ztarot.app"}),
                "username": ("STRING", {"default": ""}),
                "password": ("STRING", {"default": "", "multiline": False}),
                "verify_tls": ("BOOLEAN", {"default": True}),
                "refresh_session_each_request": ("BOOLEAN", {"default": True}),
            }
        }

    RETURN_TYPES = ("ZTAROT_SESSION", "STRING")
    RETURN_NAMES = ("session", "status")
    FUNCTION = "login"
    CATEGORY = "ZMongo/Auth"

    def login(
        self,
        base_url: str,
        username: str,
        password: str,
        verify_tls: bool,
        refresh_session_each_request: bool,
    ):
        try:
            client = ZTarotManagerSessionClient(
                base_url=base_url,
                username=username,
                password=password,
                verify_tls=verify_tls,
                refresh_session_each_request=refresh_session_each_request,
            )
            result = client.login(force=True)
            status = result.get("message") or f"Logged in as {client.username}"
            return (client, status)
        except Exception as exc:
            return (None, f"Login failed: {exc}")


class ZTarotListCollectionsNode:
    @classmethod
    def INPUT_TYPES(cls):
        return {"required": {"session": ("ZTAROT_SESSION",)}}

    RETURN_TYPES = ("STRING", "*", "STRING")
    RETURN_NAMES = ("collections_json", "collections_list", "collections_indexed")
    OUTPUT_IS_LIST = (False, True, False)
    FUNCTION = "list_collections"
    CATEGORY = "ZMongo/Data"

    def list_collections(self, session):
        if session is None:
            payload = _error_payload("No session provided.")
            empty = _comfyui_list([])
            return (_json_text(payload), empty, _indexed_list_text([]))

        try:
            payload = session.list_collections()
            collections = _extract_collection_names(payload)
            raw_list = _comfyui_list(collections)
            indexed_preview = _indexed_list_text(collections)
            return (_json_text(payload), raw_list, indexed_preview)
        except Exception as exc:
            payload = _error_payload(str(exc))
            empty = _comfyui_list([])
            return (_json_text(payload), empty, _indexed_list_text([]))


class ZTarotListDocsNode:
    @classmethod
    def INPUT_TYPES(cls):
        return {
            "required": {
                "session": ("ZTAROT_SESSION",),
                "collection_name": ("STRING", {"default": ""}),
                "limit": ("INT", {"default": 50, "min": 1, "max": 200}),
                "skip": ("INT", {"default": 0, "min": 0}),
            }
        }

    RETURN_TYPES = ("STRING", "*")
    RETURN_NAMES = ("docs_json", "doc_ids_list")
    OUTPUT_IS_LIST = (False, True)
    FUNCTION = "list_docs"
    CATEGORY = "ZMongo/Data"

    def list_docs(self, session, collection_name: str, limit: int, skip: int):
        if session is None:
            payload = _error_payload("No session provided.")
            return (_json_text(payload), _comfyui_list([]))

        try:
            payload = session.list_docs(collection_name=collection_name, limit=limit, skip=skip)
            doc_ids = _extract_doc_ids(payload)
            return (_json_text(payload), _comfyui_list(doc_ids))
        except Exception as exc:
            payload = _error_payload(str(exc))
            return (_json_text(payload), _comfyui_list([]))


class ZTarotGetDocNode:
    @classmethod
    def INPUT_TYPES(cls):
        return {
            "required": {
                "session": ("ZTAROT_SESSION",),
                "collection_name": ("STRING", {"default": ""}),
                "document_id": ("STRING", {"default": ""}),
            }
        }

    RETURN_TYPES = ("STRING",)
    RETURN_NAMES = ("document_json",)
    FUNCTION = "get_doc"
    CATEGORY = "ZMongo/Data"

    def get_doc(self, session, collection_name: str, document_id: str):
        if session is None:
            payload = _error_payload("No session provided.")
            return (_json_text(payload),)

        try:
            payload = session.get_doc(collection_name=collection_name, document_id=document_id)
            return (_json_text(payload),)
        except Exception as exc:
            payload = _error_payload(str(exc))
            return (_json_text(payload),)


class ZTarotListFlattenedFieldPathsNode:
    @classmethod
    def INPUT_TYPES(cls):
        return {
            "required": {
                "session": ("ZTAROT_SESSION",),
                "collection_name": ("STRING", {"default": ""}),
                "document_id": ("STRING", {"default": ""}),
                "contains_text": ("STRING", {"default": ""}),
                "image_only": ("BOOLEAN", {"default": False}),
            }
        }

    RETURN_TYPES = ("STRING", "*", "STRING")
    RETURN_NAMES = ("result_json", "field_paths_list", "field_paths_json")
    OUTPUT_IS_LIST = (False, True, False)
    FUNCTION = "list_flattened_field_paths"
    CATEGORY = "ZMongo/Data"

    def list_flattened_field_paths(
        self,
        session,
        collection_name: str,
        document_id: str,
        contains_text: str,
        image_only: bool,
    ):
        if session is None:
            payload = _error_payload("No session provided.")
            return (_json_text(payload), _comfyui_list([]), _string_list_text([]))

        if not collection_name.strip():
            payload = _error_payload("collection_name is required.")
            return (_json_text(payload), _comfyui_list([]), _string_list_text([]))

        if not document_id.strip():
            payload = _error_payload("document_id is required.")
            return (_json_text(payload), _comfyui_list([]), _string_list_text([]))

        try:
            doc_payload = session.get_doc(
                collection_name=collection_name,
                document_id=document_id,
            )
            document = _extract_document_from_payload(doc_payload)
            if not document:
                raise ValueError("Document not found or payload did not contain a document.")

            keys = _flattened_key_paths(document)
            filtered_keys = _filter_key_paths(
                keys,
                contains_text=contains_text,
                image_only=image_only,
            )

            payload = {
                "success": True,
                "message": f"Found {len(filtered_keys)} flattened field path(s).",
                "data": {
                    "collection_name": collection_name,
                    "document_id": document_id,
                    "contains_text": contains_text,
                    "image_only": image_only,
                    "count": len(filtered_keys),
                    "field_paths": filtered_keys,
                },
                "error": None,
            }
            return (
                _json_text(payload),
                _comfyui_list(filtered_keys),
                _string_list_text(filtered_keys),
            )

        except Exception as exc:
            payload = _error_payload(str(exc))
            return (_json_text(payload), _comfyui_list([]), _string_list_text([]))


class ZTarotDisplayImageNode:
    @classmethod
    def INPUT_TYPES(cls):
        return {
            "required": {
                "session": ("ZTAROT_SESSION",),
                "collection_name": ("STRING", {"default": ""}),
                "document_id": ("STRING", {"default": ""}),
                "field_path": ("STRING", {"default": "image_data"}),
            }
        }

    RETURN_TYPES = ("IMAGE", "STRING")
    RETURN_NAMES = ("image", "status")
    FUNCTION = "display_image"
    CATEGORY = "ZMongo/Image"

    def display_image(self, session, collection_name: str, document_id: str, field_path: str):
        if session is None:
            raise ValueError("No session provided.")

        payload = session.get_doc(collection_name=collection_name, document_id=document_id)
        document = _extract_document_from_payload(payload)

        if not document:
            raise ValueError("Document not found or payload did not contain a document.")

        image_value = None
        field_error: Exception | None = None

        try:
            image_value = safe_get_by_path(document, field_path)
        except Exception as exc:
            field_error = exc

        image_bytes: bytes | None = None

        if image_value is not None:
            try:
                image_bytes = _decode_image_bytes_from_value(image_value)
            except Exception:
                image_bytes = None

        if image_bytes is None:
            try:
                image_url = session.build_image_view_url(
                    collection_name=collection_name,
                    document_id=document_id,
                    field_path=field_path,
                )
                response = session.session.get(
                    image_url,
                    timeout=session.timeout,
                    verify=session.verify_tls,
                    allow_redirects=True,
                )
                response.raise_for_status()
                image_bytes = response.content
            except Exception as exc:
                if field_error is not None:
                    raise ValueError(
                        f"Could not decode image from field_path={field_path!r} "
                        f"and fallback image route also failed. "
                        f"Field error: {field_error}. Route error: {exc}"
                    ) from exc
                raise ValueError(f"Could not load image for field_path={field_path!r}: {exc}") from exc

        image = Image.open(io.BytesIO(image_bytes))
        comfy_image = _pil_to_comfy_image(image)

        return (
            comfy_image,
            f"Loaded image from {collection_name}/{document_id} field={field_path}",
        )


class ZTarotSaveImageNode:
    @classmethod
    def INPUT_TYPES(cls):
        return {
            "required": {
                "session": ("ZTAROT_SESSION",),
                "image": ("IMAGE",),
                "collection_name": ("STRING", {"default": ""}),
                "document_id": ("STRING", {"default": ""}),
                "field_path": ("STRING", {"default": "image_data"}),
                "filename": ("STRING", {"default": "comfy_image.png"}),
            }
        }

    RETURN_TYPES = ("STRING",)
    RETURN_NAMES = ("result_json",)
    FUNCTION = "save_image"
    CATEGORY = "ZMongo/Image"

    def save_image(
        self,
        session,
        image,
        collection_name: str,
        document_id: str,
        field_path: str,
        filename: str,
    ):
        if session is None:
            payload = _error_payload("No session provided.")
            return (_json_text(payload),)

        if not collection_name.strip():
            payload = _error_payload("collection_name is required.")
            return (_json_text(payload),)

        try:
            image_bytes = _comfy_image_to_png_bytes(image)
            payload = session.upload_image_to_field(
                collection_name=collection_name,
                image_bytes=image_bytes,
                filename=(filename or "comfy_image.png").strip() or "comfy_image.png",
                document_id=(document_id or "").strip(),
                field_path=(field_path or "image_data").strip() or "image_data",
                content_type="image/png",
            )
            return (_json_text(payload),)
        except Exception as exc:
            payload = _error_payload(str(exc))
            return (_json_text(payload),)


class ZTarotSaveImagesToArrayNode:
    @classmethod
    def INPUT_TYPES(cls):
        return {
            "required": {
                "session": ("ZTAROT_SESSION",),
                "images": ("IMAGE",),
                "collection_name": ("STRING", {"default": ""}),
                "document_id": ("STRING", {"default": ""}),
                "array_field_path": ("STRING", {"default": "images"}),
                "item_field_path": ("STRING", {"default": "image.data"}),
                "filename_prefix": ("STRING", {"default": "comfy_image"}),
                "max_images_to_save": ("INT", {"default": 8, "min": 1, "max": 512}),
            }
        }

    RETURN_TYPES = ("STRING", "STRING")
    RETURN_NAMES = ("result_json", "saved_paths_list")
    FUNCTION = "save_images_to_array"
    CATEGORY = "ZMongo/Image"

    def _normalize_item_field_path(self, array_field_path: str, item_field_path: str) -> str:
        raw = (item_field_path or "").strip().strip(".")
        array_root = (array_field_path or "").strip().strip(".")

        if not raw:
            return "image.data"

        prefix = f"{array_root}."
        if array_root and raw.startswith(prefix):
            raw = raw[len(prefix):]

        parts = [p for p in raw.split(".") if p]
        while parts and parts[0].isdigit():
            parts = parts[1:]

        return ".".join(parts) if parts else "image.data"

    def save_images_to_array(
        self,
        session,
        images,
        collection_name: str,
        document_id: str,
        array_field_path: str,
        item_field_path: str,
        filename_prefix: str,
        max_images_to_save: int,
    ):
        if session is None:
            payload = _error_payload("No session provided.")
            return (_json_text(payload), _string_list_text([]))

        if not collection_name.strip():
            payload = _error_payload("collection_name is required.")
            return (_json_text(payload), _string_list_text([]))

        if not document_id.strip():
            payload = _error_payload("document_id is required.")
            return (_json_text(payload), _string_list_text([]))

        if not array_field_path.strip():
            payload = _error_payload("array_field_path is required.")
            return (_json_text(payload), _string_list_text([]))

        try:
            full_batch = _split_comfy_image_batch(images)
            normalized_item_field_path = self._normalize_item_field_path(
                array_field_path=array_field_path,
                item_field_path=item_field_path,
            )
            batch = full_batch[:max_images_to_save] if len(full_batch) > max_images_to_save else full_batch

            doc_payload = session.get_doc(collection_name=collection_name, document_id=document_id)
            document = _extract_document_from_payload(doc_payload)
            if not document:
                raise ValueError("Document not found or payload did not contain a document.")

            start_index = _get_next_container_index(document, array_field_path)

            results: list[dict[str, Any]] = []
            saved_paths: list[str] = []
            cleaned_prefix = (filename_prefix or "comfy_image").strip() or "comfy_image"

            for offset, image_tensor in enumerate(batch):
                target_index = start_index + offset
                target_field_path = f"{array_field_path}.{target_index}.{normalized_item_field_path}"
                filename = f"{cleaned_prefix}_{target_index}.png"
                image_bytes = _comfy_image_to_png_bytes(image_tensor)

                result = session.upload_image_to_field(
                    collection_name=collection_name,
                    image_bytes=image_bytes,
                    filename=filename,
                    document_id=document_id,
                    field_path=target_field_path,
                    content_type="image/png",
                )

                results.append(result)
                saved_paths.append(target_field_path)

                if not result.get("success"):
                    error = result.get("error") or {}
                    msg = str(result.get("message") or "") + " " + str(error.get("msg") or "")
                    if "BSONObj size" in msg or "16MB" in msg or "16793600" in msg:
                        break

            success = all(bool(item.get("success")) for item in results) if results else False

            payload = {
                "success": success,
                "message": (
                    f"Saved {sum(1 for r in results if r.get('success'))} image(s) under "
                    f"{array_field_path!r} starting at index {start_index}."
                ),
                "data": {
                    "collection_name": collection_name,
                    "document_id": document_id,
                    "array_field_path": array_field_path,
                    "item_field_path": normalized_item_field_path,
                    "start_index": start_index,
                    "saved_paths": saved_paths,
                    "results": results,
                    "requested_batch_size": len(full_batch),
                    "attempted_batch_size": len(batch),
                },
                "error": None if success else {"msg": "One or more image saves failed."},
            }
            return (_json_text(payload), _string_list_text(saved_paths))

        except Exception as exc:
            payload = _error_payload(str(exc))
            return (_json_text(payload), _string_list_text([]))


class ZTarotSaveImagesAsDocumentsNode:
    @classmethod
    def INPUT_TYPES(cls):
        return {
            "required": {
                "session": ("ZTAROT_SESSION",),
                "images": ("IMAGE",),
                "collection_name": ("STRING", {"default": "images"}),
                "image_field_path": ("STRING", {"default": "image.data"}),
                "doc_key_prefix": ("STRING", {"default": "frame"}),
                "filename_prefix": ("STRING", {"default": "frame"}),
                "max_images_to_save": ("INT", {"default": 512, "min": 1, "max": 4096}),
            }
        }

    RETURN_TYPES = ("STRING", "STRING")
    RETURN_NAMES = ("result_json", "doc_ids_list")
    FUNCTION = "save_images_as_documents"
    CATEGORY = "ZMongo/Image"

    def save_images_as_documents(
        self,
        session,
        images,
        collection_name: str,
        image_field_path: str,
        doc_key_prefix: str,
        filename_prefix: str,
        max_images_to_save: int,
    ):
        if session is None:
            payload = _error_payload("No session provided.")
            return (_json_text(payload), _string_list_text([]))

        if not collection_name.strip():
            payload = _error_payload("collection_name is required.")
            return (_json_text(payload), _string_list_text([]))

        try:
            full_batch = _split_comfy_image_batch(images)
            batch = full_batch[:max_images_to_save] if len(full_batch) > max_images_to_save else full_batch

            created_ids: list[str] = []
            results: list[dict[str, Any]] = []

            cleaned_doc_key_prefix = (doc_key_prefix or "frame").strip() or "frame"
            cleaned_filename_prefix = (filename_prefix or "frame").strip() or "frame"
            cleaned_image_field_path = (image_field_path or "image.data").strip().strip(".") or "image.data"

            for index, image_tensor in enumerate(batch):
                doc_key = f"{cleaned_doc_key_prefix}_{index:06d}"

                create_result = session.create_doc(
                    collection_name=collection_name,
                    document={
                        "doc_key": doc_key,
                        "frame_index": index,
                        "source": "comfyui",
                    },
                )
                results.append(create_result)

                if not create_result.get("success"):
                    continue

                create_data = create_result.get("data", {}) if isinstance(create_result, dict) else {}
                document_id = ""

                if isinstance(create_data, dict):
                    document_id = str(
                        create_data.get("document_id")
                        or create_data.get("_id")
                        or create_data.get("inserted_id")
                        or ""
                    )

                if not document_id:
                    get_docs_result = session.list_docs(collection_name=collection_name, limit=200, skip=0)
                    docs_data = get_docs_result.get("data", {}) if isinstance(get_docs_result, dict) else {}
                    documents = docs_data.get("documents", []) if isinstance(docs_data, dict) else []
                    for doc in documents:
                        if isinstance(doc, dict) and doc.get("doc_key") == doc_key and doc.get("_id"):
                            document_id = str(doc["_id"])
                            break

                if not document_id:
                    results.append(
                        _error_payload(f"Document created for doc_key={doc_key!r} but no document_id was returned.")
                    )
                    continue

                image_bytes = _comfy_image_to_png_bytes(image_tensor)
                filename = f"{cleaned_filename_prefix}_{index:06d}.png"

                upload_result = session.upload_image_to_field(
                    collection_name=collection_name,
                    image_bytes=image_bytes,
                    filename=filename,
                    document_id=document_id,
                    field_path=cleaned_image_field_path,
                    content_type="image/png",
                )
                results.append(upload_result)

                if upload_result.get("success"):
                    created_ids.append(document_id)

            success = len(created_ids) > 0 and all(
                bool(item.get("success"))
                for item in results
                if isinstance(item, dict) and item.get("message") != ""
            )

            payload = {
                "success": success,
                "message": f"Saved {len(created_ids)} image(s) as separate documents in {collection_name!r}.",
                "data": {
                    "collection_name": collection_name,
                    "image_field_path": cleaned_image_field_path,
                    "created_document_ids": created_ids,
                    "requested_batch_size": len(full_batch),
                    "attempted_batch_size": len(batch),
                    "results": results,
                },
                "error": None if success else {"msg": "One or more image document saves failed."},
            }
            return (_json_text(payload), _string_list_text(created_ids))

        except Exception as exc:
            payload = _error_payload(str(exc))
            return (_json_text(payload), _string_list_text([]))


class ZTarotLogoutNode:
    @classmethod
    def INPUT_TYPES(cls):
        return {"required": {"session": ("ZTAROT_SESSION",)}}

    RETURN_TYPES = ("STRING",)
    RETURN_NAMES = ("result_json",)
    FUNCTION = "logout"
    CATEGORY = "ZMongo/Auth"

    def logout(self, session):
        if session is None:
            payload = _error_payload("No session provided.")
            return (_json_text(payload),)

        try:
            payload = session.logout()
            return (_json_text(payload),)
        except Exception as exc:
            payload = _error_payload(str(exc))
            return (_json_text(payload),)


NODE_CLASS_MAPPINGS = {
    "ZTarotLoginNode": ZTarotLoginNode,
    "ZTarotListCollectionsNode": ZTarotListCollectionsNode,
    "ZTarotListDocsNode": ZTarotListDocsNode,
    "ZTarotGetDocNode": ZTarotGetDocNode,
    "ZTarotListFlattenedFieldPathsNode": ZTarotListFlattenedFieldPathsNode,
    "ZTarotDisplayImageNode": ZTarotDisplayImageNode,
    "ZTarotSaveImageNode": ZTarotSaveImageNode,
    "ZTarotSaveImagesToArrayNode": ZTarotSaveImagesToArrayNode,
    "ZTarotSaveImagesAsDocumentsNode": ZTarotSaveImagesAsDocumentsNode,
    "ZTarotLogoutNode": ZTarotLogoutNode,
}

NODE_DISPLAY_NAME_MAPPINGS = {
    "ZTarotLoginNode": "ZTarot Login",
    "ZTarotListCollectionsNode": "ZTarot List Collections",
    "ZTarotListDocsNode": "ZTarot List Docs",
    "ZTarotGetDocNode": "ZTarot Get Doc",
    "ZTarotListFlattenedFieldPathsNode": "ZTarot List Flattened Field Paths",
    "ZTarotDisplayImageNode": "ZTarot Display Image",
    "ZTarotSaveImageNode": "ZTarot Save Image",
    "ZTarotSaveImagesToArrayNode": "ZTarot Save Images To Array",
    "ZTarotSaveImagesAsDocumentsNode": "ZTarot Save Images As Documents",
    "ZTarotLogoutNode": "ZTarot Logout",
}

__all__ = [
    "ZTarotLoginNode",
    "ZTarotListCollectionsNode",
    "ZTarotListDocsNode",
    "ZTarotGetDocNode",
    "ZTarotListFlattenedFieldPathsNode",
    "ZTarotDisplayImageNode",
    "ZTarotSaveImageNode",
    "ZTarotSaveImagesToArrayNode",
    "ZTarotSaveImagesAsDocumentsNode",
    "ZTarotLogoutNode",
    "NODE_CLASS_MAPPINGS",
    "NODE_DISPLAY_NAME_MAPPINGS",
]