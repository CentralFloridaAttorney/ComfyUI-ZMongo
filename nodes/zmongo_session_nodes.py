from __future__ import annotations

import base64
import binascii
import io
import json
import time
import uuid
from pathlib import Path
from typing import Any

import numpy as np
import torch
from PIL import Image
from dotenv import load_dotenv

from .data_processor import DataProcessor
from .session_client import ZTarotManagerSessionClient as ZMongoManagerSessionClient, safe_get_by_path


ENV_PATH1 = Path.home() / ".resources" / ".env"
load_dotenv(ENV_PATH1)
ENV_PATH2 = Path.home() / ".resources" / ".secrets"
load_dotenv(ENV_PATH2)


# -----------------------------------------------------------------------------
# Dirty / refresh helpers
# -----------------------------------------------------------------------------

def _extract_doc_ids_from_payload(payload: dict[str, Any]) -> list[str]:
    data = payload.get("data", {}) if isinstance(payload, dict) else {}
    documents = data.get("documents", []) if isinstance(data, dict) else []

    doc_ids: list[str] = []
    for item in documents:
        if isinstance(item, dict) and item.get("_id"):
            doc_ids.append(str(item["_id"]))

    return doc_ids

def _dirty_token(*parts: Any) -> str:
    """
    Return a unique token so ComfyUI treats database-backed nodes as changed.

    Mongo-backed nodes have external state. Without a fresh token, ComfyUI may
    reuse cached node outputs after a save, causing display/load/query nodes to
    show stale database values until a widget field is manually changed.
    """
    prefix = ":".join(str(part) for part in parts if part is not None)
    return f"{prefix}:{time.time_ns()}:{uuid.uuid4().hex}"


class AlwaysDirtyMixin:
    """Mixin for nodes that must re-run because they depend on external DB state."""

    @classmethod
    def IS_CHANGED(cls, *args, **kwargs):
        return _dirty_token(cls.__name__)


# -----------------------------------------------------------------------------
# Field-path helpers
# -----------------------------------------------------------------------------

class ZMongoLoadImagesFromDocumentsNode(AlwaysDirtyMixin):
    @classmethod
    def INPUT_TYPES(cls):
        return {
            "required": {
                "session": ("ZMONGO_SESSION",),
                "collection_name": ("STRING", {"default": "images"}),
                "image_field_path": ("STRING", {"default": "image_data"}),
                "doc_key_prefix": ("STRING", {"default": "frame"}),
                "start_index": ("INT", {"default": 0, "min": 0, "max": 1000000}),
                "max_images_to_load": ("INT", {"default": 512, "min": 1, "max": 4096}),
                "max_documents_to_scan": ("INT", {"default": 4096, "min": 1, "max": 50000}),
                "resize_to_first": ("BOOLEAN", {"default": True}),
            },
            "optional": {
                "refresh_token": ("STRING", {"default": ""}),
            },
        }

    RETURN_TYPES = ("IMAGE", "STRING", "STRING", "STRING")
    RETURN_NAMES = ("images", "json", "doc_ids", "refresh")
    FUNCTION = "load_images_from_documents"
    CATEGORY = "ZMongo/Image"

    def _load_image_from_document(
        self,
        *,
        document: dict[str, Any],
        image_field_path: str,
    ) -> tuple[Image.Image, str]:
        field_errors: list[str] = []

        for candidate_field_path in _image_field_candidates(image_field_path, "image_data"):
            try:
                image_value = safe_get_by_path(document, candidate_field_path)
                image_bytes = _decode_image_bytes_from_value(image_value)
                image = Image.open(io.BytesIO(image_bytes)).convert("RGB")
                return image, candidate_field_path
            except Exception as exc:
                field_errors.append(f"{candidate_field_path}: {exc}")

        raise ValueError(
            f"Could not decode image from document field. "
            f"Tried: {' | '.join(field_errors)}"
        )

    def _document_matches(
        self,
        *,
        document: dict[str, Any],
        doc_key_prefix: str,
        start_index: int,
    ) -> bool:
        if not isinstance(document, dict):
            return False

        if document.get("source") != "comfyui":
            return False

        if doc_key_prefix:
            doc_key = str(document.get("doc_key") or "")
            if not doc_key.startswith(doc_key_prefix):
                return False

        frame_index = document.get("frame_index")
        if isinstance(frame_index, int) and frame_index < start_index:
            return False

        return True

    def load_images_from_documents(
        self,
        session,
        collection_name: str,
        image_field_path: str,
        doc_key_prefix: str,
        start_index: int,
        max_images_to_load: int,
        max_documents_to_scan: int,
        resize_to_first: bool,
        refresh_token: str = "",
    ):
        if session is None:
            raise ValueError("No session provided.")

        cleaned_collection = (collection_name or "images").strip() or "images"
        cleaned_image_field_path = _clean_field_path(image_field_path, "image_data")
        cleaned_doc_key_prefix = (doc_key_prefix or "").strip()
        safe_start_index = max(0, int(start_index))
        safe_max_images = max(1, int(max_images_to_load))
        safe_max_scan = max(1, int(max_documents_to_scan))

        scanned_count = 0
        skip = 0
        page_limit = 250

        loaded_records: list[dict[str, Any]] = []
        errors: list[str] = []

        while scanned_count < safe_max_scan and len(loaded_records) < safe_max_images:
            current_limit = min(page_limit, safe_max_scan - scanned_count)

            docs_payload = session.list_docs(
                collection_name=cleaned_collection,
                limit=current_limit,
                skip=skip,
            )

            doc_ids = _extract_doc_ids_from_payload(docs_payload)
            if not doc_ids:
                break

            scanned_count += len(doc_ids)
            skip += len(doc_ids)

            for document_id in doc_ids:
                if len(loaded_records) >= safe_max_images:
                    break

                try:
                    doc_payload = session.get_doc(
                        collection_name=cleaned_collection,
                        document_id=document_id,
                    )
                    document = _extract_document_from_payload(doc_payload)

                    if not document:
                        errors.append(f"{document_id}: document payload was empty")
                        continue

                    if not self._document_matches(
                        document=document,
                        doc_key_prefix=cleaned_doc_key_prefix,
                        start_index=safe_start_index,
                    ):
                        continue

                    image, resolved_field_path = self._load_image_from_document(
                        document=document,
                        image_field_path=cleaned_image_field_path,
                    )

                    frame_index = document.get("frame_index")
                    if not isinstance(frame_index, int):
                        frame_index = 1000000000 + len(loaded_records)

                    loaded_records.append(
                        {
                            "document_id": document_id,
                            "doc_key": str(document.get("doc_key") or ""),
                            "frame_index": frame_index,
                            "resolved_field_path": resolved_field_path,
                            "image": image,
                        }
                    )

                except Exception as exc:
                    errors.append(f"{document_id}: {exc}")

            if len(doc_ids) < current_limit:
                break

        loaded_records.sort(
            key=lambda item: (
                int(item.get("frame_index", 1000000000)),
                str(item.get("doc_key") or ""),
                str(item.get("document_id") or ""),
            )
        )

        if not loaded_records:
            raise ValueError(
                "No saved image documents were loaded. "
                f"collection={cleaned_collection!r}, "
                f"doc_key_prefix={cleaned_doc_key_prefix!r}, "
                f"image_field_path={cleaned_image_field_path!r}, "
                f"scanned_count={scanned_count}, "
                f"errors={' | '.join(errors[:20])}"
            )

        first_image = loaded_records[0]["image"]
        first_size = first_image.size

        batch_tensors: list[torch.Tensor] = []
        loaded_doc_ids: list[str] = []
        loaded_paths: list[str] = []

        for record in loaded_records:
            image = record["image"]

            if resize_to_first and image.size != first_size:
                image = image.resize(first_size, Image.Resampling.LANCZOS)

            batch_tensors.append(_pil_to_comfy_image(image))
            loaded_doc_ids.append(str(record["document_id"]))
            loaded_paths.append(
                f"{record['document_id']}:{record['resolved_field_path']}"
            )

        images_batch = torch.cat(batch_tensors, dim=0)
        output_refresh_token = _dirty_token(
            "load_images_from_documents",
            cleaned_collection,
            cleaned_doc_key_prefix,
            cleaned_image_field_path,
            len(loaded_records),
        )

        payload = {
            "success": True,
            "message": (
                f"Loaded {len(loaded_records)} saved image document(s) "
                f"from collection {cleaned_collection!r}."
            ),
            "data": {
                "collection_name": cleaned_collection,
                "image_field_path": cleaned_image_field_path,
                "doc_key_prefix": cleaned_doc_key_prefix,
                "start_index": safe_start_index,
                "max_images_to_load": safe_max_images,
                "max_documents_to_scan": safe_max_scan,
                "scanned_count": scanned_count,
                "loaded_count": len(loaded_records),
                "loaded_doc_ids": loaded_doc_ids,
                "loaded_paths": loaded_paths,
                "errors": errors[:100],
                "refresh_token": output_refresh_token,
                "input_refresh_token": refresh_token,
            },
            "error": None,
        }

        return (
            images_batch,
            _json_text(payload),
            _string_list_text(loaded_doc_ids),
            output_refresh_token,
        )

def _clean_field_path(value: str, default: str = "image_data") -> str:
    """
    Normalize a user-entered dot path without changing its meaning.

    Important rule:
    - Do NOT append '.data' here.
    - The same field_path entered in a save node should be usable in a
      display/load node.
    """
    cleaned = (value or "").strip().strip(".")
    return cleaned or default


def _legacy_data_path(field_path: str) -> str:
    """
    Backward-compatibility path for older documents/workflows that used
    image_data.data or image.data.

    This is only used as a read fallback. Save nodes should not call this.
    """
    cleaned = _clean_field_path(field_path)
    return cleaned if cleaned.endswith(".data") else f"{cleaned}.data"


def _image_field_candidates(field_path: str, default: str = "image_data") -> list[str]:
    """
    Return read candidates in priority order:
    1. exact field path entered by the user
    2. legacy '<field>.data' path, only if different
    """
    exact = _clean_field_path(field_path, default)
    legacy = _legacy_data_path(exact)
    return list(dict.fromkeys([exact, legacy]))


# -----------------------------------------------------------------------------
# Generic helpers
# -----------------------------------------------------------------------------

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


# -----------------------------------------------------------------------------
# Image conversion helpers
# -----------------------------------------------------------------------------

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


# -----------------------------------------------------------------------------
# Document helpers
# -----------------------------------------------------------------------------

def _extract_document_from_payload(payload: dict[str, Any]) -> dict[str, Any]:
    data = payload.get("data", {}) if isinstance(payload, dict) else {}
    if isinstance(data, dict):
        document = data.get("document")
        if isinstance(document, dict):
            return document
        if "_id" in data:
            return data
    return {}


def _get_next_container_index(document: dict[str, Any], array_field_path: str) -> int:
    array_field_path = _clean_field_path(array_field_path, "images")

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


# -----------------------------------------------------------------------------
# Nodes
# -----------------------------------------------------------------------------

class ZMongoLoginNode(AlwaysDirtyMixin):
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

    RETURN_TYPES = ("ZMONGO_SESSION", "STRING")
    RETURN_NAMES = ("session", "status")
    FUNCTION = "login"
    CATEGORY = "ZMongo/00 Auth"

    def login(
        self,
        base_url: str,
        username: str,
        password: str,
        verify_tls: bool,
        refresh_session_each_request: bool,
    ):
        try:
            client = ZMongoManagerSessionClient(
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


class ZMongoListCollectionsNode(AlwaysDirtyMixin):
    @classmethod
    def INPUT_TYPES(cls):
        return {
            "required": {"session": ("ZMONGO_SESSION",)},
            "optional": {"refresh_token": ("STRING", {"default": ""})},
        }

    RETURN_TYPES = ("STRING", "*", "STRING")
    RETURN_NAMES = ("json", "list", "indexed")
    OUTPUT_IS_LIST = (False, True, False)
    FUNCTION = "list_collections"
    CATEGORY = "ZMongo/Data"

    def list_collections(self, session, refresh_token: str = ""):
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


class ZMongoListDocsNode(AlwaysDirtyMixin):
    @classmethod
    def INPUT_TYPES(cls):
        return {
            "required": {
                "session": ("ZMONGO_SESSION",),
                "collection_name": ("STRING", {"default": ""}),
                "limit": ("INT", {"default": 50, "min": 1, "max": 200}),
                "skip": ("INT", {"default": 0, "min": 0}),
            },
            "optional": {"refresh_token": ("STRING", {"default": ""})},
        }

    RETURN_TYPES = ("STRING", "*")
    RETURN_NAMES = ("json", "ids")
    OUTPUT_IS_LIST = (False, True)
    FUNCTION = "list_docs"
    CATEGORY = "ZMongo/Data"

    def list_docs(self, session, collection_name: str, limit: int, skip: int, refresh_token: str = ""):
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


class ZMongoGetDocNode(AlwaysDirtyMixin):
    @classmethod
    def INPUT_TYPES(cls):
        return {
            "required": {
                "session": ("ZMONGO_SESSION",),
                "collection_name": ("STRING", {"default": ""}),
                "document_id": ("STRING", {"default": ""}),
            },
            "optional": {"refresh_token": ("STRING", {"default": ""})},
        }

    RETURN_TYPES = ("STRING",)
    RETURN_NAMES = ("json",)
    FUNCTION = "get_doc"
    CATEGORY = "ZMongo/Data"

    def get_doc(self, session, collection_name: str, document_id: str, refresh_token: str = ""):
        if session is None:
            payload = _error_payload("No session provided.")
            return (_json_text(payload),)

        try:
            payload = session.get_doc(collection_name=collection_name, document_id=document_id)
            return (_json_text(payload),)
        except Exception as exc:
            payload = _error_payload(str(exc))
            return (_json_text(payload),)


class ZMongoListFlattenedFieldPathsNode(AlwaysDirtyMixin):
    @classmethod
    def INPUT_TYPES(cls):
        return {
            "required": {
                "session": ("ZMONGO_SESSION",),
                "collection_name": ("STRING", {"default": ""}),
                "document_id": ("STRING", {"default": ""}),
                "contains_text": ("STRING", {"default": ""}),
                "image_only": ("BOOLEAN", {"default": False}),
            },
            "optional": {"refresh_token": ("STRING", {"default": ""})},
        }

    RETURN_TYPES = ("STRING", "*", "STRING")
    RETURN_NAMES = ("json", "paths", "paths_json")
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
        refresh_token: str = "",
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


class ZMongoDisplayImageNode(AlwaysDirtyMixin):
    @classmethod
    def INPUT_TYPES(cls):
        return {
            "required": {
                "session": ("ZMONGO_SESSION",),
                "collection_name": ("STRING", {"default": ""}),
                "document_id": ("STRING", {"default": ""}),
                "field_path": ("STRING", {"default": "image_data"}),
            },
            "optional": {"refresh_token": ("STRING", {"default": ""})},
        }

    RETURN_TYPES = ("IMAGE", "STRING")
    RETURN_NAMES = ("image", "status")
    FUNCTION = "display_image"
    CATEGORY = "ZMongo/Image"

    def display_image(
        self,
        session,
        collection_name: str,
        document_id: str,
        field_path: str,
        refresh_token: str = "",
    ):
        if session is None:
            raise ValueError("No session provided.")

        requested_field_path = _clean_field_path(field_path, "image_data")

        payload = session.get_doc(collection_name=collection_name, document_id=document_id)
        document = _extract_document_from_payload(payload)

        if not document:
            raise ValueError("Document not found or payload did not contain a document.")

        image_bytes: bytes | None = None
        resolved_field_path = requested_field_path
        field_errors: list[str] = []

        # Exact path first. Legacy '<path>.data' only as a fallback.
        for candidate_field_path in _image_field_candidates(requested_field_path, "image_data"):
            try:
                image_value = safe_get_by_path(document, candidate_field_path)
                image_bytes = _decode_image_bytes_from_value(image_value)
                resolved_field_path = candidate_field_path
                break
            except Exception as exc:
                field_errors.append(f"{candidate_field_path}: {exc}")

        # Route fallback, same exact-first policy.
        if image_bytes is None:
            route_errors: list[str] = []

            for candidate_field_path in _image_field_candidates(requested_field_path, "image_data"):
                try:
                    image_url = session.build_image_view_url(
                        collection_name=collection_name,
                        document_id=document_id,
                        field_path=candidate_field_path,
                    )
                    response = session.session.get(
                        image_url,
                        timeout=session.timeout,
                        verify=session.verify_tls,
                        allow_redirects=True,
                    )
                    response.raise_for_status()
                    image_bytes = response.content
                    resolved_field_path = candidate_field_path
                    break
                except Exception as exc:
                    route_errors.append(f"{candidate_field_path}: {exc}")

            if image_bytes is None:
                raise ValueError(
                    f"Could not load image for field_path={requested_field_path!r}. "
                    f"Field errors: {' | '.join(field_errors)}. "
                    f"Route errors: {' | '.join(route_errors)}"
                )

        image = Image.open(io.BytesIO(image_bytes))
        comfy_image = _pil_to_comfy_image(image)

        return (
            comfy_image,
            f"Loaded image from {collection_name}/{document_id} field={resolved_field_path} refresh={refresh_token}",
        )


class ZMongoSaveImageNode(AlwaysDirtyMixin):
    @classmethod
    def INPUT_TYPES(cls):
        return {
            "required": {
                "session": ("ZMONGO_SESSION",),
                "image": ("IMAGE",),
                "collection_name": ("STRING", {"default": ""}),
                "document_id": ("STRING", {"default": ""}),
                "field_path": ("STRING", {"default": "image_data"}),
                "filename": ("STRING", {"default": "comfy_image.png"}),
            }
        }

    RETURN_TYPES = ("STRING", "STRING")
    RETURN_NAMES = ("json", "refresh")
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
            return (_json_text(payload), _dirty_token("save_image_error"))

        if not collection_name.strip():
            payload = _error_payload("collection_name is required.")
            return (_json_text(payload), _dirty_token("save_image_error"))

        try:
            cleaned_field_path = _clean_field_path(field_path, "image_data")
            image_bytes = _comfy_image_to_png_bytes(image)
            payload = session.upload_image_to_field(
                collection_name=collection_name,
                image_bytes=image_bytes,
                filename=(filename or "comfy_image.png").strip() or "comfy_image.png",
                document_id=(document_id or "").strip(),
                field_path=cleaned_field_path,
                content_type="image/png",
            )
            token = _dirty_token("save_image", collection_name, document_id, cleaned_field_path)
            if isinstance(payload, dict):
                payload.setdefault("data", {})
                if isinstance(payload["data"], dict):
                    payload["data"]["refresh_token"] = token
                    payload["data"]["field_path"] = cleaned_field_path
            return (_json_text(payload), token)
        except Exception as exc:
            payload = _error_payload(str(exc))
            return (_json_text(payload), _dirty_token("save_image_error"))


class ZMongoSaveImagesToArrayNode(AlwaysDirtyMixin):
    @classmethod
    def INPUT_TYPES(cls):
        return {
            "required": {
                "session": ("ZMONGO_SESSION",),
                "images": ("IMAGE",),
                "collection_name": ("STRING", {"default": ""}),
                "document_id": ("STRING", {"default": ""}),
                "array_field_path": ("STRING", {"default": "images"}),
                "item_field_path": ("STRING", {"default": "image"}),
                "filename_prefix": ("STRING", {"default": "comfy_image"}),
                "max_images_to_save": ("INT", {"default": 8, "min": 1, "max": 512}),
            }
        }

    RETURN_TYPES = ("STRING", "*", "STRING", "STRING")
    RETURN_NAMES = ("json", "paths", "refresh", "first")
    OUTPUT_IS_LIST = (False, True, False, False)
    FUNCTION = "save_images_to_array"
    CATEGORY = "ZMongo/Image"

    def _normalize_item_field_path(self, array_field_path: str, item_field_path: str) -> str:
        """
        Normalize the per-item image field without adding '.data'.

        Examples:
        - array_field_path='images', item_field_path='image' -> 'image'
        - array_field_path='images', item_field_path='images.0.image' -> 'image'
        - array_field_path='frames', item_field_path='0.image' -> 'image'
        """
        raw = _clean_field_path(item_field_path, "image")
        array_root = _clean_field_path(array_field_path, "images")

        prefix = f"{array_root}."
        if array_root and raw.startswith(prefix):
            raw = raw[len(prefix):]

        parts = [p for p in raw.split(".") if p]
        while parts and parts[0].isdigit():
            parts = parts[1:]

        return ".".join(parts) if parts else "image"

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
        error_token = _dirty_token("save_images_to_array_error")

        if session is None:
            payload = _error_payload("No session provided.")
            return (_json_text(payload), [""], error_token, "")

        cleaned_collection_name = (collection_name or "").strip()
        cleaned_document_id = (document_id or "").strip()
        cleaned_array_field_path = _clean_field_path(array_field_path, "images")
        normalized_item_field_path = self._normalize_item_field_path(
            array_field_path=cleaned_array_field_path,
            item_field_path=item_field_path,
        )

        if not cleaned_collection_name:
            payload = _error_payload("collection_name is required.")
            return (_json_text(payload), [""], error_token, "")

        if not cleaned_document_id:
            payload = _error_payload("document_id is required.")
            return (_json_text(payload), [""], error_token, "")

        if not cleaned_array_field_path:
            payload = _error_payload("array_field_path is required.")
            return (_json_text(payload), [""], error_token, "")

        try:
            full_batch = _split_comfy_image_batch(images)
            safe_max = max(1, min(int(max_images_to_save), 512))
            batch = full_batch[:safe_max]

            doc_payload = session.get_doc(
                collection_name=cleaned_collection_name,
                document_id=cleaned_document_id,
            )
            document = _extract_document_from_payload(doc_payload)
            if not document:
                raise ValueError("Document not found or payload did not contain a document.")

            start_index = _get_next_container_index(document, cleaned_array_field_path)
            cleaned_prefix = (filename_prefix or "comfy_image").strip() or "comfy_image"

            results: list[dict[str, Any]] = []
            saved_paths: list[str] = []
            failed_paths: list[str] = []

            for offset, image_tensor in enumerate(batch):
                target_index = start_index + offset
                target_field_path = f"{cleaned_array_field_path}.{target_index}.{normalized_item_field_path}"
                filename = f"{cleaned_prefix}_{target_index:06d}.png"

                try:
                    image_bytes = _comfy_image_to_png_bytes(image_tensor)
                    result = session.upload_image_to_field(
                        collection_name=cleaned_collection_name,
                        image_bytes=image_bytes,
                        filename=filename,
                        document_id=cleaned_document_id,
                        field_path=target_field_path,
                        content_type="image/png",
                    )
                except Exception as exc:
                    result = _error_payload(f"Failed to save {target_field_path}: {exc}")

                results.append(result)

                if result.get("success"):
                    saved_paths.append(target_field_path)
                else:
                    failed_paths.append(target_field_path)
                    error = result.get("error") or {}
                    msg = str(result.get("message") or "") + " " + str(error.get("msg") or "")
                    if "BSONObj size" in msg or "16MB" in msg or "16793600" in msg:
                        break

            token = _dirty_token(
                "save_images_to_array",
                cleaned_collection_name,
                cleaned_document_id,
                cleaned_array_field_path,
                len(saved_paths),
            )
            success_count = len(saved_paths)
            attempted_count = len(results)
            success = attempted_count > 0 and success_count == attempted_count

            payload = {
                "success": success,
                "message": (
                    f"Saved {success_count}/{attempted_count} image(s) under "
                    f"{cleaned_array_field_path!r} starting at index {start_index}."
                ),
                "data": {
                    "collection_name": cleaned_collection_name,
                    "document_id": cleaned_document_id,
                    "array_field_path": cleaned_array_field_path,
                    "item_field_path": normalized_item_field_path,
                    "start_index": start_index,
                    "end_index": start_index + max(success_count - 1, 0),
                    "saved_paths": saved_paths,
                    "failed_paths": failed_paths,
                    "results": results,
                    "requested_batch_size": len(full_batch),
                    "attempted_batch_size": len(batch),
                    "saved_count": success_count,
                    "failed_count": len(failed_paths),
                    "refresh_token": token,
                },
                "error": None if success else {"msg": "One or more image saves failed."},
            }
            return (_json_text(payload), (saved_paths if saved_paths else [""]), token, (saved_paths[0] if saved_paths else ""))

        except Exception as exc:
            payload = _error_payload(str(exc))
            return (_json_text(payload), [""], error_token, "")


class ZMongoSaveImagesAsDocumentsNode(AlwaysDirtyMixin):
    @classmethod
    def INPUT_TYPES(cls):
        return {
            "required": {
                "session": ("ZMONGO_SESSION",),
                "images": ("IMAGE",),
                "collection_name": ("STRING", {"default": "images"}),
                "image_field_path": ("STRING", {"default": "image_data"}),
                "doc_key_prefix": ("STRING", {"default": "frame"}),
                "filename_prefix": ("STRING", {"default": "frame"}),
                "max_images_to_save": ("INT", {"default": 512, "min": 1, "max": 4096}),
            }
        }

    RETURN_TYPES = ("STRING", "STRING", "STRING")
    RETURN_NAMES = ("json", "doc_ids", "refresh")
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
        error_token = _dirty_token("save_images_as_documents_error")

        if session is None:
            payload = _error_payload("No session provided.")
            return (_json_text(payload), _string_list_text([]), error_token)

        if not collection_name.strip():
            payload = _error_payload("collection_name is required.")
            return (_json_text(payload), _string_list_text([]), error_token)

        try:
            full_batch = _split_comfy_image_batch(images)
            batch = full_batch[:max_images_to_save] if len(full_batch) > max_images_to_save else full_batch

            created_ids: list[str] = []
            results: list[dict[str, Any]] = []

            cleaned_doc_key_prefix = (doc_key_prefix or "frame").strip() or "frame"
            cleaned_filename_prefix = (filename_prefix or "frame").strip() or "frame"
            cleaned_image_field_path = _clean_field_path(image_field_path, "image_data")

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
            token = _dirty_token("save_images_as_documents", collection_name, cleaned_image_field_path)

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
                    "refresh_token": token,
                },
                "error": None if success else {"msg": "One or more image document saves failed."},
            }
            return (_json_text(payload), _string_list_text(created_ids), token)

        except Exception as exc:
            payload = _error_payload(str(exc))
            return (_json_text(payload), _string_list_text([]), error_token)


class ZMongoSelectSavedPathNode(AlwaysDirtyMixin):
    @classmethod
    def INPUT_TYPES(cls):
        return {
            "required": {
                "saved_paths_list": ("*",),
                "index": ("INT", {"default": 0, "min": 0, "max": 100000}),
                "fallback_path": ("STRING", {"default": ""}),
            }
        }

    RETURN_TYPES = ("STRING", "STRING")
    RETURN_NAMES = ("path", "status")
    FUNCTION = "select_saved_path"
    CATEGORY = "ZMongo/Image"

    def select_saved_path(self, saved_paths_list, index: int, fallback_path: str = ""):
        paths = _comfyui_list(saved_paths_list)
        cleaned_paths = [str(path).strip() for path in paths if str(path).strip()]
        fallback = (fallback_path or "").strip()

        if not cleaned_paths:
            return (
                fallback,
                "No saved paths were returned by the save node. Check result_json for the save failure reason.",
            )

        safe_index = max(0, min(int(index), len(cleaned_paths) - 1))
        selected = cleaned_paths[safe_index]
        return (
            selected,
            f"Selected saved path {safe_index + 1}/{len(cleaned_paths)}: {selected}",
        )


class ZMongoSelectNthItemNode(AlwaysDirtyMixin):
    @classmethod
    def INPUT_TYPES(cls):
        return {
            "required": {
                "items_list": ("*",),
                "index": ("INT", {"default": 0, "min": 0, "max": 100000}),
            }
        }

    RETURN_TYPES = ("STRING",)
    RETURN_NAMES = ("item",)
    FUNCTION = "select_nth_item"
    CATEGORY = "ZMongo/Data"

    def select_nth_item(self, items_list, index: int):
        items = _comfyui_list(items_list)
        cleaned_items = [str(item).strip() for item in items if str(item).strip()]

        if not cleaned_items:
            return ("",)

        safe_index = max(0, min(int(index), len(cleaned_items) - 1))
        return (cleaned_items[safe_index],)


class ZMongoLoadImagesFromArrayNode(AlwaysDirtyMixin):
    @classmethod
    def INPUT_TYPES(cls):
        return {
            "required": {
                "session": ("ZMONGO_SESSION",),
                "collection_name": ("STRING", {"default": ""}),
                "document_id": ("STRING", {"default": ""}),
                "array_field_path": ("STRING", {"default": "images"}),
                "item_field_path": ("STRING", {"default": "image"}),
                "start_index": ("INT", {"default": 0, "min": 0, "max": 100000}),
                "max_images_to_load": ("INT", {"default": 512, "min": 1, "max": 4096}),
                "resize_to_first": ("BOOLEAN", {"default": True}),
            },
            "optional": {
                "refresh_token": ("STRING", {"default": ""}),
            },
        }

    RETURN_TYPES = ("IMAGE", "STRING", "STRING", "STRING")
    RETURN_NAMES = ("images", "json", "paths", "refresh")
    FUNCTION = "load_images_from_array"
    CATEGORY = "ZMongo/Image"

    @staticmethod
    def _normalize_item_field_path(array_field_path: str, item_field_path: str) -> str:
        raw = _clean_field_path(item_field_path, "image")
        array_root = _clean_field_path(array_field_path, "images")
        prefix = f"{array_root}."
        if array_root and raw.startswith(prefix):
            raw = raw[len(prefix):]
        parts = [part for part in raw.split(".") if part]
        while parts and parts[0].isdigit():
            parts = parts[1:]
        return ".".join(parts) if parts else "image"

    @staticmethod
    def _empty_image_batch() -> torch.Tensor:
        return torch.zeros((1, 64, 64, 3), dtype=torch.float32)

    @staticmethod
    def _array_items(array_value: Any) -> list[tuple[int, Any]]:
        if isinstance(array_value, list):
            return list(enumerate(array_value))
        if isinstance(array_value, dict):
            indexed_items: list[tuple[int, Any]] = []
            for key, value in array_value.items():
                key_str = str(key).strip()
                if key_str.isdigit():
                    indexed_items.append((int(key_str), value))
            return sorted(indexed_items, key=lambda item: item[0])
        raise ValueError(
            f"array_field_path resolved to {type(array_value).__name__}, "
            "but expected a list or numeric-keyed dict."
        )

    @staticmethod
    def _decode_pil_from_value(value: Any) -> Image.Image:
        image_bytes = _decode_image_bytes_from_value(value)
        return Image.open(io.BytesIO(image_bytes)).convert("RGB")

    def _load_pil_from_route(self, *, session, collection_name: str, document_id: str, field_path: str) -> Image.Image:
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
        return Image.open(io.BytesIO(response.content)).convert("RGB")

    def load_images_from_array(
        self,
        session,
        collection_name: str,
        document_id: str,
        array_field_path: str,
        item_field_path: str,
        start_index: int,
        max_images_to_load: int,
        resize_to_first: bool,
        refresh_token: str = "",
    ):
        token = _dirty_token("load_images_from_array", collection_name, document_id, refresh_token)

        if session is None:
            payload = _error_payload("No session provided.")
            return (self._empty_image_batch(), _json_text(payload), _string_list_text([]), token)

        cleaned_collection_name = (collection_name or "").strip()
        cleaned_document_id = (document_id or "").strip()
        cleaned_array_field_path = _clean_field_path(array_field_path, "images")
        normalized_item_field_path = self._normalize_item_field_path(cleaned_array_field_path, item_field_path)

        if not cleaned_collection_name:
            payload = _error_payload("collection_name is required.")
            return (self._empty_image_batch(), _json_text(payload), _string_list_text([]), token)
        if not cleaned_document_id:
            payload = _error_payload("document_id is required.")
            return (self._empty_image_batch(), _json_text(payload), _string_list_text([]), token)

        try:
            safe_start = max(0, int(start_index))
            safe_max = max(1, min(int(max_images_to_load), 4096))
            doc_payload = session.get_doc(collection_name=cleaned_collection_name, document_id=cleaned_document_id)
            document = _extract_document_from_payload(doc_payload)
            if not document:
                raise ValueError("Document not found or payload did not contain a document.")

            array_value = safe_get_by_path(document, cleaned_array_field_path)
            indexed_items = self._array_items(array_value)
            selected_items = [(idx, val) for idx, val in indexed_items if idx >= safe_start][:safe_max]

            loaded_pil_images: list[Image.Image] = []
            loaded_paths: list[str] = []
            failed_paths: list[str] = []
            errors: list[str] = []

            for index, item in selected_items:
                exact_path = f"{cleaned_array_field_path}.{index}.{normalized_item_field_path}"
                candidate_paths = _image_field_candidates(exact_path, exact_path)
                pil_image: Image.Image | None = None

                for candidate_path in candidate_paths:
                    try:
                        relative_path = candidate_path
                        prefix = f"{cleaned_array_field_path}.{index}."
                        if relative_path.startswith(prefix):
                            relative_path = relative_path[len(prefix):]
                        item_value = safe_get_by_path(item, relative_path)
                        pil_image = self._decode_pil_from_value(item_value)
                        exact_path = candidate_path
                        break
                    except Exception:
                        pass

                if pil_image is None:
                    for candidate_path in candidate_paths:
                        try:
                            pil_image = self._load_pil_from_route(
                                session=session,
                                collection_name=cleaned_collection_name,
                                document_id=cleaned_document_id,
                                field_path=candidate_path,
                            )
                            exact_path = candidate_path
                            break
                        except Exception as exc:
                            errors.append(f"{candidate_path}: {exc}")

                if pil_image is None:
                    failed_paths.append(exact_path)
                    continue
                loaded_pil_images.append(pil_image)
                loaded_paths.append(exact_path)

            if not loaded_pil_images:
                payload = {
                    "success": False,
                    "message": "No images could be loaded from the array.",
                    "data": {
                        "collection_name": cleaned_collection_name,
                        "document_id": cleaned_document_id,
                        "array_field_path": cleaned_array_field_path,
                        "item_field_path": normalized_item_field_path,
                        "start_index": safe_start,
                        "max_images_to_load": safe_max,
                        "loaded_paths": loaded_paths,
                        "failed_paths": failed_paths,
                        "errors": errors[-25:],
                        "refresh_token": token,
                    },
                    "error": {"msg": "No images loaded."},
                }
                return (self._empty_image_batch(), _json_text(payload), _string_list_text(loaded_paths), token)

            target_size = loaded_pil_images[0].size
            tensors: list[torch.Tensor] = []
            resized_count = 0
            for pil_image in loaded_pil_images:
                if pil_image.size != target_size:
                    if not resize_to_first:
                        raise ValueError(
                            "Loaded images have different sizes. Enable resize_to_first "
                            "or save frames at a consistent resolution."
                        )
                    try:
                        resample_filter = Image.Resampling.LANCZOS
                    except AttributeError:
                        resample_filter = Image.LANCZOS
                    pil_image = pil_image.resize(target_size, resample_filter)
                    resized_count += 1
                tensors.append(_pil_to_comfy_image(pil_image))

            image_batch = torch.cat(tensors, dim=0)
            payload = {
                "success": True,
                "message": f"Loaded {len(tensors)} image(s) from {cleaned_array_field_path!r}.",
                "data": {
                    "collection_name": cleaned_collection_name,
                    "document_id": cleaned_document_id,
                    "array_field_path": cleaned_array_field_path,
                    "item_field_path": normalized_item_field_path,
                    "start_index": safe_start,
                    "max_images_to_load": safe_max,
                    "loaded_count": len(tensors),
                    "failed_count": len(failed_paths),
                    "resized_count": resized_count,
                    "output_shape": list(image_batch.shape),
                    "loaded_paths": loaded_paths,
                    "failed_paths": failed_paths,
                    "errors": errors[-25:],
                    "refresh_token": token,
                },
                "error": None,
            }
            return (image_batch, _json_text(payload), _string_list_text(loaded_paths), token)

        except Exception as exc:
            payload = _error_payload(str(exc))
            return (self._empty_image_batch(), _json_text(payload), _string_list_text([]), token)


class ZMongoLogoutNode(AlwaysDirtyMixin):
    @classmethod
    def INPUT_TYPES(cls):
        return {"required": {"session": ("ZMONGO_SESSION",)}}

    RETURN_TYPES = ("STRING",)
    RETURN_NAMES = ("json",)
    FUNCTION = "logout"
    CATEGORY = "ZMongo/00 Auth"

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
    "ZMongoLoginNode": ZMongoLoginNode,
    "ZMongoLogoutNode": ZMongoLogoutNode,
    "ZMongoListCollectionsNode": ZMongoListCollectionsNode,
    "ZMongoListDocsNode": ZMongoListDocsNode,
    "ZMongoSelectNthItemNode": ZMongoSelectNthItemNode,
    "ZMongoGetDocNode": ZMongoGetDocNode,
    "ZMongoListFlattenedFieldPathsNode": ZMongoListFlattenedFieldPathsNode,
    "ZMongoDisplayImageNode": ZMongoDisplayImageNode,
    "ZMongoSaveImageNode": ZMongoSaveImageNode,
    "ZMongoSaveImagesToArrayNode": ZMongoSaveImagesToArrayNode,
    "ZMongoSaveImagesAsDocumentsNode": ZMongoSaveImagesAsDocumentsNode,
    "ZMongoSelectSavedPathNode": ZMongoSelectSavedPathNode,
    "ZMongoLoadImagesFromArrayNode": ZMongoLoadImagesFromArrayNode,
    "ZMongoLoadImagesFromDocumentsNode": ZMongoLoadImagesFromDocumentsNode,
}

NODE_DISPLAY_NAME_MAPPINGS = {
    "ZMongoLoginNode": "Login",
    "ZMongoLogoutNode": "Logout",
    "ZMongoListCollectionsNode": "List Collections",
    "ZMongoListDocsNode": "List Docs",
    "ZMongoSelectNthItemNode": "Select Nth",
    "ZMongoGetDocNode": "Get Doc",
    "ZMongoListFlattenedFieldPathsNode": "List Fields",
    "ZMongoDisplayImageNode": "Display Image",
    "ZMongoSaveImageNode": "Save Image",
    "ZMongoSaveImagesToArrayNode": "Save Images: Array",
    "ZMongoSaveImagesAsDocumentsNode": "Save Images: Docs",
    "ZMongoSelectSavedPathNode": "Select Path",
    "ZMongoLoadImagesFromArrayNode": "Load Images: Array",
    "ZMongoLoadImagesFromDocumentsNode": "Load Images: Docs",
}

__all__ = [
    "ZMongoLoginNode",
    "ZMongoLogoutNode",
    "ZMongoListCollectionsNode",
    "ZMongoListDocsNode",
    "ZMongoSelectNthItemNode",
    "ZMongoGetDocNode",
    "ZMongoListFlattenedFieldPathsNode",
    "ZMongoDisplayImageNode",
    "ZMongoSaveImageNode",
    "ZMongoSaveImagesToArrayNode",
    "ZMongoSaveImagesAsDocumentsNode",
    "ZMongoSelectSavedPathNode",
    "ZMongoLoadImagesFromArrayNode",
    "ZMongoLoadImagesFromDocumentsNode",
    "NODE_CLASS_MAPPINGS",
    "NODE_DISPLAY_NAME_MAPPINGS",
]