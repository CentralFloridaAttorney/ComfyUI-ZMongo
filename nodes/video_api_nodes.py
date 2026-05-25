from __future__ import annotations

import base64
import io
import json
import time
from pathlib import Path
from typing import Any, Optional

import numpy as np
import torch
from PIL import Image

from .generic_helpers import (
    AlwaysDirtyMixin,
    _dirty_token,
    _json_text,
    _error_payload,
    _success_payload,
    _parse_json_object,
    _as_comfy_list,
    _indexed_list_text,
    safe_get_by_path,
)


def _clean_scalar(value: Any) -> str:
    if isinstance(value, (list, tuple)):
        value = value[0] if value else ""
    if value is None:
        return ""

    text = str(value).strip()

    for _ in range(6):
        before = text

        if text.startswith("[") and text.endswith("]"):
            text = text[1:-1].strip()

        if text.startswith("(") and text.endswith(")"):
            text = text[1:-1].strip()

        if text.endswith(","):
            text = text[:-1].strip()

        if len(text) >= 2 and text[0] == text[-1] and text[0] in {"'", '"'}:
            text = text[1:-1].strip()

        if text == before:
            break

    return text.strip()


def _parse_metadata(metadata_json: Any) -> dict[str, Any]:
    text = _clean_scalar(metadata_json)
    if not text:
        return {}

    parsed = json.loads(text)
    if not isinstance(parsed, dict):
        raise ValueError("metadata_json must be a JSON object.")

    return parsed


def _extract_document_id(payload: dict[str, Any]) -> str:
    data = payload.get("data", {}) if isinstance(payload, dict) else {}

    if isinstance(data, dict):
        for key in ("document_id", "inserted_id", "_id", "id"):
            value = data.get(key)
            if value:
                return _clean_scalar(value)

        for nested_key in ("document", "result", "data"):
            nested = data.get(nested_key)
            if isinstance(nested, dict):
                for key in ("document_id", "inserted_id", "_id", "id"):
                    value = nested.get(key)
                    if value:
                        return _clean_scalar(value)

    return ""


def _image_tensor_to_png_bytes(frame: torch.Tensor) -> bytes:
    tensor = frame.detach().cpu().clamp(0.0, 1.0)

    if tensor.ndim != 3:
        raise ValueError(f"Expected frame tensor with shape HWC, got {tuple(tensor.shape)}.")

    array = (tensor.numpy() * 255.0).round().astype(np.uint8)

    if array.shape[-1] == 4:
        image = Image.fromarray(array, mode="RGBA").convert("RGB")
    elif array.shape[-1] == 3:
        image = Image.fromarray(array, mode="RGB")
    else:
        raise ValueError(f"Expected RGB/RGBA frame, got last dimension {array.shape[-1]}.")

    buffer = io.BytesIO()
    image.save(buffer, format="PNG")
    return buffer.getvalue()


def _png_bytes_to_image_tensor(image_bytes: bytes) -> torch.Tensor:
    image = Image.open(io.BytesIO(image_bytes)).convert("RGB")
    array = np.asarray(image).astype(np.float32) / 255.0
    return torch.from_numpy(array)[None,]


def _bytes_envelope(
    *,
    data: bytes,
    filename: str,
    content_type: str,
    metadata: Optional[dict[str, Any]] = None,
) -> dict[str, Any]:
    return {
        "__type__": "bytes",
        "encoding": "base64",
        "data": base64.b64encode(data).decode("ascii"),
        "size_bytes": len(data),
        "filename": filename,
        "content_type": content_type,
        "source": "comfyui",
        "storage_mode": "inline_zmongo_binary_envelope",
        "metadata": metadata or {},
        "saved_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
    }


def _decode_bytes_envelope(value: Any) -> bytes:
    if value is None:
        raise ValueError("Value is empty.")

    if isinstance(value, bytes):
        return value

    if isinstance(value, bytearray):
        return bytes(value)

    if isinstance(value, str):
        text = value.strip()
        if text.startswith("data:") and "," in text:
            text = text.split(",", 1)[1]
        return base64.b64decode(text, validate=False)

    if isinstance(value, dict):
        if value.get("__type__") == "bytes" and value.get("encoding") == "base64" and value.get("data"):
            return base64.b64decode(str(value["data"]), validate=False)

        for key in ("data", "bytes", "payload", "content"):
            if key in value:
                return _decode_bytes_envelope(value[key])

    raise ValueError(f"Unsupported bytes value type: {type(value).__name__}")


class ZMongoApiSaveVideoFramesNode(AlwaysDirtyMixin):
    """
    Saves a ComfyUI IMAGE batch as video frames.

    This is intentionally codec-free. It stores each frame as a PNG bytes
    envelope inside one ZMongo document.
    """

    @classmethod
    def INPUT_TYPES(cls):
        return {
            "required": {
                "session": ("ZMONGO_API_SESSION",),
                "images": ("IMAGE",),
                "collection_name": ("STRING", {"default": "videos"}),
                "field_path": ("STRING", {"default": "video_frames"}),
                "filename_prefix": ("STRING", {"default": "comfy_video_frame"}),
                "fps": ("INT", {"default": 24, "min": 1, "max": 240}),
                "max_frames": ("INT", {"default": 256, "min": 1, "max": 10000}),
                "metadata_json": ("STRING", {"default": "{}", "multiline": True}),
            },
            "optional": {
                "document_id": ("STRING", {"default": ""}),
                "doc_key": ("STRING", {"default": ""}),
            },
        }

    RETURN_TYPES = ("STRING", "STRING", "INT", "STRING", "BOOLEAN")
    RETURN_NAMES = ("json", "document_id", "frame_count", "refresh", "success")
    FUNCTION = "save_video_frames"
    CATEGORY = "ZMongo/06 Video"

    def save_video_frames(
        self,
        session,
        images,
        collection_name: str,
        field_path: str,
        filename_prefix: str,
        fps: int,
        max_frames: int,
        metadata_json: str,
        document_id: str = "",
        doc_key: str = "",
    ):
        cleaned_collection = _clean_scalar(collection_name) or "videos"
        cleaned_field_path = _clean_scalar(field_path) or "video_frames"
        cleaned_document_id = _clean_scalar(document_id)
        cleaned_doc_key = _clean_scalar(doc_key)
        cleaned_prefix = _clean_scalar(filename_prefix) or "comfy_video_frame"
        refresh = _dirty_token("save_video_frames", cleaned_collection, cleaned_document_id, cleaned_field_path)

        if session is None:
            payload = _error_payload("No API session provided.")
            return (_json_text(payload), cleaned_document_id, 0, refresh, False)

        try:
            if images is None or not isinstance(images, torch.Tensor):
                raise TypeError(f"Expected ComfyUI IMAGE tensor, got {type(images).__name__}.")

            tensor = images.detach().cpu()
            if tensor.ndim == 3:
                tensor = tensor[None, ...]

            if tensor.ndim != 4:
                raise ValueError(f"Expected IMAGE batch tensor with 4 dims, got {tuple(tensor.shape)}.")

            frame_limit = max(1, min(int(max_frames or 256), int(tensor.shape[0])))
            metadata = _parse_metadata(metadata_json)

            frames: list[dict[str, Any]] = []
            total_size = 0

            for index in range(frame_limit):
                png_bytes = _image_tensor_to_png_bytes(tensor[index])
                total_size += len(png_bytes)

                frames.append(
                    _bytes_envelope(
                        data=png_bytes,
                        filename=f"{cleaned_prefix}_{index:06d}.png",
                        content_type="image/png",
                        metadata={
                            **metadata,
                            "frame_index": index,
                            "fps": int(fps or 24),
                        },
                    )
                )

            video_payload = {
                "type": "comfyui_png_frame_sequence",
                "fps": int(fps or 24),
                "frame_count": len(frames),
                "total_size_bytes": total_size,
                "frames": frames,
                "metadata": metadata,
                "saved_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            }

            if cleaned_document_id:
                api_payload = session.save_value(
                    collection=cleaned_collection,
                    query={"_id": cleaned_document_id},
                    document_id="",
                    field_path=cleaned_field_path,
                    value=video_payload,
                    upsert_if_missing=False,
                    parse_json_strings=False,
                    normalize_for_storage=False,
                )
                final_document_id = cleaned_document_id
                operation = "update_existing_video_document"
            else:
                document = {
                    cleaned_field_path: video_payload,
                    "source": "comfyui",
                    "content_type": "application/vnd.comfyui.frame-sequence+json",
                    "frame_count": len(frames),
                    "fps": int(fps or 24),
                    "size_bytes": total_size,
                    "created_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
                }
                if cleaned_doc_key:
                    document["doc_key"] = cleaned_doc_key
                if metadata:
                    document["metadata"] = metadata

                api_payload = session.create_doc(collection=cleaned_collection, document=document)
                final_document_id = _extract_document_id(api_payload)
                operation = "create_new_video_document"

            success = bool(api_payload.get("success")) if isinstance(api_payload, dict) else False

            payload = {
                "success": success,
                "message": (
                    f"Saved {len(frames)} video frame(s) to {cleaned_collection}/{final_document_id}."
                    if success
                    else "Failed to save video frames."
                ),
                "data": {
                    "operation": operation,
                    "collection_name": cleaned_collection,
                    "document_id": final_document_id,
                    "field_path": cleaned_field_path,
                    "fps": int(fps or 24),
                    "frame_count": len(frames),
                    "total_size_bytes": total_size,
                    "refresh": refresh,
                    "api_response": api_payload,
                },
                "error": None if success else (api_payload.get("error") if isinstance(api_payload, dict) else "Save failed."),
                "status_code": api_payload.get("status_code", 0) if isinstance(api_payload, dict) else 0,
            }

            return (_json_text(payload), final_document_id, len(frames), refresh, success)

        except Exception as exc:
            payload = _error_payload(
                f"Save Video Frames failed: {exc}",
                data={
                    "collection_name": cleaned_collection,
                    "document_id": cleaned_document_id,
                    "field_path": cleaned_field_path,
                    "refresh": refresh,
                    "checks": [
                        "Connect an IMAGE batch to images.",
                        "Use a reasonable max_frames value.",
                        "Use an existing document_id only when updating an existing document.",
                        "Confirm metadata_json is valid JSON.",
                    ],
                },
                error_type=exc.__class__.__name__,
            )
            return (_json_text(payload), cleaned_document_id, 0, refresh, False)


class ZMongoApiLoadVideoFrameNode(AlwaysDirtyMixin):
    """
    Loads one PNG frame from a document created by 06 Save Video Frames.
    """

    @classmethod
    def INPUT_TYPES(cls):
        return {
            "required": {
                "session": ("ZMONGO_API_SESSION",),
                "collection_name": ("STRING", {"default": "videos"}),
                "document_id": ("STRING", {"default": ""}),
                "field_path": ("STRING", {"default": "video_frames"}),
                "frame_index": ("INT", {"default": 0, "min": 0, "max": 1000000}),
                "cache": ("BOOLEAN", {"default": False}),
            },
            "optional": {
                "refresh_token": ("STRING", {"default": ""}),
            },
        }

    RETURN_TYPES = ("IMAGE", "STRING", "STRING", "INT", "BOOLEAN")
    RETURN_NAMES = ("image", "json", "status", "frame_count", "success")
    FUNCTION = "load_video_frame"
    CATEGORY = "ZMongo/06 Video"

    def load_video_frame(
        self,
        session,
        collection_name: str,
        document_id: str,
        field_path: str,
        frame_index: int,
        cache: bool,
        refresh_token: str = "",
    ):
        cleaned_collection = _clean_scalar(collection_name) or "videos"
        cleaned_document_id = _clean_scalar(document_id)
        cleaned_field_path = _clean_scalar(field_path) or "video_frames"
        refresh = _dirty_token("load_video_frame", cleaned_collection, cleaned_document_id, cleaned_field_path, frame_index, refresh_token)

        empty = torch.zeros((1, 1, 1, 3), dtype=torch.float32)

        if session is None:
            payload = _error_payload("No API session provided.")
            return (empty, _json_text(payload), payload["message"], 0, False)

        if not cleaned_document_id:
            payload = _error_payload("document_id is required.")
            return (empty, _json_text(payload), payload["message"], 0, False)

        try:
            api_payload = session.get_doc(
                collection=cleaned_collection,
                document_id=cleaned_document_id,
                cache=cache,
            )

            data = api_payload.get("data", {}) if isinstance(api_payload, dict) else {}
            document = {}

            if isinstance(data, dict):
                if isinstance(data.get("document"), dict):
                    document = data["document"]
                elif "_id" in data:
                    document = data

            if not document:
                raise ValueError("Document not found in API payload.")

            video_payload = safe_get_by_path(document, cleaned_field_path)
            if not isinstance(video_payload, dict):
                raise ValueError(f"Field path {cleaned_field_path!r} did not contain a video frame payload.")

            frames = video_payload.get("frames")
            if not isinstance(frames, list) or not frames:
                raise ValueError(f"Video payload at {cleaned_field_path!r} has no frames list.")

            safe_index = max(0, min(int(frame_index or 0), len(frames) - 1))
            image_bytes = _decode_bytes_envelope(frames[safe_index])
            image_tensor = _png_bytes_to_image_tensor(image_bytes)

            payload = _success_payload(
                "Loaded video frame.",
                {
                    "collection_name": cleaned_collection,
                    "document_id": cleaned_document_id,
                    "field_path": cleaned_field_path,
                    "requested_frame_index": int(frame_index or 0),
                    "resolved_frame_index": safe_index,
                    "frame_count": len(frames),
                    "fps": video_payload.get("fps"),
                    "refresh": refresh,
                },
            )

            status = f"Loaded frame {safe_index + 1}/{len(frames)} from {cleaned_document_id}"
            return (image_tensor, _json_text(payload), status, len(frames), True)

        except Exception as exc:
            payload = _error_payload(
                f"Load Video Frame failed: {exc}",
                data={
                    "collection_name": cleaned_collection,
                    "document_id": cleaned_document_id,
                    "field_path": cleaned_field_path,
                    "frame_index": int(frame_index or 0),
                    "refresh": refresh,
                    "checks": [
                        "Use a document created by 06 Save Video Frames.",
                        "Confirm field_path is video_frames.",
                        "Confirm frame_index is within range.",
                    ],
                },
                error_type=exc.__class__.__name__,
            )
            return (empty, _json_text(payload), payload["message"], 0, False)


class ZMongoApiSaveVideoFileNode(AlwaysDirtyMixin):
    """
    Saves an existing video file path as one bytes envelope.

    Use this when another ComfyUI node writes an .mp4/.webm/.gif file and gives
    you the path.
    """

    @classmethod
    def INPUT_TYPES(cls):
        return {
            "required": {
                "session": ("ZMONGO_API_SESSION",),
                "video_file_path": ("STRING", {"default": "", "multiline": False}),
                "collection_name": ("STRING", {"default": "videos"}),
                "field_path": ("STRING", {"default": "video_data"}),
                "content_type": ("STRING", {"default": "video/mp4"}),
                "metadata_json": ("STRING", {"default": "{}", "multiline": True}),
            },
            "optional": {
                "document_id": ("STRING", {"default": ""}),
                "doc_key": ("STRING", {"default": ""}),
            },
        }

    RETURN_TYPES = ("STRING", "STRING", "INT", "STRING", "BOOLEAN")
    RETURN_NAMES = ("json", "document_id", "size_bytes", "refresh", "success")
    FUNCTION = "save_video_file"
    CATEGORY = "ZMongo/06 Video"

    def save_video_file(
        self,
        session,
        video_file_path: str,
        collection_name: str,
        field_path: str,
        content_type: str,
        metadata_json: str,
        document_id: str = "",
        doc_key: str = "",
    ):
        cleaned_collection = _clean_scalar(collection_name) or "videos"
        cleaned_field_path = _clean_scalar(field_path) or "video_data"
        cleaned_document_id = _clean_scalar(document_id)
        cleaned_doc_key = _clean_scalar(doc_key)
        cleaned_content_type = _clean_scalar(content_type) or "video/mp4"
        cleaned_path_text = _clean_scalar(video_file_path)

        refresh = _dirty_token("save_video_file", cleaned_collection, cleaned_document_id, cleaned_field_path)

        if session is None:
            payload = _error_payload("No API session provided.")
            return (_json_text(payload), cleaned_document_id, 0, refresh, False)

        try:
            if not cleaned_path_text:
                raise ValueError("video_file_path is required.")

            path = Path(cleaned_path_text).expanduser()
            if not path.exists() or not path.is_file():
                raise FileNotFoundError(f"Video file not found: {path}")

            metadata = _parse_metadata(metadata_json)
            video_bytes = path.read_bytes()

            video_envelope = _bytes_envelope(
                data=video_bytes,
                filename=path.name,
                content_type=cleaned_content_type,
                metadata=metadata,
            )

            if cleaned_document_id:
                api_payload = session.save_value(
                    collection=cleaned_collection,
                    query={"_id": cleaned_document_id},
                    document_id="",
                    field_path=cleaned_field_path,
                    value=video_envelope,
                    upsert_if_missing=False,
                    parse_json_strings=False,
                    normalize_for_storage=False,
                )
                final_document_id = cleaned_document_id
                operation = "update_existing_video_file_document"
            else:
                document = {
                    cleaned_field_path: video_envelope,
                    "source": "comfyui",
                    "filename": path.name,
                    "content_type": cleaned_content_type,
                    "size_bytes": len(video_bytes),
                    "created_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
                }
                if cleaned_doc_key:
                    document["doc_key"] = cleaned_doc_key
                if metadata:
                    document["metadata"] = metadata

                api_payload = session.create_doc(collection=cleaned_collection, document=document)
                final_document_id = _extract_document_id(api_payload)
                operation = "create_new_video_file_document"

            success = bool(api_payload.get("success")) if isinstance(api_payload, dict) else False

            payload = {
                "success": success,
                "message": (
                    f"Saved video file to {cleaned_collection}/{final_document_id}."
                    if success
                    else "Failed to save video file."
                ),
                "data": {
                    "operation": operation,
                    "collection_name": cleaned_collection,
                    "document_id": final_document_id,
                    "field_path": cleaned_field_path,
                    "video_file_path": str(path),
                    "filename": path.name,
                    "content_type": cleaned_content_type,
                    "size_bytes": len(video_bytes),
                    "refresh": refresh,
                    "api_response": api_payload,
                },
                "error": None if success else (api_payload.get("error") if isinstance(api_payload, dict) else "Save failed."),
                "status_code": api_payload.get("status_code", 0) if isinstance(api_payload, dict) else 0,
            }

            return (_json_text(payload), final_document_id, len(video_bytes), refresh, success)

        except Exception as exc:
            payload = _error_payload(
                f"Save Video File failed: {exc}",
                data={
                    "collection_name": cleaned_collection,
                    "document_id": cleaned_document_id,
                    "field_path": cleaned_field_path,
                    "video_file_path": cleaned_path_text,
                    "refresh": refresh,
                    "checks": [
                        "Confirm video_file_path exists on the ComfyUI machine.",
                        "Use an existing document_id only when updating an existing document.",
                        "Confirm metadata_json is valid JSON.",
                    ],
                },
                error_type=exc.__class__.__name__,
            )
            return (_json_text(payload), cleaned_document_id, 0, refresh, False)


class ZMongoApiVideoFramePathsNode(AlwaysDirtyMixin):
    """
    Lists frame dot-paths for a document created by 06 Save Video Frames.
    """

    @classmethod
    def INPUT_TYPES(cls):
        return {
            "required": {
                "session": ("ZMONGO_API_SESSION",),
                "collection_name": ("STRING", {"default": "videos"}),
                "document_id": ("STRING", {"default": ""}),
                "field_path": ("STRING", {"default": "video_frames"}),
                "cache": ("BOOLEAN", {"default": False}),
            },
            "optional": {
                "refresh_token": ("STRING", {"default": ""}),
            },
        }

    RETURN_TYPES = ("STRING", "*", "STRING", "INT", "BOOLEAN")
    RETURN_NAMES = ("json", "paths", "indexed", "count", "success")
    OUTPUT_IS_LIST = (False, True, False, False, False)
    FUNCTION = "video_frame_paths"
    CATEGORY = "ZMongo/06 Video"

    def video_frame_paths(
        self,
        session,
        collection_name: str,
        document_id: str,
        field_path: str,
        cache: bool,
        refresh_token: str = "",
    ):
        cleaned_collection = _clean_scalar(collection_name) or "videos"
        cleaned_document_id = _clean_scalar(document_id)
        cleaned_field_path = _clean_scalar(field_path) or "video_frames"
        refresh = _dirty_token("video_frame_paths", cleaned_collection, cleaned_document_id, cleaned_field_path, refresh_token)

        if session is None:
            payload = _error_payload("No API session provided.")
            return (_json_text(payload), [], _indexed_list_text([]), 0, False)

        if not cleaned_document_id:
            payload = _error_payload("document_id is required.")
            return (_json_text(payload), [], _indexed_list_text([]), 0, False)

        try:
            api_payload = session.get_doc(
                collection=cleaned_collection,
                document_id=cleaned_document_id,
                cache=cache,
            )

            data = api_payload.get("data", {}) if isinstance(api_payload, dict) else {}
            document = data.get("document") if isinstance(data, dict) and isinstance(data.get("document"), dict) else {}

            video_payload = safe_get_by_path(document, cleaned_field_path)
            frames = video_payload.get("frames") if isinstance(video_payload, dict) else []

            if not isinstance(frames, list):
                frames = []

            paths = [f"{cleaned_field_path}.frames.{index}" for index in range(len(frames))]

            payload = _success_payload(
                f"Found {len(paths)} video frame path(s).",
                {
                    "collection_name": cleaned_collection,
                    "document_id": cleaned_document_id,
                    "field_path": cleaned_field_path,
                    "count": len(paths),
                    "paths": paths,
                    "refresh": refresh,
                },
            )

            return (_json_text(payload), _as_comfy_list(paths), _indexed_list_text(paths), len(paths), True)

        except Exception as exc:
            payload = _error_payload(
                f"Video Frame Paths failed: {exc}",
                data={
                    "collection_name": cleaned_collection,
                    "document_id": cleaned_document_id,
                    "field_path": cleaned_field_path,
                    "refresh": refresh,
                },
                error_type=exc.__class__.__name__,
            )
            return (_json_text(payload), [], _indexed_list_text([]), 0, False)


NODE_CLASS_MAPPINGS = {
    "ZMongoApiSaveVideoFramesNode": ZMongoApiSaveVideoFramesNode,
    "ZMongoApiLoadVideoFrameNode": ZMongoApiLoadVideoFrameNode,
    "ZMongoApiSaveVideoFileNode": ZMongoApiSaveVideoFileNode,
    "ZMongoApiVideoFramePathsNode": ZMongoApiVideoFramePathsNode,
}

NODE_DISPLAY_NAME_MAPPINGS = {
    "ZMongoApiSaveVideoFramesNode": "06 Save Video Frames",
    "ZMongoApiLoadVideoFrameNode": "06 Load Video Frame",
    "ZMongoApiSaveVideoFileNode": "06 Save Video File",
    "ZMongoApiVideoFramePathsNode": "06 Video Frame Paths",
}

__all__ = [
    "NODE_CLASS_MAPPINGS",
    "NODE_DISPLAY_NAME_MAPPINGS",
]