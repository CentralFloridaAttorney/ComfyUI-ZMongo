from __future__ import annotations

# pylint: disable=broad-exception-caught,line-too-long,missing-class-docstring,missing-function-docstring,unused-argument

"""
ComfyUI-ZMongo Content Pack Image Sequence Nodes
================================================

Adds/repairs sequence getter nodes for video/image-sequence Content Packs:

    ZMongoContentPackGetImagesV3
    ZMongoContentPackGetImageSequenceV3

The node reads one V3 content-pack IMAGE field, normally alias ``images``, and
returns the entire sequence as one ComfyUI IMAGE batch output named ``images``.

This version accepts optional ``session`` and ``master_key_hex`` keyword inputs
so older/newer generated workflows do not fail with:

    TypeError: get_images() got an unexpected keyword argument 'session'

Install target:
    custom_nodes/ComfyUI-ZMongo/nodes/zmongo_content_pack_sequence_nodes.py

Register from nodes/__init__.py by importing this module and merging its
NODE_CLASS_MAPPINGS / NODE_DISPLAY_NAME_MAPPINGS.
"""

import base64
import io
import json
import os
import time
from copy import deepcopy
from pathlib import Path
from typing import Any, Optional

try:
    import torch
except Exception:  # pragma: no cover
    torch = None  # type: ignore[assignment]

try:
    import numpy as np
except Exception:  # pragma: no cover
    np = None  # type: ignore[assignment]

try:
    from PIL import Image, ImageOps
except Exception:  # pragma: no cover
    Image = None  # type: ignore[assignment]
    ImageOps = None  # type: ignore[assignment]

try:
    from .generic_helpers import AlwaysDirtyMixin
except Exception:  # pragma: no cover
    class AlwaysDirtyMixin:
        @classmethod
        def IS_CHANGED(cls, *args: Any, **kwargs: Any) -> float:
            return time.time()

try:
    from .zmongo_content_pack_v3_nodes import (  # type: ignore
        CONTENT_PACK_TYPE,
        _as_content_pack,
        _field_list,
        _find_field,
        _json_dumps,
        _normalize_type,
        _safe_str,
    )
except Exception:  # pragma: no cover
    CONTENT_PACK_TYPE = "ZMONGO_CONTENT_PACK"

    def _safe_str(value: Any, default: str = "") -> str:
        if value is None:
            return default
        if isinstance(value, (list, tuple)):
            value = value[0] if value else default
        return str(value).strip()

    def _json_dumps(value: Any, *, pretty: bool = True) -> str:
        try:
            if pretty:
                return json.dumps(value, indent=2, ensure_ascii=False, default=str)
            return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), default=str)
        except Exception:
            return json.dumps({"error": "json serialization failed", "repr": repr(value)}, indent=2)

    def _normalize_type(value: Any, fallback: str = "ANY") -> str:
        text = _safe_str(value).upper()
        aliases = {
            "STR": "STRING",
            "TEXT": "STRING",
            "BOOL": "BOOLEAN",
            "INTEGER": "INT",
            "NUMBER": "FLOAT",
            "DOUBLE": "FLOAT",
            "IMAGE_ASSET": "IMAGE",
            "OBJECT": "JSON",
            "DICT": "JSON",
            "LIST": "JSON",
        }
        text = aliases.get(text, text)
        return text if text in {"STRING", "INT", "FLOAT", "BOOLEAN", "IMAGE", "JSON", "ANY"} else fallback

    def _as_content_pack(value: Any) -> dict[str, Any]:
        if isinstance(value, dict):
            return value
        try:
            parsed = json.loads(str(value or "{}"))
            return parsed if isinstance(parsed, dict) else {}
        except Exception:
            return {}

    def _field_list(content_pack: Any) -> list[dict[str, Any]]:
        pack = _as_content_pack(content_pack)
        fields = pack.get("fields")
        return [item for item in fields if isinstance(item, dict)] if isinstance(fields, list) else []

    def _find_field(content_pack: Any, field_alias: str, expected_type: str = "") -> Optional[dict[str, Any]]:
        wanted = _safe_str(field_alias)
        expected = _normalize_type(expected_type, "") if expected_type else ""
        for field in _field_list(content_pack):
            if wanted in {_safe_str(field.get("alias")), _safe_str(field.get("source_path")), _safe_str(field.get("label"))}:
                if not expected or _normalize_type(field.get("comfy_type"), "ANY") == expected:
                    return field
                return field
        try:
            index = int(wanted)
            fields = _field_list(content_pack)
            if 0 <= index < len(fields):
                return fields[index]
        except Exception:
            pass
        return None


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except Exception:
        return int(default)


def _empty_image(width: int = 1, height: int = 1) -> Any:
    if torch is None:
        return None
    return torch.zeros((1, max(1, height), max(1, width), 3), dtype=torch.float32)


def _decode_base64_image_text(text: str) -> bytes:
    clean = _safe_str(text)
    if clean.startswith("data:") and "," in clean:
        clean = clean.split(",", 1)[1]
    return base64.b64decode(clean, validate=False)


def _resolve_asset_root(content_pack: Any, field: dict[str, Any], asset_root: str = "") -> str:
    explicit = _safe_str(asset_root)
    if explicit:
        return explicit

    pack = _as_content_pack(content_pack)
    candidates = [
        field.get("asset_root"),
        field.get("zip_asset_root"),
        field.get("root"),
        pack.get("asset_root"),
        pack.get("zip_asset_root"),
    ]
    assets = pack.get("assets")
    if isinstance(assets, dict):
        candidates.extend([
            assets.get("asset_root"),
            assets.get("zip_asset_root"),
            assets.get("root"),
            assets.get("directory"),
        ])
    extra = pack.get("extra")
    if isinstance(extra, dict):
        candidates.extend([
            extra.get("asset_root"),
            extra.get("zip_asset_root"),
            extra.get("root"),
        ])

    for candidate in candidates:
        text = _safe_str(candidate)
        if text:
            return text
    return ""


def _read_path_bytes(path_text: str, asset_root: str = "") -> bytes:
    path = Path(os.path.expanduser(_safe_str(path_text)))
    if not path.is_absolute() and _safe_str(asset_root):
        path = Path(os.path.expanduser(_safe_str(asset_root))) / path
    path = path.resolve()
    return path.read_bytes()


def _try_session_fetch_image_bytes(session: Any, item: Any, master_key_hex: str = "") -> Optional[bytes]:
    """Best-effort support for backend asset refs.

    The current public video pack workflow normally uses inline_base64_sequence
    or zip path assets. This helper prevents future asset-ref sequence items from
    failing when the workflow supplies a ZMongo session.
    """
    if session is None or not isinstance(item, dict):
        return None
    if not hasattr(session, "fetch_image_field"):
        return None

    asset_ref = item
    if isinstance(item.get("asset_ref"), dict):
        asset_ref = item["asset_ref"]
    elif isinstance(item.get("ref"), dict):
        asset_ref = item["ref"]

    collection = _safe_str(
        asset_ref.get("collection")
        or asset_ref.get("asset_collection")
        or ((asset_ref.get("original") or {}).get("asset_collection") if isinstance(asset_ref.get("original"), dict) else "")
    )
    document_id = _safe_str(
        asset_ref.get("document_id")
        or asset_ref.get("file_id")
        or asset_ref.get("_id")
        or ((asset_ref.get("original") or {}).get("file_id") if isinstance(asset_ref.get("original"), dict) else "")
    )
    field_path = _safe_str(asset_ref.get("field_path") or asset_ref.get("path") or item.get("field_path") or "image_data")

    if not collection or not document_id:
        return None

    image_bytes, _source = session.fetch_image_field(
        collection=collection,
        document_id=document_id,
        field_path=field_path,
        master_key_hex=master_key_hex,
    )
    return image_bytes


def _decode_image_bytes_from_item(item: Any, *, asset_root: str = "", session: Any = None, master_key_hex: str = "") -> bytes:
    if item is None:
        raise ValueError("Image sequence item is empty.")
    if isinstance(item, bytes):
        return item
    if isinstance(item, bytearray):
        return bytes(item)
    if isinstance(item, str):
        text = item.strip()
        if text.startswith("data:image/") or (len(text) > 100 and all(ch.isalnum() or ch in "+/=\n\r\t " for ch in text[:200])):
            return _decode_base64_image_text(text)
        return _read_path_bytes(text, asset_root=asset_root)
    if isinstance(item, dict):
        fetched = _try_session_fetch_image_bytes(session, item, master_key_hex=master_key_hex)
        if fetched is not None:
            return fetched

        if item.get("__type__") == "bytes" and item.get("encoding") == "base64":
            return _decode_base64_image_text(str(item.get("data") or ""))
        for key in ("value", "data", "image_data", "base64", "b64", "bytes", "image", "content"):
            if key in item and item.get(key) is not None:
                try:
                    return _decode_image_bytes_from_item(item.get(key), asset_root=asset_root, session=session, master_key_hex=master_key_hex)
                except Exception:
                    pass
        for key in ("path", "file_path", "filename"):
            if key in item and item.get(key):
                return _read_path_bytes(str(item.get(key)), asset_root=asset_root)
    raise ValueError(f"Unsupported image sequence item type: {type(item).__name__}")


def _pil_from_image_bytes(image_bytes: bytes) -> Any:
    if Image is None or ImageOps is None:
        raise RuntimeError("Pillow is required to decode content pack images.")
    with Image.open(io.BytesIO(image_bytes)) as image:
        return ImageOps.exif_transpose(image).convert("RGB")


def _pil_to_float_array(image: Any, size: Optional[tuple[int, int]] = None) -> Any:
    if np is None:
        raise RuntimeError("numpy is required to create Comfy IMAGE batches.")
    if size and image.size != size:
        resampling = Image.Resampling.LANCZOS if hasattr(Image, "Resampling") else Image.LANCZOS
        image = image.resize(size, resampling)
    return np.asarray(image).astype(np.float32) / 255.0


def _image_bytes_list_to_batch(image_bytes_list: list[bytes]) -> Any:
    if not image_bytes_list:
        return _empty_image()
    if torch is None or np is None:
        raise RuntimeError("torch and numpy are required to create Comfy IMAGE batches.")

    arrays: list[Any] = []
    target_size: Optional[tuple[int, int]] = None
    for raw in image_bytes_list:
        image = _pil_from_image_bytes(raw)
        if target_size is None:
            target_size = image.size
        arrays.append(_pil_to_float_array(image, size=target_size))

    batch = np.stack(arrays, axis=0)
    return torch.from_numpy(batch)


def _sequence_items_from_field(field: dict[str, Any]) -> list[Any]:
    value = field.get("value")
    if isinstance(value, list):
        return value
    if isinstance(value, dict):
        for key in ("frames", "images", "sequence", "items", "value"):
            candidate = value.get(key)
            if isinstance(candidate, list):
                return candidate
    if value is not None:
        return [value]

    for key in ("frames", "images", "sequence", "items"):
        candidate = field.get(key)
        if isinstance(candidate, list):
            return candidate

    sequence = field.get("sequence")
    if isinstance(sequence, dict):
        for key in ("frames", "images", "items", "value"):
            candidate = sequence.get(key)
            if isinstance(candidate, list):
                return candidate

    return []


def _find_sequence_field(content_pack: Any, alias: str, strict_type: bool) -> Optional[dict[str, Any]]:
    expected = "IMAGE" if strict_type else ""
    field = _find_field(content_pack, alias, expected)
    if field:
        return field

    for fallback_alias in ("images", "image_sequence", "video_frames", "frames", "video", "image"):
        field = _find_field(content_pack, fallback_alias, expected)
        if field:
            return field

    # Last-resort: first IMAGE field with sequence-like storage/value.
    for candidate in _field_list(content_pack):
        if _normalize_type(candidate.get("comfy_type"), "ANY") != "IMAGE":
            continue
        storage = _safe_str(candidate.get("storage"))
        value = candidate.get("value")
        if "sequence" in storage or isinstance(value, list):
            return candidate

    return None


class ZMongoContentPackGetImagesV3(AlwaysDirtyMixin):
    """Get a complete Content Pack image sequence as one ComfyUI IMAGE batch."""

    CATEGORY = "ZMongo/09 Content Packs/Get"
    FUNCTION = "get_images"
    RETURN_TYPES = ("IMAGE", "STRING", "BOOLEAN")
    RETURN_NAMES = ("images", "field_info_json", "found")

    @classmethod
    def INPUT_TYPES(cls):
        return {
            "required": {
                "content_pack": (CONTENT_PACK_TYPE,),
                "field_alias": ("STRING", {"default": "images"}),
                "strict_type": ("BOOLEAN", {"default": True}),
            },
            "optional": {
                # These inputs are intentionally accepted because generated
                # workflows may include the same optional inputs as the
                # single-image getter.
                "session": ("ZMONGO_API_SESSION",),
                "master_key_hex": ("STRING", {"default": ""}),
                "asset_root": ("STRING", {"default": ""}),
                "max_images": ("INT", {"default": 0, "min": 0, "max": 100000}),
            },
        }

    def get_images(
        self,
        content_pack: Any,
        field_alias: str = "images",
        strict_type: bool = True,
        session: Any = None,
        master_key_hex: str = "",
        asset_root: str = "",
        max_images: int = 0,
        **kwargs: Any,
    ):
        alias = _safe_str(field_alias) or "images"
        field = _find_sequence_field(content_pack, alias, bool(strict_type))

        if not field:
            return (
                _empty_image(),
                _json_dumps({
                    "message": f"Image sequence field not found: {alias}",
                    "field_alias": alias,
                    "accepted_optional_kwargs": sorted(kwargs.keys()),
                }),
                False,
            )

        actual_type = _normalize_type(field.get("comfy_type"), "ANY")
        if strict_type and actual_type != "IMAGE":
            return (
                _empty_image(),
                _json_dumps({"message": f"Type mismatch for {alias}: expected IMAGE, got {actual_type}", "field": field}),
                False,
            )

        try:
            items = _sequence_items_from_field(field)
            if not items:
                raise ValueError("The selected field does not contain sequence image items.")

            limit = _safe_int(max_images, 0)
            if limit > 0:
                items = items[:limit]

            resolved_root = _resolve_asset_root(content_pack, field, asset_root=asset_root)
            image_bytes = [
                _decode_image_bytes_from_item(
                    item,
                    asset_root=resolved_root,
                    session=session,
                    master_key_hex=_safe_str(master_key_hex),
                )
                for item in items
            ]
            images = _image_bytes_list_to_batch(image_bytes)

            info = deepcopy(field)
            info["sequence_getter"] = {
                "node": "ZMongoContentPackGetImagesV3",
                "output_name": "images",
                "decoded_count": len(image_bytes),
                "asset_root": _safe_str(resolved_root),
                "session_supplied": session is not None,
                "ignored_extra_kwargs": sorted(kwargs.keys()),
            }
            return (images, _json_dumps(info), True)
        except Exception as exc:
            info = deepcopy(field)
            info["sequence_getter_error"] = {"type": exc.__class__.__name__, "message": str(exc)}
            info["sequence_getter"] = {
                "node": "ZMongoContentPackGetImagesV3",
                "output_name": "images",
                "field_alias": alias,
                "session_supplied": session is not None,
                "ignored_extra_kwargs": sorted(kwargs.keys()),
            }
            return (_empty_image(), _json_dumps(info), False)


# Backward-compatible alias with a clearer name. Both class keys use the same implementation.
ZMongoContentPackGetImageSequenceV3 = ZMongoContentPackGetImagesV3

NODE_CLASS_MAPPINGS = {
    "ZMongoContentPackGetImagesV3": ZMongoContentPackGetImagesV3,
    "ZMongoContentPackGetImageSequenceV3": ZMongoContentPackGetImageSequenceV3,
}

NODE_DISPLAY_NAME_MAPPINGS = {
    "ZMongoContentPackGetImagesV3": "09 ZMongo Content Pack Get Images",
    "ZMongoContentPackGetImageSequenceV3": "09 ZMongo Content Pack Get Image Sequence",
}
