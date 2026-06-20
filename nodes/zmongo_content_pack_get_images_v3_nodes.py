from __future__ import annotations

# pylint: disable=too-many-locals,too-many-branches,broad-exception-caught,line-too-long,unused-argument,missing-class-docstring,missing-function-docstring

"""
ComfyUI-ZMongo Content Pack V3 multi-image getter node.

This companion module intentionally keeps the existing V3 content-pack module
unchanged while adding a registered node that can restore all matching IMAGE
fields as one Comfy IMAGE batch.
"""

import io
import time
from copy import deepcopy
from typing import Any

try:
    import torch
except Exception:  # pragma: no cover
    torch = None  # type: ignore[assignment]

try:
    import numpy as np
except Exception:  # pragma: no cover
    np = None  # type: ignore[assignment]

try:
    from PIL import Image
except Exception:  # pragma: no cover
    Image = None  # type: ignore[assignment]

try:
    from .generic_helpers import AlwaysDirtyMixin, _as_comfy_list
except Exception:  # pragma: no cover
    class AlwaysDirtyMixin:
        @classmethod
        def IS_CHANGED(cls, *args: Any, **kwargs: Any) -> float:
            return time.time()

    def _as_comfy_list(values: Any) -> list[Any]:
        if values is None:
            return []
        if isinstance(values, list):
            return values
        if isinstance(values, tuple):
            return list(values)
        return [values]

from .zmongo_content_pack_v3_nodes import (  # type: ignore
    CONTENT_PACK_TYPE,
    _coerce_field_value,
    _decode_image_bytes_from_value,
    _empty_image,
    _field_list,
    _find_field,
    _image_bytes_to_tensor,
    _json_dumps,
    _normalize_type,
    _safe_int,
    _safe_str,
)


def _csv_list(value: Any) -> list[str]:
    if isinstance(value, list):
        return [_safe_str(item) for item in value if _safe_str(item)]
    text = _safe_str(value)
    if not text:
        return []
    return [part.strip() for part in text.replace("\r", "\n").replace("\t", ",").replace("\n", ",").split(",") if part.strip()]


def _asset_ref_collection(asset_ref: dict[str, Any]) -> str:
    original = asset_ref.get("original") if isinstance(asset_ref.get("original"), dict) else {}
    return _safe_str(asset_ref.get("collection") or asset_ref.get("asset_collection") or original.get("asset_collection"))


def _asset_ref_document_id(asset_ref: dict[str, Any]) -> str:
    original = asset_ref.get("original") if isinstance(asset_ref.get("original"), dict) else {}
    return _safe_str(asset_ref.get("document_id") or asset_ref.get("file_id") or asset_ref.get("_id") or original.get("file_id"))


def _tensor_to_pil_rgb(tensor: Any) -> Any:
    if Image is None or np is None:
        raise RuntimeError("Pillow and numpy are required to resize mismatched IMAGE tensors.")
    array = tensor.detach().cpu().numpy() if torch is not None and hasattr(tensor, "detach") else np.asarray(tensor)
    if array.ndim == 4:
        array = array[0]
    if array.ndim == 2:
        array = np.stack([array, array, array], axis=-1)
    if array.ndim != 3:
        raise ValueError(f"Unsupported IMAGE tensor shape: {array.shape!r}")
    if array.shape[-1] == 1:
        array = np.repeat(array, 3, axis=-1)
    elif array.shape[-1] > 4:
        array = array[..., :3]
    array = np.nan_to_num(array)
    if array.dtype != np.uint8:
        try:
            if float(np.nanmax(array)) <= 1.0:
                array = array * 255.0
        except Exception:
            pass
        array = np.clip(array, 0, 255).astype(np.uint8)
    return Image.fromarray(array[..., :3], mode="RGB")


def _resize_tensor_like(tensor: Any, height: int, width: int) -> Any:
    if torch is None:
        return tensor
    if tensor is None or not hasattr(tensor, "shape"):
        return tensor
    try:
        if int(tensor.shape[1]) == height and int(tensor.shape[2]) == width:
            return tensor
        pil = _tensor_to_pil_rgb(tensor).resize((width, height), Image.LANCZOS if Image is not None else 1)
        buffer = io.BytesIO()
        pil.save(buffer, format="PNG")
        return _image_bytes_to_tensor(buffer.getvalue())
    except Exception:
        return tensor


def _concat_image_batches(tensors: list[Any]) -> Any:
    tensors = [tensor for tensor in tensors if tensor is not None]
    if not tensors:
        return _empty_image()
    if torch is None:
        return tensors[0]
    try:
        return torch.cat(tensors, dim=0)
    except Exception:
        pass

    try:
        first = tensors[0]
        height = int(first.shape[1])
        width = int(first.shape[2])
        resized = [_resize_tensor_like(tensor, height, width) for tensor in tensors]
        return torch.cat(resized, dim=0)
    except Exception:
        return tensors[0]


def _decode_image_value_to_batch(value: Any) -> Any:
    if isinstance(value, list):
        tensors: list[Any] = []
        for item in value:
            try:
                tensors.append(_image_bytes_to_tensor(_decode_image_bytes_from_value(item)))
            except Exception:
                continue
        return _concat_image_batches(tensors)
    return _image_bytes_to_tensor(_decode_image_bytes_from_value(value))


def _field_to_image_batch(field: dict[str, Any], session: Any = None, master_key_hex: str = "") -> Any:
    storage = _safe_str(field.get("storage"))
    value = _coerce_field_value(field)
    asset_ref = field.get("asset_ref") if isinstance(field.get("asset_ref"), dict) else {}

    if storage in {"inline_base64_sequence", "inline_image_sequence"} or isinstance(value, list):
        return _decode_image_value_to_batch(value)

    if storage == "inline_base64" or (isinstance(value, str) and value.strip().startswith("data:image/")):
        return _decode_image_value_to_batch(value)

    if isinstance(value, (str, bytes, bytearray, dict)) and not asset_ref:
        return _decode_image_value_to_batch(value)

    if asset_ref:
        if not session or not hasattr(session, "fetch_image_field"):
            raise RuntimeError("Image field is an asset reference, but no session with fetch_image_field was provided.")
        collection = _asset_ref_collection(asset_ref)
        document_id = _asset_ref_document_id(asset_ref)
        field_path = _safe_str(asset_ref.get("field_path") or asset_ref.get("path") or field.get("source_path") or "image_data")
        image_bytes, _source = session.fetch_image_field(
            collection=collection,
            document_id=document_id,
            field_path=field_path,
            master_key_hex=master_key_hex,
        )
        return _image_bytes_to_tensor(image_bytes or b"")

    return _decode_image_value_to_batch(value)


def _select_image_fields(content_pack: Any, selection_mode: str, aliases_csv: str, alias_prefix: str, max_images: int) -> list[dict[str, Any]]:
    mode = _safe_str(selection_mode, "all_images").lower()
    limit = _safe_int(max_images, 0)
    selected: list[dict[str, Any]] = []

    if mode == "aliases_csv":
        for alias in _csv_list(aliases_csv):
            field = _find_field(content_pack, alias, "IMAGE")
            if isinstance(field, dict) and _normalize_type(field.get("comfy_type"), "ANY") == "IMAGE":
                selected.append(field)
            if limit > 0 and len(selected) >= limit:
                break
        return selected

    fields = [field for field in _field_list(content_pack) if _normalize_type(field.get("comfy_type"), "ANY") == "IMAGE"]

    if mode == "alias_prefix":
        prefix = _safe_str(alias_prefix)
        if prefix:
            fields = [field for field in fields if _safe_str(field.get("alias")).startswith(prefix)]

    if limit > 0:
        fields = fields[:limit]
    return fields


class ZMongoContentPackGetImagesV3(AlwaysDirtyMixin):
    """Restore multiple IMAGE fields from a content pack as one Comfy IMAGE batch."""

    CATEGORY = "ZMongo/09 Content Packs/Get"
    FUNCTION = "get_images"
    RETURN_TYPES = ("IMAGE", "*", "STRING", "INT", "BOOLEAN")
    RETURN_NAMES = ("images", "aliases", "field_info_json", "count", "found")
    OUTPUT_IS_LIST = (False, True, False, False, False)

    @classmethod
    def INPUT_TYPES(cls):
        return {
            "required": {
                "content_pack": (CONTENT_PACK_TYPE,),
                "selection_mode": (["all_images", "alias_prefix", "aliases_csv"], {"default": "all_images"}),
            },
            "optional": {
                "aliases_csv": ("STRING", {"default": "", "multiline": True}),
                "alias_prefix": ("STRING", {"default": "image_"}),
                "max_images": ("INT", {"default": 0, "min": 0, "max": 4096}),
                "session": ("ZMONGO_API_SESSION",),
                "master_key_hex": ("STRING", {"default": ""}),
                "refresh_token": ("STRING", {"default": ""}),
            },
        }

    def get_images(
        self,
        content_pack: Any,
        selection_mode: str = "all_images",
        aliases_csv: str = "",
        alias_prefix: str = "image_",
        max_images: int = 0,
        session: Any = None,
        master_key_hex: str = "",
        refresh_token: str = "",
    ):
        fields = _select_image_fields(content_pack, selection_mode, aliases_csv, alias_prefix, max_images)
        if not fields:
            info = {
                "message": "No IMAGE fields matched the requested selection.",
                "selection_mode": selection_mode,
                "aliases_csv": aliases_csv,
                "alias_prefix": alias_prefix,
            }
            return (_empty_image(), _as_comfy_list([]), _json_dumps(info), 0, False)

        image_batches: list[Any] = []
        aliases: list[str] = []
        errors: list[dict[str, Any]] = []
        field_infos: list[dict[str, Any]] = []

        for index, field in enumerate(fields):
            alias = _safe_str(field.get("alias")) or f"image_{index}"
            try:
                image_batches.append(_field_to_image_batch(field, session=session, master_key_hex=master_key_hex))
                aliases.append(alias)
                field_infos.append(deepcopy(field))
            except Exception as exc:
                errors.append({"index": index, "alias": alias, "type": exc.__class__.__name__, "message": str(exc)})

        if not image_batches:
            info = {"message": "Matched IMAGE fields, but none could be decoded.", "errors": errors, "fields": field_infos}
            return (_empty_image(), _as_comfy_list(aliases), _json_dumps(info), 0, False)

        images = _concat_image_batches(image_batches)
        info = {
            "selection_mode": selection_mode,
            "alias_prefix": alias_prefix,
            "requested_aliases": _csv_list(aliases_csv),
            "count": len(aliases),
            "aliases": aliases,
            "error_count": len(errors),
            "errors": errors,
            "fields": field_infos,
        }
        return (images, _as_comfy_list(aliases), _json_dumps(info), len(aliases), True)


NODE_CLASS_MAPPINGS = {
    "ZMongoContentPackGetImagesV3": ZMongoContentPackGetImagesV3,
}

NODE_DISPLAY_NAME_MAPPINGS = {
    "ZMongoContentPackGetImagesV3": "09 ZMongo Content Pack Get Images",
}

__all__ = ["NODE_CLASS_MAPPINGS", "NODE_DISPLAY_NAME_MAPPINGS"]
