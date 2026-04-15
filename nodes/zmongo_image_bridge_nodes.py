from __future__ import annotations

import base64
import hashlib
import io
import json
from typing import Any, Dict, Tuple

import numpy as np
import torch
from PIL import Image

JSON_TYPE = "ZMONGO_JSON"


def _json_dumps(value: Any) -> str:
    return json.dumps(value, indent=2, sort_keys=True, ensure_ascii=False, default=str)


def _to_numpy_image(image: torch.Tensor, batch_index: int = 0) -> np.ndarray:
    if not isinstance(image, torch.Tensor):
        raise TypeError(f"Expected torch.Tensor for IMAGE input, got {type(image).__name__}")

    tensor = image.detach().cpu()

    if tensor.ndim == 4:
        if batch_index < 0 or batch_index >= tensor.shape[0]:
            raise IndexError(f"batch_index {batch_index} out of range for batch size {tensor.shape[0]}")
        tensor = tensor[batch_index]
    elif tensor.ndim != 3:
        raise ValueError(f"Expected IMAGE tensor with 3 or 4 dimensions, got shape {tuple(tensor.shape)}")

    tensor = tensor.clamp(0.0, 1.0)
    arr = (tensor.numpy() * 255.0).round().astype(np.uint8)

    if arr.shape[-1] not in (1, 3, 4):
        raise ValueError(f"Unsupported channel count {arr.shape[-1]}; expected 1, 3, or 4")

    return arr


def _encode_png_bytes(arr: np.ndarray) -> Tuple[bytes, int, int]:
    height, width = arr.shape[0], arr.shape[1]

    if arr.shape[-1] == 1:
        pil_image = Image.fromarray(arr[:, :, 0], mode="L")
    elif arr.shape[-1] == 3:
        pil_image = Image.fromarray(arr, mode="RGB")
    else:
        pil_image = Image.fromarray(arr, mode="RGBA")

    buffer = io.BytesIO()
    pil_image.save(buffer, format="PNG")
    return buffer.getvalue(), width, height


def _build_bytes_envelope(
    png_bytes: bytes,
    *,
    filename: str,
    mime_type: str,
    width: int,
    height: int,
) -> Dict[str, Any]:
    return {
        "__type__": "bytes",
        "encoding": "base64",
        "size_bytes": len(png_bytes),
        "data": base64.b64encode(png_bytes).decode("ascii"),
        "mime_type": mime_type,
        "filename": filename,
        "width": width,
        "height": height,
        "sha256": hashlib.sha256(png_bytes).hexdigest(),
    }


class ZMongoImageToJsonNode:
    CATEGORY = "ZMongo/Image"
    FUNCTION = "image_to_json"
    RETURN_TYPES = (JSON_TYPE, JSON_TYPE, "STRING", "BOOLEAN")
    RETURN_NAMES = ("result_json", "data_json", "text_output", "success")

    @classmethod
    def INPUT_TYPES(cls):
        return {
            "required": {
                "image": ("IMAGE",),
                "filename": ("STRING", {"default": "image.png"}),
                "output_mode": (["bytes_envelope", "image_record"],),
                "batch_index": ("INT", {"default": 0, "min": 0, "max": 9999}),
            }
        }

    def image_to_json(
        self,
        image: torch.Tensor,
        filename: str,
        output_mode: str,
        batch_index: int,
    ):
        try:
            arr = _to_numpy_image(image, batch_index=int(batch_index))
            png_bytes, width, height = _encode_png_bytes(arr)

            envelope = _build_bytes_envelope(
                png_bytes,
                filename=filename.strip() or "image.png",
                mime_type="image/png",
                width=width,
                height=height,
            )

            if output_mode == "image_record":
                data_value: Dict[str, Any] = {
                    "filename": envelope["filename"],
                    "mime_type": envelope["mime_type"],
                    "width": width,
                    "height": height,
                    "sha256": envelope["sha256"],
                    "image": envelope,
                }
            else:
                data_value = envelope

            payload = {
                "success": True,
                "message": "Image converted for ZMongo storage",
                "status_code": 200,
                "error": None,
                "data": data_value,
            }

            text_output = (
                f'{envelope["filename"]} | {width}x{height} | '
                f'{envelope["size_bytes"]} bytes | sha256={envelope["sha256"][:16]}...'
            )
            return (
                _json_dumps(payload),
                _json_dumps(data_value),
                text_output,
                True,
            )
        except Exception as exc:
            payload = {
                "success": False,
                "message": str(exc),
                "status_code": 0,
                "error": {"msg": str(exc), "type": exc.__class__.__name__},
                "data": {},
            }
            return (_json_dumps(payload), _json_dumps({}), "", False)


NODE_CLASS_MAPPINGS = {
    "ZMongoImageToJsonNode": ZMongoImageToJsonNode,
}

NODE_DISPLAY_NAME_MAPPINGS = {
    "ZMongoImageToJsonNode": "ZMongo Image To JSON",
}
