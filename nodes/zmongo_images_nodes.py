from __future__ import annotations

import base64
import io
import json
import time
from typing import Any, List, Optional
import torch
from PIL import Image

from .generic_helpers import AlwaysDirtyMixin, _clean_scalar_string, _dirty_token, _json_text, _error_payload, \
    _comfy_image_to_png_bytes, _build_binary_envelope, _extract_tensor_recursively, _success_payload, \
    _pil_to_comfy_image


# -----------------------------------------------------------------------------
# Image Sequence Engine Helpers
# -----------------------------------------------------------------------------



# -----------------------------------------------------------------------------
# Nodes
# -----------------------------------------------------------------------------


class ZMongoSaveImageSequenceNode(AlwaysDirtyMixin):
    """
    Splits an incoming image batch tensor into individual frame documents
    containing database binary envelopes and saves or updates them sequentially
    in a ZMongo collection.
    """

    @classmethod
    def INPUT_TYPES(cls):
        return {
            "required": {
                "session": ("ZMONGO_API_SESSION",),
                "collection_name": ("STRING", {"default": "image_sequences"}),
                "image_prefix": ("STRING", {"default": "drone_stream"}),
                "start_frame_number": ("INT", {"default": 0, "min": 0, "max": 1000000}),
            },
            "optional": {
                "images": ("IMAGE",),
                "metadata_json": ("STRING", {"default": "{}", "multiline": True}),
            },
        }

    RETURN_TYPES = ("STRING", "STRING", "INT", "BOOLEAN")
    RETURN_NAMES = ("json", "refresh", "total_saved_frames", "success")
    FUNCTION = "save_images"
    CATEGORY = "ZMongo/05 Image Sequences"

    def save_images(
        self,
        session: Any,
        collection_name: str,
        image_prefix: str,
        start_frame_number: int,
        images: Optional[torch.Tensor] = None,
        metadata_json: str = "{}",
    ):
        cleaned_collection = _clean_scalar_string(collection_name) or "image_sequences"
        cleaned_prefix = _clean_scalar_string(image_prefix) or "image_frame"
        refresh = _dirty_token(
            "save_image_sequence", cleaned_collection, cleaned_prefix
        )

        if session is None:
            return (
                _json_text(_error_payload("No API session provided.")),
                refresh,
                0,
                False,
            )

        target_tensor: Optional[torch.Tensor] = None

        if images is not None:
            target_tensor = _extract_tensor_recursively(images)

        # Emergency structural check: If it's a list/tuple of tensors passed from standard layout nodes
        if (
            target_tensor is None
            and isinstance(images, (list, tuple))
            and len(images) > 0
        ):
            if isinstance(images[0], torch.Tensor):
                target_tensor = images[0]

        if target_tensor is None:
            return (
                _json_text(
                    _error_payload(
                        "No valid image batch or frames found in the images input."
                    )
                ),
                refresh,
                0,
                False,
            )

        try:
            try:
                base_metadata = json.loads(metadata_json or "{}")
                if not isinstance(base_metadata, dict):
                    base_metadata = {"raw_metadata": base_metadata}
            except Exception:
                base_metadata = {"raw_metadata_error": "Failed to parse metadata_json"}

            # Process tensor batch bounds [B, H, W, C]
            num_frames = target_tensor.shape[0] if target_tensor.ndim == 4 else 1
            frames_saved = 0

            for idx in range(num_frames):
                frame_idx = start_frame_number + idx
                filename = f"{cleaned_prefix}_{frame_idx:04d}.png"

                frame_data = (
                    target_tensor[idx] if target_tensor.ndim == 4 else target_tensor
                )
                png_bytes = _comfy_image_to_png_bytes(frame_data)
                binary_envelope = _build_binary_envelope(png_bytes, filename)

                document_payload = {
                    "sequence_identifier": cleaned_prefix,
                    "frame_index": frame_idx,
                    "filename": filename,
                    "image_data": binary_envelope,
                    "saved_at_timestamp": time.time(),
                    "metadata": base_metadata,
                }

                query_match = {
                    "sequence_identifier": cleaned_prefix,
                    "frame_index": frame_idx,
                }

                save_response = session.save_value(
                    collection=cleaned_collection,
                    query=query_match,
                    document_id="",
                    field_path="",
                    value=document_payload,
                    upsert_if_missing=True,
                    parse_json_strings=False,
                    normalize_for_storage=False,
                )

                if save_response.get("success"):
                    frames_saved += 1

            result = _success_payload(
                f"Successfully parsed and written {frames_saved} image sequence assets to storage layer.",
                {
                    "collection": cleaned_collection,
                    "sequence_identifier": cleaned_prefix,
                    "frames_processed": num_frames,
                    "frames_successfully_written": frames_saved,
                },
            )
            return (
                _json_text(result),
                refresh,
                frames_saved,
                frames_saved == num_frames,
            )

        except Exception as exc:
            return (
                _json_text(
                    _error_payload(
                        f"Image sequence runtime write operation failed: {exc}"
                    )
                ),
                refresh,
                0,
                False,
            )


class ZMongoLoadImageSequenceNode(AlwaysDirtyMixin):
    """
    Queries a specified database sequence namespace, reconstructs ordered image
    batches, and returns a structural batch tensor for ComfyUI.
    """

    @classmethod
    def INPUT_TYPES(cls):
        return {
            "required": {
                "session": ("ZMONGO_API_SESSION",),
                "collection_name": ("STRING", {"default": "image_sequences"}),
                "image_prefix": ("STRING", {"default": "drone_stream"}),
            },
            "optional": {
                "limit": ("INT", {"default": 300, "min": 1, "max": 10000}),
                "skip": ("INT", {"default": 0, "min": 0, "max": 1000000}),
                "refresh_token": ("STRING", {"default": ""}),
            },
        }

    RETURN_TYPES = ("IMAGE", "STRING", "INT", "STRING")
    RETURN_NAMES = ("images", "json", "frame_count", "status")
    FUNCTION = "load_images"
    CATEGORY = "ZMongo/05 Image Sequences"

    def load_images(
        self,
        session: Any,
        collection_name: str,
        image_prefix: str,
        limit: int = 300,
        skip: int = 0,
        refresh_token: str = "",
    ):
        cleaned_collection = _clean_scalar_string(collection_name) or "image_sequences"
        cleaned_prefix = _clean_scalar_string(image_prefix) or "image_frame"
        refresh = _dirty_token(
            "load_image_sequence", cleaned_collection, cleaned_prefix, refresh_token
        )

        if session is None:
            err = _error_payload("No API session provided.")
            return (
                torch.zeros((1, 64, 64, 3)),
                _json_text(err),
                0,
                "Missing API connection.",
            )

        try:
            search_query = {"sequence_identifier": cleaned_prefix}

            api_response = session.query_docs(
                collection=cleaned_collection,
                query=search_query,
                many=True,
                limit=limit,
                skip=skip,
                sort=[("frame_index", 1)],
            )

            data_wrapper = api_response.get("data", {})
            documents = []
            if isinstance(data_wrapper, dict):
                documents = (
                    data_wrapper.get("documents") or data_wrapper.get("results") or []
                )
            elif isinstance(data_wrapper, list):
                documents = data_wrapper

            if not documents:
                err = _error_payload(
                    f"No sequence records matched parameters for target workspace identification: {cleaned_prefix}"
                )
                return (
                    torch.zeros((1, 64, 64, 3)),
                    _json_text(err),
                    0,
                    "No records found.",
                )

            frame_tensors: List[torch.Tensor] = []

            for doc in documents:
                image_field = doc.get("image_data")
                if not image_field or not isinstance(image_field, dict):
                    continue

                if image_field.get("__type__") == "bytes" and "data" in image_field:
                    b64_data = str(image_field["data"]).strip()
                    if "," in b64_data:
                        b64_data = b64_data.split(",", 1)[1]

                    raw_bytes = base64.b64decode(b64_data, validate=False)
                    pil_img = Image.open(io.BytesIO(raw_bytes))
                    frame_tensors.append(_pil_to_comfy_image(pil_img))

            if not frame_tensors:
                err = _error_payload(
                    "Matched documents did not contain decodable image binary envelopes."
                )
                return (
                    torch.zeros((1, 64, 64, 3)),
                    _json_text(err),
                    0,
                    "Binary unpack initialization error.",
                )

            unified_image_batch = torch.cat(frame_tensors, dim=0)

            success_payload = _success_payload(
                f"Successfully parsed and reconstructed frame buffer array tracking stack.",
                {
                    "collection": cleaned_collection,
                    "sequence_identifier": cleaned_prefix,
                    "total_frames_reconstructed": len(frame_tensors),
                },
            )

            status_summary = f"Loaded {len(frame_tensors)} frames safely for tracking group: {cleaned_prefix}"
            return (
                unified_image_batch,
                _json_text(success_payload),
                len(frame_tensors),
                status_summary,
            )

        except Exception as exc:
            err = _error_payload(
                f"Runtime extraction pipeline encountered unexpected error conditions: {exc}"
            )
            return (
                torch.zeros((1, 64, 64, 3)),
                _json_text(err),
                0,
                f"Exception failure context: {exc}",
            )


# -----------------------------------------------------------------------------
# Registration Mappings
# -----------------------------------------------------------------------------

NODE_CLASS_MAPPINGS = {
    "ZMongoSaveImageSequenceNode": ZMongoSaveImageSequenceNode,
    "ZMongoLoadImageSequenceNode": ZMongoLoadImageSequenceNode,
}

NODE_DISPLAY_NAME_MAPPINGS = {
    "ZMongoSaveImageSequenceNode": "07 Save Image Sequence to ZMongo",
    "ZMongoLoadImageSequenceNode": "07 Load Image Sequence from ZMongo",
}

__all__ = ["NODE_CLASS_MAPPINGS", "NODE_DISPLAY_NAME_MAPPINGS"]