from __future__ import annotations

import json
from typing import Any, Dict, List, Tuple


class ZMongoUniversalModelAdapterNode:
    """
    Universal adapter that consumes ZMongo model metadata and exposes a fixed
    set of outputs. It uses smart-check compatibility metadata to decide which
    outputs are active.

    Important:
    - Output socket types remain statically declared for ComfyUI.
    - Inactive outputs are returned as None and should be treated as disabled.
    - A companion JS extension can visually dim inactive sockets/widgets.
    """

    CATEGORY = "ZMongo/Models"
    FUNCTION = "adapt"

    RETURN_TYPES = (
        "MODEL",
        "CLIP",
        "VAE",
        "STRING",
        "STRING",
        "BOOLEAN",
        "STRING",
    )
    RETURN_NAMES = (
        "model",
        "clip",
        "vae",
        "status_text",
        "active_outputs_json",
        "is_compatible",
        "model_type",
    )

    # Which outputs are allowed for each detected model type.
    OUTPUTS_BY_MODEL_TYPE: Dict[str, List[str]] = {
        "checkpoint": ["model", "clip", "vae"],
        "torch_model": ["model"],
        "diffusion": ["model"],
        "controlnet": ["model"],
        "upscaler": ["model"],
        "latent_upscaler": ["model"],
        "unet": ["model"],
        "clip": ["clip"],
        "clip_vision": ["clip"],
        "vae": ["vae"],
        "llm": [],
        "embedding": [],
        "text_encoder": [],
        "onnx_model": [],
        "audio_model": [],
        "speech_model": [],
        "unknown": [],
    }

    # Which target families the selected model is allowed to drive.
    TARGETS_BY_MODEL_TYPE: Dict[str, List[str]] = {
        "checkpoint": ["MODEL", "CLIP", "VAE"],
        "torch_model": ["MODEL"],
        "diffusion": ["MODEL"],
        "controlnet": ["MODEL"],
        "upscaler": ["MODEL"],
        "latent_upscaler": ["MODEL"],
        "unet": ["MODEL"],
        "clip": ["CLIP"],
        "clip_vision": ["CLIP"],
        "vae": ["VAE"],
        "llm": [],
        "embedding": [],
        "text_encoder": [],
        "onnx_model": [],
        "audio_model": [],
        "speech_model": [],
        "unknown": [],
    }

    @classmethod
    def INPUT_TYPES(cls):
        return {
            "required": {
                "zmongo_model_json": ("STRING", {"forceInput": True}),
            },
            "optional": {
                "expected_output_family": (
                    ["AUTO", "MODEL", "CLIP", "VAE"],
                    {"default": "AUTO"},
                ),
            },
            "hidden": {
                "unique_id": "UNIQUE_ID",
            },
        }

    @classmethod
    def IS_CHANGED(cls, zmongo_model_json: str, expected_output_family: str = "AUTO", unique_id=None):
        return (zmongo_model_json, expected_output_family, unique_id)

    @classmethod
    def VALIDATE_INPUTS(cls, input_types):
        # Keep backend validation permissive here; real validation happens after
        # parsing the metadata payload because the smart-check logic depends on
        # model metadata, not just the declared socket type.
        return True

    @staticmethod
    def _safe_parse(payload: str) -> Dict[str, Any]:
        try:
            data = json.loads(payload)
            return data if isinstance(data, dict) else {}
        except Exception:
            return {}

    @classmethod
    def _compute_active_outputs(
        cls,
        model_type: str,
        expected_output_family: str,
    ) -> Tuple[List[str], bool, str]:
        allowed = cls.OUTPUTS_BY_MODEL_TYPE.get(model_type, [])
        allowed_families = cls.TARGETS_BY_MODEL_TYPE.get(model_type, [])

        if expected_output_family == "AUTO":
            return allowed, bool(allowed), "AUTO mode"

        is_compatible = expected_output_family in allowed_families
        if not is_compatible:
            return [], False, (
                f"Model type '{model_type}' is not compatible with requested "
                f"output family '{expected_output_family}'. Allowed: "
                f"{', '.join(allowed_families) if allowed_families else 'None'}"
            )

        # Keep only the outputs that map to the requested family.
        family_to_outputs = {
            "MODEL": ["model"],
            "CLIP": ["clip"],
            "VAE": ["vae"],
        }
        requested_outputs = family_to_outputs.get(expected_output_family, [])
        active = [name for name in allowed if name in requested_outputs]
        return active, True, f"Compatible with requested family '{expected_output_family}'"

    @staticmethod
    def _status_text(
        model_type: str,
        architecture: str,
        full_path: str,
        active_outputs: List[str],
        compatible: bool,
        reason: str,
    ) -> str:
        return "\n".join(
            [
                f"Model type: {model_type}",
                f"Architecture: {architecture}",
                f"Compatible: {compatible}",
                f"Active outputs: {', '.join(active_outputs) if active_outputs else 'None'}",
                f"Reason: {reason}",
                f"Path: {full_path}",
            ]
        )

    def adapt(
        self,
        zmongo_model_json: str,
        expected_output_family: str = "AUTO",
        unique_id: str | None = None,
    ):
        del unique_id

        payload = self._safe_parse(zmongo_model_json)
        model_type = str(payload.get("model_type", "unknown"))
        architecture = str(payload.get("architecture_family", "unknown"))
        full_path = str(payload.get("full_path", ""))

        active_outputs, is_compatible, reason = self._compute_active_outputs(
            model_type=model_type,
            expected_output_family=expected_output_family,
        )

        # Deliberately conservative:
        # We do not fabricate real Comfy MODEL/CLIP/VAE objects here.
        # Those should be created by specialized loader/adapter nodes.
        model_obj = None
        clip_obj = None
        vae_obj = None

        status_text = self._status_text(
            model_type=model_type,
            architecture=architecture,
            full_path=full_path,
            active_outputs=active_outputs,
            compatible=is_compatible,
            reason=reason,
        )

        active_outputs_json = json.dumps(
            {
                "active_outputs": active_outputs,
                "expected_output_family": expected_output_family,
                "model_type": model_type,
                "architecture_family": architecture,
                "compatible": is_compatible,
            },
            indent=2,
        )

        return (
            model_obj if "model" in active_outputs else None,
            clip_obj if "clip" in active_outputs else None,
            vae_obj if "vae" in active_outputs else None,
            status_text,
            active_outputs_json,
            is_compatible,
            model_type,
        )