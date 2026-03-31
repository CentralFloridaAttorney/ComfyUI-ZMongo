from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Callable, Dict, Optional, Tuple

import folder_paths
from nodes import (
    CLIPLoader,
    CLIPVisionLoader,
    CheckpointLoaderSimple,
    ControlNetLoader,
    STYLE_MODEL,
    StyleModelLoader,
    UNETLoader,
    VAELoader,
    UpscaleModelLoader,
)


class ZMongoBuiltInLoaderAdapterNode:
    """
    Universal adapter that reuses ComfyUI built-in loaders wherever possible.

    This node accepts a selected model path (or model_info_json from the
    introspector / universal loader), determines the compatible built-in loader,
    converts the selected file into the name expected by that built-in loader,
    and returns real native Comfy outputs.

    Supported families:
      - checkpoint      -> CheckpointLoaderSimple
      - vae             -> VAELoader
      - clip            -> CLIPLoader
      - text_encoder    -> CLIPLoader
      - unet            -> UNETLoader
      - diffusion       -> UNETLoader
      - controlnet      -> ControlNetLoader
      - clip_vision     -> CLIPVisionLoader
      - upscaler        -> UpscaleModelLoader
      - style_model     -> StyleModelLoader

    Unsupported families intentionally return only status metadata:
      - llm
      - embedding
      - onnx_model
      - audio_model
      - speech_model
      - generic unknowns
    """

    CATEGORY = "ZMongo/Models"
    FUNCTION = "adapt"

    RETURN_TYPES = (
        "MODEL",         # model
        "CLIP",          # clip
        "VAE",           # vae
        "MODEL",         # unet_model
        "CONTROL_NET",   # control_net
        "CLIP_VISION",   # clip_vision
        "UPSCALE_MODEL", # upscale_model
        "STYLE_MODEL",   # style_model
        "STRING",        # status_text
        "BOOLEAN",       # is_compatible
        "STRING",        # model_type
        "STRING",        # built_in_loader_name
    )
    RETURN_NAMES = (
        "model",
        "clip",
        "vae",
        "unet_model",
        "control_net",
        "clip_vision",
        "upscale_model",
        "style_model",
        "status_text",
        "is_compatible",
        "model_type",
        "built_in_loader_name",
    )

    MODEL_TYPE_TO_FOLDER = {
        "checkpoint": "checkpoints",
        "vae": "vae",
        "clip": "clip",
        "text_encoder": "text_encoders",
        "unet": "diffusion_models",
        "diffusion": "diffusion_models",
        "controlnet": "controlnet",
        "clip_vision": "clip_vision",
        "upscaler": "upscale_models",
        "style_model": "style_models",
    }

    BUILTIN_BY_MODEL_TYPE = {
        "checkpoint": "CheckpointLoaderSimple",
        "vae": "VAELoader",
        "clip": "CLIPLoader",
        "text_encoder": "CLIPLoader",
        "unet": "UNETLoader",
        "diffusion": "UNETLoader",
        "controlnet": "ControlNetLoader",
        "clip_vision": "CLIPVisionLoader",
        "upscaler": "UpscaleModelLoader",
        "style_model": "StyleModelLoader",
    }

    SUPPORTED_MODEL_TYPES = set(BUILTIN_BY_MODEL_TYPE.keys())

    @classmethod
    def INPUT_TYPES(cls):
        return {
            "required": {
                "full_path": ("STRING", {"forceInput": True}),
            },
            "optional": {
                "model_info_json": ("STRING", {"forceInput": True}),
                "clip_type": (
                    [
                        "stable_diffusion",
                        "stable_cascade",
                        "sd3",
                        "stable_audio",
                        "mochi",
                        "ltxv",
                        "pixart",
                        "cosmos",
                        "lumina2",
                        "wan",
                        "hidream",
                    ],
                    {"default": "stable_diffusion"},
                ),
            },
        }

    @classmethod
    def IS_CHANGED(
        cls,
        full_path: str,
        model_info_json: str = "",
        clip_type: str = "stable_diffusion",
    ):
        return full_path, model_info_json, clip_type

    @staticmethod
    def _none_outputs() -> Tuple[Any, Any, Any, Any, Any, Any, Any]:
        return None, None, None, None, None, None, None

    @staticmethod
    def _safe_parse_model_info(model_info_json: str) -> Dict[str, Any]:
        if not model_info_json:
            return {}
        try:
            parsed = json.loads(model_info_json)
            return parsed if isinstance(parsed, dict) else {}
        except Exception:
            return {}

    @staticmethod
    def _normalize_path(full_path: str) -> Optional[Path]:
        if not full_path:
            return None
        try:
            return Path(full_path).expanduser().resolve()
        except Exception:
            return None

    @staticmethod
    def _infer_model_type_from_path(path: Path) -> str:
        parts = [part.lower() for part in path.parts]

        if "checkpoints" in parts:
            return "checkpoint"
        if "vae" in parts:
            return "vae"
        if "clip_vision" in parts:
            return "clip_vision"
        if "clip" in parts:
            return "clip"
        if "text_encoders" in parts:
            return "text_encoder"
        if "controlnet" in parts:
            return "controlnet"
        if "upscale_models" in parts:
            return "upscaler"
        if "style_models" in parts:
            return "style_model"
        if "diffusion_models" in parts:
            return "diffusion"
        if "unet" in parts:
            return "unet"

        ext = path.suffix.lower()
        if ext == ".ckpt":
            return "checkpoint"
        if ext in {".pt", ".pth", ".bin", ".safetensors"}:
            return "unknown"

        return "unknown"

    @classmethod
    def _resolve_model_type(cls, full_path: Path, model_info: Dict[str, Any]) -> str:
        model_type = str(model_info.get("model_type", "")).strip()
        if model_type:
            return model_type
        return cls._infer_model_type_from_path(full_path)

    @classmethod
    def _relative_name_for_folder_type(cls, full_path: Path, folder_type: str) -> str:
        """
        Convert an absolute path to the name/relative path expected by ComfyUI's
        built-in loaders for the given folder type.
        """
        normalized = full_path.resolve()
        available = folder_paths.get_filename_list(folder_type)

        for entry in available:
            candidate = Path(folder_paths.get_full_path(folder_type, entry)).resolve()
            if candidate == normalized:
                return entry.replace("\\", "/")

        raise ValueError(
            f"Selected file is not registered under Comfy models folder type "
            f"'{folder_type}': {full_path}"
        )

    @staticmethod
    def _call_first_available(instance: Any, methods_with_kwargs):
        """
        Try a sequence of possible built-in method names/signatures until one works.
        This keeps the adapter resilient across small ComfyUI API changes.
        """
        last_error = None

        for method_name, kwargs in methods_with_kwargs:
            method = getattr(instance, method_name, None)
            if method is None:
                continue
            try:
                return method(**kwargs)
            except TypeError as exc:
                last_error = exc
                continue

        if last_error is not None:
            raise last_error

        raise AttributeError(
            f"No compatible loader method found on {instance.__class__.__name__}"
        )

    def _load_checkpoint(self, relative_name: str):
        loader = CheckpointLoaderSimple()
        result = self._call_first_available(
            loader,
            [
                ("load_checkpoint", {"ckpt_name": relative_name}),
            ],
        )
        if not isinstance(result, tuple) or len(result) < 3:
            raise ValueError("CheckpointLoaderSimple returned an unexpected result.")
        return result[0], result[1], result[2]

    def _load_vae(self, relative_name: str):
        loader = VAELoader()
        result = self._call_first_available(
            loader,
            [
                ("load_vae", {"vae_name": relative_name}),
            ],
        )
        if not isinstance(result, tuple) or len(result) < 1:
            raise ValueError("VAELoader returned an unexpected result.")
        return result[0]

    def _load_clip(self, relative_name: str, clip_type: str):
        loader = CLIPLoader()
        result = self._call_first_available(
            loader,
            [
                ("load_clip", {"clip_name": relative_name, "type": clip_type}),
                ("load_clip", {"clip_name": relative_name}),
            ],
        )
        if not isinstance(result, tuple) or len(result) < 1:
            raise ValueError("CLIPLoader returned an unexpected result.")
        return result[0]

    def _load_unet(self, relative_name: str):
        loader = UNETLoader()
        result = self._call_first_available(
            loader,
            [
                ("load_unet", {"unet_name": relative_name}),
                ("load_model", {"unet_name": relative_name}),
            ],
        )
        if not isinstance(result, tuple) or len(result) < 1:
            raise ValueError("UNETLoader returned an unexpected result.")
        return result[0]

    def _load_controlnet(self, relative_name: str):
        loader = ControlNetLoader()
        result = self._call_first_available(
            loader,
            [
                ("load_controlnet", {"control_net_name": relative_name}),
                ("load_controlnet", {"controlnet_name": relative_name}),
            ],
        )
        if not isinstance(result, tuple) or len(result) < 1:
            raise ValueError("ControlNetLoader returned an unexpected result.")
        return result[0]

    def _load_clip_vision(self, relative_name: str):
        loader = CLIPVisionLoader()
        result = self._call_first_available(
            loader,
            [
                ("load_clip", {"clip_name": relative_name}),
                ("load_clip_vision", {"clip_name": relative_name}),
                ("load_clip_vision", {"clip_vision_name": relative_name}),
            ],
        )
        if not isinstance(result, tuple) or len(result) < 1:
            raise ValueError("CLIPVisionLoader returned an unexpected result.")
        return result[0]

    def _load_upscale_model(self, relative_name: str):
        loader = UpscaleModelLoader()
        result = self._call_first_available(
            loader,
            [
                ("load_model", {"model_name": relative_name}),
                ("load_model", {"upscale_model": relative_name}),
            ],
        )
        if not isinstance(result, tuple) or len(result) < 1:
            raise ValueError("UpscaleModelLoader returned an unexpected result.")
        return result[0]

    def _load_style_model(self, relative_name: str):
        loader = StyleModelLoader()
        result = self._call_first_available(
            loader,
            [
                ("load_style_model", {"style_model_name": relative_name}),
                ("load_model", {"style_model_name": relative_name}),
                ("load_model", {"model_name": relative_name}),
            ],
        )
        if not isinstance(result, tuple) or len(result) < 1:
            raise ValueError("StyleModelLoader returned an unexpected result.")
        return result[0]

    def adapt(
        self,
        full_path: str,
        model_info_json: str = "",
        clip_type: str = "stable_diffusion",
    ):
        model_obj, clip_obj, vae_obj, unet_obj, controlnet_obj, clipvision_obj, upscale_obj = self._none_outputs()
        style_obj = None

        path = self._normalize_path(full_path)
        if path is None or not path.exists() or not path.is_file():
            return (
                model_obj,
                clip_obj,
                vae_obj,
                unet_obj,
                controlnet_obj,
                clipvision_obj,
                upscale_obj,
                style_obj,
                "Invalid or missing model path.",
                False,
                "unknown",
                "",
            )

        model_info = self._safe_parse_model_info(model_info_json)
        model_type = self._resolve_model_type(path, model_info)
        built_in_loader_name = self.BUILTIN_BY_MODEL_TYPE.get(model_type, "")

        if model_type not in self.SUPPORTED_MODEL_TYPES:
            return (
                model_obj,
                clip_obj,
                vae_obj,
                unet_obj,
                controlnet_obj,
                clipvision_obj,
                upscale_obj,
                style_obj,
                (
                    f"No compatible built-in Comfy loader for model type "
                    f"'{model_type}'. Path: {path}"
                ),
                False,
                model_type,
                built_in_loader_name,
            )

        folder_type = self.MODEL_TYPE_TO_FOLDER[model_type]

        try:
            relative_name = self._relative_name_for_folder_type(path, folder_type)

            if model_type == "checkpoint":
                model_obj, clip_obj, vae_obj = self._load_checkpoint(relative_name)

            elif model_type == "vae":
                vae_obj = self._load_vae(relative_name)

            elif model_type in {"clip", "text_encoder"}:
                clip_obj = self._load_clip(relative_name, clip_type)

            elif model_type in {"unet", "diffusion"}:
                unet_obj = self._load_unet(relative_name)

            elif model_type == "controlnet":
                controlnet_obj = self._load_controlnet(relative_name)

            elif model_type == "clip_vision":
                clipvision_obj = self._load_clip_vision(relative_name)

            elif model_type == "upscaler":
                upscale_obj = self._load_upscale_model(relative_name)

            elif model_type == "style_model":
                style_obj = self._load_style_model(relative_name)

            status_text = (
                f"Loaded with built-in loader '{built_in_loader_name}'.\n"
                f"Model type: {model_type}\n"
                f"Folder type: {folder_type}\n"
                f"Relative name: {relative_name}\n"
                f"Path: {path}"
            )

            return (
                model_obj,
                clip_obj,
                vae_obj,
                unet_obj,
                controlnet_obj,
                clipvision_obj,
                upscale_obj,
                style_obj,
                status_text,
                True,
                model_type,
                built_in_loader_name,
            )

        except Exception as exc:
            return (
                model_obj,
                clip_obj,
                vae_obj,
                unet_obj,
                controlnet_obj,
                clipvision_obj,
                upscale_obj,
                style_obj,
                (
                    f"Built-in loader adapter failed.\n"
                    f"Model type: {model_type}\n"
                    f"Built-in loader: {built_in_loader_name}\n"
                    f"Path: {path}\n"
                    f"Error: {exc}"
                ),
                False,
                model_type,
                built_in_loader_name,
            )