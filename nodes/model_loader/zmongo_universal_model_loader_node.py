from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple


class ZMongoUniversalModelLoaderNode:
    """
    Universal model loader facade for ZMongo.

    This node does not try to fully instantiate every possible model backend.
    Instead, it:

    1. Accepts a selected model path.
    2. Classifies the model.
    3. Returns a universal internal payload (JSON string).
    4. Activates only the outputs that make sense for the selected model.
    5. Exposes status / compatibility information for downstream nodes.

    Important:
    - Comfy output socket types are static.
    - So this node declares a fixed set of outputs and populates only the valid ones.
    - Core Comfy outputs such as MODEL / CLIP / VAE are only populated when the
      selected file appears compatible with those families.
    """

    CATEGORY = "ZMongo/Models"
    FUNCTION = "load_model"

    RETURN_TYPES = (
        "STRING",   # zmongo_model_json
        "MODEL",    # model
        "CLIP",     # clip
        "VAE",      # vae
        "STRING",   # model_info_json
        "STRING",   # status_text
        "BOOLEAN",  # is_valid
        "STRING",   # model_type
        "STRING",   # allowed_outputs
    )
    RETURN_NAMES = (
        "zmongo_model_json",
        "model",
        "clip",
        "vae",
        "model_info_json",
        "status_text",
        "is_valid",
        "model_type",
        "allowed_outputs",
    )

    FOLDER_TYPE_HINTS: Dict[str, Tuple[str, str]] = {
        "llm": ("llm", "llama_cpp"),
        "embeddings": ("embedding", "embedding_loader"),
        "encoders": ("embedding", "embedding_loader"),
        "clip": ("clip", "clip_loader"),
        "clip_vision": ("clip_vision", "clip_vision_loader"),
        "vae": ("vae", "vae_loader"),
        "unet": ("unet", "torch_loader"),
        "checkpoints": ("checkpoint", "checkpoint_loader"),
        "diffusion_models": ("diffusion", "diffusion_loader"),
        "loras": ("lora", "lora_loader"),
        "controlnet": ("controlnet", "controlnet_loader"),
        "upscale_models": ("upscaler", "upscale_loader"),
        "latent_upscale_models": ("latent_upscaler", "latent_upscale_loader"),
        "onnx": ("onnx_model", "onnxruntime"),
        "vision": ("vision", "vision_loader"),
        "audio": ("audio_model", "audio_loader"),
        "speech_models": ("speech_model", "audio_loader"),
        "text_encoders": ("text_encoder", "text_encoder_loader"),
    }

    EXTENSION_HINTS: Dict[str, Tuple[str, str]] = {
        ".gguf": ("llm", "llama_cpp"),
        ".onnx": ("onnx_model", "onnxruntime"),
        ".safetensors": ("torch_model", "torch_loader"),
        ".ckpt": ("checkpoint", "checkpoint_loader"),
        ".pt": ("torch_model", "torch_loader"),
        ".pth": ("torch_model", "torch_loader"),
        ".bin": ("binary_model", "custom_loader"),
        ".json": ("config", "config_parser"),
        ".yaml": ("config", "config_parser"),
        ".yml": ("config", "config_parser"),
        ".model": ("generic_model", "custom_loader"),
        ".index": ("index", "index_loader"),
    }

    ARCHITECTURE_HINTS = {
        "bge": "bge",
        "e5": "e5",
        "gte": "gte",
        "clip": "clip",
        "siglip": "siglip",
        "llama": "llama",
        "mistral": "mistral",
        "qwen": "qwen",
        "gemma": "gemma",
        "bert": "bert",
        "t5": "t5",
        "whisper": "whisper",
        "sd": "stable_diffusion",
        "flux": "flux",
    }

    MODEL_OUTPUT_COMPATIBILITY: Dict[str, List[str]] = {
        "llm": ["zmongo_model_json"],
        "embedding": ["zmongo_model_json"],
        "text_encoder": ["zmongo_model_json"],
        "onnx_model": ["zmongo_model_json"],
        "audio_model": ["zmongo_model_json"],
        "speech_model": ["zmongo_model_json"],
        "clip": ["zmongo_model_json", "clip"],
        "clip_vision": ["zmongo_model_json", "clip"],
        "vae": ["zmongo_model_json", "vae"],
        "checkpoint": ["zmongo_model_json", "model", "clip", "vae"],
        "torch_model": ["zmongo_model_json", "model"],
        "diffusion": ["zmongo_model_json", "model"],
        "controlnet": ["zmongo_model_json", "model"],
        "upscaler": ["zmongo_model_json", "model"],
        "latent_upscaler": ["zmongo_model_json", "model"],
        "unet": ["zmongo_model_json", "model"],
    }

    @classmethod
    def INPUT_TYPES(cls):
        return {
            "required": {
                "full_path": ("STRING", {"forceInput": True}),
            },
            "optional": {
                "model_info_json": ("STRING", {"forceInput": True}),
            },
        }

    @classmethod
    def IS_CHANGED(cls, full_path: str, model_info_json: str = ""):
        return full_path, model_info_json

    @staticmethod
    def _normalize_path(full_path: str) -> Optional[Path]:
        if not full_path:
            return None
        try:
            return Path(full_path).expanduser().resolve()
        except Exception:
            return None

    @classmethod
    def _folder_hint(cls, path: Path) -> Optional[Tuple[str, str]]:
        for part in reversed([p.lower() for p in path.parts]):
            if part in cls.FOLDER_TYPE_HINTS:
                return cls.FOLDER_TYPE_HINTS[part]
        return None

    @classmethod
    def _extension_hint(cls, path: Path) -> Optional[Tuple[str, str]]:
        return cls.EXTENSION_HINTS.get(path.suffix.lower())

    @classmethod
    def _architecture_family(cls, path: Path) -> str:
        haystack = " ".join([part.lower() for part in path.parts])
        for token, family in cls.ARCHITECTURE_HINTS.items():
            if token in haystack:
                return family
        return "unknown"

    @classmethod
    def _classify(cls, path: Path) -> Tuple[str, str]:
        ext = path.suffix.lower()
        folder_hint = cls._folder_hint(path)
        ext_hint = cls._extension_hint(path)

        if ext == ".gguf":
            return "llm", "llama_cpp"
        if ext == ".onnx":
            return "onnx_model", "onnxruntime"
        if ext in {".json", ".yaml", ".yml"}:
            if folder_hint:
                return f"config_for_{folder_hint[0]}", folder_hint[1]
            return "config", "config_parser"
        if ext in {".safetensors", ".pt", ".pth", ".ckpt", ".bin", ".model"}:
            if folder_hint:
                return folder_hint
            if ext_hint:
                return ext_hint
        if folder_hint:
            return folder_hint
        if ext_hint:
            return ext_hint
        return "unknown", "manual_loader_required"

    @staticmethod
    def _find_sidecar_files(path: Path) -> List[str]:
        candidates = [
            "config.json",
            "tokenizer.json",
            "tokenizer_config.json",
            "model_index.json",
            "params.json",
            "metadata.json",
            "config.yaml",
            "config.yml",
        ]
        found: List[str] = []
        for candidate in candidates:
            candidate_path = path.parent / candidate
            if candidate_path.exists() and candidate_path.is_file():
                found.append(str(candidate_path))
        return found

    @staticmethod
    def _try_parse_model_info(model_info_json: str) -> Dict[str, Any]:
        if not model_info_json:
            return {}
        try:
            parsed = json.loads(model_info_json)
            return parsed if isinstance(parsed, dict) else {}
        except Exception:
            return {}

    @classmethod
    def _build_model_info(cls, path: Path) -> Dict[str, Any]:
        model_type, loader_hint = cls._classify(path)
        architecture_family = cls._architecture_family(path)
        allowed_outputs = cls.MODEL_OUTPUT_COMPATIBILITY.get(model_type, ["zmongo_model_json"])

        return {
            "full_path": str(path),
            "filename": path.name,
            "stem": path.stem,
            "extension": path.suffix.lower(),
            "parent_folder": path.parent.name,
            "model_type": model_type,
            "loader_hint": loader_hint,
            "architecture_family": architecture_family,
            "sidecar_files": cls._find_sidecar_files(path),
            "allowed_outputs": allowed_outputs,
            "exists": path.exists(),
            "is_file": path.is_file(),
        }

    @staticmethod
    def _make_status(model_info: Dict[str, Any]) -> str:
        allowed_outputs = model_info.get("allowed_outputs", [])
        return "\n".join(
            [
                f"Loaded: {model_info.get('filename', '')}",
                f"Type: {model_info.get('model_type', 'unknown')}",
                f"Architecture: {model_info.get('architecture_family', 'unknown')}",
                f"Loader hint: {model_info.get('loader_hint', 'unknown')}",
                f"Allowed outputs: {', '.join(allowed_outputs) if allowed_outputs else 'None'}",
                f"Path: {model_info.get('full_path', '')}",
            ]
        )

    @staticmethod
    def _empty_core_outputs() -> Tuple[Any, Any, Any]:
        return None, None, None

    def _build_zmongo_model_payload(self, model_info: Dict[str, Any]) -> str:
        payload = {
            "kind": "zmongo_model",
            "full_path": model_info.get("full_path", ""),
            "model_type": model_info.get("model_type", "unknown"),
            "architecture_family": model_info.get("architecture_family", "unknown"),
            "loader_hint": model_info.get("loader_hint", "unknown"),
            "allowed_outputs": model_info.get("allowed_outputs", []),
            "sidecar_files": model_info.get("sidecar_files", []),
        }
        return json.dumps(payload, indent=2)

    def _activate_core_outputs(
        self,
        model_info: Dict[str, Any],
    ) -> Tuple[Any, Any, Any]:
        """
        Returns placeholders for MODEL / CLIP / VAE-compatible cases.

        Note:
        This node intentionally does not instantiate arbitrary backend objects
        for every model family. For now:
        - checkpoint-like families can mark MODEL/CLIP/VAE as logically enabled
        - actual object creation should be delegated to adapter/loader nodes later
        """
        model_type = model_info.get("model_type", "unknown")
        allowed_outputs = set(model_info.get("allowed_outputs", []))

        model_obj = None
        clip_obj = None
        vae_obj = None

        # Deliberately conservative:
        # We only advertise activation through metadata/status here.
        # Returning real core objects should be done in a dedicated adapter node.
        if model_type == "checkpoint":
            model_obj = None
            clip_obj = None
            vae_obj = None
        elif "model" in allowed_outputs:
            model_obj = None
        elif "clip" in allowed_outputs:
            clip_obj = None
        elif "vae" in allowed_outputs:
            vae_obj = None

        return model_obj, clip_obj, vae_obj

    def load_model(self, full_path: str, model_info_json: str = ""):
        parsed_path = self._normalize_path(full_path)
        if parsed_path is None or not parsed_path.exists() or not parsed_path.is_file():
            empty_payload = json.dumps(
                {
                    "kind": "zmongo_model",
                    "full_path": "",
                    "model_type": "unknown",
                    "architecture_family": "unknown",
                    "loader_hint": "invalid_path",
                    "allowed_outputs": [],
                    "sidecar_files": [],
                },
                indent=2,
            )
            empty_info = json.dumps(
                {
                    "full_path": "",
                    "model_type": "unknown",
                    "loader_hint": "invalid_path",
                    "architecture_family": "unknown",
                    "allowed_outputs": [],
                },
                indent=2,
            )
            return (
                empty_payload,
                None,
                None,
                None,
                empty_info,
                "Invalid or missing model path.",
                False,
                "unknown",
                "",
            )

        provided_info = self._try_parse_model_info(model_info_json)
        if provided_info and provided_info.get("full_path") == str(parsed_path):
            model_info = provided_info
            model_info.setdefault("allowed_outputs", self.MODEL_OUTPUT_COMPATIBILITY.get(
                model_info.get("model_type", "unknown"),
                ["zmongo_model_json"],
            ))
            model_info.setdefault("architecture_family", self._architecture_family(parsed_path))
            model_info.setdefault("loader_hint", self._classify(parsed_path)[1])
            model_info.setdefault("sidecar_files", self._find_sidecar_files(parsed_path))
        else:
            model_info = self._build_model_info(parsed_path)

        zmongo_model_json = self._build_zmongo_model_payload(model_info)
        model_obj, clip_obj, vae_obj = self._activate_core_outputs(model_info)

        status_text = self._make_status(model_info)
        is_valid = model_info.get("model_type", "unknown") != "unknown"
        model_type = str(model_info.get("model_type", "unknown"))
        allowed_outputs = ", ".join(model_info.get("allowed_outputs", []))

        return (
            zmongo_model_json,
            model_obj,
            clip_obj,
            vae_obj,
            json.dumps(model_info, indent=2),
            status_text,
            is_valid,
            model_type,
            allowed_outputs,
        )