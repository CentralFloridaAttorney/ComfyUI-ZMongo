from __future__ import annotations

import json
from pathlib import Path
from typing import Dict, List, Tuple


class ZMongoModelIntrospectorNode:
    CATEGORY = "ZMongo/Models"
    FUNCTION = "inspect_model"
    RETURN_TYPES = (
        "STRING",  # model_info_json
        "STRING",  # summary_text
        "STRING",  # model_type
        "STRING",  # architecture_family
        "STRING",  # can_connect_to
        "STRING",  # can_receive_from
        "STRING",  # full_path
    )
    RETURN_NAMES = (
        "model_info_json",
        "summary_text",
        "model_type",
        "architecture_family",
        "can_connect_to",
        "can_receive_from",
        "full_path",
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
    }

    COMPATIBILITY_MAP: Dict[str, Dict[str, List[str]]] = {
        "llm": {
            "can_connect_to": [
                "ZMongoLLMLoaderNode",
                "ZMongoPromptRunnerNode",
                "ZMongoChatModelNode",
            ],
            "can_receive_from": [
                "ZMongoUniversalModelSelectorNode",
                "ZMongoModelTypeRouterNode",
            ],
        },
        "embedding": {
            "can_connect_to": [
                "ZMongoEmbeddingLoaderNode",
                "ZMongoVectorIndexNode",
                "ZMongoEmbeddingInferenceNode",
            ],
            "can_receive_from": [
                "ZMongoUniversalModelSelectorNode",
                "ZMongoModelTypeRouterNode",
            ],
        },
        "clip": {
            "can_connect_to": [
                "ZMongoCLIPLoaderNode",
                "ZMongoVisionInferenceNode",
            ],
            "can_receive_from": [
                "ZMongoUniversalModelSelectorNode",
                "ZMongoModelTypeRouterNode",
            ],
        },
        "clip_vision": {
            "can_connect_to": [
                "ZMongoCLIPVisionLoaderNode",
                "ZMongoVisionInferenceNode",
            ],
            "can_receive_from": [
                "ZMongoUniversalModelSelectorNode",
                "ZMongoModelTypeRouterNode",
            ],
        },
        "onnx_model": {
            "can_connect_to": [
                "ZMongoONNXLoaderNode",
                "ZMongoVisionInferenceNode",
                "ZMongoEmbeddingInferenceNode",
            ],
            "can_receive_from": [
                "ZMongoUniversalModelSelectorNode",
                "ZMongoModelTypeRouterNode",
            ],
        },
        "checkpoint": {
            "can_connect_to": [
                "ZMongoCheckpointLoaderNode",
                "ZMongoTorchInferenceNode",
            ],
            "can_receive_from": [
                "ZMongoUniversalModelSelectorNode",
                "ZMongoModelTypeRouterNode",
            ],
        },
        "torch_model": {
            "can_connect_to": [
                "ZMongoTorchLoaderNode",
                "ZMongoTorchInferenceNode",
            ],
            "can_receive_from": [
                "ZMongoUniversalModelSelectorNode",
                "ZMongoModelTypeRouterNode",
            ],
        },
        "text_encoder": {
            "can_connect_to": [
                "ZMongoTextEncoderLoaderNode",
                "ZMongoEmbeddingInferenceNode",
            ],
            "can_receive_from": [
                "ZMongoUniversalModelSelectorNode",
                "ZMongoModelTypeRouterNode",
            ],
        },
        "vae": {
            "can_connect_to": [
                "ZMongoVAELoaderNode",
            ],
            "can_receive_from": [
                "ZMongoUniversalModelSelectorNode",
                "ZMongoModelTypeRouterNode",
            ],
        },
        "diffusion": {
            "can_connect_to": [
                "ZMongoDiffusionLoaderNode",
            ],
            "can_receive_from": [
                "ZMongoUniversalModelSelectorNode",
                "ZMongoModelTypeRouterNode",
            ],
        },
        "controlnet": {
            "can_connect_to": [
                "ZMongoControlNetLoaderNode",
            ],
            "can_receive_from": [
                "ZMongoUniversalModelSelectorNode",
                "ZMongoModelTypeRouterNode",
            ],
        },
        "upscaler": {
            "can_connect_to": [
                "ZMongoUpscaleLoaderNode",
            ],
            "can_receive_from": [
                "ZMongoUniversalModelSelectorNode",
                "ZMongoModelTypeRouterNode",
            ],
        },
        "latent_upscaler": {
            "can_connect_to": [
                "ZMongoLatentUpscaleLoaderNode",
            ],
            "can_receive_from": [
                "ZMongoUniversalModelSelectorNode",
                "ZMongoModelTypeRouterNode",
            ],
        },
        "audio_model": {
            "can_connect_to": [
                "ZMongoAudioLoaderNode",
                "ZMongoAudioInferenceNode",
            ],
            "can_receive_from": [
                "ZMongoUniversalModelSelectorNode",
                "ZMongoModelTypeRouterNode",
            ],
        },
        "speech_model": {
            "can_connect_to": [
                "ZMongoSpeechLoaderNode",
                "ZMongoSpeechInferenceNode",
            ],
            "can_receive_from": [
                "ZMongoUniversalModelSelectorNode",
                "ZMongoModelTypeRouterNode",
            ],
        },
        "unknown": {
            "can_connect_to": [],
            "can_receive_from": [
                "ZMongoUniversalModelSelectorNode",
                "ZMongoModelTypeRouterNode",
            ],
        },
    }

    @classmethod
    def INPUT_TYPES(cls):
        return {
            "required": {
                "full_path": ("STRING", {"forceInput": True}),
            }
        }

    @classmethod
    def IS_CHANGED(cls, full_path: str):
        return full_path

    @staticmethod
    def _normalize_path(full_path: str) -> Path | None:
        if not full_path:
            return None
        try:
            return Path(full_path).expanduser().resolve()
        except Exception:
            return None

    @classmethod
    def _folder_hint(cls, path: Path) -> Tuple[str, str] | None:
        for part in reversed([p.lower() for p in path.parts]):
            if part in cls.FOLDER_TYPE_HINTS:
                return cls.FOLDER_TYPE_HINTS[part]
        return None

    @classmethod
    def _extension_hint(cls, path: Path) -> Tuple[str, str] | None:
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
    def _format_list(items: List[str]) -> str:
        return ", ".join(items) if items else "None"

    def inspect_model(self, full_path: str):
        path = self._normalize_path(full_path)
        if path is None or not path.exists() or not path.is_file():
            model_info = {
                "full_path": "",
                "model_type": "unknown",
                "loader_hint": "invalid_path",
                "architecture_family": "unknown",
                "filename": "",
                "extension": "",
                "parent_folder": "",
                "sidecar_files": [],
                "can_connect_to": [],
                "can_receive_from": [],
            }
            summary = "Invalid or missing model path."
            return (
                json.dumps(model_info, indent=2),
                summary,
                "unknown",
                "unknown",
                "",
                "",
                "",
            )

        model_type, loader_hint = self._classify(path)
        architecture_family = self._architecture_family(path)
        compatibility = self.COMPATIBILITY_MAP.get(model_type, self.COMPATIBILITY_MAP["unknown"])
        can_connect_to = compatibility["can_connect_to"]
        can_receive_from = compatibility["can_receive_from"]
        sidecar_files = self._find_sidecar_files(path)

        model_info = {
            "full_path": str(path),
            "model_type": model_type,
            "loader_hint": loader_hint,
            "architecture_family": architecture_family,
            "filename": path.name,
            "stem": path.stem,
            "extension": path.suffix.lower(),
            "parent_folder": path.parent.name,
            "sidecar_files": sidecar_files,
            "can_connect_to": can_connect_to,
            "can_receive_from": can_receive_from,
        }

        summary_lines = [
            f"Model: {path.name}",
            f"Type: {model_type}",
            f"Architecture: {architecture_family}",
            f"Loader hint: {loader_hint}",
            f"Extension: {path.suffix.lower()}",
            f"Folder: {path.parent.name}",
            f"Can connect to: {self._format_list(can_connect_to)}",
            f"Can receive from: {self._format_list(can_receive_from)}",
            f"Sidecar files: {self._format_list(sidecar_files)}",
        ]
        summary_text = "\n".join(summary_lines)

        return (
            json.dumps(model_info, indent=2),
            summary_text,
            model_type,
            architecture_family,
            self._format_list(can_connect_to),
            self._format_list(can_receive_from),
            str(path),
        )