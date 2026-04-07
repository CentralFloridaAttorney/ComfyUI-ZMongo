from __future__ import annotations

import json
from typing import Dict, List, Tuple


class ZMongoModelCompatibilityDisplayNode:
    CATEGORY = "ZMongo/Models"
    FUNCTION = "describe_compatibility"
    RETURN_TYPES = (
        "BOOLEAN",  # is_compatible
        "STRING",   # status_text
        "STRING",   # compatible_targets
        "STRING",   # compatible_sources
        "STRING",   # selected_model_type
        "STRING",   # selected_architecture
    )
    RETURN_NAMES = (
        "is_compatible",
        "status_text",
        "compatible_targets",
        "compatible_sources",
        "selected_model_type",
        "selected_architecture",
    )

    EXPECTED_NODE_TO_MODEL_TYPES: Dict[str, List[str]] = {
        "ZMongoLLMLoaderNode": ["llm"],
        "ZMongoPromptRunnerNode": ["llm"],
        "ZMongoChatModelNode": ["llm"],

        "ZMongoEmbeddingLoaderNode": ["embedding", "text_encoder", "onnx_model"],
        "ZMongoVectorIndexNode": ["embedding"],
        "ZMongoEmbeddingInferenceNode": ["embedding", "text_encoder", "onnx_model"],

        "ZMongoCLIPLoaderNode": ["clip", "onnx_model"],
        "ZMongoCLIPVisionLoaderNode": ["clip_vision", "onnx_model"],
        "ZMongoVisionInferenceNode": ["clip", "clip_vision", "onnx_model", "vision"],

        "ZMongoCheckpointLoaderNode": ["checkpoint", "torch_model"],
        "ZMongoTorchLoaderNode": ["torch_model", "checkpoint"],
        "ZMongoTorchInferenceNode": ["torch_model", "checkpoint", "diffusion"],

        "ZMongoVAELoaderNode": ["vae"],
        "ZMongoDiffusionLoaderNode": ["diffusion", "checkpoint"],
        "ZMongoControlNetLoaderNode": ["controlnet"],
        "ZMongoUpscaleLoaderNode": ["upscaler"],
        "ZMongoLatentUpscaleLoaderNode": ["latent_upscaler"],

        "ZMongoAudioLoaderNode": ["audio_model"],
        "ZMongoAudioInferenceNode": ["audio_model"],
        "ZMongoSpeechLoaderNode": ["speech_model"],
        "ZMongoSpeechInferenceNode": ["speech_model"],
    }

    @classmethod
    def INPUT_TYPES(cls):
        return {
            "required": {
                "model_info_json": ("STRING", {"forceInput": True}),
                "expected_target_node": (sorted(list(cls.EXPECTED_NODE_TO_MODEL_TYPES.keys())),),
            }
        }

    @classmethod
    def IS_CHANGED(cls, model_info_json: str, expected_target_node: str):
        return model_info_json, expected_target_node

    @staticmethod
    def _safe_load_model_info(model_info_json: str) -> dict:
        try:
            value = json.loads(model_info_json)
            return value if isinstance(value, dict) else {}
        except Exception:
            return {}

    @staticmethod
    def _csv(items: List[str]) -> str:
        return ", ".join(items) if items else "None"

    def describe_compatibility(self, model_info_json: str, expected_target_node: str):
        model_info = self._safe_load_model_info(model_info_json)

        selected_model_type = str(model_info.get("model_type", "unknown"))
        selected_architecture = str(model_info.get("architecture_family", "unknown"))
        compatible_targets = list(model_info.get("can_connect_to", []))
        compatible_sources = list(model_info.get("can_receive_from", []))

        expected_types = self.EXPECTED_NODE_TO_MODEL_TYPES.get(expected_target_node, [])
        is_compatible = selected_model_type in expected_types

        if is_compatible:
            status_text = (
                f"Compatible: model_type '{selected_model_type}' can connect to "
                f"'{expected_target_node}'."
            )
        else:
            status_text = (
                f"Incompatible: model_type '{selected_model_type}' does not match "
                f"'{expected_target_node}'. Expected one of: {self._csv(expected_types)}."
            )

        status_text += (
            f"\nSelected architecture: {selected_architecture}"
            f"\nCan connect to: {self._csv(compatible_targets)}"
            f"\nCan receive from: {self._csv(compatible_sources)}"
        )

        return (
            is_compatible,
            status_text,
            self._csv(compatible_targets),
            self._csv(compatible_sources),
            selected_model_type,
            selected_architecture,
        )