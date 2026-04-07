import os
import threading
from typing import Any, Dict, Tuple

from llama_cpp import Llama


# Simple in-process cache so the same model is not reloaded every execution.
# Keyed by the fully resolved model path + core load settings.
_MODEL_CACHE: Dict[Tuple[str, int, int, bool], Llama] = {}
_MODEL_CACHE_LOCK = threading.Lock()


def _coerce_bool(value: Any, default: bool = False) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return default
    return str(value).strip().lower() in {"1", "true", "yes", "on"}


def _safe_int(value: Any, default: int) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


class ZMongoFastGGUFLLMNode:
    """
    Lightweight local GGUF LLM node for ComfyUI.

    Inputs:
      - model_path: full path to a local .gguf file
      - system_prompt: system instruction text
      - user_prompt: user message text

    Outputs:
      - response_text: model reply text
      - raw_json: serialized raw response payload
    """

    CATEGORY = "ZMongo/LLM"
    FUNCTION = "generate"
    RETURN_TYPES = ("STRING", "STRING")
    RETURN_NAMES = ("response_text", "raw_json")
    OUTPUT_NODE = True

    @classmethod
    def INPUT_TYPES(cls):
        return {
            "required": {
                "model_path": ("STRING", {
                    "default": "",
                    "multiline": False
                }),
                "system_prompt": ("STRING", {
                    "default": "You are a helpful assistant.",
                    "multiline": True
                }),
                "user_prompt": ("STRING", {
                    "default": "",
                    "multiline": True
                }),
            },
            "optional": {
                "max_tokens": ("INT", {
                    "default": 256,
                    "min": 1,
                    "max": 8192,
                    "step": 1
                }),
                "temperature": ("FLOAT", {
                    "default": 0.2,
                    "min": 0.0,
                    "max": 2.0,
                    "step": 0.01
                }),
                "top_p": ("FLOAT", {
                    "default": 0.95,
                    "min": 0.0,
                    "max": 1.0,
                    "step": 0.01
                }),
                "context_size": ("INT", {
                    "default": 4096,
                    "min": 256,
                    "max": 131072,
                    "step": 256
                }),
                "gpu_layers": ("INT", {
                    "default": 0,
                    "min": 0,
                    "max": 200,
                    "step": 1
                }),
                "threads": ("INT", {
                    "default": max(1, (os.cpu_count() or 4) // 2),
                    "min": 1,
                    "max": 128,
                    "step": 1
                }),
                "use_mmap": ("BOOLEAN", {"default": True}),
                "use_mlock": ("BOOLEAN", {"default": False}),
                "stop": ("STRING", {
                    "default": "",
                    "multiline": False
                }),
            }
        }

    @classmethod
    def IS_CHANGED(cls, **kwargs):
        # Re-run when any input changes.
        return float("nan")

    def _get_llm(
        self,
        model_path: str,
        context_size: int,
        gpu_layers: int,
        threads: int,
        use_mmap: bool,
        use_mlock: bool,
    ) -> Llama:
        resolved_path = os.path.abspath(os.path.expanduser(model_path))
        cache_key = (resolved_path, context_size, gpu_layers, use_mmap)

        with _MODEL_CACHE_LOCK:
            llm = _MODEL_CACHE.get(cache_key)
            if llm is not None:
                return llm

            llm = Llama(
                model_path=resolved_path,
                n_ctx=context_size,
                n_gpu_layers=gpu_layers,
                n_threads=threads,
                use_mmap=use_mmap,
                use_mlock=use_mlock,
                verbose=False,
            )
            _MODEL_CACHE[cache_key] = llm
            return llm

    def generate(
        self,
        model_path: str,
        system_prompt: str,
        user_prompt: str,
        max_tokens: int = 256,
        temperature: float = 0.2,
        top_p: float = 0.95,
        context_size: int = 4096,
        gpu_layers: int = 0,
        threads: int = 4,
        use_mmap: bool = True,
        use_mlock: bool = False,
        stop: str = "",
    ):
        try:
            if not model_path or not str(model_path).strip():
                raise ValueError("model_path is required.")

            resolved_path = os.path.abspath(os.path.expanduser(model_path))
            if not os.path.isfile(resolved_path):
                raise FileNotFoundError(f"GGUF model not found: {resolved_path}")

            if not resolved_path.lower().endswith(".gguf"):
                raise ValueError(f"Expected a .gguf file, got: {resolved_path}")

            llm = self._get_llm(
                model_path=resolved_path,
                context_size=_safe_int(context_size, 4096),
                gpu_layers=_safe_int(gpu_layers, 0),
                threads=_safe_int(threads, 4),
                use_mmap=_coerce_bool(use_mmap, True),
                use_mlock=_coerce_bool(use_mlock, False),
            )

            stop_sequences = None
            if stop and str(stop).strip():
                stop_sequences = [s for s in str(stop).split("|") if s]

            response = llm.create_chat_completion(
                messages=[
                    {"role": "system", "content": system_prompt or ""},
                    {"role": "user", "content": user_prompt or ""},
                ],
                max_tokens=_safe_int(max_tokens, 256),
                temperature=float(temperature),
                top_p=float(top_p),
                stop=stop_sequences,
            )

            text = ""
            choices = response.get("choices") or []
            if choices:
                message = choices[0].get("message") or {}
                text = message.get("content", "") or ""

            import json
            raw_json = json.dumps(response, ensure_ascii=False, indent=2, default=str)

            return (text, raw_json)

        except Exception as exc:
            error_text = f"ZMongoFastGGUFLLMNode error: {exc}"
            return (error_text, error_text)


NODE_CLASS_MAPPINGS = {
    "ZMongoFastGGUFLLMNode": ZMongoFastGGUFLLMNode,
}

NODE_DISPLAY_NAME_MAPPINGS = {
    "ZMongoFastGGUFLLMNode": "ZMongo Fast GGUF LLM",
}