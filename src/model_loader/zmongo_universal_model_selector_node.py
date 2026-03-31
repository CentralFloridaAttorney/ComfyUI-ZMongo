from __future__ import annotations

from pathlib import Path
from typing import Iterable, List, Tuple

import folder_paths


class ZMongoUniversalModelSelectorNode:
    """
    Universal recursive model selector for files stored under:

        ComfyUI/models/zmongo/

    It does not load the model itself. It only returns normalized path data so
    downstream ZMongo nodes can decide how to use the selected file.
    """

    CATEGORY = "ZMongo/Models"
    FUNCTION = "select_model"
    RETURN_TYPES = ("STRING", "STRING", "STRING", "STRING", "STRING")
    RETURN_NAMES = ("full_path", "relative_path", "filename", "stem", "extension")

    # File extensions commonly used by Comfy / ML / embeddings / LLM ecosystems.
    ALLOWED_EXTENSIONS = {
        ".bin",
        ".ckpt",
        ".gguf",
        ".index",
        ".json",
        ".mar",
        ".model",
        ".onnx",
        ".pt",
        ".pth",
        ".py",
        ".safetensors",
        ".t7",
        ".tar",
        ".uf2",
        ".yaml",
        ".yml",
    }

    @classmethod
    def _zmongo_root(cls) -> Path:
        return Path(folder_paths.models_dir) / "zmongo"

    @classmethod
    def _iter_model_files(cls, root: Path) -> Iterable[Path]:
        if not root.exists() or not root.is_dir():
            return []

        files: List[Path] = []
        for path in root.rglob("*"):
            if not path.is_file():
                continue
            if path.suffix.lower() not in cls.ALLOWED_EXTENSIONS:
                continue
            files.append(path)

        files.sort(key=lambda p: str(p.relative_to(root)).lower())
        return files

    @classmethod
    def _relative_choices(cls) -> List[str]:
        root = cls._zmongo_root()
        files = cls._iter_model_files(root)

        choices = [str(path.relative_to(root)).replace("\\", "/") for path in files]

        if not choices:
            choices = ["<no models found in models/zmongo>"]

        return choices

    @classmethod
    def INPUT_TYPES(cls):
        return {
            "required": {
                "model_relative_path": (cls._relative_choices(),),
            },
            "optional": {
                "refresh_token": (
                    "INT",
                    {
                        "default": 0,
                        "min": 0,
                        "max": 999999,
                        "step": 1,
                        "display": "number",
                    },
                ),
            },
        }

    @classmethod
    def IS_CHANGED(cls, model_relative_path: str, refresh_token: int = 0):
        """
        Re-executes when selection changes or when refresh_token changes.
        This does not rebuild the combo live in the UI, but it helps rerun nodes
        after you change the filesystem and bump the token.
        """
        return (model_relative_path, refresh_token)

    @staticmethod
    def _empty_result() -> Tuple[str, str, str, str, str]:
        return ("", "", "", "", "")

    def select_model(
        self,
        model_relative_path: str,
        refresh_token: int = 0,
    ) -> Tuple[str, str, str, str, str]:
        del refresh_token  # execution-only knob for Comfy cache / rerun behavior

        root = self._zmongo_root()

        if model_relative_path == "<no models found in models/zmongo>":
            return self._empty_result()

        full_path = (root / model_relative_path).resolve()

        if not full_path.exists() or not full_path.is_file():
            return self._empty_result()

        return (
            str(full_path),
            model_relative_path.replace("\\", "/"),
            full_path.name,
            full_path.stem,
            full_path.suffix.lower(),
        )