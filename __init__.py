"""
ComfyUI-ZMongo custom node package.

Root ComfyUI loader.  Keep this file thin so ComfyUI imports one canonical
mapping source from the nodes package.
"""

from __future__ import annotations

try:
    from .nodes import NODE_CLASS_MAPPINGS, NODE_DISPLAY_NAME_MAPPINGS
except Exception as exc:
    import traceback

    print(f"[ComfyUI-ZMongo] Failed to import node mappings from .nodes: {exc}")
    traceback.print_exc()

    NODE_CLASS_MAPPINGS = {}
    NODE_DISPLAY_NAME_MAPPINGS = {}

WEB_DIRECTORY = "./js"

print(f"[ComfyUI-ZMongo] Root registered nodes: {len(NODE_CLASS_MAPPINGS)}")

__all__ = [
    "NODE_CLASS_MAPPINGS",
    "NODE_DISPLAY_NAME_MAPPINGS",
    "WEB_DIRECTORY",
]