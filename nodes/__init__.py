from __future__ import annotations

from typing import Dict, Tuple

from . import zmongo_api_nodes
from . import gemini_api_nodes
from . import document_api_nodes
from . import zmongo_images_nodes


def _merge_node_mappings(
    *mapping_sets: Tuple[str, Dict[str, object], Dict[str, str]]
) -> tuple[Dict[str, object], Dict[str, str]]:
    class_mappings: Dict[str, object] = {}
    display_mappings: Dict[str, str] = {}

    for module_name, class_map, display_map in mapping_sets:
        for key, value in class_map.items():
            if key in class_mappings:
                raise RuntimeError(
                    f"Duplicate NODE_CLASS_MAPPINGS key {key!r} detected while loading {module_name}."
                )
            class_mappings[key] = value

        for key, value in display_map.items():
            if key in display_mappings:
                raise RuntimeError(
                    f"Duplicate NODE_DISPLAY_NAME_MAPPINGS key {key!r} detected while loading {module_name}."
                )
            display_mappings[key] = value

    return class_mappings, display_mappings


NODE_CLASS_MAPPINGS, NODE_DISPLAY_NAME_MAPPINGS = _merge_node_mappings(
    (
        "zmongo_api_nodes",
        zmongo_api_nodes.NODE_CLASS_MAPPINGS,
        zmongo_api_nodes.NODE_DISPLAY_NAME_MAPPINGS,
    ),
    (
        "gemini_api_nodes",
        gemini_api_nodes.NODE_CLASS_MAPPINGS,
        gemini_api_nodes.NODE_DISPLAY_NAME_MAPPINGS,
    ),
    (
        "document_api_nodes",
        document_api_nodes.NODE_CLASS_MAPPINGS,
        document_api_nodes.NODE_DISPLAY_NAME_MAPPINGS,
    ),
    (
        "zmongo_images_nodes",
        zmongo_images_nodes.NODE_CLASS_MAPPINGS,
        zmongo_images_nodes.NODE_DISPLAY_NAME_MAPPINGS,
    ),
)

__all__ = [
    "NODE_CLASS_MAPPINGS",
    "NODE_DISPLAY_NAME_MAPPINGS",
]