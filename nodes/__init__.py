from __future__ import annotations

from typing import Dict, Tuple

from . import zmongo_api_nodes
from . import gemini_api_nodes
from . import zmongo_image_nodes
from . import zmongo_images_nodes
from . import document_api_nodes
from . import zmongo_preset_nodes
from . import zmongo_discovery_nodes
from . import zmongo_dynamic_preset
from . import zmongo_helper_nodes



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
        "zmongo_image_nodes",
        zmongo_image_nodes.NODE_CLASS_MAPPINGS,
        zmongo_image_nodes.NODE_DISPLAY_NAME_MAPPINGS,
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
    (
        "zmongo_preset_nodes",
        zmongo_preset_nodes.NODE_CLASS_MAPPINGS,
        zmongo_preset_nodes.NODE_DISPLAY_NAME_MAPPINGS,
    ),
    (
        "zmongo_discovery_nodes",
        zmongo_discovery_nodes.NODE_CLASS_MAPPINGS,
        zmongo_discovery_nodes.NODE_DISPLAY_NAME_MAPPINGS,
    ),
    (
        "zmongo_dynamic_preset",
        zmongo_dynamic_preset.NODE_CLASS_MAPPINGS,
        zmongo_dynamic_preset.NODE_DISPLAY_NAME_MAPPINGS,
    ),
    (
        "zmongo_helper_nodes",
        zmongo_helper_nodes.NODE_CLASS_MAPPINGS,
        zmongo_helper_nodes.NODE_DISPLAY_NAME_MAPPINGS,
    ),
)

print(f"[ComfyUI-ZMongo] nodes package registered nodes: {len(NODE_CLASS_MAPPINGS)}")

__all__ = [
    "NODE_CLASS_MAPPINGS",
    "NODE_DISPLAY_NAME_MAPPINGS",
]
