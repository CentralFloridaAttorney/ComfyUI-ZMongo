from __future__ import annotations

from typing import Dict, Tuple

from nodes import zmongo_image_nodes, zmongo_api_nodes, video_api_nodes, zmongo_images_nodes, gemini_api_nodes, \
    document_api_nodes, zmongo_preset_nodes, zmongo_discovery_nodes, zmongo_helper_nodes


def _ensure_comfy_node_def_v2_metadata(
    *,
    module_name: str,
    node_key: str,
    node_class: object,
    display_name: str,
) -> None:
    """
    Ensure every registered node exposes explicit metadata used by ComfyUI's
    Node Definition JSON v2.0 export.

    ComfyUI can derive some of these values at runtime, but setting them here
    makes custom nodes more robust for strict schema validation.
    """
    if not isinstance(getattr(node_class, "DESCRIPTION", None), str):
        setattr(
            node_class,
            "DESCRIPTION",
            f"{display_name or node_key} node provided by ComfyUI-ZMongo.",
        )

    if not isinstance(getattr(node_class, "OUTPUT_NODE", None), bool):
        setattr(node_class, "OUTPUT_NODE", False)

    if not isinstance(getattr(node_class, "CATEGORY", None), str):
        setattr(node_class, "CATEGORY", "ZMongo")

    if not isinstance(getattr(node_class, "FUNCTION", None), str):
        raise RuntimeError(
            f"{module_name}.{node_key} is missing required ComfyUI FUNCTION metadata."
        )

    if not callable(getattr(node_class, "INPUT_TYPES", None)):
        raise RuntimeError(
            f"{module_name}.{node_key} is missing required ComfyUI INPUT_TYPES method."
        )

    if not isinstance(getattr(node_class, "RETURN_TYPES", None), tuple):
        raise RuntimeError(
            f"{module_name}.{node_key} is missing required ComfyUI RETURN_TYPES tuple."
        )

    return_types = getattr(node_class, "RETURN_TYPES", ())
    return_names = getattr(node_class, "RETURN_NAMES", tuple())

    if return_names and len(return_names) != len(return_types):
        raise RuntimeError(
            f"{module_name}.{node_key} RETURN_NAMES length does not match RETURN_TYPES length."
        )

    output_is_list = getattr(node_class, "OUTPUT_IS_LIST", None)
    if output_is_list is not None and len(output_is_list) != len(return_types):
        raise RuntimeError(
            f"{module_name}.{node_key} OUTPUT_IS_LIST length does not match RETURN_TYPES length."
        )


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

            display_name = display_map.get(key, key)
            _ensure_comfy_node_def_v2_metadata(
                module_name=module_name,
                node_key=key,
                node_class=value,
                display_name=display_name,
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
        "zmongo_images_nodes",
        zmongo_images_nodes.NODE_CLASS_MAPPINGS,
        zmongo_images_nodes.NODE_DISPLAY_NAME_MAPPINGS,
    ),
    (
        "video_api_nodes",
        video_api_nodes.NODE_CLASS_MAPPINGS,
        video_api_nodes.NODE_DISPLAY_NAME_MAPPINGS,
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