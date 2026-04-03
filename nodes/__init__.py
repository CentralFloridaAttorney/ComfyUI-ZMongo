import logging

logger = logging.getLogger(__name__)

NODE_CLASS_MAPPINGS = {}
NODE_DISPLAY_NAME_MAPPINGS = {}

def _merge_node_module(module, module_name: str) -> None:
    class_mappings = getattr(module, "NODE_CLASS_MAPPINGS", {})
    display_mappings = getattr(module, "NODE_DISPLAY_NAME_MAPPINGS", {})

    if not isinstance(class_mappings, dict):
        raise TypeError(f"{module_name}.NODE_CLASS_MAPPINGS must be a dict")

    if not isinstance(display_mappings, dict):
        raise TypeError(f"{module_name}.NODE_DISPLAY_NAME_MAPPINGS must be a dict")

    overlapping_classes = set(NODE_CLASS_MAPPINGS).intersection(class_mappings)
    overlapping_displays = set(NODE_DISPLAY_NAME_MAPPINGS).intersection(display_mappings)

    if overlapping_classes:
        raise ValueError(
            f"Duplicate node class mapping(s) from {module_name}: {sorted(overlapping_classes)}"
        )

    if overlapping_displays:
        raise ValueError(
            f"Duplicate node display mapping(s) from {module_name}: {sorted(overlapping_displays)}"
        )

    NODE_CLASS_MAPPINGS.update(class_mappings)
    NODE_DISPLAY_NAME_MAPPINGS.update(display_mappings)

# Module Loading Sequence
modules_to_load = [
    (".zmongo_basic_nodes", "nodes.zmongo_basic_nodes"),
    (".zmongo_utility_nodes", "nodes.zmongo_utility_nodes"),
    (".zmongo_workflow_nodes", "nodes.zmongo_workflow_nodes"),
    (".zmongo_chat_nodes", "nodes.zmongo_chat_nodes"),
    (".zmongo_adventure_nodes", "nodes.zmongo_adventure_nodes"), # Added Adventure Nodes
]

for import_path, log_name in modules_to_load:
    try:
        # Dynamic import to maintain relative pathing
        from importlib import import_module
        module = import_module(import_path, package=__package__)
        _merge_node_module(module, log_name)
    except Exception as exc:
        logger.exception("Failed to load %s: %s", log_name, exc)

__all__ = [
    "NODE_CLASS_MAPPINGS",
    "NODE_DISPLAY_NAME_MAPPINGS",
]