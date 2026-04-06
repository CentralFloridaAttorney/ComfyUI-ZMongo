import logging
from importlib import import_module
from types import ModuleType
from typing import Dict, Iterable, Tuple

logger = logging.getLogger(__name__)

NODE_CLASS_MAPPINGS: Dict[str, object] = {}
NODE_DISPLAY_NAME_MAPPINGS: Dict[str, str] = {}


def _merge_node_module(module: ModuleType, module_name: str) -> None:
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


def _get_package_name() -> str:
    if isinstance(__name__, str) and __name__ and __name__ != "__main__":
        return __name__

    if isinstance(__package__, str) and __package__:
        return __package__

    raise RuntimeError("Unable to determine package name for node loader")


def _load_modules(modules_to_load: Iterable[Tuple[str, str]]) -> None:
    package_name = _get_package_name()

    for module_basename, log_name in modules_to_load:
        try:
            module = import_module(f".{module_basename}", package=package_name)
            _merge_node_module(module, log_name)
            logger.debug("Loaded node module: %s.%s", package_name, module_basename)
        except Exception:
            logger.exception("Failed to load %s", log_name)


MODULES_TO_LOAD = [
    ("zmongo_adventure_nodes", "nodes.zmongo_adventure_nodes"),
    ("zmongo_basic_nodes", "nodes.zmongo_basic_nodes"),
    ("zmongo_chat_nodes", "nodes.zmongo_chat_nodes"),
    ("zmongo_field_selector_node", "nodes.zmongo_field_selector_node"),
    ("zmongo_flattened_field_selector_node", "nodes.zmongo_flattened_field_selector_node"),
    ("zmongo_nodes", "nodes.zmongo_nodes"),
    ("zmongo_record_editor_node", "nodes.zmongo_record_editor_node"),
    ("zmongo_record_splitter", "nodes.zmongo_record_splitter"),
    ("zmongo_starter_nodes", "nodes.zmongo_starter_nodes"),
    ("zmongo_tabular_record_view_node", "nodes.zmongo_tabular_record_view_node"),
    ("zmongo_utility_nodes", "nodes.zmongo_utility_nodes"),
    ("zmongo_workflow_nodes", "nodes.zmongo_workflow_nodes"),
]

_load_modules(MODULES_TO_LOAD)

__all__ = [
    "NODE_CLASS_MAPPINGS",
    "NODE_DISPLAY_NAME_MAPPINGS",
]