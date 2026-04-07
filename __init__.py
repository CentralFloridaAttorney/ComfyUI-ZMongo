import logging
import os
import sys
from importlib import import_module
from types import ModuleType
from typing import Dict, Iterable, Tuple

logger = logging.getLogger(__name__)

PACKAGE_DIR = os.path.dirname(os.path.abspath(__file__))
if PACKAGE_DIR not in sys.path:
    sys.path.insert(0, PACKAGE_DIR)

WEB_DIRECTORY = "./web/js"

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


def _load_modules(modules_to_load: Iterable[Tuple[str, str]]) -> None:
    for module_name, log_name in modules_to_load:
        try:
            module = import_module(module_name)
            _merge_node_module(module, log_name)
            logger.debug("Loaded node module: %s", module_name)
        except Exception:
            logger.exception("Failed to load %s", log_name)


def _load_route_modules(route_modules: Iterable[Tuple[str, str]]) -> None:
    for module_name, log_name in route_modules:
        try:
            import_module(module_name)
            logger.debug("Loaded route module: %s", module_name)
        except Exception:
            logger.exception("Failed to load %s", log_name)


ROUTE_MODULES_TO_LOAD = [
    ("zmongo_routes", "routes.zmongo_routes"),
]

MODULES_TO_LOAD = [
    ("nodes.zmongo_adventure_nodes", "nodes.zmongo_adventure_nodes"),
    ("nodes.zmongo_basic_nodes", "nodes.zmongo_basic_nodes"),
    ("nodes.zmongo_chat_nodes", "nodes.zmongo_chat_nodes"),
    ("nodes.zmongo_field_selector_node", "nodes.zmongo_field_selector_node"),
    ("nodes.zmongo_flattened_field_selector_node", "nodes.zmongo_flattened_field_selector_node"),
    ("nodes.zmongo_nodes", "nodes.zmongo_nodes"),
    ("nodes.zmongo_record_editor_node", "nodes.zmongo_record_editor_node"),
    ("nodes.zmongo_record_splitter", "nodes.zmongo_record_splitter"),
    ("nodes.zmongo_starter_nodes", "nodes.zmongo_starter_nodes"),
    ("nodes.zmongo_tabular_record_view_node", "nodes.zmongo_tabular_record_view_node"),
    ("nodes.zmongo_utility_nodes", "nodes.zmongo_utility_nodes"),
    ("nodes.zmongo_workflow_nodes", "nodes.zmongo_workflow_nodes"),
    ("nodes.zmongo_remote_nodes", "nodes.zmongo_remote_nodes"),
]

_load_route_modules(ROUTE_MODULES_TO_LOAD)
_load_modules(MODULES_TO_LOAD)

__all__ = [
    "NODE_CLASS_MAPPINGS",
    "NODE_DISPLAY_NAME_MAPPINGS",
    "WEB_DIRECTORY",
]