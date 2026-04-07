import logging
import mimetypes

from server import PromptServer

logger = logging.getLogger(__name__)

# -----------------------------------------------------------------------------
# Public ComfyUI exports
# -----------------------------------------------------------------------------

NODE_CLASS_MAPPINGS = {}
NODE_DISPLAY_NAME_MAPPINGS = {}
WEB_DIRECTORY = "./js"

__all__ = [
    "NODE_CLASS_MAPPINGS",
    "NODE_DISPLAY_NAME_MAPPINGS",
    "WEB_DIRECTORY",
]

# -----------------------------------------------------------------------------
# MIME types for frontend assets
# -----------------------------------------------------------------------------

mimetypes.add_type("application/javascript", ".js")
mimetypes.add_type("text/css", ".css")

# -----------------------------------------------------------------------------
# Import node mappings from your internal node package
# -----------------------------------------------------------------------------

def _merge_mappings(module, module_name: str) -> None:
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


# Prefer a single package-level import from your node bundle.
# This assumes your internal node package exports the two mapping dicts.
try:
    from .src.nodes import __init__ as _nodes_module  # type: ignore[attr-defined]
except Exception:
    _nodes_module = None

if _nodes_module is None:
    try:
        from .src import NODE_CLASS_MAPPINGS as _SRC_CLASS_MAPPINGS
        from .src import NODE_DISPLAY_NAME_MAPPINGS as _SRC_DISPLAY_NAME_MAPPINGS

        class _SrcModuleProxy:
            NODE_CLASS_MAPPINGS = _SRC_CLASS_MAPPINGS
            NODE_DISPLAY_NAME_MAPPINGS = _SRC_DISPLAY_NAME_MAPPINGS

        _merge_mappings(_SrcModuleProxy, "ComfyUI-ZMongo.src")
    except Exception as exc:
        logger.exception("Failed to import node mappings from .src: %s", exc)
        raise
else:
    _merge_mappings(_nodes_module, "ComfyUI-ZMongo.src.nodes")

# -----------------------------------------------------------------------------
# Backend HTTP routes
# -----------------------------------------------------------------------------

try:
    from .src.zai_mongo_manager.zmongo_server import ZMongoServer
except Exception as exc:
    logger.exception("Failed to import ZMongoServer: %s", exc)
    raise

_zmongo_server = ZMongoServer()


@PromptServer.instance.routes.get("/zai/zmongo/healthz")
async def zmongo_healthz(request):
    return await _zmongo_server.healthz(request)


@PromptServer.instance.routes.get("/zai/zmongo/collections")
async def zmongo_collections(request):
    return await _zmongo_server.list_collections(request)


@PromptServer.instance.routes.get("/zai/zmongo/docs/{coll}")
async def zmongo_docs(request):
    return await _zmongo_server.get_docs(request)


@PromptServer.instance.routes.get("/zai/zmongo/doc/{coll}/{id}")
async def zmongo_doc(request):
    return await _zmongo_server.get_single_doc(request)


@PromptServer.instance.routes.post("/zai/zmongo/create")
async def zmongo_create(request):
    return await _zmongo_server.create_doc(request)


@PromptServer.instance.routes.post("/zai/zmongo/update")
async def zmongo_update(request):
    return await _zmongo_server.update_doc(request)


@PromptServer.instance.routes.post("/zai/zmongo/delete")
async def zmongo_delete(request):
    return await _zmongo_server.delete_doc(request)


@PromptServer.instance.routes.get("/zai/zmongo/dashboard")
async def zmongo_dashboard(request):
    return await _zmongo_server.serve_dashboard(request)