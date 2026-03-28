import logging
from datetime import datetime, timezone

from aiohttp import web

try:
    from .zmongo_manager import ZMongoManager
except Exception:
    from zmongo_manager import ZMongoManager

try:
    from .zmongo_toolbag.data_processing import DataProcessor
except Exception:
    from zmongo_toolbag.data_processing import DataProcessor

try:
    from .zmongo_toolbag.zembedder import ZEmbedder
except Exception:
    from zmongo_toolbag.zembedder import ZEmbedder

try:
    from .zmongo_toolbag.zmongo import ZMongo
except Exception:
    from zmongo_toolbag.zmongo import ZMongo

try:
    from .zmongo_toolbag.safe_result import SafeResult
except Exception:
    from zmongo_toolbag.safe_result import SafeResult
logger = logging.getLogger(__name__)

try:
    # Available in PyMongo >= 4.x
    from bson import BSON, decode_file_iter

    HAVE_BSON_STREAM = True
except Exception:
    HAVE_BSON_STREAM = False


logger = logging.getLogger(__name__)

PRESET_COLLECTION = "comfyui_presets"
zmongo = ZMongo()


def _doc_to_jsonable(doc):
    if not doc:
        return doc
    out = dict(doc)
    if "_id" in out:
        out["_id"] = str(out["_id"])
    return out


async def list_presets(request):
    workflow = request.query.get("workflow")
    query = {"workflow": workflow} if workflow else {}

    res = await zmongo.find_many_async(PRESET_COLLECTION, query=query, limit=1000)
    if not res.success:
        return web.json_response({"success": False, "error": res.error}, status=500)

    docs = [_doc_to_jsonable(d) for d in (res.data or [])]
    return web.json_response({"success": True, "presets": docs})


async def save_preset(request):
    try:
        payload = await request.json()
    except Exception as e:
        return web.json_response({"success": False, "error": f"Invalid JSON: {e}"}, status=400)

    name = payload.get("name")
    workflow = payload.get("workflow", "default")
    group = payload.get("group", "Default")
    updates = payload.get("updates", {})

    if not name:
        return web.json_response({"success": False, "error": "Missing preset name"}, status=400)

    now = datetime.now(timezone.utc).isoformat()

    query = {"workflow": workflow, "name": name}
    doc = {
        "workflow": workflow,
        "name": name,
        "group": group,
        "updates": updates,
        "updated_at": now,
    }

    res = await zmongo.insert_or_update_async(PRESET_COLLECTION, query, doc)
    if not res.success:
        return web.json_response({"success": False, "error": res.error}, status=500)

    return web.json_response({"success": True, "message": "Preset saved"})


async def delete_preset(request):
    workflow = request.match_info.get("workflow")
    name = request.match_info.get("name")

    if not workflow or not name:
        return web.json_response({"success": False, "error": "Missing workflow or name"}, status=400)

    res = await zmongo.delete_many_async(PRESET_COLLECTION, {"workflow": workflow, "name": name})
    if not res.success:
        return web.json_response({"success": False, "error": res.error}, status=500)

    return web.json_response({"success": True, "message": "Preset deleted"})


def register_preset_routes(prompt_server):
    routes = prompt_server.routes

    routes.get("/zmongo/presets")(list_presets)
    routes.post("/zmongo/presets")(save_preset)
    routes.delete("/zmongo/presets/{workflow}/{name}")(delete_preset)

    logger.info("[ZMongo] Preset API routes registered")