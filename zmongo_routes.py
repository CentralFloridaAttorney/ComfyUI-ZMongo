import logging
import mimetypes

from server import PromptServer

from .zmongo_server import ZMongoServer

logger = logging.getLogger(__name__)

mimetypes.add_type("application/javascript", ".js")
mimetypes.add_type("text/css", ".css")

z_manager = ZMongoServer()
routes = PromptServer.instance.routes


@routes.get("/zai/zmongo/healthz")
async def zmongo_healthz(request):
    return await z_manager.healthz(request)


@routes.get("/zai/zmongo/collections")
async def zmongo_collections(request):
    return await z_manager.list_collections(request)


@routes.get("/zai/zmongo/docs/{coll}")
async def zmongo_docs(request):
    return await z_manager.get_docs(request)


@routes.get("/zai/zmongo/doc/{coll}/{id}")
async def zmongo_doc(request):
    return await z_manager.get_single_doc(request)


@routes.post("/zai/zmongo/update")
async def zmongo_update(request):
    return await z_manager.update_doc(request)


@routes.post("/zai/zmongo/create")
async def zmongo_create(request):
    return await z_manager.create_doc(request)


@routes.post("/zai/zmongo/delete")
async def zmongo_delete(request):
    return await z_manager.delete_doc(request)


@routes.post("/zai/zmongo/save-value")
async def zmongo_save_value(request):
    return await z_manager.save_value(request)


@routes.get("/zai/zmongo/dashboard")
async def zmongo_dashboard(request):
    return await z_manager.serve_dashboard(request)


@routes.post("/zai/zmongo/collections/create")
async def zmongo_create_collection(request):
    return await z_manager.create_collection(request)


@routes.post("/zai/zmongo/collections/delete")
async def zmongo_delete_collection(request):
    return await z_manager.delete_collection(request)


logger.info("ZMongo routes registered.")