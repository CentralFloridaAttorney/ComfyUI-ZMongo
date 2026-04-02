import logging
from aiohttp import web

from zmongo_field_selector_node import ZMongoFieldSelectorNode

logger = logging.getLogger(__name__)


def register_zmongo_field_selector_routes(prompt_server_instance):
    if not prompt_server_instance:
        logger.warning("PromptServer instance not available; ZMongo routes not registered.")
        return

    routes = prompt_server_instance.routes

    @routes.get("/zmongo/flattened_fields")
    async def zmongo_get_flattened_fields(request):
        try:
            collection_name = request.rel_url.query.get("collection_name", "").strip()
            fields = ZMongoFieldSelectorNode.get_flattened_field_names(collection_name)
            return web.json_response(
                {
                    "success": True,
                    "collection_name": collection_name,
                    "fields": fields,
                }
            )
        except Exception as exc:
            logger.exception("Failed to get flattened fields: %s", exc)
            return web.json_response(
                {
                    "success": False,
                    "error": str(exc),
                    "fields": [],
                },
                status=500,
            )