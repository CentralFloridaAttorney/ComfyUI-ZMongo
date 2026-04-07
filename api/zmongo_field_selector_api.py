import logging
from aiohttp import web
from ..zmongo_toolbag.data_processor import DataProcessor
from ..zmongo_toolbag.zmongo import ZMongo

logger = logging.getLogger("ComfyUI")


def register_zmongo_field_selector_routes(prompt_server_instance):
    routes = prompt_server_instance.routes

    @routes.get("/api/zmongo/get_fields")
    async def get_fields_endpoint(request):
        try:
            uri = request.query.get("uri")
            db_name = request.query.get("db")
            collection = request.query.get("collection")

            if not all([uri, db_name, collection]):
                return web.json_response({"fields": ["Error: Missing Params"]}, status=200)

            # Match the Registry key seen in your logs: "uri|db"
            instance_key = f"{uri}|{db_name}"

            from ..nodes.zmongo_basic_nodes import ZMONGO_REGISTRY
            zmongo = ZMONGO_REGISTRY.get(instance_key)

            if not zmongo:
                try:
                    zmongo = ZMongo(uri=uri, db_name=db_name)
                    ZMONGO_REGISTRY[instance_key] = zmongo
                except Exception as e:
                    return web.json_response({"fields": [f"Conn Error: {str(e)}"]})

            # Fetch the first record to generate the field list
            sample = zmongo.db[collection].find_one()

            if not sample:
                # Returning this as a list item ensures the dropdown isn't empty ""
                return web.json_response({"fields": ["(Collection Empty)"]})

            fields = DataProcessor.sorted_flattened_keys(sample)
            if "_id" in fields:
                fields.remove("_id")

            return web.json_response({"fields": fields or ["(No Fields Detected)"]})

        except Exception as e:
            logger.exception("ZMongo API Error")
            return web.json_response({"fields": [f"API Error: {str(e)}"]}, status=200)