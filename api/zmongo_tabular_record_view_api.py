import logging
from aiohttp import web

from .zmongo_tabular_record_view_node import ZMongoTabularRecordViewNode

logger = logging.getLogger(__name__)


def register_zmongo_tabular_record_view_routes(prompt_server_instance):
    if not prompt_server_instance:
        logger.warning("PromptServer instance not available; tabular record routes not registered.")
        return

    routes = prompt_server_instance.routes

    @routes.get("/zmongo/tabular_records")
    async def zmongo_get_tabular_records(request):
        try:
            collection_name = request.rel_url.query.get("collection_name", "").strip()

            headings, flat_records, record_ids = ZMongoTabularRecordViewNode.get_table_payload(collection_name)
            default_record_id = record_ids[0] if record_ids else ""

            return web.json_response(
                {
                    "success": True,
                    "collection_name": collection_name,
                    "headings": headings,
                    "flat_records": flat_records,
                    "record_ids": record_ids,
                    "default_record_id": default_record_id,
                    "record_count": len(flat_records),
                }
            )
        except Exception as exc:
            logger.exception("Failed to get tabular records: %s", exc)
            return web.json_response(
                {
                    "success": False,
                    "error": str(exc),
                    "headings": [],
                    "flat_records": [],
                    "record_ids": [],
                    "default_record_id": "",
                    "record_count": 0,
                },
                status=500,
            )

    @routes.get("/zmongo/tabular_records_search")
    async def zmongo_get_tabular_records_search(request):
        try:
            collection_name = request.rel_url.query.get("collection_name", "").strip()
            search_text = request.rel_url.query.get("search_text", "")
            flattened_field_name = request.rel_url.query.get("flattened_field_name", "").strip()

            headings, flat_records, record_ids = (
                ZMongoTabularRecordViewNode.get_filtered_table_payload(
                    collection_name=collection_name,
                    search_text=search_text,
                    flattened_field_name=flattened_field_name,
                )
            )

            default_record_id = record_ids[0] if record_ids else ""

            return web.json_response(
                {
                    "success": True,
                    "collection_name": collection_name,
                    "search_text": search_text,
                    "flattened_field_name": flattened_field_name,
                    "headings": headings,
                    "flat_records": flat_records,
                    "record_ids": record_ids,
                    "default_record_id": default_record_id,
                    "record_count": len(flat_records),
                }
            )
        except Exception as exc:
            logger.exception("Failed to search tabular records: %s", exc)
            return web.json_response(
                {
                    "success": False,
                    "error": str(exc),
                    "headings": [],
                    "flat_records": [],
                    "record_ids": [],
                    "default_record_id": "",
                    "record_count": 0,
                },
                status=500,
            )