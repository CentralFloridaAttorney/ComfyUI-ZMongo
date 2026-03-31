import json
import logging
from typing import Any, Dict, List

from aiohttp import web
from bson import ObjectId

from .zmongo_record_editor_node import ZMongoRecordEditorNode
from .zmongo_toolbag.zmongo import ZMongo
from .zmongo_toolbag.data_processor import DataProcessor

logger = logging.getLogger(__name__)


def register_zmongo_record_editor_routes(prompt_server_instance):
    if not prompt_server_instance:
        logger.warning("PromptServer instance not available; record editor routes not registered.")
        return

    routes = prompt_server_instance.routes

    def _clean_record_id(value: Any) -> str:
        raw = "" if value is None else str(value)
        cleaned = raw.strip()

        if len(cleaned) >= 2 and cleaned[0] == cleaned[-1] and cleaned[0] in {"'", '"'}:
            cleaned = cleaned[1:-1].strip()

        return cleaned

    def _build_id_candidates(value: Any) -> List[Any]:
        cleaned = _clean_record_id(value)
        if not cleaned:
            return []

        candidates: List[Any] = [cleaned]
        if ObjectId.is_valid(cleaned):
            oid = ObjectId(cleaned)
            candidates.insert(0, oid)

        return candidates

    def _parse_json_or_string(value: Any) -> Any:
        raw = "" if value is None else str(value)
        stripped = raw.strip()
        if not stripped:
            return ""
        try:
            return json.loads(stripped)
        except Exception:
            return raw

    def _result_succeeded(result: Any) -> bool:
        return bool(result) and bool(getattr(result, "success", False))

    def _result_data(result: Any) -> Dict[str, Any]:
        data = getattr(result, "data", {}) or {}
        return data if isinstance(data, dict) else {}

    @routes.get("/zmongo/record_editor/load")
    async def zmongo_record_editor_load(request):
        try:
            collection_name = request.rel_url.query.get("collection_name", "").strip()
            record_id = _clean_record_id(request.rel_url.query.get("record_id", ""))
            selected_record_json = request.rel_url.query.get("selected_record_json", "")

            record = ZMongoRecordEditorNode.fetch_record(
                collection_name=collection_name,
                record_id=record_id,
                fallback_record_json=selected_record_json,
            )

            if not record:
                return web.json_response(
                    {
                        "success": True,
                        "collection_name": collection_name,
                        "record_id": record_id,
                        "record": {},
                        "fields": [],
                    }
                )

            resolved_record_id = str(record.get("_id", record_id))

            pairs = ZMongoRecordEditorNode.get_flattened_record_pairs(
                collection_name=collection_name,
                record_id=resolved_record_id,
                fallback_record_json=selected_record_json,
            )

            fields = [{"path": path, "value": value} for path, value in pairs]

            return web.json_response(
                {
                    "success": True,
                    "collection_name": collection_name,
                    "record_id": resolved_record_id,
                    "record": DataProcessor.to_json_compatible(record),
                    "fields": fields,
                }
            )
        except Exception as exc:
            logger.exception("Failed to load record editor payload: %s", exc)
            return web.json_response(
                {
                    "success": False,
                    "error": str(exc),
                    "record": {},
                    "fields": [],
                },
                status=500,
            )

    @routes.post("/zmongo/record_editor/save")
    async def zmongo_record_editor_save(request):
        try:
            payload: Dict[str, Any] = await request.json()

            collection_name = str(payload.get("collection_name", "")).strip()
            record_id = _clean_record_id(payload.get("record_id", ""))
            changes = payload.get("changes", {})

            if not collection_name:
                return web.json_response(
                    {"success": False, "error": "collection_name is required"},
                    status=400,
                )

            if not record_id:
                return web.json_response(
                    {"success": False, "error": "record_id is required"},
                    status=400,
                )

            if not isinstance(changes, dict) or not changes:
                return web.json_response(
                    {"success": False, "error": "changes must be a non-empty object"},
                    status=400,
                )

            normalized_changes = {
                str(path).strip(): _parse_json_or_string(value)
                for path, value in changes.items()
                if str(path).strip()
            }

            if not normalized_changes:
                return web.json_response(
                    {"success": False, "error": "No valid change paths were provided"},
                    status=400,
                )

            zmongo = ZMongo()
            update_doc = {"$set": normalized_changes}

            result = None
            matched_any_candidate = False

            for candidate_id in _build_id_candidates(record_id):
                result = zmongo.update_one(
                    collection_name,
                    {"_id": candidate_id},
                    update_doc,
                    upsert=False,
                )

                if _result_succeeded(result):
                    data = _result_data(result)
                    matched_count = int(data.get("matched_count", 0) or 0)
                    modified_count = int(data.get("modified_count", 0) or 0)
                    upserted_id = data.get("upserted_id")

                    if matched_count > 0:
                        matched_any_candidate = True

                    if modified_count > 0 or upserted_id is not None or matched_count > 0:
                        break

            if result is None:
                return web.json_response(
                    {"success": False, "error": "record_id is required"},
                    status=400,
                )

            refreshed = ZMongoRecordEditorNode.fetch_record(
                collection_name=collection_name,
                record_id=record_id,
                fallback_record_json="",
            )

            if not matched_any_candidate and not refreshed:
                return web.json_response(
                    {
                        "success": False,
                        "error": f"No record found for _id '{record_id}' in collection '{collection_name}'",
                        "result": getattr(result, "to_dict", lambda: {})(),
                        "collection_name": collection_name,
                        "record_id": record_id,
                        "record": {},
                    },
                    status=404,
                )

            return web.json_response(
                {
                    "success": bool(getattr(result, "success", False)),
                    "message": getattr(result, "message", "Saved"),
                    "error": getattr(result, "error", None),
                    "result": getattr(result, "to_dict", lambda: {})(),
                    "collection_name": collection_name,
                    "record_id": record_id,
                    "record": DataProcessor.to_json_compatible(refreshed),
                }
            )
        except Exception as exc:
            logger.exception("Failed to save record editor changes: %s", exc)
            return web.json_response(
                {
                    "success": False,
                    "error": str(exc),
                },
                status=500,
            )