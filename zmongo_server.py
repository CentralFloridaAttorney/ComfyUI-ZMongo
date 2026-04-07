import json
import os

from aiohttp import web
from bson import json_util
from bson.objectid import ObjectId
from motor.motor_asyncio import AsyncIOMotorClient


class ZMongoServer:
    def __init__(self):
        mongo_uri = os.getenv("ZMONGO_URI", "mongodb://localhost:27017")
        db_name = os.getenv("ZMONGO_DB_NAME", "zai_comfyui")

        self.client = AsyncIOMotorClient(
            mongo_uri,
            serverSelectionTimeoutMS=5000,
        )
        self.db = self.client[db_name]

        self.base_path = os.path.dirname(os.path.realpath(__file__))
        self.web_path = os.path.join(self.base_path, "web")

    def _serialize(self, data):
        return json.loads(json_util.dumps(data))

    def _error(self, message, status=400):
        return web.json_response({"error": str(message)}, status=status)

    @staticmethod
    def _normalize_object_id(value):
        raw = str(value or "").strip()
        if not raw:
            return ""
        return ObjectId(raw) if ObjectId.is_valid(raw) else raw

    async def list_collections(self, request):
        try:
            collections = await self.db.list_collection_names()
            collections = sorted(collections)
            return web.json_response(
                {
                    "success": True,
                    "collections": collections,
                    "count": len(collections),
                }
            )
        except Exception as e:
            return self._error(e, status=500)

    async def get_docs(self, request):
        coll_name = request.match_info.get("coll")
        if not coll_name:
            return self._error("Missing collection name", status=400)

        try:
            limit = min(max(int(request.query.get("limit", 50)), 1), 500)
            skip = max(int(request.query.get("skip", 0)), 0)
            sort_field = request.query.get("sort_field", "_id")
            sort_dir_raw = request.query.get("sort_dir", "desc").lower()
            sort_dir = -1 if sort_dir_raw == "desc" else 1

            cursor = (
                self.db[coll_name]
                .find({})
                .sort(sort_field, sort_dir)
                .skip(skip)
                .limit(limit)
            )
            docs = await cursor.to_list(length=limit)
            total = await self.db[coll_name].count_documents({})

            return web.json_response(
                {
                    "success": True,
                    "collection": coll_name,
                    "docs": self._serialize(docs),
                    "total": total,
                    "skip": skip,
                    "limit": limit,
                }
            )
        except Exception as e:
            return self._error(e, status=500)

    async def get_single_doc(self, request):
        coll_name = request.match_info.get("coll")
        doc_id = request.match_info.get("id")

        if not coll_name or not doc_id:
            return self._error("Missing collection or id", status=400)

        try:
            normalized_id = self._normalize_object_id(doc_id)
            doc = await self.db[coll_name].find_one({"_id": normalized_id})
            if doc:
                return web.json_response(self._serialize(doc))
            return web.json_response({"error": "Document not found"}, status=404)
        except Exception as e:
            return web.json_response({"error": f"Invalid ID format: {str(e)}"}, status=400)

    async def update_doc(self, request):
        try:
            data = await request.json()
            coll_name = (data.get("collection") or "").strip()
            doc_id = data.get("id")

            if not coll_name:
                return self._error("Missing collection", status=400)
            if not doc_id:
                return self._error("Missing id", status=400)

            normalized_id = self._normalize_object_id(doc_id)

            update_data = data.get("update")
            key = (data.get("key") or "").strip()
            value = data.get("value")

            if isinstance(update_data, dict):
                payload = dict(update_data)
                payload.pop("_id", None)
            elif key:
                payload = {key: value}
            else:
                return self._error("Provide either update object or key/value", status=400)

            result = await self.db[coll_name].update_one(
                {"_id": normalized_id},
                {"$set": payload},
            )

            updated_doc = await self.db[coll_name].find_one({"_id": normalized_id})

            return web.json_response(
                {
                    "success": True,
                    "matched_count": result.matched_count,
                    "modified_count": result.modified_count,
                    "document": self._serialize(updated_doc) if updated_doc else None,
                }
            )
        except Exception as e:
            return web.json_response({"error": str(e)}, status=500)

    async def create_doc(self, request):
        try:
            data = await request.json()
            coll_name = (data.get("collection") or "").strip()
            document = data.get("document", {})

            if not coll_name:
                return self._error("Missing collection", status=400)
            if not isinstance(document, dict):
                return self._error("document must be an object", status=400)

            document.pop("_id", None)

            result = await self.db[coll_name].insert_one(document)
            created_doc = await self.db[coll_name].find_one({"_id": result.inserted_id})

            return web.json_response(
                {
                    "success": True,
                    "inserted_id": str(result.inserted_id),
                    "document": self._serialize(created_doc) if created_doc else None,
                }
            )
        except Exception as e:
            return self._error(e, status=500)

    async def delete_doc(self, request):
        try:
            data = await request.json()
            coll_name = (data.get("collection") or "").strip()
            doc_id = data.get("id")

            if not coll_name:
                return self._error("Missing collection", status=400)
            if not doc_id:
                return self._error("Missing id", status=400)

            normalized_id = self._normalize_object_id(doc_id)

            result = await self.db[coll_name].delete_one({"_id": normalized_id})

            return web.json_response(
                {
                    "success": True,
                    "deleted_count": result.deleted_count,
                }
            )
        except Exception as e:
            return self._error(e, status=500)

    async def save_value(self, request):
        """
        Workflow/panel-friendly save route.

        Supported payloads:
        - document_id + field_path + value
        - query + field_path + value
        - document_id + full value object
        - query + full value object
        - create new document when no document_id/query is supplied
        """
        try:
            data = await request.json()

            coll_name = (data.get("collection") or "").strip()
            document_id = str(data.get("document_id") or "").strip()
            query = data.get("query") or {}
            field_path = str(data.get("field_path") or "").strip()
            value = data.get("value")
            upsert_if_missing = bool(data.get("upsert_if_missing", True))
            metadata = data.get("metadata") or {}

            if not coll_name:
                return self._error("Missing collection", status=400)
            if query and not isinstance(query, dict):
                return self._error("query must be an object", status=400)
            if metadata and not isinstance(metadata, dict):
                return self._error("metadata must be an object", status=400)

            if document_id:
                target_query = {"_id": self._normalize_object_id(document_id)}
            elif query:
                target_query = query
            else:
                target_query = None

            if target_query is not None:
                if field_path:
                    update_doc = {field_path: value}
                elif isinstance(value, dict):
                    update_doc = dict(value)
                    update_doc.pop("_id", None)
                else:
                    update_doc = {"value": value}

                if metadata:
                    update_doc["metadata.last_remote_save"] = metadata

                result = await self.db[coll_name].update_one(
                    target_query,
                    {"$set": update_doc},
                    upsert=upsert_if_missing,
                )

                updated_doc = await self.db[coll_name].find_one(target_query)

                return web.json_response(
                    {
                        "success": True,
                        "matched_count": result.matched_count,
                        "modified_count": result.modified_count,
                        "upserted_id": str(result.upserted_id) if result.upserted_id else None,
                        "document": self._serialize(updated_doc) if updated_doc else None,
                    }
                )

            if field_path:
                new_doc = {field_path: value}
            elif isinstance(value, dict):
                new_doc = dict(value)
                new_doc.pop("_id", None)
            else:
                new_doc = {"value": value}

            if metadata:
                existing_meta = new_doc.get("metadata")
                if isinstance(existing_meta, dict):
                    existing_meta["last_remote_save"] = metadata
                else:
                    new_doc["metadata"] = {"last_remote_save": metadata}

            result = await self.db[coll_name].insert_one(new_doc)
            created_doc = await self.db[coll_name].find_one({"_id": result.inserted_id})

            return web.json_response(
                {
                    "success": True,
                    "inserted_id": str(result.inserted_id),
                    "document": self._serialize(created_doc) if created_doc else None,
                }
            )
        except Exception as e:
            return self._error(e, status=500)

    async def serve_dashboard(self, request):
        dashboard_path = os.path.join(self.web_path, "dashboard.html")

        if os.path.exists(dashboard_path):
            return web.FileResponse(dashboard_path)

        fallback_html = """
<!doctype html>
<html>
<head>
    <meta charset="utf-8">
    <title>ZMongo Dashboard</title>
    <style>
        body {
            font-family: Arial, sans-serif;
            margin: 24px;
            background: #111;
            color: #eee;
        }
        h1 { margin-top: 0; }
        a { color: #7db7ff; }
        code {
            background: #222;
            padding: 2px 6px;
            border-radius: 4px;
        }
        .card {
            background: #1b1b1b;
            border: 1px solid #333;
            border-radius: 8px;
            padding: 16px;
            max-width: 900px;
        }
    </style>
</head>
<body>
    <div class="card">
        <h1>ZMongo Dashboard</h1>
        <p>No <code>web/dashboard.html</code> file was found, but the API is running.</p>
        <ul>
            <li><a href="/zai/zmongo/collections">/zai/zmongo/collections</a></li>
        </ul>
    </div>
</body>
</html>
"""
        return web.Response(text=fallback_html, content_type="text/html")

    async def healthz(self, request):
        try:
            result = await self.client.admin.command("ping")
            return web.json_response(
                {
                    "ok": True,
                    "status": "healthy",
                    "service": "zmongo",
                    "mongo_ping": result,
                    "db_name": self.db.name,
                },
                status=200,
            )
        except Exception as e:
            return web.json_response(
                {
                    "ok": False,
                    "status": "unhealthy",
                    "service": "zmongo",
                    "error": str(e),
                    "db_name": self.db.name,
                },
                status=503,
            )

    async def create_collection(self, request):
        try:
            data = await request.json()
            coll_name = ((data.get("collection") or data.get("name") or "")).strip()

            if not coll_name:
                return self._error("Missing collection", status=400)

            existing = await self.db.list_collection_names()
            if coll_name in existing:
                return web.json_response(
                    {
                        "success": True,
                        "created": False,
                        "collection": coll_name,
                        "message": "Collection already exists",
                    }
                )

            await self.db.create_collection(coll_name)

            return web.json_response(
                {
                    "success": True,
                    "created": True,
                    "collection": coll_name,
                }
            )
        except Exception as e:
            return self._error(e, status=500)

    async def delete_collection(self, request):
        try:
            data = await request.json()
            coll_name = ((data.get("collection") or data.get("name") or "")).strip()
            force = bool(data.get("force", False))

            if not coll_name:
                return self._error("Missing collection", status=400)

            existing = await self.db.list_collection_names()
            if coll_name not in existing:
                return self._error("Collection not found", status=404)

            if not force:
                count = await self.db[coll_name].count_documents({})
                if count > 0:
                    return self._error(
                        f"Collection '{coll_name}' is not empty. Pass force=true to delete it.",
                        status=400,
                    )

            await self.db.drop_collection(coll_name)

            return web.json_response(
                {
                    "success": True,
                    "deleted": True,
                    "collection": coll_name,
                }
            )
        except Exception as e:
            return self._error(e, status=500)