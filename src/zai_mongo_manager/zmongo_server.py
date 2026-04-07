import os
import json
from aiohttp import web
from motor.motor_asyncio import AsyncIOMotorClient
from bson.objectid import ObjectId
from bson import json_util


class ZMongoServer:
    def __init__(self):
        # Using a 5-second timeout so ComfyUI doesn't hang if Mongo is down
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
        """Standardizes BSON for JSON responses."""
        return json.loads(json_util.dumps(data))

    def _error(self, message, status=400):
        return web.json_response({"error": str(message)}, status=status)

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
            limit = min(int(request.query.get("limit", 50)), 500)
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
            doc = await self.db[coll_name].find_one({"_id": ObjectId(doc_id)})
            if doc:
                return web.json_response(self._serialize(doc))
            return web.json_response({"error": "Document not found"}, status=404)
        except Exception as e:
            return web.json_response({"error": f"Invalid ID format: {str(e)}"}, status=400)

    async def update_doc(self, request):
        try:
            data = await request.json()
            coll_name = data.get("collection")
            doc_id = data.get("id")
            update_data = data.get("update")

            if not coll_name:
                return self._error("Missing collection", status=400)
            if not doc_id:
                return self._error("Missing id", status=400)
            if not isinstance(update_data, dict):
                return self._error("update must be an object", status=400)

            # Defensive check: MongoDB won't let you update the _id field
            update_data.pop("_id", None)

            result = await self.db[coll_name].update_one(
                {"_id": ObjectId(doc_id)},
                {"$set": update_data}
            )

            updated_doc = await self.db[coll_name].find_one({"_id": ObjectId(doc_id)})

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
            coll_name = data.get("collection")
            document = data.get("document", {})

            if not coll_name:
                return self._error("Missing collection", status=400)
            if not isinstance(document, dict):
                return self._error("document must be an object", status=400)

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
            coll_name = data.get("collection")
            doc_id = data.get("id")

            if not coll_name:
                return self._error("Missing collection", status=400)
            if not doc_id:
                return self._error("Missing id", status=400)

            result = await self.db[coll_name].delete_one({"_id": ObjectId(doc_id)})

            return web.json_response(
                {
                    "success": True,
                    "deleted_count": result.deleted_count,
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