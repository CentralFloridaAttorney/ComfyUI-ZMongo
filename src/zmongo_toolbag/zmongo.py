import os
import asyncio
import threading
import weakref
import logging
import time
from datetime import timezone, datetime
from typing import Any, Dict, List, Optional, Union

from motor import motor_asyncio
from bson.objectid import ObjectId

from .safe_result import SafeResult
from .zmongo_response_result import ZMongoResponseResult

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class ZMongo:
    """
    Final Production Version of ZMongo Helper.
    Synchronizes API expectations across all test suites including retriever,
    embedder, and core database tests.
    """

    def __init__(self, uri: Optional[str] = None, db_name: Optional[str] = None):
        self.uri = uri or os.getenv("MONGO_URI", "mongodb://127.0.0.1:27017")
        self.db_name = db_name or os.getenv("MONGO_DATABASE_NAME", "test")

        self._loop = asyncio.new_event_loop()
        self._thread = threading.Thread(target=self._run_event_loop, daemon=True)
        self._thread.start()

        self._async_clients: weakref.WeakKeyDictionary = weakref.WeakKeyDictionary()
        self.caches: Dict[str, Dict[str, Any]] = {}

    def _run_event_loop(self) -> None:
        asyncio.set_event_loop(self._loop)
        try:
            self._loop.run_forever()
        except Exception as e:
            logger.error(f"ZMongo background loop died: {e!r}")

    def _client_for_async(self) -> motor_asyncio.AsyncIOMotorClient:
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            loop = self._loop

        cli = self._async_clients.get(loop)
        if cli is None:
            cli = motor_asyncio.AsyncIOMotorClient(self.uri)
            self._async_clients[loop] = cli
        return cli

    @property
    def db(self):
        return self._client_for_async()[self.db_name]

    def _normalize_query(self, query: Dict[str, Any]) -> Dict[str, Any]:
        """Ensures _id is a BSON ObjectId for matching."""
        if query and "_id" in query and isinstance(query["_id"], str):
            if ObjectId.is_valid(query["_id"]):
                query["_id"] = ObjectId(query["_id"])
        return query

    def run_sync(self, coro_func_or_coro, *args, **kwargs) -> Any:
        """Fixed to handle both functions and coroutine objects."""
        if asyncio.iscoroutine(coro_func_or_coro):
            coro = coro_func_or_coro
        else:
            coro = coro_func_or_coro(*args, **kwargs)

        fut = asyncio.run_coroutine_threadsafe(coro, self._loop)
        try:
            return fut.result(timeout=30)
        except Exception as e:
            return SafeResult.fail(str(e))

    # ------------------------------------------------------------
    # Core Async API
    # ------------------------------------------------------------

    async def update_one_async(self, coll: str, query: Dict[str, Any], update: Dict[str, Any],
                               upsert: bool = False) -> SafeResult:
        try:
            self.clear_cache(coll)
            query = self._normalize_query(query)
            if not any(k.startswith("$") for k in update.keys()):
                update = {"$set": update}
            r = await self.db[coll].update_one(query, update, upsert=upsert)
            return SafeResult.ok({"modified_count": r.modified_count, "upserted_id": r.upserted_id})
        except Exception as e:
            return SafeResult.fail(str(e))

    async def update_many_async(self, coll: str, query_or_ops: Union[Dict, List], update: Optional[Dict] = None,
                                upsert: bool = False) -> SafeResult:
        try:
            self.clear_cache(coll)
            if isinstance(query_or_ops, list):
                r = await self.db[coll].bulk_write(query_or_ops)
                return SafeResult.ok({"modified_count": r.modified_count, "deleted_count": r.deleted_count})

            query = self._normalize_query(query_or_ops)
            if update and not any(k.startswith("$") for k in update.keys()):
                update = {"$set": update}
            r = await self.db[coll].update_many(query, update, upsert=upsert)
            return SafeResult.ok({"modified_count": r.modified_count, "matched_count": r.matched_count})
        except Exception as e:
            return SafeResult.fail(str(e))

    async def find_many_async(self, coll: str, query: Optional[Dict[str, Any]] = None, sort=None,
                              limit=1000) -> SafeResult:
        try:
            query = self._normalize_query(query or {})
            cursor = self.db[coll].find(query)
            if sort: cursor = cursor.sort(sort)
            if limit: cursor = cursor.limit(limit)
            docs = await cursor.to_list(length=limit)
            return SafeResult.ok(docs)
        except Exception as e:
            return SafeResult.fail(str(e))

    async def sync_timestamp_async(self) -> SafeResult:
        try:
            start = time.time()
            res = await self.db.command("isMaster")
            end = time.time()
            server_time = res.get("localTime")
            if server_time.tzinfo is None: server_time = server_time.replace(tzinfo=timezone.utc)
            return SafeResult.ok({
                "server_time": server_time,
                "latency_seconds": end - start,
                "offset_seconds": (server_time - datetime.now(timezone.utc)).total_seconds()
            })
        except Exception as e:
            return SafeResult.fail(str(e))

    # ------------------------------------------------------------
    # Core Sync API
    # ------------------------------------------------------------

    def find_one(self, coll: str, query: Dict[str, Any], cache: bool = False, **kwargs) -> SafeResult:
        async def _logic():
            q = self._normalize_query(query)
            if cache:
                cache_key = str(q)
                if coll in self.caches and cache_key in self.caches[coll]:
                    return self.caches[coll][cache_key]
                doc = await self.db[coll].find_one(q, **kwargs)
                if coll not in self.caches: self.caches[coll] = {}
                self.caches[coll][cache_key] = doc
                return doc
            return await self.db[coll].find_one(q, **kwargs)

        return SafeResult.ok(self.run_sync(_logic))

    def find_many(self, coll: str, query: Optional[Dict[str, Any]] = None, sort=None, limit=1000) -> SafeResult:
        return self.run_sync(self.find_many_async, coll, query, sort, limit)

    def insert_one(self, coll: str, doc: Dict[str, Any]) -> SafeResult:
        async def _logic():
            r = await self.db[coll].insert_one(doc)
            return SafeResult.ok({"inserted_id": r.inserted_id})

        return self.run_sync(_logic)

    def insert_many(self, coll: str, docs: List[Dict[str, Any]]) -> SafeResult:
        async def _logic():
            r = await self.db[coll].insert_many(docs)
            return SafeResult.ok({"inserted_ids": r.inserted_ids})

        return self.run_sync(_logic)

    def update_one(self, coll: str, query: Dict[str, Any], update: Dict[str, Any], upsert: bool = False) -> SafeResult:
        return self.run_sync(self.update_one_async, coll, query, update, upsert)

    def update_many(self, coll: str, query: Dict[str, Any], update: Dict[str, Any], upsert: bool = False) -> SafeResult:
        return self.run_sync(self.update_many_async, coll, query, update, upsert)

    def delete_many(self, coll: str, query: Dict[str, Any]) -> SafeResult:
        async def _logic():
            r = await self.db[coll].delete_many(self._normalize_query(query))
            return SafeResult.ok({"deleted_count": r.deleted_count})

        return self.run_sync(_logic)

    def count_documents(self, coll: str, query: Dict[str, Any]) -> SafeResult:
        async def _logic():
            c = await self.db[coll].count_documents(self._normalize_query(query))
            return SafeResult.ok(c)

        return self.run_sync(_logic)

    def sync_timestamp(self) -> SafeResult:
        return self.run_sync(self.sync_timestamp_async)

    # ------------------------------------------------------------
    # Legacy Hooks
    # ------------------------------------------------------------

    def delete_one(self, collection_name: str, filter_doc: Dict[str, Any]) -> ZMongoResponseResult:
        async def _logic():
            r = await self.db[collection_name].delete_one(self._normalize_query(filter_doc))
            return ZMongoResponseResult(success=True, data={"deleted_count": r.deleted_count})

        return self.run_sync(_logic)

    def delete_all_documents(self, coll: str) -> SafeResult:
        return self.delete_many(coll, {})

    def delete_documents(self, coll: str, query: Dict[str, Any]) -> SafeResult:
        return self.delete_many(coll, query)

    def insert_documents(self, coll: str, docs: List[Dict[str, Any]], **kwargs) -> SafeResult:
        return self.insert_many(coll, docs)

    def clear_cache(self, coll: str) -> None:
        self.caches.pop(coll, None)

    def drop_database(self, database_name: str) -> SafeResult:
        async def _logic():
            await self._client_for_async().drop_database(database_name)
            return SafeResult.ok(True)

        return self.run_sync(_logic)

    def close(self) -> None:
        self._loop.call_soon_threadsafe(self._loop.stop)

    # ------------------------------------------------------------
    # Core Async API
    # ------------------------------------------------------------

    async def insert_one_async(self, coll: str, doc: Dict[str, Any]) -> SafeResult:
        try:
            r = await self.db[coll].insert_one(doc)
            return SafeResult.ok({"inserted_id": r.inserted_id})
        except Exception as e:
            return SafeResult.fail(str(e))

    async def insert_many_async(self, coll: str, docs: List[Dict[str, Any]]) -> SafeResult:
        try:
            r = await self.db[coll].insert_many(docs)
            return SafeResult.ok({"inserted_ids": r.inserted_ids})
        except Exception as e:
            return SafeResult.fail(str(e))

    async def find_one_async(self, coll: str, query: Dict[str, Any], **kwargs) -> SafeResult:
        try:
            query = self._normalize_query(query)
            kwargs.pop('cache', None)
            doc = await self.db[coll].find_one(query, **kwargs)
            return SafeResult.ok(doc)
        except Exception as e:
            return SafeResult.fail(str(e))

    async def delete_many_async(self, coll: str, query: Dict[str, Any]) -> SafeResult:
        try:
            query = self._normalize_query(query)
            r = await self.db[coll].delete_many(query)
            return SafeResult.ok({"deleted_count": r.deleted_count})
        except Exception as e:
            return SafeResult.fail(str(e))

    async def insert_or_update_async(self, coll: str, q_or_doc: Dict[str, Any],
                                     data: Optional[Dict[str, Any]] = None) -> SafeResult:
        try:
            if data is None:
                query = {"_id": q_or_doc.get("_id")}
                update = {"$set": {k: v for k, v in q_or_doc.items() if k != "_id"}}
            else:
                query = q_or_doc
                update = {"$set": data} if not any(k.startswith("$") for k in data) else data
            r = await self.db[coll].update_one(query, update, upsert=True)
            return SafeResult.ok({"upserted_id": r.upserted_id, "modified_count": r.modified_count})
        except Exception as e:
            return SafeResult.fail(str(e))

    async def aggregate_async(self, coll: str, pipeline: List[Dict[str, Any]]) -> SafeResult:
        try:
            cursor = self.db[coll].aggregate(pipeline)
            docs = await cursor.to_list(length=None)
            return SafeResult.ok(docs)
        except Exception as e:
            return SafeResult.fail(str(e))


    async def count_documents_async(self, coll: str, query: Dict[str, Any]) -> SafeResult:
        try:
            count = await self.db[coll].count_documents(self._normalize_query(query))
            return SafeResult.ok(count)
        except Exception as e:
            return SafeResult.fail(str(e))
    # ------------------------------------------------------------
    # Core Sync API
    # ------------------------------------------------------------

    def insert_or_update(self, coll: str, q: Dict[str, Any], d: Optional[Dict[str, Any]] = None) -> SafeResult:
        return self.run_sync(self.insert_or_update_async, coll, q, d)

    def aggregate(self, coll: str, pipeline: List[Dict[str, Any]]) -> SafeResult:
        return self.run_sync(self.aggregate_async, coll, pipeline)

    async def list_collections_async(self) -> SafeResult:
        try:
            names = await self.db.list_collection_names()
            return SafeResult.ok({"collections": names})
        except Exception as e:
            return SafeResult.fail(str(e))

    def list_collections(self) -> SafeResult:
        return self.run_sync(self.list_collections_async)

    # ------------------------------------------------------------
    # Core Async API
    # ------------------------------------------------------------

    # --- Methods for Test Compatibility ---

    async def drop_database_async(self, database_name: str) -> SafeResult:
        await self._client_for_async().drop_database(database_name)
        return SafeResult.ok(True)

    # ------------------------------------------------------------
    # Async Methods (Fixed for Missing Attributes)
    # ------------------------------------------------------------

    async def insert_documents_async(self, coll: str, docs: List[Dict[str, Any]]) -> SafeResult:
        try:
            r = await self.db[coll].insert_many(docs)
            return SafeResult.ok({"inserted_ids": r.inserted_ids})
        except Exception as e:
            return SafeResult.fail(str(e))
