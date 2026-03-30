import os
import asyncio
import threading
import weakref
import logging
import time
from datetime import timezone, datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

from dotenv import load_dotenv
from motor import motor_asyncio
from bson import ObjectId

from .safe_result import SafeResult

ENV_PATH = Path.home() / ".resources" / ".env"
load_dotenv(ENV_PATH)


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class ZMongo:
    """
    Production version of ZMongo helper using SafeResult consistently.
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
        except Exception as exc:
            logger.error("ZMongo background loop died: %r", exc)

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
        if query and "_id" in query and isinstance(query["_id"], str):
            if ObjectId.is_valid(query["_id"]):
                query["_id"] = ObjectId(query["_id"])
        return query

    def run_sync(self, coro_func_or_coro, *args, **kwargs) -> Any:
        if asyncio.iscoroutine(coro_func_or_coro):
            coro = coro_func_or_coro
        else:
            coro = coro_func_or_coro(*args, **kwargs)

        fut = asyncio.run_coroutine_threadsafe(coro, self._loop)
        try:
            return fut.result(timeout=30)
        except Exception as exc:
            return SafeResult.fail(str(exc))

    def clear_cache(self, coll: str) -> None:
        self.caches.pop(coll, None)

    async def update_one_async(
        self,
        coll: str,
        query: Dict[str, Any],
        update: Dict[str, Any],
        upsert: bool = False,
    ) -> SafeResult:
        try:
            self.clear_cache(coll)
            query = self._normalize_query(query)
            if not any(k.startswith("$") for k in update.keys()):
                update = {"$set": update}
            result = await self.db[coll].update_one(query, update, upsert=upsert)
            return SafeResult.ok(
                {"modified_count": result.modified_count, "upserted_id": result.upserted_id}
            )
        except Exception as exc:
            return SafeResult.fail(str(exc))

    async def update_many_async(
        self,
        coll: str,
        query_or_ops: Union[Dict[str, Any], List[Any]],
        update: Optional[Dict[str, Any]] = None,
        upsert: bool = False,
    ) -> SafeResult:
        try:
            self.clear_cache(coll)
            if isinstance(query_or_ops, list):
                result = await self.db[coll].bulk_write(query_or_ops)
                return SafeResult.ok(
                    {
                        "modified_count": result.modified_count,
                        "deleted_count": result.deleted_count,
                    }
                )

            query = self._normalize_query(query_or_ops)
            if update and not any(k.startswith("$") for k in update.keys()):
                update = {"$set": update}

            result = await self.db[coll].update_many(query, update, upsert=upsert)
            return SafeResult.ok(
                {
                    "modified_count": result.modified_count,
                    "matched_count": result.matched_count,
                }
            )
        except Exception as exc:
            return SafeResult.fail(str(exc))

    async def find_many_async(
        self,
        coll: str,
        query: Optional[Dict[str, Any]] = None,
        sort=None,
        limit: Optional[int] = 1000,
    ) -> SafeResult:
        try:
            query = self._normalize_query(query or {})
            cursor = self.db[coll].find(query)
            if sort:
                cursor = cursor.sort(sort)
            if limit:
                cursor = cursor.limit(limit)
            docs = await cursor.to_list(length=limit)
            return SafeResult.ok(docs)
        except Exception as exc:
            return SafeResult.fail(str(exc))

    async def sync_timestamp_async(self) -> SafeResult:
        try:
            start = time.time()
            res = await self.db.command("isMaster")
            end = time.time()
            server_time = res.get("localTime")
            if server_time.tzinfo is None:
                server_time = server_time.replace(tzinfo=timezone.utc)
            return SafeResult.ok(
                {
                    "server_time": server_time,
                    "latency_seconds": end - start,
                    "offset_seconds": (
                        server_time - datetime.now(timezone.utc)
                    ).total_seconds(),
                }
            )
        except Exception as exc:
            return SafeResult.fail(str(exc))

    async def find_one(self, coll: str, query: Dict[str, Any], cache: bool = False, **kwargs) -> SafeResult:
        async def _logic():
            normalized = self._normalize_query(query)
            if cache:
                cache_key = str(normalized)
                if coll in self.caches and cache_key in self.caches[coll]:
                    return self.caches[coll][cache_key]
                doc = await self.db[coll].find_one(normalized, **kwargs)
                self.caches.setdefault(coll, {})[cache_key] = doc
                return doc
            return await self.db[coll].find_one(normalized, **kwargs)

        try:
            return SafeResult.ok(await _logic())
        except Exception as exc:
            return SafeResult.fail(str(exc))

    async def find_many(
        self,
        coll: str,
        query: Optional[Dict[str, Any]] = None,
        sort=None,
        limit: Optional[int] = 1000,
    ) -> SafeResult:
        return await self.find_many_async(coll, query, sort, limit)

    async def insert_one(self, coll: str, doc: Dict[str, Any]) -> SafeResult:
        try:
            result = await self.db[coll].insert_one(doc)
            return SafeResult.ok({"inserted_id": result.inserted_id})
        except Exception as exc:
            return SafeResult.fail(str(exc))

    async def insert_many(self, coll: str, docs: List[Dict[str, Any]]) -> SafeResult:
        try:
            result = await self.db[coll].insert_many(docs)
            return SafeResult.ok({"inserted_ids": result.inserted_ids})
        except Exception as exc:
            return SafeResult.fail(str(exc))

    async def update_one(
        self,
        coll: str,
        query: Dict[str, Any],
        update: Dict[str, Any],
        upsert: bool = False,
    ) -> SafeResult:
        return await self.update_one_async(coll, query, update, upsert)

    async def update_many(
        self,
        coll: str,
        query: Dict[str, Any],
        update: Dict[str, Any],
        upsert: bool = False,
    ) -> SafeResult:
        return await self.update_many_async(coll, query, update, upsert)

    async def delete_many(self, coll: str, query: Dict[str, Any]) -> SafeResult:
        try:
            result = await self.db[coll].delete_many(self._normalize_query(query))
            return SafeResult.ok({"deleted_count": result.deleted_count})
        except Exception as exc:
            return SafeResult.fail(str(exc))

    async def count_documents(self, coll: str, query: Dict[str, Any]) -> SafeResult:
        try:
            count = await self.db[coll].count_documents(self._normalize_query(query))
            return SafeResult.ok(count)
        except Exception as exc:
            return SafeResult.fail(str(exc))

    async def sync_timestamp(self) -> SafeResult:
        return await self.sync_timestamp_async()

    async def delete_one(self, collection_name: str, filter_doc: Dict[str, Any]) -> SafeResult:
        try:
            result = await self.db[collection_name].delete_one(self._normalize_query(filter_doc))
            return SafeResult.ok({"deleted_count": result.deleted_count})
        except Exception as exc:
            return SafeResult.fail(str(exc))

    def delete_all_documents(self, coll: str):
        return self.run_sync(self.delete_many(coll, {}))

    def delete_documents(self, coll: str, query: Dict[str, Any]):
        return self.run_sync(self.delete_many(coll, query))

    def insert_documents(self, coll: str, docs: List[Dict[str, Any]], **kwargs):
        return self.run_sync(self.insert_many(coll, docs))

    async def drop_database(self, database_name: str) -> SafeResult:
        try:
            await self._client_for_async().drop_database(database_name)
            return SafeResult.ok(True)
        except Exception as exc:
            return SafeResult.fail(str(exc))

    def close(self) -> None:
        self._loop.call_soon_threadsafe(self._loop.stop)

    async def insert_one_async(self, coll: str, doc: Dict[str, Any]) -> SafeResult:
        try:
            result = await self.db[coll].insert_one(doc)
            return SafeResult.ok({"inserted_id": result.inserted_id})
        except Exception as exc:
            return SafeResult.fail(str(exc))

    async def insert_many_async(self, coll: str, docs: List[Dict[str, Any]]) -> SafeResult:
        try:
            result = await self.db[coll].insert_many(docs)
            return SafeResult.ok({"inserted_ids": result.inserted_ids})
        except Exception as exc:
            return SafeResult.fail(str(exc))

    async def find_one_async(self, coll: str, query: Dict[str, Any], **kwargs) -> SafeResult:
        try:
            query = self._normalize_query(query)
            kwargs.pop("cache", None)
            doc = await self.db[coll].find_one(query, **kwargs)
            return SafeResult.ok(doc)
        except Exception as exc:
            return SafeResult.fail(str(exc))

    async def delete_many_async(self, coll: str, query: Dict[str, Any]) -> SafeResult:
        try:
            query = self._normalize_query(query)
            result = await self.db[coll].delete_many(query)
            return SafeResult.ok({"deleted_count": result.deleted_count})
        except Exception as exc:
            return SafeResult.fail(str(exc))

    async def insert_or_update_async(
        self,
        coll: str,
        q_or_doc: Dict[str, Any],
        data: Optional[Dict[str, Any]] = None,
    ) -> SafeResult:
        try:
            if data is None:
                query = {"_id": q_or_doc.get("_id")}
                query = self._normalize_query(query)
                update = {"$set": {k: v for k, v in q_or_doc.items() if k != "_id"}}
            else:
                query = self._normalize_query(dict(q_or_doc))
                update = {"$set": data} if not any(k.startswith("$") for k in data) else data

            result = await self.db[coll].update_one(query, update, upsert=True)
            self.clear_cache(coll)
            return SafeResult.ok(
                {"upserted_id": result.upserted_id, "modified_count": result.modified_count}
            )
        except Exception as exc:
            return SafeResult.fail(str(exc))

    async def aggregate_async(self, coll: str, pipeline: List[Dict[str, Any]]) -> SafeResult:
        try:
            cursor = self.db[coll].aggregate(pipeline)
            docs = await cursor.to_list(length=None)
            return SafeResult.ok(docs)
        except Exception as exc:
            return SafeResult.fail(str(exc))

    async def count_documents_async(self, coll: str, query: Dict[str, Any]) -> SafeResult:
        try:
            count = await self.db[coll].count_documents(self._normalize_query(query))
            return SafeResult.ok(count)
        except Exception as exc:
            return SafeResult.fail(str(exc))

    async def insert_or_update(self, coll: str, q: Dict[str, Any], d: Optional[Dict[str, Any]] = None) -> SafeResult:
        return await self.insert_or_update_async(coll, q, d)

    async def aggregate(self, coll: str, pipeline: List[Dict[str, Any]]) -> SafeResult:
        return await self.aggregate_async(coll, pipeline)

    async def list_collections_async(self) -> SafeResult:
        try:
            names = await self.db.list_collection_names()
            return SafeResult.ok({"collections": names})
        except Exception as exc:
            return SafeResult.fail(str(exc))

    async def list_collections(self) -> SafeResult:
        return await self.list_collections_async()

    async def drop_database_async(self, database_name: str) -> SafeResult:
        try:
            await self._client_for_async().drop_database(database_name)
            return SafeResult.ok(True)
        except Exception as exc:
            return SafeResult.fail(str(exc))

    async def insert_documents_async(self, coll: str, docs: List[Dict[str, Any]]) -> SafeResult:
        try:
            result = await self.db[coll].insert_many(docs)
            return SafeResult.ok({"inserted_ids": result.inserted_ids})
        except Exception as exc:
            return SafeResult.fail(str(exc))
