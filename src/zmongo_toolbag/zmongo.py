import asyncio
import json
import logging
import os
import threading
import time
import weakref
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Tuple, Optional, Union

from bson.objectid import ObjectId
from dotenv import load_dotenv
from motor import motor_asyncio
from pymongo.errors import PyMongoError

from .data_processor import DataProcessor
from .safe_result import SafeResult

ENV_PATH = Path.home() / ".resources" / ".env"
load_dotenv(ENV_PATH)

logger = logging.getLogger(__name__)


class ZMongo:
    """Production-oriented MongoDB helper with SafeResult-based responses and Projection support."""

    DEFAULT_TIMEOUT_SECONDS = 30.0

    def __init__(
            self,
            uri: Optional[str] = None,
            db_name: Optional[str] = None,
            coll_name: Optional[str] = None,
            *,
            cache_enabled: bool = True,
            cache_ttl_seconds: int = 5,
            run_sync_timeout_seconds: float = DEFAULT_TIMEOUT_SECONDS,
    ) -> None:
        self.uri = uri or os.getenv("MONGO_URI", "mongodb://127.0.0.1:27017")
        self.db_name = db_name or os.getenv("MONGO_DATABASE_NAME", "test")
        self.coll_name = coll_name or os.getenv("MONGO_COLLECTION_NAME", "default")

        self.cache_enabled = cache_enabled
        self.cache_ttl_seconds = max(0, int(cache_ttl_seconds))
        self.run_sync_timeout_seconds = float(run_sync_timeout_seconds)

        self._loop = asyncio.new_event_loop()
        self._loop_ready = threading.Event()
        self._closed = False
        self._close_lock = threading.Lock()

        self._thread = threading.Thread(
            target=self._run_event_loop,
            name=f"ZMongoLoop-{id(self)}",
            daemon=True,
        )
        self._thread.start()
        self._loop_ready.wait(timeout=5)

        self._async_clients: "weakref.WeakKeyDictionary[asyncio.AbstractEventLoop, motor_asyncio.AsyncIOMotorClient]" = (
            weakref.WeakKeyDictionary()
        )

        self._cache: Dict[str, Dict[str, Tuple[Any, float]]] = {}
        self._cache_lock = threading.RLock()

    # --- Internal Lifecycle & Loop Management ---

    def _run_event_loop(self) -> None:
        asyncio.set_event_loop(self._loop)
        self._loop_ready.set()
        try:
            self._loop.run_forever()
        except Exception:
            logger.exception("ZMongo background event loop crashed")

    def _ensure_not_closed(self) -> None:
        if self._closed:
            raise RuntimeError("ZMongo instance is closed")

    def close(self) -> None:
        with self._close_lock:
            if self._closed:
                return
            self._closed = True

        try:
            for client in list(self._async_clients.values()):
                try:
                    client.close()
                except Exception:
                    logger.debug("Failed closing Mongo client", exc_info=True)
        except Exception:
            logger.debug("Failed iterating Mongo clients during close", exc_info=True)

        with self._cache_lock:
            self._cache.clear()

        try:
            if self._loop.is_running():
                self._loop.call_soon_threadsafe(self._loop.stop)
        except Exception:
            logger.debug("Failed stopping event loop", exc_info=True)

        try:
            if self._thread.is_alive():
                self._thread.join(timeout=2.0)
        except Exception:
            logger.debug("Failed joining background thread", exc_info=True)

        try:
            if not self._loop.is_closed():
                self._loop.close()
        except Exception:
            logger.debug("Failed closing event loop", exc_info=True)

    def __enter__(self) -> "ZMongo":
        self._ensure_not_closed()
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()

    async def __aenter__(self) -> "ZMongo":
        self._ensure_not_closed()
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        self.close()

    def _client_for_current_loop(self) -> motor_asyncio.AsyncIOMotorClient:
        self._ensure_not_closed()
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            loop = self._loop

        client = self._async_clients.get(loop)
        if client is None:
            client = motor_asyncio.AsyncIOMotorClient(self.uri)
            self._async_clients[loop] = client
        return client

    @property
    def db(self):
        return self._client_for_current_loop()[self.db_name]

    # --- Utilities ---

    @staticmethod
    def _exception_payload(exc: Exception, operation: Optional[str] = None) -> Dict[str, Any]:
        payload = {"error_type": exc.__class__.__name__, "error": str(exc)}
        if operation:
            payload["operation"] = operation
        return payload

    @classmethod
    def _fail(
            cls,
            exc: Exception,
            *,
            operation: Optional[str] = None,
            status_code: int = 500,
            message: Optional[str] = None,
            data: Optional[Any] = None,
    ) -> SafeResult:
        payload = cls._exception_payload(exc, operation=operation)
        return SafeResult.fail(
            error=payload,
            data=data,
            status_code=status_code,
            message=message or str(exc),
        )

    def _normalize_query(self, query: Optional[Dict[str, Any]]) -> Dict[str, Any]:
        query = dict(query or {})
        if "_id" in query and isinstance(query["_id"], str) and ObjectId.is_valid(query["_id"]):
            query["_id"] = ObjectId(query["_id"])
        return query

    @staticmethod
    def _json_key(obj: Any) -> str:
        safe_obj = DataProcessor.to_json_compatible(obj)
        return json.dumps(safe_obj, sort_keys=True, default=str, separators=(",", ":"))

    def _make_cache_key(
            self,
            *,
            operation: str,
            coll: str,
            query: Optional[Dict[str, Any]] = None,
            projection: Optional[Dict[str, Any]] = None,
            extra: Optional[Dict[str, Any]] = None,
    ) -> str:
        payload = {
            "operation": operation,
            "collection": coll,
            "query": self._normalize_query(query),
            "projection": projection,  # Included to avoid cache collision
            "extra": extra or {},
        }
        return self._json_key(payload)

    # --- Caching Logic ---

    def _get_cached(self, coll: str, key: str) -> Optional[Any]:
        if not self.cache_enabled or self.cache_ttl_seconds <= 0:
            return None
        now = time.time()
        with self._cache_lock:
            coll_cache = self._cache.get(coll)
            if not coll_cache:
                return None
            entry = coll_cache.get(key)
            if entry is None:
                return None
            value, expires_at = entry
            if now >= expires_at:
                coll_cache.pop(key, None)
                return None
            return value

    def _set_cached(self, coll: str, key: str, value: Any) -> None:
        if not self.cache_enabled or self.cache_ttl_seconds <= 0:
            return
        expires_at = time.time() + self.cache_ttl_seconds
        with self._cache_lock:
            self._cache.setdefault(coll, {})[key] = (value, expires_at)

    def clear_cache(self, coll: Optional[str] = None) -> None:
        with self._cache_lock:
            if coll is None:
                self._cache.clear()
            else:
                self._cache.pop(coll, None)

    # --- Execution Core ---

    def run_sync(self, coro_or_factory, *args, timeout: Optional[float] = None, **kwargs) -> SafeResult:
        self._ensure_not_closed()
        coro = coro_or_factory if asyncio.iscoroutine(coro_or_factory) else coro_or_factory(*args, **kwargs)
        if not asyncio.iscoroutine(coro):
            return SafeResult.fail(error={"error": "run_sync expected coroutine"}, status_code=500)

        future = asyncio.run_coroutine_threadsafe(coro, self._loop)
        try:
            result = future.result(timeout=timeout or self.run_sync_timeout_seconds)
            return result if isinstance(result, SafeResult) else SafeResult.ok(result)
        except asyncio.TimeoutError as exc:
            future.cancel()
            return self._fail(exc, operation="run_sync", status_code=504, message="Mongo operation timed out")
        except Exception as exc:
            future.cancel()
            return self._fail(exc, operation="run_sync", status_code=500)

    # --- Async Operations ---

    async def find_one_async(
            self,
            coll: Optional[str] = None,
            query: Optional[Dict[str, Any]] = None,
            projection: Optional[Dict[str, Any]] = None,
            *,
            cache: bool = False,
            **kwargs,
    ) -> SafeResult:
        try:
            self._ensure_not_closed()
            coll = coll or self.coll_name
            normalized_query = self._normalize_query(query or {})

            cache_key = self._make_cache_key(
                operation="find_one",
                coll=coll,
                query=normalized_query,
                projection=projection,
                extra={"kwargs": kwargs},
            )

            if cache:
                cached = self._get_cached(coll, cache_key)
                if cached is not None:
                    return SafeResult.ok({"document": cached, "cache_hit": True, "collection": coll})

            doc = await self.db[coll].find_one(normalized_query, projection=projection, **kwargs)

            if cache and doc is not None:
                self._set_cached(coll, cache_key, doc)

            return SafeResult.ok({"document": doc, "cache_hit": False, "collection": coll, "query": normalized_query})
        except Exception as exc:
            return self._fail(exc, operation="find_one")

    async def find_many_async(
            self,
            coll: Optional[str] = None,
            query: Optional[Dict[str, Any]] = None,
            projection: Optional[Dict[str, Any]] = None,
            *,
            sort: Optional[Union[List[Tuple[str, int]], Tuple[str, int]]] = None,
            limit: Optional[int] = 1000,
            cache: bool = False,
    ) -> SafeResult:
        try:
            self._ensure_not_closed()
            coll = coll or self.coll_name
            normalized_query = self._normalize_query(query or {})

            cache_key = self._make_cache_key(
                operation="find_many",
                coll=coll,
                query=normalized_query,
                projection=projection,
                extra={"sort": sort, "limit": limit},
            )

            if cache:
                cached = self._get_cached(coll, cache_key)
                if cached is not None:
                    return SafeResult.ok({"documents": cached, "count": len(cached), "cache_hit": True})

            cursor = self.db[coll].find(normalized_query, projection=projection)
            if sort: cursor = cursor.sort(sort)
            if limit is not None: cursor = cursor.limit(limit)

            docs = await cursor.to_list(length=limit)
            if cache: self._set_cached(coll, cache_key, docs)

            return SafeResult.ok({"documents": docs, "count": len(docs), "cache_hit": False, "collection": coll})
        except Exception as exc:
            return self._fail(exc, operation="find_many")

    async def aggregate_async(self, coll: Optional[str] = None,
                              pipeline: Optional[List[Dict[str, Any]]] = None) -> SafeResult:
        try:
            self._ensure_not_closed()
            coll = coll or self.coll_name
            cursor = self.db[coll].aggregate(pipeline or [])
            docs = await cursor.to_list(length=None)
            return SafeResult.ok({"documents": docs, "count": len(docs), "collection": coll})
        except Exception as exc:
            return self._fail(exc, operation="aggregate")

    async def count_documents_async(self, coll: Optional[str] = None, query: Optional[Dict[str, Any]] = None, *,
                                    cache: bool = False) -> SafeResult:
        try:
            self._ensure_not_closed()
            coll = coll or self.coll_name
            normalized_query = self._normalize_query(query or {})
            cache_key = self._make_cache_key(operation="count_documents", coll=coll, query=normalized_query)

            if cache:
                cached = self._get_cached(coll, cache_key)
                if cached is not None:
                    return SafeResult.ok({"count": cached, "cache_hit": True})

            count = await self.db[coll].count_documents(normalized_query)
            if cache: self._set_cached(coll, cache_key, count)
            return SafeResult.ok({"count": count, "cache_hit": False, "collection": coll})
        except Exception as exc:
            return self._fail(exc, operation="count_documents")

    async def insert_one_async(self, coll: Optional[str] = None, doc: Optional[Dict[str, Any]] = None) -> SafeResult:
        try:
            self._ensure_not_closed()
            coll = coll or self.coll_name
            result = await self.db[coll].insert_one(doc or {})
            self.clear_cache(coll)
            return SafeResult.ok({"inserted_id": result.inserted_id, "collection": coll})
        except Exception as exc:
            return self._fail(exc, operation="insert_one")

    async def insert_many_async(self, coll: Optional[str] = None,
                                docs: Optional[List[Dict[str, Any]]] = None) -> SafeResult:
        try:
            self._ensure_not_closed()
            coll = coll or self.coll_name
            result = await self.db[coll].insert_many(docs or [])
            self.clear_cache(coll)
            return SafeResult.ok({"inserted_ids": result.inserted_ids, "inserted_count": len(result.inserted_ids)})
        except Exception as exc:
            return self._fail(exc, operation="insert_many")

    async def update_one_async(self, coll: Optional[str] = None, query: Optional[Dict[str, Any]] = None,
                               update: Optional[Dict[str, Any]] = None, *, upsert: bool = False) -> SafeResult:
        try:
            self._ensure_not_closed()
            coll = coll or self.coll_name
            normalized_query = self._normalize_query(query or {})
            update_doc = update if any(k.startswith("$") for k in (update or {}).keys()) else {"$set": update or {}}
            result = await self.db[coll].update_one(normalized_query, update_doc, upsert=upsert)
            self.clear_cache(coll)
            return SafeResult.ok({"matched_count": result.matched_count, "modified_count": result.modified_count,
                                  "upserted_id": result.upserted_id})
        except Exception as exc:
            return self._fail(exc, operation="update_one")

    async def update_many_async(self, coll: Optional[str] = None,
                                query_or_ops: Optional[Union[Dict[str, Any], List[Any]]] = None,
                                update: Optional[Dict[str, Any]] = None, *, upsert: bool = False) -> SafeResult:
        try:
            self._ensure_not_closed()
            coll = coll or self.coll_name
            if isinstance(query_or_ops, list):
                result = await self.db[coll].bulk_write(query_or_ops)
                self.clear_cache(coll)
                return SafeResult.ok({"bulk_api_result": getattr(result, "bulk_api_result", None),
                                      "modified_count": getattr(result, "modified_count", None)})

            normalized_query = self._normalize_query(query_or_ops or {})
            update_doc = update if any(k.startswith("$") for k in (update or {}).keys()) else {"$set": update or {}}
            result = await self.db[coll].update_many(normalized_query, update_doc, upsert=upsert)
            self.clear_cache(coll)
            return SafeResult.ok({"matched_count": result.matched_count, "modified_count": result.modified_count})
        except Exception as exc:
            return self._fail(exc, operation="update_many")

    async def delete_one_async(self, coll: Optional[str] = None, query: Optional[Dict[str, Any]] = None) -> SafeResult:
        try:
            self._ensure_not_closed()
            coll = coll or self.coll_name
            result = await self.db[coll].delete_one(self._normalize_query(query or {}))
            self.clear_cache(coll)
            return SafeResult.ok({"deleted_count": result.deleted_count})
        except Exception as exc:
            return self._fail(exc, operation="delete_one")

    async def delete_many_async(self, coll: Optional[str] = None, query: Optional[Dict[str, Any]] = None) -> SafeResult:
        try:
            self._ensure_not_closed()
            coll = coll or self.coll_name
            result = await self.db[coll].delete_many(self._normalize_query(query or {}))
            self.clear_cache(coll)
            return SafeResult.ok({"deleted_count": result.deleted_count})
        except Exception as exc:
            return self._fail(exc, operation="delete_many")

    async def save_value_async(
            self,
            coll: Optional[str] = None,
            value: Any = None,
            *,
            query: Optional[Dict[str, Any]] = None,
            field_path: Optional[str] = None,
            upsert: bool = True,
            parse_json_strings: bool = True,
            normalize_for_storage: bool = False,
    ) -> SafeResult:
        try:
            self._ensure_not_closed()
            coll = coll or self.coll_name
            normalized_query = self._normalize_query(query or {})
            parsed_value = value

            if parse_json_strings and isinstance(value, str) and value.strip():
                try:
                    parsed_value = json.loads(value.strip())
                except:
                    pass

            if normalize_for_storage:
                parsed_value = DataProcessor.to_json_compatible(parsed_value)

            if not normalized_query and isinstance(parsed_value, dict) and parsed_value.get("_id"):
                normalized_query = self._normalize_query({"_id": parsed_value["_id"]})

            update_payload = {str(field_path).strip(): parsed_value} if field_path else (
                parsed_value if isinstance(parsed_value, dict) else {"value": parsed_value})
            if "_id" in update_payload and isinstance(update_payload, dict): update_payload.pop("_id", None)

            if not normalized_query:
                if not upsert: return SafeResult.fail(error={"error": "No query/upsert"}, status_code=400)
                result = await self.db[coll].insert_one(update_payload)
                self.clear_cache(coll)
                return SafeResult.ok({"operation": "inserted_new", "inserted_id": result.inserted_id})

            result = await self.db[coll].update_one(normalized_query, {"$set": update_payload}, upsert=upsert)
            self.clear_cache(coll)
            return SafeResult.ok(
                {"operation": "upserted" if result.upserted_id else "updated", "upserted_id": result.upserted_id})
        except Exception as exc:
            return self._fail(exc, operation="save_value")

    async def list_collections_async(self) -> SafeResult:
        try:
            self._ensure_not_closed()
            names = await self.db.list_collection_names()
            return SafeResult.ok({"collections": names, "count": len(names)})
        except Exception as exc:
            return self._fail(exc, operation="list_collections")

    async def drop_database_async(self, database_name: Optional[str] = None) -> SafeResult:
        try:
            self._ensure_not_closed()
            name = database_name or self.db_name
            await self._client_for_current_loop().drop_database(name)
            self.clear_cache()
            return SafeResult.ok({"dropped_database": name})
        except Exception as exc:
            return self._fail(exc, operation="drop_database")

    async def ping_async(self) -> SafeResult:
        try:
            self._ensure_not_closed()
            start = time.time()
            await self.db.command({"ping": 1})
            return SafeResult.ok({"ok": True, "latency": time.time() - start})
        except Exception as exc:
            return self._fail(exc, operation="ping")

    async def sync_timestamp_async(self) -> SafeResult:
        try:
            self._ensure_not_closed()
            try:
                result = await self.db.command({"hello": 1})
            except PyMongoError:
                result = await self.db.command({"isMaster": 1})
            server_time = result.get("localTime")
            if server_time and server_time.tzinfo is None: server_time = server_time.replace(tzinfo=timezone.utc)
            return SafeResult.ok({"server_time": server_time})
        except Exception as exc:
            return self._fail(exc, operation="sync_timestamp")

    # --- Synchronous Wrappers ---

    def find_one(self, coll: Optional[str] = None, query: Optional[Dict[str, Any]] = None,
                 projection: Optional[Dict[str, Any]] = None, *, cache: bool = False, **kwargs) -> SafeResult:
        return self.run_sync(self.find_one_async, coll, query, projection, cache=cache, **kwargs)

    def find_many(self, coll: Optional[str] = None, query: Optional[Dict[str, Any]] = None,
                  projection: Optional[Dict[str, Any]] = None, *, sort=None, limit=1000, cache=False) -> SafeResult:
        return self.run_sync(self.find_many_async, coll, query, projection, sort=sort, limit=limit, cache=cache)

    def aggregate(self, coll: Optional[str] = None, pipeline: Optional[List[Dict[str, Any]]] = None) -> SafeResult:
        return self.run_sync(self.aggregate_async, coll, pipeline)

    def count_documents(self, coll: Optional[str] = None, query: Optional[Dict[str, Any]] = None, *,
                        cache: bool = False) -> SafeResult:
        return self.run_sync(self.count_documents_async, coll, query, cache=cache)

    def insert_one(self, coll: Optional[str] = None, doc: Optional[Dict[str, Any]] = None) -> SafeResult:
        return self.run_sync(self.insert_one_async, coll, doc)

    def insert_many(self, coll: Optional[str] = None, docs: Optional[List[Dict[str, Any]]] = None) -> SafeResult:
        return self.run_sync(self.insert_many_async, coll, docs)

    def update_one(self, coll: Optional[str] = None, query: Optional[Dict[str, Any]] = None,
                   update: Optional[Dict[str, Any]] = None, *, upsert: bool = False) -> SafeResult:
        return self.run_sync(self.update_one_async, coll, query, update, upsert=upsert)

    def update_many(self, coll: Optional[str] = None, query_or_ops=None, update=None, *, upsert=False) -> SafeResult:
        return self.run_sync(self.update_many_async, coll, query_or_ops, update, upsert=upsert)

    def delete_one(self, coll: Optional[str] = None, query: Optional[Dict[str, Any]] = None) -> SafeResult:
        return self.run_sync(self.delete_one_async, coll, query)

    def delete_many(self, coll: Optional[str] = None, query: Optional[Dict[str, Any]] = None) -> SafeResult:
        return self.run_sync(self.delete_many_async, coll, query)

    def list_collections(self) -> SafeResult:
        return self.run_sync(self.list_collections_async)

    def save_value(self, coll: Optional[str] = None, value=None, *, query=None, field_path=None, upsert=True,
                   parse_json_strings=True, normalize_for_storage=False) -> SafeResult:
        return self.run_sync(self.save_value_async, coll, value, query=query, field_path=field_path, upsert=upsert,
                             parse_json_strings=parse_json_strings, normalize_for_storage=normalize_for_storage)

    def drop_database(self, database_name: Optional[str] = None) -> SafeResult:
        return self.run_sync(self.drop_database_async, database_name)

    def ping(self) -> SafeResult:
        return self.run_sync(self.ping_async)

    def sync_timestamp(self) -> SafeResult:
        return self.run_sync(self.sync_timestamp_async)