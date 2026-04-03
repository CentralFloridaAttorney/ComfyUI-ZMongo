import asyncio
import json
import logging
import os
import threading
import time
import weakref
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, Union

from bson import ObjectId
from dotenv import load_dotenv
from motor import motor_asyncio
from pymongo.errors import PyMongoError

from .data_processor import DataProcessor
from .safe_result import SafeResult

ENV_PATH = Path.home() / ".resources" / ".env"
load_dotenv(ENV_PATH)

logger = logging.getLogger(__name__)


class ZMongo:
    """Production-oriented MongoDB helper with SafeResult-based responses."""

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
        # Added default collection name support
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

    @staticmethod
    def _exception_payload(exc: Exception, operation: Optional[str] = None) -> Dict[str, Any]:
        payload = {
            "error_type": exc.__class__.__name__,
            "error": str(exc),
        }
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
        extra: Optional[Dict[str, Any]] = None,
    ) -> str:
        payload = {
            "operation": operation,
            "collection": coll,
            "query": self._normalize_query(query),
            "extra": extra or {},
        }
        return self._json_key(payload)

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
                if not coll_cache:
                    self._cache.pop(coll, None)
                return None
            return value

    def _set_cached(self, coll: str, key: str, value: Any) -> None:
        if not self.cache_enabled or self.cache_ttl_seconds <= 0:
            return

        expires_at = time.time() + self.cache_ttl_seconds
        with self._cache_lock:
            self._cache.setdefault(coll, {})[key] = (value, expires_at)

    def clear_cache(self, coll: Optional[str] = None) -> None:
        # Defaults to clearing the specific default collection if None provided
        # Or you can keep it as is (clearing ALL) - usually clear_cache(None) means clear all.
        with self._cache_lock:
            if coll is None:
                self._cache.clear()
            else:
                self._cache.pop(coll, None)

    def run_sync(self, coro_or_factory, *args, timeout: Optional[float] = None, **kwargs) -> SafeResult:
        self._ensure_not_closed()

        if asyncio.iscoroutine(coro_or_factory):
            coro = coro_or_factory
        else:
            coro = coro_or_factory(*args, **kwargs)

        if not asyncio.iscoroutine(coro):
            return SafeResult.fail(
                error={
                    "error_type": "TypeError",
                    "error": "run_sync expected a coroutine or coroutine factory",
                },
                status_code=500,
                message="run_sync misuse",
            )

        future = asyncio.run_coroutine_threadsafe(coro, self._loop)

        try:
            result = future.result(timeout=timeout or self.run_sync_timeout_seconds)
            if isinstance(result, SafeResult):
                return result
            return SafeResult.ok(result)
        except asyncio.TimeoutError as exc:
            future.cancel()
            return self._fail(exc, operation="run_sync", status_code=504, message="Mongo operation timed out")
        except Exception as exc:
            future.cancel()
            return self._fail(exc, operation="run_sync", status_code=500)

    async def find_one_async(
        self,
        coll: Optional[str] = None,
        query: Optional[Dict[str, Any]] = None,
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
                extra={"kwargs": kwargs},
            )

            if cache:
                cached = self._get_cached(coll, cache_key)
                if cached is not None:
                    return SafeResult.ok(
                        {
                            "document": cached,
                            "cache_hit": True,
                            "collection": coll,
                            "query": normalized_query,
                        }
                    )

            doc = await self.db[coll].find_one(normalized_query, **kwargs)

            if cache and doc is not None:
                self._set_cached(coll, cache_key, doc)

            return SafeResult.ok(
                {
                    "document": doc,
                    "cache_hit": False,
                    "collection": coll,
                    "query": normalized_query,
                }
            )
        except Exception as exc:
            return self._fail(exc, operation="find_one")

    async def find_many_async(
        self,
        coll: Optional[str] = None,
        query: Optional[Dict[str, Any]] = None,
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
                extra={"sort": sort, "limit": limit},
            )

            if cache:
                cached = self._get_cached(coll, cache_key)
                if cached is not None:
                    return SafeResult.ok(
                        {
                            "documents": cached,
                            "count": len(cached),
                            "cache_hit": True,
                            "collection": coll,
                            "query": normalized_query,
                        }
                    )

            cursor = self.db[coll].find(normalized_query)
            if sort:
                cursor = cursor.sort(sort)
            if limit is not None:
                cursor = cursor.limit(limit)

            docs = await cursor.to_list(length=limit)
            if cache:
                self._set_cached(coll, cache_key, docs)

            return SafeResult.ok(
                {
                    "documents": docs,
                    "count": len(docs),
                    "cache_hit": False,
                    "collection": coll,
                    "query": normalized_query,
                }
            )
        except Exception as exc:
            return self._fail(exc, operation="find_many")

    async def aggregate_async(self, coll: Optional[str] = None, pipeline: Optional[List[Dict[str, Any]]] = None) -> SafeResult:
        try:
            self._ensure_not_closed()
            coll = coll or self.coll_name
            cursor = self.db[coll].aggregate(pipeline or [])
            docs = await cursor.to_list(length=None)
            return SafeResult.ok(
                {
                    "documents": docs,
                    "count": len(docs),
                    "collection": coll,
                    "pipeline": pipeline,
                }
            )
        except Exception as exc:
            return self._fail(exc, operation="aggregate")

    async def count_documents_async(
        self,
        coll: Optional[str] = None,
        query: Optional[Dict[str, Any]] = None,
        *,
        cache: bool = False,
    ) -> SafeResult:
        try:
            self._ensure_not_closed()
            coll = coll or self.coll_name
            normalized_query = self._normalize_query(query or {})

            cache_key = self._make_cache_key(
                operation="count_documents",
                coll=coll,
                query=normalized_query,
            )

            if cache:
                cached = self._get_cached(coll, cache_key)
                if cached is not None:
                    return SafeResult.ok(
                        {
                            "count": cached,
                            "cache_hit": True,
                            "collection": coll,
                            "query": normalized_query,
                        }
                    )

            count = await self.db[coll].count_documents(normalized_query)
            if cache:
                self._set_cached(coll, cache_key, count)

            return SafeResult.ok(
                {
                    "count": count,
                    "cache_hit": False,
                    "collection": coll,
                    "query": normalized_query,
                }
            )
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

    async def insert_many_async(self, coll: Optional[str] = None, docs: Optional[List[Dict[str, Any]]] = None) -> SafeResult:
        try:
            self._ensure_not_closed()
            coll = coll or self.coll_name
            result = await self.db[coll].insert_many(docs or [])
            self.clear_cache(coll)
            return SafeResult.ok(
                {
                    "inserted_ids": result.inserted_ids,
                    "inserted_count": len(result.inserted_ids),
                    "collection": coll,
                }
            )
        except Exception as exc:
            return self._fail(exc, operation="insert_many")

    async def update_one_async(
        self,
        coll: Optional[str] = None,
        query: Optional[Dict[str, Any]] = None,
        update: Optional[Dict[str, Any]] = None,
        *,
        upsert: bool = False,
    ) -> SafeResult:
        try:
            self._ensure_not_closed()
            coll = coll or self.coll_name
            normalized_query = self._normalize_query(query or {})
            update = update or {}
            update_doc = update if any(k.startswith("$") for k in update.keys()) else {"$set": update}

            result = await self.db[coll].update_one(normalized_query, update_doc, upsert=upsert)
            self.clear_cache(coll)

            return SafeResult.ok(
                {
                    "matched_count": result.matched_count,
                    "modified_count": result.modified_count,
                    "upserted_id": result.upserted_id,
                    "collection": coll,
                    "query": normalized_query,
                }
            )
        except Exception as exc:
            return self._fail(exc, operation="update_one")

    async def update_many_async(
        self,
        coll: Optional[str] = None,
        query_or_ops: Optional[Union[Dict[str, Any], List[Any]]] = None,
        update: Optional[Dict[str, Any]] = None,
        *,
        upsert: bool = False,
    ) -> SafeResult:
        try:
            self._ensure_not_closed()
            coll = coll or self.coll_name

            if isinstance(query_or_ops, list):
                result = await self.db[coll].bulk_write(query_or_ops)
                self.clear_cache(coll)
                return SafeResult.ok(
                    {
                        "bulk_api_result": getattr(result, "bulk_api_result", None),
                        "inserted_count": getattr(result, "inserted_count", None),
                        "matched_count": getattr(result, "matched_count", None),
                        "modified_count": getattr(result, "modified_count", None),
                        "deleted_count": getattr(result, "deleted_count", None),
                        "upserted_count": getattr(result, "upserted_count", None),
                        "collection": coll,
                    }
                )

            normalized_query = self._normalize_query(query_or_ops or {})
            if update is None:
                return SafeResult.fail(
                    error={"error_type": "ValueError", "error": "update is required for non-bulk update_many"},
                    status_code=400,
                    message="Missing update document",
                )

            update_doc = update if any(k.startswith("$") for k in update.keys()) else {"$set": update}
            result = await self.db[coll].update_many(normalized_query, update_doc, upsert=upsert)
            self.clear_cache(coll)

            return SafeResult.ok(
                {
                    "matched_count": result.matched_count,
                    "modified_count": result.modified_count,
                    "collection": coll,
                    "query": normalized_query,
                }
            )
        except Exception as exc:
            return self._fail(exc, operation="update_many")

    async def delete_one_async(self, coll: Optional[str] = None, query: Optional[Dict[str, Any]] = None) -> SafeResult:
        try:
            self._ensure_not_closed()
            coll = coll or self.coll_name
            normalized_query = self._normalize_query(query or {})
            result = await self.db[coll].delete_one(normalized_query)
            self.clear_cache(coll)
            return SafeResult.ok(
                {"deleted_count": result.deleted_count, "collection": coll, "query": normalized_query}
            )
        except Exception as exc:
            return self._fail(exc, operation="delete_one")

    async def delete_many_async(self, coll: Optional[str] = None, query: Optional[Dict[str, Any]] = None) -> SafeResult:
        try:
            self._ensure_not_closed()
            coll = coll or self.coll_name
            normalized_query = self._normalize_query(query or {})
            result = await self.db[coll].delete_many(normalized_query)
            self.clear_cache(coll)
            return SafeResult.ok(
                {"deleted_count": result.deleted_count, "collection": coll, "query": normalized_query}
            )
        except Exception as exc:
            return self._fail(exc, operation="delete_many")

    async def insert_or_update_async(
        self,
        coll: Optional[str] = None,
        query_or_document: Optional[Dict[str, Any]] = None,
        data: Optional[Dict[str, Any]] = None,
    ) -> SafeResult:
        try:
            self._ensure_not_closed()
            coll = coll or self.coll_name
            query_or_document = query_or_document or {}

            if data is None:
                if "_id" not in query_or_document:
                    return SafeResult.fail(
                        error={"error_type": "ValueError", "error": "Document must include _id when data is omitted"},
                        status_code=400,
                        message="Missing _id for insert_or_update",
                    )

                query = self._normalize_query({"_id": query_or_document["_id"]})
                update_doc = {"$set": {k: v for k, v in query_or_document.items() if k != "_id"}}
            else:
                query = self._normalize_query(query_or_document)
                update_doc = data if any(k.startswith("$") for k in data.keys()) else {"$set": data}

            result = await self.db[coll].update_one(query, update_doc, upsert=True)
            self.clear_cache(coll)

            return SafeResult.ok(
                {
                    "matched_count": result.matched_count,
                    "modified_count": result.modified_count,
                    "upserted_id": result.upserted_id,
                    "collection": coll,
                    "query": query,
                }
            )
        except Exception as exc:
            return self._fail(exc, operation="insert_or_update")

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

            if parse_json_strings and isinstance(value, str):
                stripped = value.strip()
                if stripped:
                    try:
                        parsed_value = json.loads(stripped)
                    except Exception:
                        parsed_value = value

            if normalize_for_storage:
                parsed_value = DataProcessor.to_json_compatible(parsed_value)

            if not normalized_query and isinstance(parsed_value, dict) and parsed_value.get("_id") is not None:
                normalized_query = self._normalize_query({"_id": parsed_value["_id"]})

            if field_path and str(field_path).strip():
                update_payload = {str(field_path).strip(): parsed_value}
            elif isinstance(parsed_value, dict):
                update_payload = dict(parsed_value)
                update_payload.pop("_id", None)
            else:
                update_payload = {"value": parsed_value}

            if not normalized_query and not upsert:
                return SafeResult.fail(
                    error={"error_type": "ValueError", "error": "No query provided and upsert is False"},
                    status_code=400,
                    message="Cannot determine target document",
                )

            if not normalized_query:
                result = await self.db[coll].insert_one(update_payload)
                self.clear_cache(coll)
                return SafeResult.ok(
                    {
                        "operation": "inserted_new",
                        "inserted_id": result.inserted_id,
                        "collection": coll,
                        "field_path": field_path,
                        "saved_value": parsed_value,
                    }
                )

            result = await self.db[coll].update_one(
                normalized_query,
                {"$set": update_payload},
                upsert=upsert,
            )
            self.clear_cache(coll)

            return SafeResult.ok(
                {
                    "operation": "inserted_via_upsert" if result.upserted_id is not None else "updated_existing",
                    "matched_count": result.matched_count,
                    "modified_count": result.modified_count,
                    "upserted_id": result.upserted_id,
                    "collection": coll,
                    "query": normalized_query,
                    "field_path": field_path,
                    "saved_value": parsed_value,
                }
            )
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
            end = time.time()
            return SafeResult.ok({"ok": True, "latency_seconds": end - start, "database": self.db_name})
        except Exception as exc:
            return self._fail(exc, operation="ping")

    async def sync_timestamp_async(self) -> SafeResult:
        try:
            self._ensure_not_closed()
            start = time.time()

            try:
                result = await self.db.command({"hello": 1})
            except PyMongoError:
                result = await self.db.command({"isMaster": 1})

            end = time.time()
            server_time = result.get("localTime")
            if server_time and server_time.tzinfo is None:
                server_time = server_time.replace(tzinfo=timezone.utc)

            return SafeResult.ok(
                {
                    "server_time": server_time,
                    "latency_seconds": end - start,
                    "offset_seconds": (
                        (server_time - datetime.now(timezone.utc)).total_seconds() if server_time is not None else None
                    ),
                }
            )
        except Exception as exc:
            return self._fail(exc, operation="sync_timestamp")

    def find_one(self, coll: Optional[str] = None, query: Optional[Dict[str, Any]] = None, *, cache: bool = False, **kwargs) -> SafeResult:
        return self.run_sync(self.find_one_async, coll, query, cache=cache, **kwargs)

    def find_many(
        self,
        coll: Optional[str] = None,
        query: Optional[Dict[str, Any]] = None,
        *,
        sort: Optional[Union[List[Tuple[str, int]], Tuple[str, int]]] = None,
        limit: Optional[int] = 1000,
        cache: bool = False,
    ) -> SafeResult:
        return self.run_sync(self.find_many_async, coll, query, sort=sort, limit=limit, cache=cache)

    def aggregate(self, coll: Optional[str] = None, pipeline: Optional[List[Dict[str, Any]]] = None) -> SafeResult:
        return self.run_sync(self.aggregate_async, coll, pipeline)

    def count_documents(self, coll: Optional[str] = None, query: Optional[Dict[str, Any]] = None, *, cache: bool = False) -> SafeResult:
        return self.run_sync(self.count_documents_async, coll, query, cache=cache)

    def insert_one(self, coll: Optional[str] = None, doc: Optional[Dict[str, Any]] = None) -> SafeResult:
        return self.run_sync(self.insert_one_async, coll, doc)

    def insert_many(self, coll: Optional[str] = None, docs: Optional[List[Dict[str, Any]]] = None) -> SafeResult:
        return self.run_sync(self.insert_many_async, coll, docs)

    def update_one(
        self,
        coll: Optional[str] = None,
        query: Optional[Dict[str, Any]] = None,
        update: Optional[Dict[str, Any]] = None,
        *,
        upsert: bool = False,
    ) -> SafeResult:
        return self.run_sync(self.update_one_async, coll, query, update, upsert=upsert)

    def update_many(
        self,
        coll: Optional[str] = None,
        query_or_ops: Optional[Union[Dict[str, Any], List[Any]]] = None,
        update: Optional[Dict[str, Any]] = None,
        *,
        upsert: bool = False,
    ) -> SafeResult:
        return self.run_sync(self.update_many_async, coll, query_or_ops, update, upsert=upsert)

    def delete_one(self, coll: Optional[str] = None, query: Optional[Dict[str, Any]] = None) -> SafeResult:
        return self.run_sync(self.delete_one_async, coll, query)

    def delete_many(self, coll: Optional[str] = None, query: Optional[Dict[str, Any]] = None) -> SafeResult:
        return self.run_sync(self.delete_many_async, coll, query)

    def delete_all_documents(self, coll: Optional[str] = None) -> SafeResult:
        return self.run_sync(self.delete_many_async, coll, {})

    def delete_documents(self, coll: Optional[str] = None, query: Optional[Dict[str, Any]] = None) -> SafeResult:
        return self.run_sync(self.delete_many_async, coll, query)

    def insert_documents(self, coll: Optional[str] = None, docs: Optional[List[Dict[str, Any]]] = None) -> SafeResult:
        return self.run_sync(self.insert_many_async, coll, docs)

    def insert_or_update(
        self,
        coll: Optional[str] = None,
        query_or_document: Optional[Dict[str, Any]] = None,
        data: Optional[Dict[str, Any]] = None,
    ) -> SafeResult:
        return self.run_sync(self.insert_or_update_async, coll, query_or_document, data)

    def save_value(
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
        return self.run_sync(
            self.save_value_async,
            coll,
            value,
            query=query,
            field_path=field_path,
            upsert=upsert,
            parse_json_strings=parse_json_strings,
            normalize_for_storage=normalize_for_storage,
        )

    def list_collections(self) -> SafeResult:
        return self.run_sync(self.list_collections_async)

    def drop_database(self, database_name: Optional[str] = None) -> SafeResult:
        return self.run_sync(self.drop_database_async, database_name)

    def ping(self) -> SafeResult:
        return self.run_sync(self.ping_async)

    def sync_timestamp(self) -> SafeResult:
        return self.run_sync(self.sync_timestamp_async)