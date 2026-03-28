import os
import asyncio
import threading
import weakref
import logging
from datetime import timezone, datetime
from typing import Any, Dict, List, Optional, Tuple, Awaitable

from motor import motor_asyncio
from pymongo.results import DeleteResult

# Use the shared SafeResult from zmongo_toolbag so tests & routes all agree
from .safe_result import SafeResult
from .zmongo_response_result import ZMongoResponseResult

# Cache type (if available); otherwise fall back to a simple dict
try:
    from .buffered_ttl_cache import BufferedAsyncTTLCache
except ImportError:  # pragma: no cover - used only when the cache module is missing
    BufferedAsyncTTLCache = dict  # type: ignore

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class ZMongo:
    """
    Lightweight MongoDB helper with:
      - A dedicated background event loop for sync wrappers
      - Per-event-loop async Motor clients
      - SafeResult-wrapped operations
    """

    def __init__(self, uri: Optional[str] = None, db_name: Optional[str] = None):
        self.uri = uri or os.getenv("MONGO_URI", "mongodb://127.0.0.1:27017")
        self.db_name = db_name or os.getenv("MONGO_DATABASE_NAME", "test")

        # Background event loop for sync API
        self._loop = asyncio.new_event_loop()
        self._thread = threading.Thread(target=self._run_event_loop, daemon=True)
        self._thread.start()

        # Background client bound to the background loop
        self._client_bg = motor_asyncio.AsyncIOMotorClient(self.uri)
        self.db = self._client_bg[self.db_name]

        # Async clients bound per "current" event loop (for async callers)
        self._async_clients: "weakref.WeakKeyDictionary[asyncio.AbstractEventLoop, motor_asyncio.AsyncIOMotorClient]" = (
            weakref.WeakKeyDictionary()
        )

        # Optional per-collection caches
        self.caches: Dict[str, Any] = {}

    # ------------------------------------------------------------
    # Loop Management
    # ------------------------------------------------------------
    def _run_event_loop(self) -> None:
        """Run the dedicated background asyncio loop."""
        asyncio.set_event_loop(self._loop)
        try:
            self._loop.run_forever()
        except Exception as e:
            logger.error(f"Event loop terminated unexpectedly: {e!r}")

    def close(self) -> None:
        """Gracefully close MongoDB client and stop background loop."""
        try:
            if self._client_bg:
                self._client_bg.close()
            if self._loop.is_running():
                self._loop.call_soon_threadsafe(self._loop.stop)
                self._thread.join(timeout=5)
            logger.info("🧹 ZMongo background loop stopped cleanly.")
        except Exception as e:
            logger.error(f"ZMongo close() failed: {e!r}")

        # New method to handle bulk operations in zmongo.py
        async def bulk_write_async(self, coll: str, operations: List[Any]) -> SafeResult:
            """Execute a list of mixed write operations asynchronously."""
            try:
                collection = self._client_for_async()[self.db_name][coll]
                r = await collection.bulk_write(operations)
                # Motor's BulkWriteResult contains detailed counts
                return SafeResult.ok({
                    "inserted_count": r.inserted_count,
                    "modified_count": r.modified_count,
                    "deleted_count": r.deleted_count,
                    "upserted_count": r.upserted_count,
                    # ... include other counts as needed
                })
            except Exception as e:
                logger.exception("bulk_write_async failed")
                return SafeResult.fail(str(e))

        # New method to handle bulk operations in zmongo.py
    async def bulk_write_async(self, coll: str, operations: List[Any]) -> SafeResult:
        """Execute a list of mixed write operations asynchronously."""
        try:
            collection = self._client_for_async()[self.db_name][coll]
            r = await collection.bulk_write(operations)
            # Motor's BulkWriteResult contains detailed counts
            return SafeResult.ok({
                "inserted_count": r.inserted_count,
                "modified_count": r.modified_count,
                "deleted_count": r.deleted_count,
                "upserted_count": r.upserted_count,
                # ... include other counts as needed
            })
        except Exception as e:
            logger.exception("bulk_write_async failed")
            return SafeResult.fail(str(e))

    # Add a sync wrapper for the new method (e.g., in the Sync Wrappers section)
    def bulk_write(self, *a, **kw) -> SafeResult:
        return self.run_sync(self.bulk_write_async(*a, **kw))

    def _client_for_async(self) -> motor_asyncio.AsyncIOMotorClient:
        """
        Get (or create) a Motor client bound to the *current* async event loop.

        This is for code already inside an async function.
        """
        loop = asyncio.get_running_loop()
        cli = self._async_clients.get(loop)
        if cli is None:
            cli = motor_asyncio.AsyncIOMotorClient(self.uri)
            self._async_clients[loop] = cli
        return cli

    # ------------------------------------------------------------
    # Async Drop Database
    # ------------------------------------------------------------
    async def drop_database_async(self, database_name: str) -> SafeResult:
        """
        Asynchronously drops the specified database.

        Args:
            database_name: The name of the database to drop.

        Returns:
            SafeResult with data=True on successful drop.
        """
        try:
            client = self._client_for_async()

            # Motor's client.drop_database returns a coroutine
            await client.drop_database(database_name)

            # Check if the database being dropped is the one we hold a primary reference to
            if database_name == self.db_name:
                logger.warning(
                    f"Dropped the primary database '{self.db_name}'. "
                    "Future operations via self.db may fail. Consider re-initializing ZMongo."
                )

            return SafeResult.ok(True)
        except Exception as e:
            logger.exception(f"drop_database_async failed for '{database_name}'")
            return SafeResult.fail(str(e))

    # ------------------------------------------------------------
    # Sync Wrapper
    # ------------------------------------------------------------
    def drop_database(self, database_name: str) -> SafeResult:
        """
        Synchronous wrapper for drop_database_async.

        Args:
            database_name: The name of the database to drop.

        Returns:
            SafeResult with data=True on successful drop.
        """
        return self.run_sync(self.drop_database_async(database_name))

    def run_sync(self, coro: "Awaitable[SafeResult]") -> SafeResult:
        fut = asyncio.run_coroutine_threadsafe(coro, self._loop)
        try:
            return fut.result(timeout=20)
        except Exception as e:
            logger.exception("run_sync encountered an error")
            return SafeResult.fail(str(e))

    # ------------------------------------------------------------
    # Async Motor Operations
    # ------------------------------------------------------------
    async def insert_many_async(self, coll: str, docs: List[Dict[str, Any]]) -> SafeResult:
        """Insert multiple documents asynchronously."""
        try:
            collection = self._client_for_async()[self.db_name][coll]
            r = await collection.insert_many(docs)
            return SafeResult.ok({"inserted_ids": r.inserted_ids})
        except Exception as e:
            logger.exception("insert_many_async failed")
            return SafeResult.fail(str(e))

    async def update_many_async(
        self,
        coll: str,
        query: Dict[str, Any],
        update: Dict[str, Any],
        upsert: bool = False,
    ) -> SafeResult:
        """Update multiple documents asynchronously."""
        try:
            collection = self._client_for_async()[self.db_name][coll]
            r = await collection.update_many(query, update, upsert=upsert)
            return SafeResult.ok(
                {
                    "matched_count": r.matched_count,
                    "modified_count": r.modified_count,
                    "upserted_id": r.upserted_id,
                }
            )
        except Exception as e:
            logger.exception("update_many_async failed")
            return SafeResult.fail(str(e))

    async def insert_one_async(self, coll: str, doc: Dict[str, Any]) -> SafeResult:
        """Insert a single document asynchronously."""
        try:
            collection = self._client_for_async()[self.db_name][coll]
            r = await collection.insert_one(doc)
            return SafeResult.ok({"inserted_id": r.inserted_id})
        except Exception as e:
            logger.exception("insert_one_async failed")
            return SafeResult.fail(str(e))

    async def find_one_async(self, coll: str, query: Dict[str, Any], **kw) -> SafeResult:
        """
        Retrieve one document with automatic ObjectId normalization and optional caching.
        Returns SafeResult.ok(doc) where doc may be None if no match.
        """
        try:
            if "_id" in query and isinstance(query["_id"], str):
                from bson.objectid import ObjectId
                if ObjectId.is_valid(query["_id"]):
                    query["_id"] = ObjectId(query["_id"])

            # --- Handle Caching (if implemented) and clean kwargs ---
            # Remove any non-MongoDB arguments (like 'cache') before passing to Motor:
            kw.pop('cache', None)

            collection = self._client_for_async()[self.db_name][coll]
            doc = await collection.find_one(query, **kw)  # Pass remaining kwargs to Motor
            return SafeResult.ok(doc)
        except Exception as e:
            logger.exception("find_one_async failed")
            return SafeResult.fail(str(e))

    async def delete_all_documents(self, coll: str) -> int:
        """
        Deletes all documents from a single specified collection.

        Args:
            coll: The name of the collection to clear.

        Returns:
            The number of documents deleted.
        """
        try:
            # Access the specific collection
            collection = self.db[coll]

            # Use delete_many with an empty filter {} to match all documents
            result = await collection.delete_many({})

            print(f"Successfully deleted {result.deleted_count} documents from the collection '{coll}'.")
            return result.deleted_count

        except Exception as e:
            print(f"An error occurred while clearing collection '{coll}': {e}")
            return 0

    async def delete_one(self, collection_name: str, filter_doc: Dict[str, Any]) -> ZMongoResponseResult:
        """
        Deletes a single document from the specified collection that matches the filter.

        Args:
            collection_name: The name of the collection to delete from.
            filter_doc: The filter document (e.g., {"_id": "doc2"}).

        Returns:
            A ZMongoResponseResult object matching the required test format.
        """
        try:
            # 1. Access the collection using self.db
            collection = self.db[collection_name]

            # 2. Execute the underlying delete_one operation (returns a MotorDeleteResult)
            # The result object has the 'deleted_count' property.
            result = await collection.delete_one(filter_doc)

            # 3. Get the deleted count directly from the result object
            deleted_count = result.deleted_count

            # The operation is considered successful if acknowledged (which it is by default)
            # and if the driver call completed without exception.
            is_success = result.acknowledged

            # 4. Structure the result to match the test script's expectation
            return ZMongoResponseResult(
                success=is_success,
                data={"deleted_count": deleted_count}
            )

        except Exception as e:
            # Handle potential connection or database errors
            print(f"Error during delete_one operation on '{collection_name}': {e}")
            return ZMongoResponseResult(
                success=False,
                data={"deleted_count": 0, "error": str(e)}
            )


    async def delete_many_async(self, coll: str, query: Dict[str, Any]) -> SafeResult:
        """Delete documents safely with ObjectId conversion."""
        try:
            if "_id" in query and isinstance(query["_id"], str):
                from bson.objectid import ObjectId

                if ObjectId.is_valid(query["_id"]):
                    query["_id"] = ObjectId(query["_id"])

            collection = self._client_for_async()[self.db_name][coll]
            result = await collection.delete_many(query)
            return SafeResult.ok({"deleted_count": result.deleted_count})
        except Exception as e:
            logger.exception("delete_many_async failed")
            return SafeResult.fail(str(e))

    async def update_one_async(
        self,
        coll: str,
        query: Dict[str, Any],
        update: Dict[str, Any],
        upsert: bool = False,
    ) -> SafeResult:
        """Update a single document asynchronously."""
        try:
            collection = self._client_for_async()[self.db_name][coll]
            r = await collection.update_one(query, update, upsert=upsert)
            return SafeResult.ok(
                {
                    "matched_count": r.matched_count,
                    "modified_count": r.modified_count,
                    "upserted_id": r.upserted_id,
                }
            )
        except Exception as e:
            logger.exception("update_one_async failed")
            return SafeResult.fail(str(e))

    async def aggregate_async(self, coll: str, pipeline: List[Dict[str, Any]]) -> SafeResult:
        """Run an aggregation pipeline asynchronously."""
        try:
            collection = self._client_for_async()[self.db_name][coll]
            cursor = collection.aggregate(pipeline)
            docs = await cursor.to_list(None)
            return SafeResult.ok(docs)
        except Exception as e:
            logger.exception("aggregate_async failed")
            return SafeResult.fail(str(e))

    async def list_collections_async(self) -> SafeResult:
        """
        Asynchronously list all collections in the current database.
        Returns SafeResult containing {"collections": [...]}.
        """
        try:
            client = self._client_for_async()
            names = await client[self.db_name].list_collection_names()
            return SafeResult.ok({"collections": names})
        except Exception as e:
            logger.exception("list_collections_async failed")
            return SafeResult.fail(str(e))

    async def sync_timestamp_async(self) -> SafeResult:
        """
        Asynchronously retrieve the MongoDB server's current time and latency.

        Returns SafeResult.ok with:
          - 'server_time': MongoDB's reported server time (UTC-aware datetime)
          - 'local_time': local system time in UTC (datetime)
          - 'offset_seconds': server_time - local_time, in seconds
          - 'latency_seconds': round-trip latency of the serverStatus command
        """
        import time

        try:
            client = self._client_for_async()
            admin_db = client["admin"]

            # Run a lightweight serverStatus command to get localTime and measure latency
            start = time.time()
            status = await admin_db.command("serverStatus")
            end = time.time()

            server_time = status.get("localTime")
            if server_time is None:
                return SafeResult.fail("serverStatus did not return localTime")

            if not isinstance(server_time, datetime):
                # Unexpected type – better to fail loudly than miscompute offset
                return SafeResult.fail(
                    f"serverStatus.localTime is not a datetime (got {type(server_time)!r})"
                )

            # Ensure server_time is UTC-aware
            if server_time.tzinfo is None:
                server_time = server_time.replace(tzinfo=timezone.utc)
            else:
                server_time = server_time.astimezone(timezone.utc)

            # Local UTC time for comparison
            local_now_utc = datetime.now(timezone.utc)

            offset = (server_time - local_now_utc).total_seconds()
            latency = end - start

            return SafeResult.ok(
                {
                    "server_time": server_time,
                    "local_time": local_now_utc,
                    "offset_seconds": offset,
                    "latency_seconds": latency,
                }
            )
        except Exception as e:
            logger.exception("sync_timestamp_async failed")
            return SafeResult.fail(str(e))


    async def find_many_async(
        self,
        coll: str,
        query: Optional[Dict[str, Any]] = None,
        projection: Optional[Dict[str, int]] = None,
        limit: int = 1000,
        sort: Optional[List[Tuple[str, int]]] = None,
    ) -> SafeResult:
        """
        Asynchronously find multiple documents from a collection.

        Args:
            coll: Collection name.
            query: MongoDB filter dict.
            projection: Optional projection dict (fields to include/exclude).
            limit: Maximum number of documents to return.
            sort: Optional list of (field, direction) tuples.

        Returns:
            SafeResult containing a list of documents.
        """
        try:
            collection = self._client_for_async()[self.db_name][coll]
            cursor = collection.find(query or {}, projection)
            if sort:
                cursor = cursor.sort(sort)
            if limit:
                cursor = cursor.limit(limit)
            docs = await cursor.to_list(length=limit)
            return SafeResult.ok(docs)
        except Exception as e:
            logger.exception("find_many_async failed")
            return SafeResult.fail(str(e))

    async def insert_or_update_async(
            self,
            coll: str,
            document: Dict[str, Any],  # Renamed for clarity, assumes this contains the full doc, including _id
            data: Optional[Dict[str, Any]] = None,
            upsert: bool = True,
    ) -> SafeResult:
        try:
            collection = self._client_for_async()[self.db_name][coll]

            if data is None:
                # --- Update mode ---
                if "_id" not in document:
                    # If data is None and no _id, this must be an insert attempt
                    result = await collection.insert_one(document)
                    return SafeResult.ok({"upserted_id": result.inserted_id, "modified_count": 0})

                # --- Default update/upsert (query based on _id, update is the rest) ---
                query = {"_id": document["_id"]}
                # Prepare update by removing _id from the document
                update_data = {k: v for k, v in document.items() if k != "_id"}

                # Wrap update data in $set
                if not update_data:
                    return SafeResult.fail("Cannot update/upsert with empty data.")

                update_op = {"$set": update_data}

                result = await collection.update_one(query, update_op, upsert=upsert)
                return SafeResult.ok(
                    {
                        "upserted_id": result.upserted_id,
                        "modified_count": result.modified_count,
                    }
                )

            # --- Custom update mode (if data is provided as a separate dict) ---
            # ... (original custom update logic using query_or_doc as query and data as update)
            # This path relies on the caller separating query and update already.
            # Assuming query_or_doc is the query filter here:
            query = document  # use the whole document dict as the query

            if not any(k.startswith("$") for k in data.keys()):
                data = {"$set": data}

            result = await collection.update_one(query, data, upsert=upsert)
            return SafeResult.ok(
                {
                    "upserted_id": result.upserted_id,
                    "modified_count": result.modified_count,
                }
            )

        except Exception as e:
            logger.exception("insert_or_update_async failed")
            return SafeResult.fail(str(e))

    # ------------------------------------------------------------
    # Async Implementation Helpers
    # ------------------------------------------------------------
    async def _count_documents_async(self, coll: str, query: dict) -> SafeResult:
        """
        Asynchronous count_documents implementation.

        Returns:
            SafeResult(success=True, data=count) on success,
            SafeResult(success=False, error=message) on failure.
        """
        try:
            collection = self._client_for_async()[self.db_name][coll]
            count = await collection.count_documents(query or {})
            return SafeResult.ok(count)
        except Exception as e:
            logger.exception("count_documents_async failed")
            return SafeResult.fail(f"count_documents_async failed: {e}")

    # ------------------------------------------------------------
    # Sync Wrappers
    # ------------------------------------------------------------
    def insert_one(self, *a, **kw) -> SafeResult:
        return self.run_sync(self.insert_one_async(*a, **kw))

    def find_one(self, *a, **kw) -> SafeResult:
        return self.run_sync(self.find_one_async(*a, **kw))

    def update_one(self, *a, **kw) -> SafeResult:
        return self.run_sync(self.update_one_async(*a, **kw))

    def delete_many(self, *a, **kw) -> SafeResult:
        return self.run_sync(self.delete_many_async(*a, **kw))

    def aggregate(self, *a, **kw) -> SafeResult:
        return self.run_sync(self.aggregate_async(*a, **kw))

    def insert_many(self, *a, **kw) -> SafeResult:
        return self.run_sync(self.insert_many_async(*a, **kw))

    def update_many(self, *a, **kw) -> SafeResult:
        return self.run_sync(self.update_many_async(*a, **kw))

    def list_collections(self) -> SafeResult:
        """Synchronous wrapper for list_collections_async."""
        return self.run_sync(self.list_collections_async())

    def sync_timestamp(self) -> SafeResult:
        """
        Synchronous wrapper for sync_timestamp_async.
        Verifies MongoDB connectivity and clock offset.
        """
        return self.run_sync(self.sync_timestamp_async())

    def find_many(
        self,
        coll: str,
        query: Optional[Dict[str, Any]] = None,
        projection: Optional[Dict[str, int]] = None,
        limit: int = 1000,
        sort: Optional[List[Tuple[str, int]]] = None,
    ) -> SafeResult:
        """Synchronous wrapper for find_many_async."""
        return self.run_sync(self.find_many_async(coll, query, projection, limit, sort))

    def insert_or_update(
        self,
        coll: str,
        query_or_doc: Dict[str, Any],
        data: Optional[Dict[str, Any]] = None,
        upsert: bool = True,
    ) -> SafeResult:
        """
        Sync wrapper for insert_or_update_async.
        Runs safely inside ZMongo’s dedicated async loop.
        """
        return self.run_sync(self.insert_or_update_async(coll, query_or_doc, data, upsert))

    def count_documents(self, coll: str, query: dict) -> SafeResult:
        """
        Count documents in a collection (synchronous wrapper).
        Returns a SafeResult with .data = integer count.
        """
        try:
            return self.run_sync(self._count_documents_async(coll, query))
        except Exception as e:
            logger.exception("count_documents failed")
            return SafeResult.fail(f"count_documents failed: {e}")

    # ------------------------------------------------------------
    # Utilities
    # ------------------------------------------------------------
    def clear_cache(self, coll: str) -> None:
        """Clear any per-collection cache entry if present."""
        if coll in self.caches:
            self.caches.pop(coll, None)

    async def distinct_async(self, coll: str, key: str, query: Optional[Dict[str, Any]] = None) -> SafeResult:
        """Asynchronously retrieve distinct values for a key."""
        try:
            collection = self._client_for_async()[self.db_name][coll]
            # Use distinct() method on Motor collection
            values = await collection.distinct(key, query or {})
            return SafeResult.ok(values)
        except Exception as e:
            logger.exception("distinct_async failed")
            return SafeResult.fail(str(e))

    # Add sync wrapper (e.g., in the Sync Wrappers section)
    def distinct(self, coll: str, key: str, query: Optional[Dict[str, Any]] = None) -> SafeResult:
        return self.run_sync(self.distinct_async(coll, key, query))

    # --- In CodexRepository class (or similar) ---
    def get_distinct_field_values(self, collection_name, field_name):
        # Assuming CodexRepository uses the ZMongo 'distinct' method
        return self.zmongo_instance.distinct(collection_name, field_name)

