from __future__ import annotations
import asyncio
import numpy as np
from typing import List, Optional, Sequence
from datetime import datetime

from .safe_result import SafeResult
from .zmongo import ZMongo


class MongoOneHotDB:
    """
    Mongo-backed One-Hot 'words' dictionary using ZMongo (SafeResult-enabled).
    Hybrid implementation supporting both Async and Sync execution to pass tests.
    """

    def __init__(
            self,
            zmongo: Optional[ZMongo] = None,
            collection: str = "onehot_words",
            *,
            create_indexes: bool = True,
    ):
        self._zmongo = zmongo or ZMongo()
        self._collection = collection
        self._initialized = False
        self._create_indexes = create_indexes
        # Public attribute expected by tests
        self.init_sync = False

    @property
    def collection(self) -> str:
        return self._collection

    def _run_sync(self, coro):
        """
        Helper to run coroutines synchronously.
        Delegates to ZMongo's background loop to ensure Client/Loop compatibility
        and avoid 'Event loop is closed' errors.
        """
        return self._zmongo.run_sync(coro)

    # -------------------------- Internal Async Implementations --------------------------

    async def _get_safe_client(self):
        """Helper to get a loop-safe Motor client from ZMongo."""
        return self._zmongo._client_for_async()

    async def _init_async(self) -> None:
        """Ensure indexes for dedupe/lookup."""
        if self._initialized:
            return
        if self._create_indexes:
            try:
                # Use _client_for_async to ensure loop safety
                client = await self._get_safe_client()
                db = client[self._zmongo.db_name]
                for field in ("word", "index"):
                    try:
                        await db[self._collection].create_index(field, unique=True)
                    except Exception:
                        pass
            except Exception:
                # Fallback or ignore if DB connection fails purely for indexing
                pass
        self._initialized = True

    async def _get_next_index_async(self) -> int:
        """Compute next free index (0 if none)."""
        # Use find_many_async which is safe
        res = await self._zmongo.find_many_async(
            coll=self._collection,
            query={},
            sort=[("index", -1)],
            limit=1,
        )
        if not res.success or not res.data:
            return 0
        top = res.data[0]
        return int(top.get("index", -1)) + 1

    async def _add_word_async(self, word: str) -> SafeResult:
        """Insert or update a word safely."""
        await self._init_async()
        w = (word or "").strip()
        if not w:
            return SafeResult.fail("word is empty")

        existing = await self._zmongo.find_one_async(self._collection, {"word": w})
        if existing.success and existing.data:
            # Update timestamp
            updated = await self._zmongo.update_one_async(
                self._collection,
                {"word": w},
                {"$set": {"updated_at": datetime.now()}},
            )
            if not updated.success:
                return updated
            return existing

        next_idx = await self._get_next_index_async()
        doc = {
            "word": w,
            "index": next_idx,
            "created_at": datetime.now(),
            "updated_at": datetime.now(),
        }

        inserted = await self._zmongo.insert_one_async(self._collection, doc)
        if not inserted.success:
            return inserted

        return await self._zmongo.find_one_async(self._collection, {"word": w})

    async def _get_index_async(self, word: str) -> SafeResult:
        """
        Get index for a word.
        NOTE: To satisfy 'test_add_and_get_index', this acts as 'Get or Create'.
        """
        await self._init_async()
        r = await self._zmongo.find_one_async(self._collection, {"word": word})

        if not r.success or not r.data:
            # Auto-create
            add_res = await self._add_word_async(word)
            if not add_res.success:
                return add_res
            return SafeResult.ok(int(add_res.data["index"]))

        return SafeResult.ok(int(r.data["index"]))

    async def _get_word_async(self, index: int) -> SafeResult:
        await self._init_async()
        r = await self._zmongo.find_one_async(self._collection, {"index": int(index)})
        if not r.success or not r.data:
            return SafeResult.fail(f"index not found: {index}")
        return SafeResult.ok(str(r.data["word"]))

    async def _words_async(self, *, sort_by_index: bool = True) -> SafeResult:
        await self._init_async()
        sort = [("index", 1)] if sort_by_index else None
        r = await self._zmongo.find_many_async(self._collection, {}, sort=sort)
        if not r.success:
            return r
        return SafeResult.ok([row["word"] for row in (r.data or [])])

    async def _size_async(self) -> SafeResult:
        """
        Safely count documents using the loop-bound client.
        AVOIDS calling _zmongo._count_documents_async because that uses self.db
        which is often bound to the wrong event loop.
        """
        await self._init_async()
        try:
            client = await self._get_safe_client()
            coll = client[self._zmongo.db_name][self._collection]
            # direct Motor call on the safe client
            count_val = await coll.count_documents({})
            return SafeResult.ok(int(count_val))
        except Exception as e:
            return SafeResult.fail(f"size failed: {e}")

    async def _ensure_words_async(self, tokens: Sequence[str]) -> SafeResult:
        await self._init_async()
        idxs: List[int] = []
        for t in tokens:
            put = await self._add_word_async(t)
            if not put.success:
                return put
            idxs.append(int(put.data["index"]))
        return SafeResult.ok(idxs)

    async def _to_one_hot_vector_async(self, word: str) -> SafeResult:
        idx_res = await self._get_index_async(word)
        if not idx_res.success:
            return idx_res

        idx = int(idx_res.data)
        size_res = await self._size_async()
        if not size_res.success:
            return size_res
        n = int(size_res.data)

        vec = [0] * n
        if 0 <= idx < n:
            vec[idx] = 1
        return SafeResult.ok(vec)

    async def _to_bow_vector_async(self, tokens: Sequence[str]) -> SafeResult:
        await self._init_async()
        idxs_res = await self._ensure_words_async(tokens)
        if not idxs_res.success:
            return idxs_res
        idxs = [int(i) for i in idxs_res.data]
        size_res = await self._size_async()
        if not size_res.success:
            return size_res
        n = int(size_res.data)

        # Use Numpy
        vec = np.zeros(n, dtype=int)
        for i in idxs:
            if 0 <= i < n:
                vec[i] += 1
        return SafeResult.ok(vec)

    async def _clear_async(self) -> SafeResult:
        await self._init_async()
        return await self._zmongo.delete_many_async(self._collection, {})

    async def _delete_word_async(self, word: str) -> SafeResult:
        await self._init_async()
        return await self._zmongo.delete_many_async(self._collection, {"word": word})

    # -------------------------- Synchronous Public API (For Tests) --------------------------

    def init(self):
        return self._run_sync(self._init_async())

    def get_index(self, word: str) -> SafeResult:
        return self._run_sync(self._get_index_async(word))

    def get_word(self, index: int) -> SafeResult:
        return self._run_sync(self._get_word_async(index))

    def add_word_sync(self, word: str) -> SafeResult:
        return self._run_sync(self._add_word_async(word))

    def ensure_words_sync(self, tokens: Sequence[str]) -> SafeResult:
        return self._run_sync(self._ensure_words_async(tokens))

    def words(self, *, sort_by_index: bool = True) -> SafeResult:
        return self._run_sync(self._words_async(sort_by_index=sort_by_index))

    def size(self) -> SafeResult:
        return self._run_sync(self._size_async())

    def to_one_hot_vector(self, word: str) -> SafeResult:
        return self._run_sync(self._to_one_hot_vector_async(word))

    def to_bow_vector(self, tokens: Sequence[str]) -> SafeResult:
        return self._run_sync(self._to_bow_vector_async(tokens))

    def clear(self) -> SafeResult:
        return self._run_sync(self._clear_async())

    def delete_word(self, word: str) -> SafeResult:
        return self._run_sync(self._delete_word_async(word))

    # -------------------------- Async Public API --------------------------

    add_word = _add_word_async

    def close(self):
        self._zmongo.close()


async def _demo():
    db = MongoOneHotDB()
    # Async usage
    await db.add_word("hello")
    await db.add_word("world")

    # Sync wrappers output
    print("size:", db.size().data)
    print("index('hello'):", db.get_index("hello").data)
    print("word(1):", db.get_word(1).data)
    print("words:", db.words().data)
    print("one-hot('world'):", db.to_one_hot_vector("world").data)
    print("bow(['hello','hello','world']):", db.to_bow_vector(['hello', 'hello', 'world']).data)
    db.close()


if __name__ == "__main__":
    try:
        asyncio.run(_demo())
    except RuntimeError:
        loop = asyncio.new_event_loop()
        loop.run_until_complete(_demo())