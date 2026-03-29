"""
LocalVectorSearch – SafeResult-compatible vector similarity search engine
for ZMongo repositories (sync or async).
"""

from __future__ import annotations

import asyncio
import logging
import threading
from concurrent.futures import Future
from typing import Any, Dict, List, Optional

import numpy as np
from bson.objectid import ObjectId

from safe_result import SafeResult
# Standard imports without leading dots for this environment

logger = logging.getLogger(__name__)

def _run_async_in_new_thread(coro) -> Any:
    fut: Future[Any] = Future()
    def _runner():
        try:
            fut.set_result(asyncio.run(coro))
        except Exception as e:
            fut.set_exception(e)
    t = threading.Thread(target=_runner, daemon=True)
    t.start()
    return fut.result()

class LocalVectorSearch:
    def __init__(
        self,
        repository: Any,
        collection: str,
        embedding_field: str = "embedding",
        chunked_embeddings: bool = False,
        score_mode: str = "cosine_0_1",
        field_key: Optional[str] = None,
        vector_key: str = "vectors",
        max_docs: int = 1_000_000,
    ):
        self.repo = repository
        self.collection = collection
        self.embedding_field = embedding_field
        self.chunked_embeddings = chunked_embeddings
        self.score_mode = score_mode
        self.field_key = field_key
        self.vector_key = vector_key
        self.max_docs = max_docs
        self._index_matrix: Optional[np.ndarray] = None
        self._meta_docs: List[Dict[str, Any]] = []
        self._load_lock = asyncio.Lock()

    def clear_index(self) -> None:
        """Resets the in-memory index matrix and metadata for graceful shutdown."""
        self._index_matrix = None
        self._meta_docs = []
        logger.info("Local vector index cleared.")

    async def _await_repo_result(self, maybe_result: Any) -> SafeResult:
        if asyncio.iscoroutine(maybe_result):
            maybe_result = await maybe_result
        return maybe_result if isinstance(maybe_result, SafeResult) else SafeResult.ok(maybe_result)

    async def _find_all_docs(self) -> SafeResult:
        try:
            # Ensure find_many actually returns a list of docs in its .data field
            repo_call = self.repo.find_many(self.collection, query={}, limit=self.max_docs)
            res = await self._await_repo_result(repo_call)
            return res
        except Exception as e:
            return SafeResult.fail(f"find_many failed: {e}")

    def _extract_vectors_from_doc(self, doc: Dict[str, Any]) -> List[List[float]]:
        emb_container = doc.get(self.embedding_field)
        if not emb_container: return []
        emb_entry = emb_container
        if self.field_key is not None and isinstance(emb_container, dict):
            emb_entry = emb_container.get(self.field_key)
            if not emb_entry: return []
        vec_data = emb_entry
        if isinstance(emb_entry, dict) and self.vector_key in emb_entry:
            vec_data = emb_entry.get(self.vector_key)
        if not vec_data: return []
        if isinstance(vec_data, list) and vec_data and isinstance(vec_data[0], list):
            if self.chunked_embeddings:
                return [v for v in vec_data if isinstance(v, list) and v]
            return [vec_data[0]] if vec_data[0] else []
        if isinstance(vec_data, list):
            return [vec_data] if vec_data else []
        return []

    async def _ensure_index(self) -> SafeResult:
        if self._index_matrix is not None:
            return SafeResult.ok({"count": int(self._index_matrix.shape[0])})
        async with self._load_lock:
            if self._index_matrix is not None:
                return SafeResult.ok({"count": int(self._index_matrix.shape[0])})
            return await self.rebuild_index()

    def rebuild_index_sync(self) -> SafeResult:
        try:
            loop = asyncio.get_running_loop()
            if loop.is_running(): return _run_async_in_new_thread(self.rebuild_index())
        except RuntimeError: pass
        return asyncio.run(self.rebuild_index())

    async def search(self, query_vector: List[float], top_k: int = 5) -> SafeResult:
        ensure_res = await self._ensure_index()
        if not ensure_res.success: return ensure_res
        if self._index_matrix is None or self._index_matrix.size == 0: return SafeResult.ok([])
        try:
            q = np.array(query_vector, dtype=float)
            dim = int(self._index_matrix.shape[1])
            if q.shape[0] != dim: return SafeResult.fail(f"Dimension mismatch.")
            M = self._index_matrix
            qn, Mn = np.linalg.norm(q), np.linalg.norm(M, axis=1)
            denom = Mn * qn
            valid = denom > 0
            cos = np.zeros(M.shape[0], dtype=float)
            cos[valid] = (M[valid] @ q) / denom[valid]
            scores = self._to_output_scores(cos)
            k = min(max(1, int(top_k)), int(M.shape[0]))
            idx = np.argpartition(scores, -k)[-k:]
            idx = idx[np.argsort(scores[idx])[::-1]]
            out = [{"document": self._meta_docs[i], "retrieval_score": float(scores[i])} for i in idx]
            return SafeResult.ok(out)
        except Exception as e: return SafeResult.fail(str(e))

    def _to_output_scores(self, cos: np.ndarray) -> np.ndarray:
        mode = (self.score_mode or "cosine_0_1").lower().strip()
        if mode == "cosine_0_1": return 0.5 * (cos + 1.0)
        return cos.astype(float)

    async def rebuild_index(self) -> SafeResult:
        """
        Fetches all documents and builds the in-memory vector index.
        Explicitly unwraps SafeResult data to prevent iteration errors.
        """
        try:
            # 1. Fetch documents
            docs_res = await self._find_all_docs()

            # 2. Check for failure early
            if not docs_res.success:
                logger.error(f"Index rebuild failed at fetch: {docs_res.error}")
                return docs_res

            # 3. CRITICAL FIX: Ensure we are iterating over the LIST inside the result
            # We access .data and fallback to an empty list if it's None.
            docs = docs_res.data
            if isinstance(docs, SafeResult):  # Handle accidental nesting
                docs = docs.data

            if not isinstance(docs, list):
                # If ZMongo returns a dict (like the 'deleted_count' one in your logs),
                # we need to make sure we aren't trying to iterate over it like a list of docs.
                if isinstance(docs, dict) and "documents" in docs:
                    docs = docs["documents"]
                else:
                    docs = []

            meta, vecs = [], []
            for d in docs:  # This will no longer throw TypeError
                if not isinstance(d, dict):
                    continue

                extracted = self._extract_vectors_from_doc(d)
                for v in extracted:
                    vecs.append(v)
                    meta.append(d)

            # 4. Final Matrix Assembly
            if not vecs:
                self._index_matrix = np.zeros((0, 0), dtype=float)
                self._meta_docs = []
                return SafeResult.ok({"count": 0})

            M = np.array(vecs, dtype=float)
            if M.dtype == object:
                return SafeResult.fail("Embeddings have inconsistent dimensions.")

            self._index_matrix, self._meta_docs = M, meta
            logger.info(f"Successfully indexed {M.shape[0]} vectors.")
            return SafeResult.ok({"count": int(M.shape[0]), "dim": int(M.shape[1])})

        except Exception as e:
            logger.exception("Rebuild index crashed")
            return SafeResult.fail(str(e))

async def setup_demo():
    print("--- Initializing test_vector_search Demo ---")
    from zmongo_toolbag.zmongo import ZMongo
    db = ZMongo()
    from zmongo_toolbag.zembedder import ZEmbedder
    embedder = ZEmbedder()
    collection = "test_vector_search"

    # 1. Clean the collection
    db.delete_all_documents(collection)

    # 2. Sample records for testing
    records = [
        {
            "_id": ObjectId(),
            "citation": "Binger v. King Pest Control",
            "text": "Landmark Florida case regarding pre-trial disclosure of expert witnesses."
        },
        {
            "_id": ObjectId(),
            "citation": "Negligence Overview",
            "text": "The four elements of negligence are duty, breach, causation, and damages."
        },
        {
            "_id": ObjectId(),
            "citation": "Premises Liability",
            "text": "Property owners owe a duty of care to invited guests to maintain safe conditions."
        }
    ]

    # 3. Batch insert and embed
    for doc in records:
        print(f"Persisting and embedding: {doc['citation']}")
        await db.insert_one_async(collection, doc)

        # Generate embeddings in the retrieval.document style
        await embedder.get_embedding(
            collection=collection,
            document_id=doc["_id"],
            text_field="text",
            embedding_field="embedding.text"
        )

    print("--- Rebuilding Local Search Index ---")
    await embedder.vector_search.rebuild_index()
    print("Demo setup complete. Collection 'test_vector_search' is ready for testing.")
    embedder.close()


if __name__ == "__main__":
    # Import inside main to prevent top-level circular dependency
    asyncio.run(setup_demo())

    async def demo():
        print("\n=== LocalVectorSearch Demo ===")
        from zmongo_toolbag.zmongo import ZMongo
        repo = ZMongo()
        from zmongo_toolbag.zembedder import ZEmbedder
        embedder = ZEmbedder()
        searcher = LocalVectorSearch(repository=repo, collection="knowledge_base")
        await searcher.rebuild_index()
        embedder.close()

    asyncio.run(demo())