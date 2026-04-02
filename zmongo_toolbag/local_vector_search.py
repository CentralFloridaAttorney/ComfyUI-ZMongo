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

from .safe_result import SafeResult

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
            # Standardizing on ZMongo find_many
            repo_call = self.repo.find_many(self.collection, query={}, limit=self.max_docs)
            return await self._await_repo_result(repo_call)
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

    async def search(self, query_vector: Any, top_k: int = 5) -> SafeResult:
        ensure_res = await self._ensure_index()
        if not ensure_res.success: return ensure_res

        if self._index_matrix is None or self._index_matrix.size == 0:
            return SafeResult.ok([])

        try:
            # FORCE conversion to a 1D float array regardless of input type
            q = np.array(query_vector, dtype=float).flatten()

            dim = int(self._index_matrix.shape[1])
            if q.shape[0] != dim:
                return SafeResult.fail(f"Dimension mismatch: Query {q.shape[0]} != Index {dim}")
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
        Fetches all documents from the repository, extracts vectors,
        and builds the in-memory NumPy search matrix.
        """
        try:
            # 1. Fetch documents from the ZMongo repository
            docs_res = await self._find_all_docs()
            if not docs_res.success:
                return docs_res

            all_docs = docs_res.data if isinstance(docs_res.data, list) else []

            temp_vectors = []
            temp_meta = []
            expected_dim = None

            # 2. Extract vectors and maintain metadata mapping
            for doc in all_docs:
                extracted_vectors = self._extract_vectors_from_doc(doc)
                for vec in extracted_vectors:
                    # Validate consistency of dimensions
                    current_dim = len(vec)
                    if expected_dim is None:
                        expected_dim = current_dim
                    elif current_dim != expected_dim:
                        logger.warning(
                            f"Skipping vector with mismatched dimension: {current_dim} (expected {expected_dim})")
                        continue

                    temp_vectors.append(vec)
                    temp_meta.append(doc)

            # 3. Handle empty datasets gracefully
            if not temp_vectors:
                self._index_matrix = np.empty((0, 0), dtype=float)
                self._meta_docs = []
                logger.info("Rebuild finished: No vectors found.")
                return SafeResult.ok({"count": 0, "dim": 0})

            # 4. Convert to NumPy matrix for fast similarity math
            matrix = np.array(temp_vectors, dtype=float)

            # Final safety check for malformed numpy arrays (e.g., ragged nested sequences)
            if matrix.dtype == object:
                return SafeResult.fail("Failed to build index: Vectors have inconsistent lengths.")

            self._index_matrix = matrix
            self._meta_docs = temp_meta

            logger.info(f"Index rebuilt successfully: {matrix.shape[0]} vectors, {matrix.shape[1]} dimensions.")
            return SafeResult.ok({
                "count": int(matrix.shape[0]),
                "dim": int(matrix.shape[1])
            })

        except Exception as e:
            logger.exception("Failed to rebuild local vector index")
            return SafeResult.fail(f"Rebuild index error: {str(e)}")
