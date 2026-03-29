import asyncio
import logging
import threading
from concurrent.futures import Future
from typing import Any, Dict, List, Optional

from langchain_core.callbacks import CallbackManagerForRetrieverRun
from langchain_core.documents import Document
from langchain_core.retrievers import BaseRetriever
from pydantic import ConfigDict, Field

from zmongo_retriever.zmongo_toolbag.local_vector_search import LocalVectorSearch
from zmongo_retriever.zmongo_toolbag.safe_result import SafeResult
from zmongo_retriever.zmongo_toolbag.zembedder import ZEmbedder
from zmongo_retriever.zmongo_toolbag.zmongo import ZMongo

# ----------------------------------------------------------------------
# Constants / Default Values
# ----------------------------------------------------------------------
_DEFAULT_COLLECTION = "retriever_demo_kb"
_DEFAULT_EMBED_FIELD = "embedding"
_DEFAULT_CONTENT_FIELD = "text"
_DEFAULT_VECTOR_KEY = "dense"

logger = logging.getLogger(__name__)


def _run_async_in_new_thread(coro) -> Any:
    """Run an async coroutine in a dedicated thread to avoid deadlocks."""
    fut: Future[Any] = Future()

    def _runner() -> None:
        try:
            fut.set_result(asyncio.run(coro))
        except Exception as exc:  # pragma: no cover
            fut.set_exception(exc)

    t = threading.Thread(target=_runner, daemon=True)
    t.start()
    return fut.result()


class ZRetriever(BaseRetriever):
    """
    LangChain-compatible retriever backed by:
    - ZMongo for storage
    - ZEmbedder for dense query embeddings
    - LocalVectorSearch for in-memory similarity search
    """

    model_config = ConfigDict(arbitrary_types_allowed=True, extra="allow")

    repository: ZMongo = Field(...)
    embedder: ZEmbedder = Field(...)
    vector_searcher: LocalVectorSearch = Field(...)

    collection_name: str = Field(default=_DEFAULT_COLLECTION)
    embedding_field: str = Field(default=_DEFAULT_EMBED_FIELD)
    content_field: str = Field(default=_DEFAULT_CONTENT_FIELD)

    top_k: int = Field(default=10, ge=1)
    similarity_threshold: float = Field(default=0.6)

    def _get_relevant_documents(
        self,
        query: str,
        *,
        run_manager: Optional[CallbackManagerForRetrieverRun] = None,
    ) -> List[Document]:
        """
        Sync entrypoint for LangChain.

        If already inside an event loop, run the async retriever in a dedicated
        thread to avoid nested-loop failures.
        """
        try:
            loop = asyncio.get_running_loop()
            if loop.is_running():
                return _run_async_in_new_thread(
                    self._aget_relevant_documents(query, run_manager=run_manager)
                )
        except RuntimeError:
            pass

        return asyncio.run(self._aget_relevant_documents(query, run_manager=run_manager))

    async def _aget_relevant_documents(
        self,
        query: str,
        *,
        run_manager: Optional[CallbackManagerForRetrieverRun] = None,
    ) -> List[Document]:
        """
        Async entrypoint.

        Current ZEmbedder returns:
            {
                "dense": [...],
                "sparse": [...],
                "count": N
            }

        So the query vector must come from data["dense"][0].
        """
        if not query or not str(query).strip():
            return []

        try:
            emb_res = await self.embedder.embed_many([query])
            if not emb_res.success:
                logger.warning("Query embedding failed: %s", getattr(emb_res, "error", emb_res))
                return []

            emb_data = emb_res.data or {}
            dense_vectors = emb_data.get("dense") or []
            if not dense_vectors:
                logger.info("No dense query vector returned by embedder.")
                return []

            query_vector = dense_vectors[0]

            search_res = await self.vector_searcher.search(
                query_vector=query_vector,
                top_k=self.top_k,
            )
            if not search_res.success:
                logger.warning("Vector search failed: %s", getattr(search_res, "error", search_res))
                return []

            hits = search_res.data or []
            if not hits:
                return []

            return self._format_and_filter_results(hits)

        except Exception:
            logger.exception("Retriever query failed")
            return []

    def _format_and_filter_results(self, hits: List[Dict[str, Any]]) -> List[Document]:
        """
        Convert LocalVectorSearch hits into LangChain Document objects.
        """
        docs: List[Document] = []

        for hit in hits:
            score = float(hit.get("retrieval_score", 0.0))
            if score < self.similarity_threshold:
                continue

            doc_data = hit.get("document") or {}

            page_content = doc_data.get(self.content_field)
            if not page_content:
                for fallback in ("text", "content", "page_content", "body"):
                    page_content = doc_data.get(fallback)
                    if page_content:
                        break

            page_content = str(page_content or "").strip()
            if not page_content:
                continue

            metadata = dict(doc_data)
            metadata["retrieval_score"] = score

            docs.append(Document(page_content=page_content, metadata=metadata))

        return docs

    async def rebuild_index(self) -> SafeResult:
        return await self.vector_searcher.rebuild_index()


# ----------------------------------------------------------------------
# Demo
# ----------------------------------------------------------------------
async def _demo_async() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )

    db_client = ZMongo()
    embedder = ZEmbedder()
    collection = _DEFAULT_COLLECTION

    try:
        # 1. CLEAN START
        logger.info("Cleaning collection '%s'...", collection)
        delete_res = await db_client.delete_many_async(collection, {})
        if not delete_res.success:
            logger.warning("Collection cleanup failed: %s", getattr(delete_res, "error", delete_res))

        # 2. INSERT DATA
        knowledge = [
            {"topic": "Biology", "text": "Mitochondria generate energy in the cell."},
            {"topic": "Astronomy", "text": "Jupiter is the largest planet in our solar system."},
            {"topic": "History", "text": "The Roman Empire shaped the foundation of Western civilization."},
        ]

        logger.info("Inserting and embedding fresh data...")
        for item in knowledge:
            ins_res = await db_client.insert_one_async(collection, item)
            if not ins_res.success:
                logger.warning("Insert failed for item %s: %s", item, getattr(ins_res, "error", ins_res))
                continue

            inserted_id = (ins_res.data or {}).get("inserted_id")
            if inserted_id is None:
                logger.warning("Insert succeeded but no inserted_id returned for item: %s", item)
                continue

            emb_store_res = await embedder.get_embedding(
                text=item["text"],
                collection=collection,
                document_id=inserted_id,
                embedding_field=_DEFAULT_EMBED_FIELD,
            )
            if not emb_store_res.success:
                logger.warning(
                    "Embedding persistence failed for inserted_id=%s: %s",
                    inserted_id,
                    getattr(emb_store_res, "error", emb_store_res),
                )

        # 3. INITIALIZE VECTOR SEARCH
        vector_search = LocalVectorSearch(
            repository=db_client,
            collection=collection,
            embedding_field=_DEFAULT_EMBED_FIELD,
            vector_key=_DEFAULT_VECTOR_KEY,  # embedding["dense"]
        )

        retriever = ZRetriever(
            repository=db_client,
            embedder=embedder,
            vector_searcher=vector_search,
            collection_name=collection,
            embedding_field=_DEFAULT_EMBED_FIELD,
            content_field=_DEFAULT_CONTENT_FIELD,
            top_k=5,
            similarity_threshold=0.5,
        )

        # 4. BUILD INDEX
        idx_res = await retriever.rebuild_index()
        if idx_res.success:
            idx_data = idx_res.data or {}
            logger.info(
                "Index rebuild: success=%s count=%s dim=%s",
                idx_res.success,
                idx_data.get("count"),
                idx_data.get("dim"),
            )
        else:
            logger.error("Index rebuild failed: %s", getattr(idx_res, "error", idx_res))
            return

        # 5. TEST QUERY
        query = "Which organelle provides energy in the cell?"
        results = await retriever.ainvoke(query)

        print(f"\n--- Final Results for: '{query}' ---")
        if not results:
            print("No matches found above threshold.")
            return

        for i, doc in enumerate(results, 1):
            content = doc.page_content or "[EMPTY CONTENT]"
            score = float(doc.metadata.get("retrieval_score", 0.0))
            topic = doc.metadata.get("topic", "Unknown")
            print(f"Result {i}: {content}")
            print(f"   Topic: {topic}")
            print(f"   Score: {score:.4f}")

    finally:
        try:
            embedder.close()
        except Exception:
            logger.exception("Failed to close embedder cleanly")

        try:
            db_client.close()
        except Exception:
            logger.exception("Failed to close db client cleanly")


if __name__ == "__main__":
    asyncio.run(_demo_async())