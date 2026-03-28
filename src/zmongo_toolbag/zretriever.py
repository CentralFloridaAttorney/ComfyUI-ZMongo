import asyncio
import logging
import threading
from concurrent.futures import Future
from typing import Any, Dict, List, Optional

import numpy as np
from langchain_core.callbacks import CallbackManagerForRetrieverRun
from langchain_core.documents import Document
from langchain_core.retrievers import BaseRetriever
from pydantic import ConfigDict, Field

from .local_vector_search import LocalVectorSearch
from .safe_result import SafeResult
from .zembedder import ZEmbedder
from .zmongo import ZMongo

# ----------------------------------------------------------------------
# Constants / Default Values
# ----------------------------------------------------------------------
_DEFAULT_COLLECTION = "knowledge_base"
_DEFAULT_EMBED_FIELD = "embedding"
_DEFAULT_CONTENT_FIELD = "text"

EMBEDDING_STYLE_RETRIEVAL_QUERY = "query"
EMBEDDING_STYLE_RETRIEVAL_DOCUMENT = "document"

logger = logging.getLogger(__name__)


def _run_async_in_new_thread(coro) -> Any:
    """
    Run an async coroutine in a dedicated thread with its own event loop.
    This avoids deadlocks when called from within an already-running loop thread.
    """
    fut: Future[Any] = Future()

    def _runner():
        try:
            result = asyncio.run(coro)
            fut.set_result(result)
        except Exception as e:
            fut.set_exception(e)

    t = threading.Thread(target=_runner, daemon=True)
    t.start()
    return fut.result()


class ZRetriever(BaseRetriever):
    """
    LangChain-compatible retriever using SafeResult-based ZMongo + ZEmbedder.

    Key fix:
    - Never block the *current* running event loop thread by scheduling work onto it
      and waiting on .result(). If called from a running loop, we run async work in
      a separate thread+loop.
    - In async path, do not call embedder.get_embedding_sync() (it can deadlock inside a loop).
      Use await embedder.embed_many(...).
    """
    model_config = ConfigDict(arbitrary_types_allowed=True, extra="allow")

    repository: ZMongo = Field(...)
    embedder: ZEmbedder = Field(...)
    vector_searcher: LocalVectorSearch = Field(...)

    collection_name: str = Field(default=_DEFAULT_COLLECTION)
    embedding_field: str = Field(default=_DEFAULT_EMBED_FIELD)
    content_field: str = Field(default=_DEFAULT_CONTENT_FIELD)

    top_k: int = Field(default=10)
    similarity_threshold: float = Field(default=0.8)
    query_embedding_style: str = Field(default=EMBEDDING_STYLE_RETRIEVAL_QUERY)

    # -----------------------------
    # LangChain sync entrypoint
    # -----------------------------
    def _get_relevant_documents(
        self, query: str, *, run_manager: Optional[CallbackManagerForRetrieverRun] = None
    ) -> List[Document]:
        """
        Sync retriever path.

        - If no running loop: safe to asyncio.run(...)
        - If a running loop exists in this thread: do NOT call run_coroutine_threadsafe(loop).result()
          (deadlock). Instead, run the async path in a separate thread.
        """
        try:
            loop = asyncio.get_running_loop()
            loop_running = loop.is_running()
        except RuntimeError:
            loop_running = False

        if not loop_running:
            return asyncio.run(self._aget_relevant_documents(query, run_manager=run_manager))

        return _run_async_in_new_thread(self._aget_relevant_documents(query, run_manager=run_manager))

    # -----------------------------
    # LangChain async entrypoint
    # -----------------------------
    async def _aget_relevant_documents(
        self, query: str, *, run_manager: Optional[CallbackManagerForRetrieverRun] = None
    ) -> List[Document]:
        logger.debug("ZRetriever query: %s", query)

        # 1) Embed query (ASYNC) - avoid get_embedding_sync() inside async context
        try:
            dim = getattr(self.embedder, "default_output_dim", None)
            emb_res = await self.embedder.embed_many(
                [query],
                style=self.query_embedding_style,
                output_dimensionality=dim,
            )
        except Exception as exc:
            logger.exception("Embedding exception: %s", exc)
            return []

        if not emb_res or not getattr(emb_res, "success", False):
            err = getattr(emb_res, "error", "Unknown error")
            logger.error("Embedding failed: %s", err)
            return []

        vectors = (emb_res.data or {}).get("vectors") or []
        if not vectors:
            return []

        qvec = np.array(vectors[0], dtype=float)

        # 2) Vector search (await)
        try:
            search_res = await self.vector_searcher.search(query_vector=qvec, top_k=self.top_k)
        except Exception as exc:
            logger.exception("Vector search exception: %s", exc)
            return []

        hits = search_res.data if isinstance(search_res, SafeResult) else search_res
        if not hits:
            return []

        return self._format_and_filter_results(hits)

    # -----------------------------
    # Helpers
    # -----------------------------
    def _format_and_filter_results(self, hits: List[Dict[str, Any]]) -> List[Document]:
        docs: List[Document] = []
        for hit in hits:
            score = float(hit.get("retrieval_score", 0.0))
            if score < float(self.similarity_threshold):
                continue

            doc_data = hit.get("document", hit) or {}
            page_content = doc_data.get(self.content_field, "") or ""

            # keep original fields but avoid nesting giant structures in metadata if you prefer
            metadata = dict(doc_data)
            metadata["retrieval_score"] = score

            docs.append(Document(page_content=page_content, metadata=metadata))
        return docs


# ----------------------------------------------------------------------
# Demo
# ----------------------------------------------------------------------
async def _demo_async():
    from bson.objectid import ObjectId

    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    logger.info("Starting ZRetriever real-data demo...")

    db_client = ZMongo()
    embedder = ZEmbedder()
    collection = _DEFAULT_COLLECTION

    # delete_all_documents is sync in your ZMongo
    # del_res = db_client.delete_all_documents(collection)
    # logger.info("Delete all result: %s", del_res)

    knowledge = [
        {"_id": ObjectId(), "topic": "Biology", "text": "Mitochondria generate energy in the cell."},
        {"_id": ObjectId(), "topic": "Astronomy", "text": "Jupiter is the largest planet."},
        {"_id": ObjectId(), "topic": "History", "text": "The Roman Empire shaped Western civilization."},
    ]

    for doc in knowledge:
        await db_client.insert_one_async(collection, doc)

        # document embedding: sync call is OK here only if it doesn't deadlock your loop.
        # To keep everything consistent, do async embedding too:
        embedder.get_embedding_sync(
            [doc["text"]],
            style=EMBEDDING_STYLE_RETRIEVAL_DOCUMENT,
            collection=collection,
            document_id=doc["_id"],
            embedding_field=_DEFAULT_EMBED_FIELD,
            output_dimensionality=getattr(embedder, "default_output_dim", None),
        )

        # 1) Confirm embeddings are actually stored on docs in Mongo
        docs_res = await db_client.find_many_async(collection, {}, limit=10)
        print("Docs in collection:", len(docs_res.data or []))

        for d in (docs_res.data or []):
            emb = d.get(_DEFAULT_EMBED_FIELD)
            print("doc", d.get("_id"), "has embedding?", bool(emb), "type:", type(emb).__name__, "len:",
                  (len(emb) if isinstance(emb, list) else None))

        # 2) Force rebuild and confirm index size
        from zmongo_toolbag import local_vector_search
        idx_res = await local_vector_search.rebuild_index()
        print("Index rebuild:", idx_res.success, idx_res.data, getattr(idx_res, "error", None))

    vector_search = LocalVectorSearch(
        repository=db_client,
        collection=collection,
        embedding_field=_DEFAULT_EMBED_FIELD,
    )

    retriever = ZRetriever(
        repository=db_client,
        embedder=embedder,
        vector_searcher=vector_search,
        collection_name=collection,
        similarity_threshold=0.75,
        top_k=2,
    )

    query = "Which organelle provides energy in the cell?"

    # In async demos, prefer ainvoke to avoid sync-in-async entirely.
    results = await retriever.ainvoke(query)
    print(f"\n\n--- Query: {query} ---")
    print(f"Results: {results}")

    for i, doc in enumerate(results, 1):
        print(f"\n--- Result {i} ---")
        print(f"Content: {doc.page_content}")
        print(f"Metadata: {doc.metadata}")

    embedder.close()


if __name__ == "__main__":
    try:
        asyncio.run(_demo_async())
    except KeyboardInterrupt:
        pass