"""
ZEmbedder – Local BGE-M3 hybrid embedding service for ZMongo.

Architecture goals:
- Keep this as a core service-kernel component.
- Produce BSON-safe dense and sparse embeddings.
- Support async embedding generation without blocking the event loop.
- Allow repository injection instead of always creating its own ZMongo.
- Make LocalVectorSearch optional and configurable.
- Preserve backward compatibility with likely callers.
"""

from __future__ import annotations

import asyncio
import logging
import os
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence

from dotenv import load_dotenv
from pymilvus.model.hybrid import BGEM3EmbeddingFunction

from .data_processor import DataProcessor
from .local_vector_search import LocalVectorSearch
from .safe_result import SafeResult
from .zmongo import ZMongo

RESOURCE_PATH = Path.home() / ".resources"
load_dotenv(RESOURCE_PATH / ".env")
load_dotenv(RESOURCE_PATH / ".secrets")

logger = logging.getLogger(__name__)

DEFAULT_MODEL_PATH = "/home/comfyuser/.resources/preserved_models/encoders/bge-m3"
DEFAULT_DB_NAME = "wiki_kb"
DEFAULT_DEVICE = "cuda:0"
DEFAULT_MODEL_NAME = "bge-m3-local"
DEFAULT_EMBEDDING_FIELD = "embedding"


class ZEmbedder:
    """
    Local BGE-M3 embedding service.

    Responsibilities:
    - Embed one or many texts into dense and sparse vectors
    - Optionally fetch source text from a repository document
    - Optionally persist embeddings back into MongoDB
    - Optionally perform local in-memory similarity search via LocalVectorSearch
    """

    def __init__(
        self,
        db_name: str = DEFAULT_DB_NAME,
        model_path: str = DEFAULT_MODEL_PATH,
        device: str = DEFAULT_DEVICE,
        output_dimensionality: int = 1024,
        *,
        repository: Optional[ZMongo] = None,
        default_collection: str = "knowledge_base",
        embedding_field: str = DEFAULT_EMBEDDING_FIELD,
        model_name: str = DEFAULT_MODEL_NAME,
        use_fp16: bool = True,
        auto_create_vector_search: bool = True,
    ) -> None:
        self.device = device
        self.default_output_dim = int(output_dimensionality)
        self.default_collection = str(default_collection)
        self.embedding_field = str(embedding_field)
        self.model_name = str(model_name)
        self._owns_repository = repository is None

        self.repository: ZMongo = repository or ZMongo(db_name=db_name)

        model_path = str(model_path)
        if not os.path.exists(model_path):
            logger.error("Model not found at %s", model_path)
            raise FileNotFoundError(f"BGE-M3 not found at {model_path}")

        self.model = BGEM3EmbeddingFunction(
            model_name=model_path,
            device=device,
            use_fp16=bool(use_fp16),
        )

        self.vector_search: Optional[LocalVectorSearch] = None
        if auto_create_vector_search:
            self.vector_search = self._build_vector_search(
                collection=self.default_collection,
                embedding_field=self.embedding_field,
            )

        logger.info(
            "ZEmbedder initialized | model=%s | device=%s | collection=%s",
            self.model_name,
            self.device,
            self.default_collection,
        )

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------
    def _build_vector_search(
        self,
        *,
        collection: str,
        embedding_field: str,
    ) -> LocalVectorSearch:
        """
        Current LocalVectorSearch expects:
        - embedding_field to point to the container field, e.g. 'embedding'
        - field_key to identify 'dense' inside that container
        """
        return LocalVectorSearch(
            repository=self.repository,
            collection=collection,
            embedding_field=embedding_field,
            field_key="dense",
            vector_key="vectors",
            score_mode="cosine_0_1",
        )

    @staticmethod
    def _to_dense_list(vec: Any) -> List[float]:
        if vec is None:
            return []
        if hasattr(vec, "tolist"):
            vec = vec.tolist()
        return [float(x) for x in vec]

    @staticmethod
    def _to_sparse_dict(sparse: Any) -> Dict[str, float]:
        """
        Normalize BGE-M3 sparse output into a plain dict[str, float].
        Handles dict-like outputs and scipy sparse-style objects.
        """
        if sparse is None:
            return {}

        if isinstance(sparse, dict):
            return {str(k): float(v) for k, v in sparse.items()}

        if hasattr(sparse, "tocoo"):
            coo = sparse.tocoo()
            return {str(int(col)): float(val) for col, val in zip(coo.col, coo.data)}

        indices = getattr(sparse, "indices", None)
        data = getattr(sparse, "data", None)
        if indices is not None and data is not None:
            return {str(int(i)): float(v) for i, v in zip(indices, data)}

        raise TypeError(f"Unsupported sparse vector type: {type(sparse)!r}")

    @staticmethod
    def _clean_texts(texts: Sequence[Any]) -> List[str]:
        cleaned: List[str] = []
        for item in texts:
            if item is None:
                continue
            text = str(item).strip()
            if text:
                cleaned.append(text)
        return cleaned

    @staticmethod
    def _ensure_document_from_result(fetch_res: SafeResult) -> Optional[Dict[str, Any]]:
        if not fetch_res.success:
            return None

        data = fetch_res.data
        if isinstance(data, dict):
            document = data.get("document")
            if isinstance(document, dict):
                return document
            if "documents" in data and isinstance(data["documents"], list) and data["documents"]:
                first_doc = data["documents"][0]
                if isinstance(first_doc, dict):
                    return first_doc

        original = fetch_res.original()
        if isinstance(original, dict):
            document = original.get("document")
            if isinstance(document, dict):
                return document

        return None

    def _make_embedding_payload(
        self,
        *,
        text: str,
        dense_vec: List[float],
        sparse_vec: Dict[str, float],
    ) -> Dict[str, Any]:
        return {
            "dense": dense_vec,
            "sparse": sparse_vec,
            "model": self.model_name,
            "dimensionality": len(dense_vec),
            "text_length": len(text),
        }

    async def _encode_documents(self, texts: List[str]) -> Dict[str, Any]:
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(
            None,
            self.model.encode_documents,
            texts,
        )

    async def _encode_queries(self, texts: List[str]) -> Dict[str, Any]:
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(
            None,
            self.model.encode_queries,
            texts,
        )

    # ------------------------------------------------------------------
    # Core embedding APIs
    # ------------------------------------------------------------------
    async def embed_many(self, texts: List[str]) -> SafeResult:
        """
        Embed a list of texts for storage / corpus use.
        """
        try:
            cleaned = self._clean_texts(texts)
            if not cleaned:
                return SafeResult.ok(
                    {
                        "dense": [],
                        "sparse": [],
                        "texts": [],
                        "count": 0,
                        "model": self.model_name,
                    }
                )

            embeddings = await self._encode_documents(cleaned)

            dense_vectors = [self._to_dense_list(vec) for vec in embeddings["dense"]]
            sparse_vectors = [self._to_sparse_dict(vec) for vec in embeddings["sparse"]]

            return SafeResult.ok(
                {
                    "dense": dense_vectors,
                    "sparse": sparse_vectors,
                    "texts": cleaned,
                    "count": len(cleaned),
                    "dimensionality": len(dense_vectors[0]) if dense_vectors else 0,
                    "model": self.model_name,
                }
            )
        except Exception as exc:
            logger.exception("Batch embedding failed")
            return SafeResult.from_exception(exc, operation="embed_many")

    async def embed_query(self, text: str) -> SafeResult:
        """
        Embed one query string for retrieval/search use.
        """
        try:
            cleaned = str(text or "").strip()
            if not cleaned:
                return SafeResult.fail("No text content provided or found.")

            embeddings = await self._encode_queries([cleaned])

            dense_vec = self._to_dense_list(embeddings["dense"][0])
            sparse_vec = self._to_sparse_dict(embeddings["sparse"][0])

            return SafeResult.ok(
                {
                    "dense": dense_vec,
                    "sparse": sparse_vec,
                    "text": cleaned,
                    "dimensionality": len(dense_vec),
                    "model": self.model_name,
                }
            )
        except Exception as exc:
            logger.exception("Single query embedding generation failed")
            return SafeResult.from_exception(exc, operation="embed_query")

    async def get_embedding(
        self,
        text: Optional[str] = None,
        *,
        collection: Optional[str] = None,
        document_id: Optional[Any] = None,
        text_field: str = "text",
        embedding_field: str = DEFAULT_EMBEDDING_FIELD,
        persist: bool = True,
        use_query_encoder: bool = True,
        query: Optional[Dict[str, Any]] = None,
        **kwargs,
    ) -> SafeResult:
        """
        Backward-compatible entry point:
        - embed direct text, or
        - fetch text from a document,
        - optionally persist the result back into MongoDB.
        """
        del kwargs

        try:
            resolved_collection = collection or self.default_collection
            resolved_text = str(text).strip() if text is not None else ""

            source_document: Optional[Dict[str, Any]] = None

            if not resolved_text and resolved_collection and (document_id is not None or query):
                fetch_query = query or {"_id": document_id}
                fetch_res = await self.repository.find_one_async(
                    resolved_collection,
                    fetch_query,
                )
                if not fetch_res.success:
                    return fetch_res

                source_document = self._ensure_document_from_result(fetch_res)
                if source_document is None:
                    return SafeResult.fail(
                        "Document fetch succeeded but no document payload was found."
                    )

                fetched_text = DataProcessor.get_value(source_document, text_field)
                if fetched_text is None:
                    return SafeResult.fail(
                        f"No text content found at path '{text_field}'."
                    )

                resolved_text = str(fetched_text).strip()

            if not resolved_text:
                return SafeResult.fail("No text content provided or found.")

            embed_res = (
                await self.embed_query(resolved_text)
                if use_query_encoder
                else await self.embed_many([resolved_text])
            )
            if not embed_res.success:
                return embed_res

            embedded = embed_res.data or {}
            if not isinstance(embedded, dict):
                return SafeResult.fail("Unexpected embedding payload shape.")

            dense_vec = embedded.get("dense")
            sparse_vec = embedded.get("sparse")

            if isinstance(dense_vec, list) and dense_vec and isinstance(dense_vec[0], list):
                dense_vec = dense_vec[0]
            if isinstance(sparse_vec, list) and sparse_vec and isinstance(sparse_vec[0], dict):
                sparse_vec = sparse_vec[0]

            dense_vec = self._to_dense_list(dense_vec)
            sparse_vec = self._to_sparse_dict(sparse_vec)

            payload = {
                "dense": dense_vec,
                "sparse": sparse_vec,
                "text": resolved_text,
                "dimensionality": len(dense_vec),
                "model": self.model_name,
            }

            if persist and resolved_collection and (document_id is not None or source_document is not None or query):
                persist_query = query
                if not persist_query:
                    if source_document and source_document.get("_id") is not None:
                        persist_query = {"_id": source_document["_id"]}
                    elif document_id is not None:
                        persist_query = {"_id": document_id}

                if persist_query:
                    update_payload = {
                        text_field: resolved_text,
                        embedding_field: self._make_embedding_payload(
                            text=resolved_text,
                            dense_vec=dense_vec,
                            sparse_vec=sparse_vec,
                        ),
                    }

                    persist_res = await self.repository.insert_or_update_async(
                        resolved_collection,
                        persist_query,
                        update_payload,
                    )
                    if not persist_res.success:
                        return persist_res

                    payload["persisted"] = True
                    payload["collection"] = resolved_collection
                    payload["query"] = DataProcessor.to_json_compatible(persist_query)
                else:
                    payload["persisted"] = False
            else:
                payload["persisted"] = False

            return SafeResult.ok(payload)

        except Exception as exc:
            logger.exception("Embedding generation failed")
            return SafeResult.from_exception(exc, operation="get_embedding")

    # ------------------------------------------------------------------
    # Persistence / retrieval helpers
    # ------------------------------------------------------------------
    async def persist_embedding(
        self,
        *,
        collection: str,
        query: Dict[str, Any],
        text: str,
        embedding_result: Dict[str, Any],
        text_field: str = "text",
        embedding_field: str = DEFAULT_EMBEDDING_FIELD,
    ) -> SafeResult:
        try:
            dense_vec = self._to_dense_list(embedding_result.get("dense"))
            sparse_vec = self._to_sparse_dict(embedding_result.get("sparse"))

            update_payload = {
                text_field: text,
                embedding_field: self._make_embedding_payload(
                    text=text,
                    dense_vec=dense_vec,
                    sparse_vec=sparse_vec,
                ),
            }

            return await self.repository.insert_or_update_async(
                collection,
                query,
                update_payload,
            )
        except Exception as exc:
            logger.exception("Persist embedding failed")
            return SafeResult.from_exception(exc, operation="persist_embedding")

    def configure_vector_search(
        self,
        *,
        collection: Optional[str] = None,
        embedding_field: Optional[str] = None,
    ) -> LocalVectorSearch:
        self.vector_search = self._build_vector_search(
            collection=collection or self.default_collection,
            embedding_field=embedding_field or self.embedding_field,
        )
        return self.vector_search

    async def rebuild_search_index(
        self,
        *,
        collection: Optional[str] = None,
        embedding_field: Optional[str] = None,
    ) -> SafeResult:
        try:
            searcher = self.vector_search
            target_collection = collection or self.default_collection
            target_embedding_field = embedding_field or self.embedding_field

            if (
                searcher is None
                or searcher.collection != target_collection
                or searcher.embedding_field != target_embedding_field
            ):
                searcher = self.configure_vector_search(
                    collection=target_collection,
                    embedding_field=target_embedding_field,
                )

            return await searcher.rebuild_index()
        except Exception as exc:
            logger.exception("Rebuild search index failed")
            return SafeResult.from_exception(exc, operation="rebuild_search_index")

    async def find_similar_documents(
        self,
        *,
        query_text: str,
        target_collection: Optional[str] = None,
        n_results: int = 5,
        embedding_field: Optional[str] = None,
        rebuild_index: bool = False,
    ) -> SafeResult:
        """
        Backward-compatible retrieval entry point expected by your node layer.
        """
        try:
            cleaned_query = str(query_text or "").strip()
            if not cleaned_query:
                return SafeResult.fail("Query text is empty.")

            embed_res = await self.embed_query(cleaned_query)
            if not embed_res.success:
                return embed_res

            query_payload = embed_res.data or {}
            if not isinstance(query_payload, dict):
                return SafeResult.fail("Unexpected query embedding payload shape.")

            dense_vec = self._to_dense_list(query_payload.get("dense"))

            collection_name = target_collection or self.default_collection
            field_name = embedding_field or self.embedding_field

            searcher = self.vector_search
            if (
                searcher is None
                or searcher.collection != collection_name
                or searcher.embedding_field != field_name
            ):
                searcher = self.configure_vector_search(
                    collection=collection_name,
                    embedding_field=field_name,
                )

            if rebuild_index:
                rebuild_res = await searcher.rebuild_index()
                if not rebuild_res.success:
                    return rebuild_res

            search_res = await searcher.search(dense_vec, top_k=n_results)
            if not search_res.success:
                return search_res

            raw_results = search_res.data if isinstance(search_res.data, list) else []

            return SafeResult.ok(
                {
                    "results": raw_results,
                    "count": len(raw_results),
                    "query_text": cleaned_query,
                    "collection": collection_name,
                    "model": self.model_name,
                }
            )
        except Exception as exc:
            logger.exception("Similarity search failed")
            return SafeResult.from_exception(exc, operation="find_similar_documents")

    # ------------------------------------------------------------------
    # Sync helpers for non-async callers
    # ------------------------------------------------------------------
    def embed_many_sync(self, texts: List[str]) -> SafeResult:
        return self.repository.run_sync(self.embed_many, texts)

    def get_embedding_sync(
        self,
        text: Optional[str] = None,
        **kwargs,
    ) -> SafeResult:
        return self.repository.run_sync(self.get_embedding, text=text, **kwargs)

    def rebuild_search_index_sync(self, **kwargs) -> SafeResult:
        return self.repository.run_sync(self.rebuild_search_index, **kwargs)

    def find_similar_documents_sync(self, **kwargs) -> SafeResult:
        return self.repository.run_sync(self.find_similar_documents, **kwargs)

    # ------------------------------------------------------------------
    # Cleanup
    # ------------------------------------------------------------------
    def close(self) -> None:
        if self.vector_search is not None:
            try:
                self.vector_search.clear_index()
            except Exception:
                logger.debug("Failed clearing vector search index", exc_info=True)

        if self._owns_repository and hasattr(self, "repository"):
            try:
                self.repository.close()
            except Exception:
                logger.debug("Failed closing repository", exc_info=True)