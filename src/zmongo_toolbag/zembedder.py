"""
ZEmbedder – Gemini-based embedding manager with chunking, caching, SafeResult and ZMongo compatibility.
"""

import asyncio
import logging
from pathlib import Path
from typing import Any, Dict, List, Optional, Literal

from dotenv import load_dotenv

from data_processing import DataProcessor
# Fixed imports without leading dots

from gemini_embedding_model import (
    EMBEDDING_STYLE_RETRIEVAL_DOCUMENT,
    EMBEDDING_STYLE_RETRIEVAL_QUERY, GeminiEmbeddingModel,
)
from local_vector_search import LocalVectorSearch
from safe_result import SafeResult
from zmongo import ZMongo

load_dotenv(Path.home() / ".resources" / ".env")
load_dotenv(Path.home() / ".resources" / ".secrets")
logger = logging.getLogger(__name__)

CHUNK_STYLE_FIXED: Literal["fixed"] = "fixed"
CHUNK_STYLE_SENTENCE: Literal["sentence"] = "sentence"
CHUNK_STYLE_PARAGRAPH: Literal["paragraph"] = "paragraph"
ChunkStyle = Literal["fixed", "sentence", "paragraph"]

# --- CONFIGURATION ---
collection = "cases"
text_field = "citation"
embedding_field = "embedding.citation"
doc_id_str = "6941b65ba887fa041899b125"
output_dim = 768

class ZEmbedder:
    def __init__(self, model: Optional[Any] = None, output_dimensionality: Optional[int] = 768):
        self.repository = ZMongo()
        embedding_root = "embedding"
        field_key = embedding_field.split('.')[-1] if '.' in embedding_field else embedding_field

        self.vector_search = LocalVectorSearch(
            repository=self.repository,
            collection=collection,
            embedding_field=embedding_root,
            field_key=field_key,
            vector_key="vectors"
        )

        self.model = model or self._load_default_model(output_dimensionality)
        self.default_output_dim = output_dimensionality
        logger.info("✅ ZEmbedder initialized (dim=%s)", getattr(self.model, "output_dimensionality", "default"))

    @staticmethod
    def _load_default_model(output_dimensionality: Optional[int]) -> GeminiEmbeddingModel:
        return GeminiEmbeddingModel(model_name="gemini-embedding-001", output_dimensionality=output_dimensionality)

    async def _await_repo_result(self, maybe_result: Any) -> SafeResult:
        if asyncio.iscoroutine(maybe_result): maybe_result = await maybe_result
        return maybe_result if isinstance(maybe_result, SafeResult) else SafeResult.ok(maybe_result)

    def _split_text_into_chunks(self, text: str, *, chunk_style: ChunkStyle = CHUNK_STYLE_FIXED, chunk_size: int = 1500, overlap: int = 150) -> List[str]:
        if not text: return []
        if chunk_style == CHUNK_STYLE_FIXED: return self._split_fixed(text, chunk_size=chunk_size, overlap_chars=overlap)
        return [text]

    def _split_fixed(self, text: str, *, chunk_size: int, overlap_chars: int) -> List[str]:
        chunks, n = [], len(text)
        if n == 0: return chunks
        start, step = 0, max(1, chunk_size - max(0, overlap_chars))
        while start < n:
            end = min(n, start + chunk_size)
            chunks.append(text[start:end])
            if end >= n: break
            start += step
        return chunks

    async def _get_embeddings_from_chunks(self, chunks: List[str], *, embedding_style: str, output_dimensionality: Optional[int]) -> Dict[str, List[float]]:
        if not chunks: return {}
        # Delegate to model
        vectors = await self.model.embed(chunks, style=embedding_style, output_dimensionality=output_dimensionality)
        return dict(zip(chunks, vectors))

    async def get_embedding(self, text: Optional[str] = None, *, collection: Optional[str] = None, document_id: Optional[Any] = None, text_field: str = "text", embedding_field: str = "embedding", skip_if_present: bool = False, as_safe_result: bool = True, **kwargs) -> Any:
        style = kwargs.get("embedding_style") or EMBEDDING_STYLE_RETRIEVAL_DOCUMENT
        dim = kwargs.get("output_dimensionality") or self.default_output_dim

        if not text and collection and document_id:
            fetch_res = await self._await_repo_result(self.repository.find_one(collection, {"_id": document_id}))
            if not fetch_res.success: return fetch_res if as_safe_result else fetch_res.to_dict()
            text = DataProcessor.get_value(fetch_res.data, text_field)

        chunks = self._split_text_into_chunks(text)
        embedding_map = await self._get_embeddings_from_chunks(chunks, embedding_style=style, output_dimensionality=dim)
        vectors = [embedding_map[c] for c in chunks if c in embedding_map]

        if collection and document_id:
            update_payload = {}
            DataProcessor.set_value(update_payload, embedding_field, vectors)
            # Safe unique insert/update logic
            await self._await_repo_result(self.repository.insert_or_update(collection, {"_id": document_id}, update_payload))

        result = SafeResult.ok({"vectors": vectors, "dimensionality": len(vectors[0]) if vectors else 0})
        return result if as_safe_result else result.data

    def get_embedding_sync(self, *args, **kwargs) -> SafeResult:
        try: return asyncio.run(self.get_embedding(*args, **kwargs))
        except Exception as e: return SafeResult.fail(str(e))

    async def find_similar_documents(self, query_text: str, n_results: int = 5, target_collection: str = collection, **kwargs) -> Any:
        dim = kwargs.get("output_dimensionality") or self.default_output_dim
        # 1. Embed query
        q_emb_res = await self.get_embedding(query_text, embedding_style=EMBEDDING_STYLE_RETRIEVAL_QUERY, output_dimensionality=dim)
        if not q_emb_res.success: return q_emb_res

        # 2. Search using LocalVectorSearch index
        search_res = await self.vector_search.search(query_vector=q_emb_res.data["vectors"][0], top_k=n_results)
        if not search_res.success: return search_res

        payload = {"query": query_text, "results": search_res.data}
        return SafeResult.ok(payload)

    def close(self):
        """Standardize shutdown sequence."""
        if hasattr(self, "repository"): self.repository.close()
        if hasattr(self, "vector_search"): self.vector_search.clear_index()

if __name__ == "__main__":
   embedder = ZEmbedder(output_dimensionality=output_dim)
   # Print results to console as seen in the execution log
   results = asyncio.run(embedder.find_similar_documents(query_text="Binger v. King Pest Control", n_results=3))
   print(results)
   embedder.close()