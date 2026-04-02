"""
ZEmbedder – Local BGE-M3 Hybrid Embedding manager for Blackwell RTX 5080.
Integrates with ZMongo, LocalVectorSearch, and SafeResult.
"""

import asyncio
import logging
import os
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

from dotenv import load_dotenv
from pymilvus.model.hybrid import BGEM3EmbeddingFunction

# Internal toolbag imports
from .data_processor import DataProcessor
from .safe_result import SafeResult
from .zmongo import ZMongo
from .local_vector_search import LocalVectorSearch

# Load environment resources
RESOURCE_PATH = Path.home() / ".resources"
load_dotenv(RESOURCE_PATH / ".env")
load_dotenv(RESOURCE_PATH / ".secrets")

logger = logging.getLogger(__name__)

# Constants for BGE-M3
DEFAULT_MODEL_PATH = "/home/comfyuser/.resources/preserved_models/encoders/bge-m3"
DEFAULT_DB_NAME = "wiki_kb"
class ZEmbedder:
    """
    Local BGE-M3 Embedding manager.
    Produces Dense and Sparse vectors for hybrid RAG pipelines.
    """
    def __init__(
        self,
        db_name: str = DEFAULT_DB_NAME,
        model_path: str = DEFAULT_MODEL_PATH,
        device: str = "cuda:0",
        output_dimensionality: int = 1024 # BGE-M3 default dense dim
    ):
        self.repository = ZMongo(db_name=db_name)
        self.device = device
        self.default_output_dim = output_dimensionality

        # Verify local model path
        if not os.path.exists(model_path):
            logger.error(f"❌ Model not found at {model_path}. Please download it first.")
            raise FileNotFoundError(f"BGE-M3 not found at {model_path}")

        # Initialize the Hybrid Model on Blackwell GPU
        self.model = BGEM3EmbeddingFunction(
            model_name=model_path,
            device=device,
            use_fp16=True  # Optimized for RTX 5080
        )

        # Internal searcher for backward compatibility
        self.vector_search = LocalVectorSearch(
            repository=self.repository,
            collection="knowledge_base",
            embedding_field="embedding.dense",
            vector_key="vectors"
        )
        logger.info(f"✅ ZEmbedder (BGE-M3) initialized on {device}")

    @staticmethod
    def _to_dense_list(vec: Any) -> List[float]:
        if hasattr(vec, "tolist"):
            return vec.tolist()
        return list(vec)

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

        # scipy sparse row / matrix style
        if hasattr(sparse, "tocoo"):
            coo = sparse.tocoo()
            return {str(int(col)): float(val) for col, val in zip(coo.col, coo.data)}

        # some libs may expose indices/data separately
        indices = getattr(sparse, "indices", None)
        data = getattr(sparse, "data", None)
        if indices is not None and data is not None:
            return {str(int(i)): float(v) for i, v in zip(indices, data)}

        raise TypeError(f"Unsupported sparse vector type: {type(sparse)!r}")

    async def embed_many(self, texts: List[str]) -> SafeResult:
        """Embeds multiple texts using hybrid logic."""
        try:
            if not texts:
                return SafeResult.ok({"dense": [], "sparse": [], "count": 0})

            loop = asyncio.get_running_loop()
            embeddings = await loop.run_in_executor(
                None,
                self.model.encode_documents,
                texts,
            )

            dense_vectors = [self._to_dense_list(vec) for vec in embeddings["dense"]]
            sparse_vectors = [self._to_sparse_dict(vec) for vec in embeddings["sparse"]]

            return SafeResult.ok({
                "dense": dense_vectors,
                "sparse": sparse_vectors,
                "count": len(texts),
            })
        except Exception as e:
            logger.exception("Batch embedding failed")
            return SafeResult.fail(e)

    async def get_embedding(
            self,
            text: Optional[str] = None,
            *,
            collection: Optional[str] = None,
            document_id: Optional[Any] = None,
            text_field: str = "text",
            embedding_field: str = "embedding",
            **kwargs,
    ) -> SafeResult:
        """
        Fetches or embeds text. Returns a SafeResult containing
        BSON-safe dense and sparse vectors.
        """
        try:
            if not text and collection and document_id is not None:
                fetch_res = await self.repository.find_one_async(
                    collection,
                    {"_id": document_id},
                )
                if not fetch_res.success:
                    return fetch_res

                text = DataProcessor.get_value(fetch_res.data, text_field)

            if not text:
                return SafeResult.fail("No text content provided or found.")

            loop = asyncio.get_running_loop()
            embs = await loop.run_in_executor(
                None,
                self.model.encode_queries,
                [text],
            )

            dense_vec = self._to_dense_list(embs["dense"][0])
            sparse_vec = self._to_sparse_dict(embs["sparse"][0])

            if collection and document_id is not None:
                update_payload = {
                    text_field: text,
                    embedding_field: {
                        "dense": dense_vec,
                        "sparse": sparse_vec,
                        "model": "bge-m3-local",
                        "dimensionality": len(dense_vec),
                    },
                }

                persist_res = await self.repository.insert_or_update(
                    collection,
                    {"_id": document_id},
                    update_payload,
                )
                if not persist_res.success:
                    return persist_res

            return SafeResult.ok({
                "dense": dense_vec,
                "sparse": sparse_vec,
                "text": text,
                "dimensionality": len(dense_vec),
            })
        except Exception as e:
            logger.exception("Single embedding generation failed")
            return SafeResult.fail(e)

    def close(self):
        """Cleanup resources."""
        if hasattr(self, "repository"):
            self.repository.close()
        if hasattr(self, "vector_search"):
            self.vector_search.clear_index()


# --- Async Demo ---
if __name__ == "__main__":
    async def run_demo():
        embedder = ZEmbedder()

        test_text = "Legal compliance for AI Blackwell architecture."
        res = await embedder.get_embedding(text=test_text)

        if res.success:
            print(f"✅ Successfully generated hybrid embeddings.")
            print(f"Dense Dim: {res.data['dimensionality']}")
            print(f"Sparse Keys: {list(res.data['sparse'].keys())[:5]}...")

        embedder.close()

    asyncio.run(run_demo())