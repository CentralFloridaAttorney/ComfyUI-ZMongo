import asyncio
from pathlib import Path
from typing import List, Optional, Dict, Any

import numpy as np
from dotenv import load_dotenv
import os  # Added os import

load_dotenv(Path.home() / ".resources" / ".secrets")
load_dotenv(Path.home() / ".resources" / ".env")

EMBEDDING_STYLE_RETRIEVAL_DOCUMENT = "retrieval.document"
EMBEDDING_STYLE_RETRIEVAL_QUERY = "retrieval.query"


class GeminiEmbeddingModel:
    """
    Gemini-based embedding model wrapper.

    Uses google.genai.Client().models.embed_content with gemini-embedding-001
    and maps ZEmbedder's styles to Gemini task types.
    """

    def __init__(
            self,
            model_name: str = "gemini-embedding-001",
            output_dimensionality: Optional[int] = None,
    ):
        # Lazy import so the module can still be imported without the package
        from google import genai

        self.name = model_name

        # Explicitly fetch API key from environment for robustness
        api_key = os.getenv("GEMINI_API_KEY")
        self.client = genai.Client(api_key=api_key)  # Pass the key explicitly

        self.output_dimensionality = output_dimensionality

    @staticmethod
    def _map_style_to_task_type(style: Optional[str]) -> str:
        """
        Map internal styles to Gemini task_type values.
        Defaults to SEMANTIC_SIMILARITY if unknown.
        """
        if style == EMBEDDING_STYLE_RETRIEVAL_DOCUMENT:
            return "RETRIEVAL_DOCUMENT"
        if style == EMBEDDING_STYLE_RETRIEVAL_QUERY:
            return "RETRIEVAL_QUERY"
        # Reasonable default for "just embed this text"
        return "SEMANTIC_SIMILARITY"

    def _build_config(self, task_type: str):
        """
        Build EmbedContentConfig if we have task_type and/or output_dim.
        """
        from google.genai import types as genai_types

        kwargs: Dict[str, Any] = {}
        if task_type:
            kwargs["task_type"] = task_type
        if self.output_dimensionality:
            kwargs["output_dimensionality"] = self.output_dimensionality

        if not kwargs:
            return None

        return genai_types.EmbedContentConfig(**kwargs)

    async def embed(self, texts: List[str], style: Optional[str] = None, **kwargs):
        """
        Asynchronous wrapper around the synchronous Gemini client.
        Returns a list of float vectors (one per text).
        """
        task_type = self._map_style_to_task_type(style)
        config = self._build_config(task_type)

        def _call() -> List[List[float]]:
            if config is None:
                result = self.client.models.embed_content(
                    model=self.name,
                    contents=texts,
                )
            else:
                result = self.client.models.embed_content(
                    model=self.name,
                    contents=texts,
                    config=config,
                )

            vectors: List[List[float]] = []
            for embedding_obj in result.embeddings:
                values = np.array(embedding_obj.values, dtype=float)

                # If we requested a non-default dimension (e.g. 768 / 1536),
                # normalize to unit length as recommended in the docs.
                if self.output_dimensionality and self.output_dimensionality != 3072:
                    norm = np.linalg.norm(values)
                    if norm:
                        values = values / norm

                vectors.append(values.tolist())

            return vectors

        # Run the blocking HTTP call in a thread so our async code stays clean
        return await asyncio.to_thread(_call)


if __name__ == "__main__":

    # --- Example Usage ---

    async def main():
        print("Initializing GeminiEmbeddingModel...")

        # Initialize the model. We can optionally request a specific dimension.
        # gemini-embedding-001 defaults to 3072, but we can test a reduced size.
        # Note: Reduced dimensionality requires the client library to be installed.
        embedder = GeminiEmbeddingModel(output_dimensionality=768)

        texts_to_embed = [
            "The quick brown fox jumps over the lazy dog.",
            "A fast, russet-colored canine leaps above a listless hound.",
            "Quantum entanglement is a phenomenon in physics.",
        ]

        print(f"\nTexts to embed ({len(texts_to_embed)}):")
        for i, text in enumerate(texts_to_embed):
            print(f"  {i + 1}. {text}")

        # --- 1. Semantic Similarity Embedding (Default) ---
        print("\n--- 1. Generating SEMANTIC_SIMILARITY embeddings (Default) ---")

        # Default style, good for comparing two general pieces of text
        similarity_embeddings = await embedder.embed(texts_to_embed, style=None)

        print(f"  Shape of first embedding: {np.array(similarity_embeddings[0]).shape}")

        # --- 2. Retrieval Document Embedding ---
        print("\n--- 2. Generating RETRIEVAL_DOCUMENT embeddings ---")

        # Style for documents being indexed in a retrieval system (e.g., RAG)
        document_embeddings = await embedder.embed(
            texts_to_embed,
            style=EMBEDDING_STYLE_RETRIEVAL_DOCUMENT
        )

        print(f"  Shape of first embedding: {np.array(document_embeddings[0]).shape}")

        # --- 3. Retrieval Query Embedding ---
        print("\n--- 3. Generating RETRIEVAL_QUERY embeddings ---")

        # Style for the user's query against the indexed documents
        query_embeddings = await embedder.embed(
            texts_to_embed,
            style=EMBEDDING_STYLE_RETRIEVAL_QUERY
        )

        print(f"  Shape of first embedding: {np.array(query_embeddings[0]).shape}")

        print("\nDemo finished successfully!")

        # Example of vector comparison (using the first text in the list)
        vector_a = np.array(similarity_embeddings[0])
        vector_b = np.array(document_embeddings[0])

        # Cosine similarity is a common metric for vector closeness
        dot_product = np.dot(vector_a, vector_b)
        norm_a = np.linalg.norm(vector_a)
        norm_b = np.linalg.norm(vector_b)

        # The vectors are normalized within the class if output_dimensionality is set,
        # so norm_a and norm_b should be close to 1.

        cosine_similarity = dot_product / (norm_a * norm_b)

        print(
            f"\nCosine Similarity between first 'similarity' vector and first 'document' vector: {cosine_similarity:.4f}")


    # Execute the asynchronous main function
    asyncio.run(main())