"""
ZEmbedder – Gemini-based embedding manager with chunking, caching, SafeResult and ZMongo compatibility.

Key features:
- Uses GeminiEmbeddingModel (gemini-embedding-001) by default.
- Supports chunking styles: fixed | sentence | paragraph.
- Cache-first embedding using an `_embedding_cache` collection (when supported by repository).
- Works with SafeResult-based repositories (e.g., ZMongo).
"""

import asyncio
import hashlib
import logging
import re
from pathlib import Path
from typing import Any, Dict, List, Optional, Literal

import numpy as np
from dotenv import load_dotenv

from .safe_result import SafeResult
from gemini_embedding_model import (
    GeminiEmbeddingModel,
    EMBEDDING_STYLE_RETRIEVAL_DOCUMENT,
    EMBEDDING_STYLE_RETRIEVAL_QUERY,
)
load_dotenv(Path.home() / ".resources" / ".env")
load_dotenv(Path.home() / ".resources" / ".secrets")
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Chunking style literals
# ---------------------------------------------------------------------------
CHUNK_STYLE_FIXED: Literal["fixed"] = "fixed"
CHUNK_STYLE_SENTENCE: Literal["sentence"] = "sentence"
CHUNK_STYLE_PARAGRAPH: Literal["paragraph"] = "paragraph"
ChunkStyle = Literal["fixed", "sentence", "paragraph"]
# --- CONFIGURATION ---
collection = "cases"
text_field = "case_name"
embedding_field = "embedding.case_name"
doc_id_str = "683fc0a4a3251f9c5e2208cd"
# 768 is a good balance for efficiency vs performance
output_dim = 768

class ZEmbedder:
    """
    Central class for managing text embeddings and persistence in MongoDB,
    with Gemini-based embeddings, chunking, and optional caching.

    - Default model: GeminiEmbeddingModel (gemini-embedding-001).
    - Repository: expected to be ZMongo-like, returning SafeResult.
    """

    def __init__(
        self,
        repository: Any,
        model: Optional[Any] = None,
        output_dimensionality: Optional[int] = 768,
    ):
        """
        Parameters
        ----------
        repository :
            Database repository (e.g., ZMongo instance).
        model :
            Optional preconfigured embedding model. If None, a GeminiEmbeddingModel
            will be created with the given output_dimensionality.
        output_dimensionality :
            Target embedding dimensionality for Gemini. If None, Gemini's default
            (currently 3072) is used. Typical values: 768, 1536, 3072.
        """
        self.repository = repository
        self.model = model or self._load_default_model(output_dimensionality)
        self.default_output_dim = output_dimensionality

        dim = getattr(self.model, "output_dimensionality", None)
        logger.info(
            "✅ ZEmbedder initialized with GeminiEmbeddingModel: %s (dim=%s)",
            getattr(self.model, "name", "unknown"),
            dim if dim is not None else "default",
        )

    # ------------------------------------------------------------------
    # Internal: model loader
    # ------------------------------------------------------------------
    @staticmethod
    def _load_default_model(output_dimensionality: Optional[int]) -> GeminiEmbeddingModel:
        """
        Load GeminiEmbeddingModel as the default embedding remote_court_access.
        """
        return GeminiEmbeddingModel(
            model_name="gemini-embedding-001",
            output_dimensionality=output_dimensionality,
        )

    # ------------------------------------------------------------------
    # Internal: normalize repository result into SafeResult
    # ------------------------------------------------------------------
    async def _await_repo_result(self, maybe_result: Any) -> SafeResult:
        """
        Normalize a repository result (SafeResult, coroutine, or bare data)
        into a SafeResult.
        """
        if asyncio.iscoroutine(maybe_result):
            maybe_result = await maybe_result

        if isinstance(maybe_result, SafeResult):
            return maybe_result

        return SafeResult.ok(maybe_result)

    # ------------------------------------------------------------------
    # Chunking helpers
    # ------------------------------------------------------------------
    def _split_text_into_chunks(
        self,
        text: str,
        *,
        chunk_style: ChunkStyle = CHUNK_STYLE_FIXED,
        chunk_size: int = 1500,
        overlap: int = 150,
    ) -> List[str]:
        """
        Splits text into chunks based on style.

        chunk_size:
          - fixed: max chars per chunk
          - sentence/paragraph: approx max chars per chunk (units are packed)
        overlap:
          - fixed: overlapping chars
          - sentence/paragraph: overlapping unit count
        """
        if not text:
            return []

        if chunk_style == CHUNK_STYLE_FIXED:
            return self._split_fixed(text, chunk_size=chunk_size, overlap_chars=overlap)
        elif chunk_style == CHUNK_STYLE_SENTENCE:
            units = self._split_sentences(text)
            return self._pack_units(units, target_chars=chunk_size, overlap_units=max(0, overlap))
        elif chunk_style == CHUNK_STYLE_PARAGRAPH:
            units = self._split_paragraphs(text)
            return self._pack_units(units, target_chars=chunk_size, overlap_units=max(0, overlap))

        raise ValueError(f"Unsupported chunk_style: {chunk_style}")

    def _split_fixed(self, text: str, *, chunk_size: int, overlap_chars: int) -> List[str]:
        chunks: List[str] = []
        n = len(text)
        if n == 0:
            return chunks
        start = 0
        step = max(1, chunk_size - max(0, overlap_chars))
        while start < n:
            end = min(n, start + chunk_size)
            chunks.append(text[start:end])
            if end >= n:
                break
            start += step
        return chunks

    def _split_sentences(self, text: str) -> List[str]:
        norm = re.sub(r"[ \t]+", " ", text.strip())
        parts = re.split(r"(?<=[.!?])\s+(?=[\"'(\[]?[A-Z0-9])", norm)
        return [p.strip() for p in parts if p.strip()] or [norm]

    def _split_paragraphs(self, text: str) -> List[str]:
        parts = re.split(r"\n\s*\n+", text.strip())
        return [p.strip() for p in parts if p.strip()] or [text.strip()]

    def _pack_units(self, units: List[str], *, target_chars: int, overlap_units: int) -> List[str]:
        if not units:
            return []
        chunks: List[str] = []
        i = 0
        overlap_units = max(0, overlap_units)
        while i < len(units):
            current: List[str] = []
            current_len = 0
            while i < len(units):
                u = units[i]
                add_len = len(u) + (1 if current else 0)
                if current and current_len + add_len > target_chars:
                    break
                current.append(u)
                current_len += add_len
                i += 1
            if not current:
                current = [units[i]]
                i += 1
            chunks.append("\n".join(current).strip())
            if overlap_units > 0 and i < len(units):
                i = max(0, i - overlap_units)
        return chunks

    # ------------------------------------------------------------------
    # Internal embedding helpers (chunk cache)
    # ------------------------------------------------------------------
    def _chunk_hash(
        self,
        chunk: str,
        *,
        embedding_style: str,
        output_dimensionality: Optional[int],
    ) -> str:
        """
        Cache key: SHA256 of style + dim + chunk text.
        """
        tag = f"{embedding_style}:{output_dimensionality or 'default'}:"
        return hashlib.sha256((tag + chunk).encode("utf-8")).hexdigest()

    async def _embed_chunks(
        self,
        chunks: List[str],
        *,
        embedding_style: str,
        output_dimensionality: Optional[int],
    ) -> Dict[str, List[float]]:
        """
        Embed a list of chunks using the underlying GeminiEmbeddingModel.
        Returns a mapping {chunk_text: embedding_vector}.
        """
        if not chunks:
            return {}
        # Delegate to the model; we assume it supports style + dim via kwargs.
        vectors = await self.model.embed(
            chunks,
            style=embedding_style,
            output_dimensionality=output_dimensionality,
        )
        if not vectors:
            raise RuntimeError("Model returned no vectors.")
        if len(vectors) != len(chunks):
            raise RuntimeError("Model returned mismatched number of vectors vs chunks.")
        return dict(zip(chunks, vectors))

    async def _get_embeddings_from_chunks(
        self,
        chunks: List[str],
        *,
        embedding_style: str,
        output_dimensionality: Optional[int],
    ) -> Dict[str, List[float]]:
        """
        Cache-first: check repository cache, embed misses with chosen style/dimension,
        then backfill cache.

        Cache documents are stored in collection `_embedding_cache` with fields:
          - text_hash
          - embedding
          - source_text
          - embedding_style
          - output_dimensionality
        """
        if not chunks:
            return {}

        # Precompute hashes and mapping
        hashes = [
            self._chunk_hash(
                c,
                embedding_style=embedding_style,
                output_dimensionality=output_dimensionality,
            )
            for c in chunks
        ]
        hash_to_chunk = dict(zip(hashes, chunks))

        found_embeddings: Dict[str, List[float]] = {}
        cached_hashes: set[str] = set()

        # --------------------------------------------------------
        # 1. Try cache if repository supports it
        # --------------------------------------------------------
        if hasattr(self.repository, "find_documents"):
            try:
                cache_res = await self._await_repo_result(
                    self.repository.find_documents(
                        "_embedding_cache",
                        {"text_hash": {"$in": hashes}},
                    )
                )
                if cache_res.success and cache_res.data:
                    for rec in cache_res.data:
                        h = rec.get("text_hash")
                        src = rec.get("source_text")
                        emb = rec.get("embedding")
                        if h and src is not None and emb is not None:
                            cached_hashes.add(h)
                            found_embeddings[src] = emb
                logger.info(
                    "Found %d of %d chunks in embedding cache.",
                    len(found_embeddings),
                    len(chunks),
                )
            except Exception as exc:
                logger.warning("Error querying embedding cache: %s", exc)
        else:
            logger.debug("Repository has no 'find_documents'; skipping cache lookup.")

        # --------------------------------------------------------
        # 2. Embed missing chunks
        # --------------------------------------------------------
        missing_hashes = set(hashes) - cached_hashes
        chunks_to_embed = [hash_to_chunk[h] for h in missing_hashes]

        if chunks_to_embed:
            api_embeddings = await self._embed_chunks(
                chunks_to_embed,
                embedding_style=embedding_style,
                output_dimensionality=output_dimensionality,
            )
            # Backfill cache if supported
            if hasattr(self.repository, "insert_documents") and api_embeddings:
                try:
                    new_cache_entries = []
                    for chunk, emb in api_embeddings.items():
                        text_hash = self._chunk_hash(
                            chunk,
                            embedding_style=embedding_style,
                            output_dimensionality=output_dimensionality,
                        )
                        new_cache_entries.append(
                            {
                                "text_hash": text_hash,
                                "embedding": emb,
                                "source_text": chunk,
                                "embedding_style": embedding_style,
                                "output_dimensionality": output_dimensionality
                                or getattr(self.model, "output_dimensionality", None)
                                or 3072,
                            }
                        )
                    if new_cache_entries:
                        await self._await_repo_result(
                            self.repository.insert_documents("_embedding_cache", new_cache_entries)
                        )
                except Exception as exc:
                    logger.warning("Error inserting into embedding cache: %s", exc)

            found_embeddings.update(api_embeddings)

        return found_embeddings

    # ------------------------------------------------------------------
    # Core embedding logic (public API)
    # ------------------------------------------------------------------
    async def get_embedding(
        self,
        text: Optional[str] = None,
        *,
        collection: Optional[str] = None,
        document_id: Optional[Any] = None,
        embedding_style: Optional[str] = EMBEDDING_STYLE_RETRIEVAL_DOCUMENT,
        embedding_field: str = "embedding",
        text_field: str = "text",
        skip_if_present: bool = False,
        # chunking
        chunk_style: ChunkStyle = CHUNK_STYLE_FIXED,
        chunk_size: int = 1500,
        overlap: int = 150,
        # embedding
        output_dimensionality: Optional[int] = None,
        as_safe_result: bool = True,
        **kwargs: Any,
    ) -> Any:
        """
        Generate (and optionally store) embeddings for a given text or document,
        using full chunking and optional caching.

        If collection and document_id are provided, this method will:
          1. Load the document (if text is not explicitly provided),
          2. Optionally check for existing embeddings (skip_if_present),
          3. Split text into chunks (fixed/sentence/paragraph),
          4. Generate chunk embeddings (with caching),
          5. Persist them back to the document under `embedding_field`.

        Returns
        -------
        SafeResult or dict
            When as_safe_result=True, returns SafeResult; otherwise returns dict.
        """
        doc: Optional[Dict[str, Any]] = None
        style = embedding_style or EMBEDDING_STYLE_RETRIEVAL_DOCUMENT
        dim = output_dimensionality if output_dimensionality is not None else self.default_output_dim

        # ------------------------------------------------------------
        # 1. Load text from document if not explicitly provided
        # ------------------------------------------------------------
        if not text and collection and document_id is not None:
            fetch_result = await self._await_repo_result(
                self.repository.find_one(collection, {"_id": document_id})
            )
            if not fetch_result.success or not fetch_result.data:
                msg = fetch_result.error or "Document not found or empty."
                result = SafeResult.fail(f"Could not load document: {msg}")
                return result if as_safe_result else result.to_dict()

            doc = fetch_result.data
            if text_field not in doc or not doc[text_field]:
                result = SafeResult.fail("Document text not provided or missing from source document.")
                return result if as_safe_result else result.to_dict()

            text = doc[text_field]

        if not text:
            result = SafeResult.fail("Document text not provided or missing from source document.")
            return result if as_safe_result else result.to_dict()

        # ------------------------------------------------------------
        # 2. Skip if embeddings already exist and skip_if_present=True
        # ------------------------------------------------------------
        if skip_if_present and collection and document_id is not None:
            existing_res = await self._await_repo_result(
                self.repository.find_one(collection, {"_id": document_id})
            )
            if existing_res.success and existing_res.data:
                data = existing_res.data
                existing_vectors = data.get(embedding_field)
                if existing_vectors:
                    payload = {
                        "vectors": existing_vectors,
                        "from_cache": True,
                        "chunk_count": len(existing_vectors),
                        "dimensionality": (
                            len(existing_vectors[0])
                            if isinstance(existing_vectors, list)
                            and existing_vectors
                            and isinstance(existing_vectors[0], (list, tuple))
                            else None
                        ),
                    }
                    result = SafeResult.ok(payload)
                    return result if as_safe_result else result.to_dict()

        # ------------------------------------------------------------
        # 3. Chunk the text
        # ------------------------------------------------------------
        chunks = self._split_text_into_chunks(
            text,
            chunk_style=chunk_style,
            chunk_size=chunk_size,
            overlap=overlap,
        )
        if not chunks:
            result = SafeResult.fail("No chunks produced from text; check chunk_size and style.")
            return result if as_safe_result else result.to_dict()

        # ------------------------------------------------------------
        # 4. Generate embeddings via chunk cache
        # ------------------------------------------------------------
        try:
            embedding_map = await self._get_embeddings_from_chunks(
                chunks,
                embedding_style=style,
                output_dimensionality=dim,
            )
        except Exception as exc:
            logger.exception("Embedding model error: %s", exc)
            result = SafeResult.fail(f"Embedding model error: {exc}")
            return result if as_safe_result else result.to_dict()

        vectors: List[List[float]] = [embedding_map[c] for c in chunks if c in embedding_map]
        if not vectors:
            result = SafeResult.fail("No embedding vectors generated.")
            return result if as_safe_result else result.to_dict()

        # ------------------------------------------------------------
        # 5. Save embeddings to MongoDB (ZMongo-safe)
        # ------------------------------------------------------------
        if collection and document_id is not None:
            update_doc = {embedding_field: vectors}
            try:
                if hasattr(self.repository, "update_document"):
                    repo_call = self.repository.update_one(
                        collection,
                        {"_id": document_id},
                        {"$set": update_doc} if "$set" not in update_doc else update_doc,
                    )
                elif hasattr(self.repository, "update_one"):
                    repo_call = self.repository.update_one(
                        collection,
                        {"_id": document_id},
                        {"$set": update_doc},
                    )
                else:
                    repo_call = None
                    logger.warning(
                        "Repository has neither 'update_document' nor 'update_one'; "
                        "embeddings will not be persisted."
                    )

                if repo_call is not None:
                    update_res = await self._await_repo_result(repo_call)
                    if not update_res.success:
                        logger.warning("Failed to save embeddings: %s", update_res.error)
            except Exception as exc:
                logger.warning("Error saving embeddings: %s", exc)

        # ------------------------------------------------------------
        # 6. Return SafeResult / dict
        # ------------------------------------------------------------
        result_payload = {
            "vectors": vectors,
            "chunk_count": len(vectors),
            "dimensionality": len(vectors[0]),
            "from_cache": False,
        }
        result = SafeResult.ok(result_payload)
        return result if as_safe_result else result_payload

    # ------------------------------------------------------------------
    # Sync helper wrapper
    # ------------------------------------------------------------------
    def get_embedding_sync(self, *args: Any, **kwargs: Any) -> SafeResult:
        """
        Synchronous wrapper around get_embedding().

        Always returns a SafeResult.
        """
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            loop = None

        if loop and loop.is_running():
            fut = asyncio.run_coroutine_threadsafe(self.get_embedding(*args, **kwargs), loop)
            try:
                result = fut.result(timeout=120)
            except Exception as exc:
                return SafeResult.fail(f"get_embedding_sync error (existing loop): {exc}")
        else:
            try:
                result = asyncio.run(self.get_embedding(*args, **kwargs))
            except Exception as exc:
                return SafeResult.fail(f"get_embedding_sync error: {exc}")

        return result if isinstance(result, SafeResult) else SafeResult.ok(result)

    # ------------------------------------------------------------------
    # Utility: cosine similarity
    # ------------------------------------------------------------------
    @staticmethod
    def cosine_similarity(vec_a: List[float], vec_b: List[float]) -> float:
        """Compute cosine similarity between two vectors."""
        a = np.array(vec_a, dtype=float)
        b = np.array(vec_b, dtype=float)
        denom = np.linalg.norm(a) * np.linalg.norm(b)
        return float(np.dot(a, b) / denom) if denom else 0.0

    # ------------------------------------------------------------------
    # Utility: batch embedding (already-chunked texts)
    # ------------------------------------------------------------------
    async def embed_many(self, texts: List[str], **kwargs: Any) -> SafeResult:
        """
        Simple batch embedding when you already manage chunking yourself.
        For full text + chunking per text, prefer calling get_embedding() in a loop.
        """
        try:
            vectors = await self.model.embed(texts, **kwargs)
            return SafeResult.ok({"vectors": vectors})
        except Exception as exc:
            return SafeResult.fail(f"embed_many failed: {exc}")

    def close(self) -> None:
        """Graceful shutdown placeholder for compatibility."""
        try:
            if hasattr(self, "model") and hasattr(self.model, "close"):
                self.model.close()
        except Exception:
            pass


# ----------------------------------------------------------------------
# __main__ demo: embed a single field on a single record
# ----------------------------------------------------------------------
def _single_record_demo() -> None:
    """
    Demo: embed a single field on a single record using Gemini + chunking.

    - Collection: 'cases'
    - Document _id: 683fc0a4a3251f9c5e2208cb
    - Text field: 'case_name'
    - Embedding field: 'embedding.case_name'
    """
    import sys
    from bson.objectid import ObjectId
    from zmongo import ZMongo  # assumes zmongo.py is importable as 'zmongo'

    logging.basicConfig(level=logging.INFO)

    try:
        doc_id = ObjectId(doc_id_str)
    except Exception as exc:
        logger.error("Invalid ObjectId '%s': %r", doc_id_str, exc)
        sys.exit(1)

    repo = ZMongo()
    embedder = ZEmbedder(repository=repo, output_dimensionality=768)

    try:
        res = embedder.get_embedding_sync(
            collection=collection,
            document_id=doc_id,
            text_field=text_field,
            embedding_field=embedding_field,
            embedding_style=EMBEDDING_STYLE_RETRIEVAL_DOCUMENT,
            chunk_style=CHUNK_STYLE_FIXED,
            chunk_size=1500,
            overlap=150,
            skip_if_present=False,
            as_safe_result=True,
        )

        if not res.success:
            logger.error("Embedding failed: %s", res.error)
            sys.exit(1)

        data = res.data or {}
        vectors = data.get("vectors")
        if not vectors:
            logger.error("No vectors returned in result.")
            sys.exit(1)

        dim = len(vectors[0])
        logger.info(
            "✅ Successfully embedded '%s' for document %s into field '%s' "
            "(chunks=%d, dim=%d).",
            text_field,
            doc_id_str,
            embedding_field,
            len(vectors),
            dim,
        )
    finally:
        try:
            embedder.close()
        except Exception:
            pass
        try:
            repo.close()
        except Exception:
            pass


def all_record_demo() -> None:
    """
    Demo: Iterate over ALL documents in 'cases', generating embeddings
    for 'case_name' -> 'embedding.case_name'.
    """
    import asyncio
    import sys
    # Assumes zmongo.py is importable as 'zmongo'
    from zmongo import ZMongo

    # Configure logging for the demo
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%H:%M:%S"
    )
    logger = logging.getLogger("all_record_demo")

    repo = ZMongo()
    embedder = ZEmbedder(repository=repo, output_dimensionality=output_dim)

    async def _process_all():
        logger.info(f"🚀 Starting batch embedding for collection: '{collection}'")

        # 1. Fetch all candidate documents (just _ids to keep memory low)
        # We only care about docs that HAVE the text field.
        query = {text_field: {"$exists": True, "$ne": None}}
        projection = {"_id": 1}

        # Use internal helper to await the repo result safely
        res = await embedder._await_repo_result(
            repo.find_many_async(collection, query, projection)
        )

        if not res.success:
            logger.error(f"❌ Failed to query documents: {res.error}")
            return

        docs = res.data or []
        total = len(docs)
        logger.info(f"Found {total} documents to process.")

        stats = {"success": 0, "skipped": 0, "failed": 0}

        for i, doc in enumerate(docs):
            doc_id = doc.get("_id")

            # Call get_embedding
            # skip_if_present=True means it will check the DB first and return early
            # if the embedding field already exists.
            result = await embedder.get_embedding(
                collection=collection,
                document_id=doc_id,
                text_field=text_field,
                embedding_field=embedding_field,
                embedding_style=EMBEDDING_STYLE_RETRIEVAL_DOCUMENT,
                chunk_style=CHUNK_STYLE_FIXED,
                chunk_size=1500,
                overlap=150,
                skip_if_present=True,
                as_safe_result=True
            )

            if result.success:
                # 'from_cache' is True if it found existing vectors in the document (skipped)
                # or strictly used the embedding cache without API calls.
                if result.data.get("from_cache") is True and result.data.get("vectors"):
                    stats["skipped"] += 1
                else:
                    stats["success"] += 1
            else:
                stats["failed"] += 1
                logger.warning(f"⚠️ Failed doc {doc_id}: {result.error}")

            # Log progress every 10 docs or on the last one
            if (i + 1) % 10 == 0 or (i + 1) == total:
                logger.info(
                    f"Progress: {i + 1}/{total} | "
                    f"✅ New: {stats['success']} | "
                    f"⏭️ Skipped: {stats['skipped']} | "
                    f"❌ Failed: {stats['failed']}"
                )

        logger.info("🏁 Batch processing complete.")

    try:
        asyncio.run(_process_all())
    except KeyboardInterrupt:
        logger.warning("\n🛑 Batch process interrupted by user.")
    except Exception as e:
        logger.exception(f"🔥 Fatal error in batch process: {e}")
    finally:
        embedder.close()
        try:
            repo.close()
        except Exception:
            pass



if __name__ == "__main__":
    _single_record_demo()
    # all_record_demo()
