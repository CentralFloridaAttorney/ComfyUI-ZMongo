"""
ZEmbedder – local BGE-M3 hybrid embedding manager.

This module integrates:
- ZMongo
- LocalVectorSearch
- SafeResult

It uses FlagEmbedding directly and does not depend on PyMilvus.
"""

import asyncio
import logging
from importlib.metadata import PackageNotFoundError, version
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

from dotenv import load_dotenv

from zmongo_toolbag.data_processor import DataProcessor
from zmongo_toolbag.local_vector_search import LocalVectorSearch
from zmongo_toolbag.safe_result import SafeResult
from zmongo_toolbag.zmongo import ZMongo

RESOURCE_PATH = Path.home() / ".resources"
load_dotenv(RESOURCE_PATH / ".env")
load_dotenv(RESOURCE_PATH / ".secrets")

logger = logging.getLogger(__name__)

DEFAULT_MODEL_PATH = "/home/comfyuser/.resources/preserved_models/encoders/bge-m3"
DEFAULT_DB_NAME = "wiki_kb"
DEFAULT_COLLECTION = "knowledge_base"
DEFAULT_EMBEDDING_FIELD = "embedding"
DEFAULT_VECTOR_KEY = "dense"


def _check_bge_m3_dependencies() -> None:
    try:
        flagembedding_ver = version("FlagEmbedding")
    except PackageNotFoundError as exc:
        raise RuntimeError("FlagEmbedding is not installed in this venv.") from exc

    try:
        transformers_ver = version("transformers")
    except PackageNotFoundError as exc:
        raise RuntimeError("transformers is not installed in this venv.") from exc

    logger.info(
        "Detected dependency versions: FlagEmbedding=%s transformers=%s",
        flagembedding_ver,
        transformers_ver,
    )


def _is_hf_model_dir(path: Path) -> bool:
    required = ("config.json", "tokenizer_config.json")
    return path.is_dir() and all((path / name).exists() for name in required)


def _resolve_bge_m3_model_dir(model_path: Union[str, Path]) -> Path:
    base = Path(model_path).expanduser().resolve()

    if not base.exists():
        raise FileNotFoundError(f"BGE-M3 model path does not exist: {base}")

    if _is_hf_model_dir(base):
        return base

    snapshots_dir = base / "snapshots"
    if snapshots_dir.is_dir():
        for child in sorted(snapshots_dir.iterdir()):
            if _is_hf_model_dir(child):
                return child

    for candidate in sorted(base.rglob("config.json")):
        parent = candidate.parent
        if _is_hf_model_dir(parent):
            return parent

    raise FileNotFoundError(
        f"No usable BGE-M3 model directory found under '{base}'. "
        "Expected files such as config.json and tokenizer_config.json."
    )


def _list_dir_brief(path: Path, limit: int = 20) -> str:
    try:
        items = sorted(p.name for p in path.iterdir())
        if len(items) > limit:
            return ", ".join(items[:limit]) + ", ..."
        return ", ".join(items)
    except Exception:
        return "<unable to list directory>"


class ZEmbedder:
    """Local BGE-M3 embedding manager using FlagEmbedding directly."""

    def __init__(
        self,
        db_name: str = DEFAULT_DB_NAME,
        model_path: str = DEFAULT_MODEL_PATH,
        device: str = "cuda:0",
        output_dimensionality: int = 1024,
        collection_name: str = DEFAULT_COLLECTION,
        embedding_field: str = DEFAULT_EMBEDDING_FIELD,
        vector_key: str = DEFAULT_VECTOR_KEY,
    ) -> None:
        self.repository = ZMongo(db_name=db_name)
        self.device = device
        self.default_output_dim = output_dimensionality
        self.collection_name = collection_name
        self.embedding_field = embedding_field
        self.vector_key = vector_key

        _check_bge_m3_dependencies()

        try:
            from FlagEmbedding import BGEM3FlagModel
        except Exception as exc:
            raise RuntimeError("Failed to import BGEM3FlagModel.") from exc

        raw_model_path = Path(model_path).expanduser()

        if not raw_model_path.exists():
            logger.error("❌ Model path does not exist: %s", raw_model_path)
            raise FileNotFoundError(f"BGE-M3 model path does not exist: {raw_model_path}")

        try:
            resolved_model_dir = _resolve_bge_m3_model_dir(raw_model_path)
        except Exception:
            logger.error(
                "❌ Could not resolve a valid BGE-M3 model directory from %s. Contents: %s",
                raw_model_path,
                _list_dir_brief(raw_model_path) if raw_model_path.is_dir() else "<not a directory>",
            )
            raise

        self.model_path = str(resolved_model_dir)
        logger.info("Using BGE-M3 model directory: %s", self.model_path)

        try:
            self.model = BGEM3FlagModel(
                self.model_path,
                use_fp16=(device != "cpu"),
                devices=device,
            )
        except Exception as exc:
            raise RuntimeError(
                f"Failed to initialize BGEM3FlagModel from '{self.model_path}'. "
                "Make sure this directory contains a full Hugging Face model export "
                "(for example: config.json, tokenizer_config.json, tokenizer.json or vocab files, and model weights)."
            ) from exc

        self.vector_search = LocalVectorSearch(
            repository=self.repository,
            collection=self.collection_name,
            embedding_field=self.embedding_field,
            vector_key=self.vector_key,
        )
        logger.info("✅ ZEmbedder (BGE-M3) initialized on %s", device)

    @staticmethod
    def _to_dense_list(vec: Any) -> List[float]:
        if hasattr(vec, "tolist"):
            return vec.tolist()
        return list(vec)

    @staticmethod
    def _to_sparse_dict(sparse: Any) -> Dict[str, float]:
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
    def _normalize_embedding_payload(self, payload: Dict[str, Any], count: int) -> Dict[str, Any]:
        """
        Normalize direct FlagEmbedding/BGE-M3 output into:
            {
                "dense": List[List[float]],
                "sparse": List[Dict[str, float]],
                "count": int,
            }

        Handles payloads like:
            {
                "dense_vecs": [np.ndarray, ...],
                "lexical_weights": [defaultdict(...), ...],
                "colbert_vecs": ...
            }
        """
        if not isinstance(payload, dict):
            raise TypeError(f"Embedding payload must be a dict, got {type(payload)!r}")

        if "dense_vecs" in payload and payload["dense_vecs"] is not None:
            dense_raw = payload["dense_vecs"]
        elif "dense" in payload and payload["dense"] is not None:
            dense_raw = payload["dense"]
        else:
            dense_raw = []

        if "lexical_weights" in payload and payload["lexical_weights"] is not None:
            sparse_raw = payload["lexical_weights"]
        elif "sparse" in payload and payload["sparse"] is not None:
            sparse_raw = payload["sparse"]
        else:
            sparse_raw = []

        dense_vectors = [self._to_dense_list(vec) for vec in dense_raw]
        sparse_vectors = [self._to_sparse_dict(vec) for vec in sparse_raw]

        if sparse_vectors and len(sparse_vectors) != len(dense_vectors):
            raise ValueError(
                f"Dense/sparse count mismatch: dense={len(dense_vectors)} sparse={len(sparse_vectors)}"
            )

        if not sparse_vectors:
            sparse_vectors = [{} for _ in dense_vectors]

        if len(dense_vectors) != count:
            raise ValueError(
                f"Embedding count mismatch: expected {count}, got {len(dense_vectors)}"
            )

        return {
            "dense": dense_vectors,
            "sparse": sparse_vectors,
            "count": count,
        }

    async def embed_many(self, texts: List[str]) -> SafeResult:
        try:
            clean_texts = [str(text).strip() for text in texts if str(text).strip()]
            if not clean_texts:
                return SafeResult.ok({"dense": [], "sparse": [], "count": 0})

            loop = asyncio.get_running_loop()

            def _embed() -> Dict[str, Any]:
                return self.model.encode(
                    clean_texts,
                    return_dense=True,
                    return_sparse=True,
                    return_colbert_vecs=False,
                )

            raw_embeddings = await loop.run_in_executor(None, _embed)
            payload = self._normalize_embedding_payload(raw_embeddings, len(clean_texts))
            return SafeResult.ok(payload)

        except Exception as exc:
            logger.exception("Batch embedding failed")
            return SafeResult.fail(exc)

    async def get_embedding(
        self,
        text: Optional[str] = None,
        *,
        collection: Optional[str] = None,
        document_id: Optional[Any] = None,
        text_field: str = "text",
        embedding_field: str = DEFAULT_EMBEDDING_FIELD,
        persist: bool = True,
        **_: Any,
    ) -> SafeResult:
        try:
            resolved_text = text

            if not resolved_text and collection and document_id is not None:
                fetch_res = await self.repository.find_one_async(collection, {"_id": document_id})
                if not fetch_res.success:
                    return fetch_res

                source_doc = fetch_res.data or {}
                resolved_text = DataProcessor.get_value(source_doc, text_field)

            if not resolved_text or not str(resolved_text).strip():
                return SafeResult.fail("No text content provided or found.")

            embed_res = await self.embed_many([str(resolved_text)])
            if not embed_res.success:
                return embed_res

            embed_data = embed_res.data or {}
            dense_vec = embed_data["dense"][0]
            sparse_vec = embed_data["sparse"][0]

            result_payload = {
                "dense": dense_vec,
                "sparse": sparse_vec,
                "text": str(resolved_text),
                "dimensionality": len(dense_vec),
            }

            if persist and collection and document_id is not None:
                update_payload = {
                    text_field: str(resolved_text),
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

            return SafeResult.ok(result_payload)

        except Exception as exc:
            logger.exception("Single embedding generation failed")
            return SafeResult.fail(exc)

    async def embed_and_store_many(
        self,
        collection: str,
        documents: List[Dict[str, Any]],
        *,
        text_field: str = "text",
        embedding_field: str = DEFAULT_EMBEDDING_FIELD,
    ) -> SafeResult:
        try:
            if not documents:
                return SafeResult.ok({"inserted_count": 0, "embedded_count": 0})

            clean_docs: List[Dict[str, Any]] = []
            texts: List[str] = []

            for doc in documents:
                text = DataProcessor.get_value(doc, text_field)
                if text is None or not str(text).strip():
                    return SafeResult.fail(
                        f"Document missing non-empty text_field '{text_field}': {doc}"
                    )
                clean_docs.append(dict(doc))
                texts.append(str(text))

            embed_res = await self.embed_many(texts)
            if not embed_res.success:
                return embed_res

            payload = embed_res.data or {}
            dense_vectors = payload["dense"]
            sparse_vectors = payload["sparse"]

            inserted_count = 0
            embedded_count = 0

            for doc, dense_vec, sparse_vec in zip(clean_docs, dense_vectors, sparse_vectors):
                doc[embedding_field] = {
                    "dense": dense_vec,
                    "sparse": sparse_vec,
                    "model": "bge-m3-local",
                    "dimensionality": len(dense_vec),
                }

                insert_res = await self.repository.insert_one_async(collection, doc)
                if not insert_res.success:
                    return insert_res

                inserted_count += 1
                embedded_count += 1

            return SafeResult.ok(
                {
                    "inserted_count": inserted_count,
                    "embedded_count": embedded_count,
                    "count": inserted_count,
                }
            )

        except Exception as exc:
            logger.exception("Batch embed-and-store failed")
            return SafeResult.fail(exc)

    def close(self) -> None:
        if hasattr(self, "repository"):
            self.repository.close()
        if hasattr(self, "vector_search"):
            self.vector_search.clear_index()


if __name__ == "__main__":
    async def run_demo() -> None:
        logging.basicConfig(level=logging.INFO)

        embedder: Optional[ZEmbedder] = None
        try:
            embedder = ZEmbedder()

            test_text = "Legal compliance for AI Blackwell architecture."
            res = await embedder.get_embedding(text=test_text, persist=False)

            if not res.success:
                print(f"❌ Embedding failed: {getattr(res, 'error', res)}")
                return

            print("✅ Successfully generated hybrid embeddings.")
            print(f"Dense Dim: {res.data['dimensionality']}")
            print(f"Sparse Keys: {list((res.data.get('sparse') or {}).keys())[:5]}...")

        except Exception as exc:
            logger.exception("Demo failed")
            print(f"❌ Demo failed: {exc}")
        finally:
            if embedder is not None:
                embedder.close()

    asyncio.run(run_demo())