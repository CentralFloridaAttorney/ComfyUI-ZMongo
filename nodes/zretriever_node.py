from zmongo_manager import ZMongoManager
from zmongo_toolbag.data_processing import DataProcessor
from zmongo_toolbag.zembedder import ZEmbedder


class ZRetrieverNode:
    """LangChain-compatible semantic search using Gemini embeddings."""

    @classmethod
    def INPUT_TYPES(cls):
        return {
            "required": {
                "query_text": ("STRING", {"multiline": True}),
                "collection": ("STRING", {"default": "legal_codex"}),
                "similarity_threshold": ("FLOAT", {"default": 0.7, "min": 0.0, "max": 1.0}),
                "n_results": ("INT", {"default": 3, "min": 1, "max": 10}),
            }
        }

    RETURN_TYPES = ("STRING", "STRING")
    RETURN_NAMES = ("top_result_json", "summary")
    FUNCTION = "retrieve"
    CATEGORY = "ZMongo/AI"

    def retrieve(self, query_text, collection, similarity_threshold, n_results):
        manager = ZMongoManager.get_instance()
        embedder = ZEmbedder(output_dimensionality=768)

        # Schedule retrieval coroutine on the ZMongo background loop
        res = manager.client.run_sync(
            embedder.find_similar_documents,
            query_text=query_text,
            target_collection=collection,
            n_results=n_results
        )

        if not res.success:
            return ("{}", f"Search failed: {res.error}")

        # Filter and summarize results based on threshold
        hits = [h for h in res.data["results"] if h["retrieval_score"] >= similarity_threshold]
        summary = "\n".join([f"Score: {h['retrieval_score']:.4f} | ID: {h['document'].get('_id')}" for h in hits])

        top_hit = DataProcessor.to_json(hits[0]["document"]) if hits else "{}"
        return (top_hit, summary)