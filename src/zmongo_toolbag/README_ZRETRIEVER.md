## ZRetriever: Embedding-based Retrieval for ZMongo

`ZRetriever` is a LangChain-compatible retriever that sits on top of:

* **ZMongo** – your SafeResult-based Mongo wrapper
* **ZEmbedder** – manages embeddings (Gemini by default)
* **LocalVectorSearch** – pure NumPy cosine similarity over stored vectors

The pipeline is:

1. Embed docs with `ZEmbedder` (CLI or direct call).
2. Embeddings are stored under `embedding.<field_name>.vectors`.
3. `ZRetriever` embeds the query and calls `LocalVectorSearch` to find similar docs.
4. Results are returned as LangChain `Document` objects.

---

### Data Model

Each document follows a per-field embedding layout, for example:

```jsonc
{
  "_id": "…",
  "topic": "Biology",
  "text": "Mitochondria generate energy in the cell.",
  "case_name": "Johnson v. Davis",
  "case_text": "… full opinion text …",
  "embedding": {
    "text": {
      "model": "gemini-embedding-001",
      "style": "retrieval.document",
      "dimensionality": 768,
      "vectors": [[ /* 768 floats */ ]]
    },
    "case_name": {
      "model": "gemini-embedding-001",
      "style": "retrieval.document",
      "dimensionality": 768,
      "vectors": [[ /* 768 floats */ ]]
    },
    "case_text": {
      "model": "gemini-embedding-001",
      "style": "retrieval.document",
      "dimensionality": 768,
      "vectors": [[ /* 768 floats */ ]]
    }
  }
}
```

* All embeddings live under one top-level field: `embedding`.
* Each embedded field (e.g. `text`, `case_name`, `case_text`) gets its own entry.
* Each entry is a small metadata bundle plus a `vectors` list.

`ZEmbedder` uses `DataProcessor` to set and read these dot-paths (e.g. `embedding.case_name`) so multiple embedded fields can coexist in the same record without clobbering each other.

---

### Environment Setup

Both `ZEmbedder` and `ZRetriever` load environment variables from:

* `~/.resources/.env`
* `~/.resources/.secrets`

Typical configuration:

```dotenv
# ~/.resources/.env
MONGO_URI=mongodb://127.0.0.1:27017
MONGO_DATABASE_NAME=test

GEMINI_EMBEDDING_MODEL=gemini-embedding-001
GEMINI_EMBEDDING_DIM=768
```

```dotenv
# ~/.resources/.secrets
GOOGLE_API_KEY=your_real_gemini_api_key_here
# or GEMINI_API_KEY=...
```

If Gemini isn’t configured, `ZEmbedder` can fall back to a dummy embedding model for local testing.

---

### Step 1 – Insert Documents with ZMongo

Example:

```python
from bson.objectid import ObjectId
from .zmongo import ZMongo

db = ZMongo()  # uses env defaults
collection = "zretriever_default_kb"

docs = [
    {"_id": ObjectId(), "topic": "Biology", "text": "Mitochondria generate energy in the cell."},
    {"_id": ObjectId(), "topic": "Astronomy", "text": "Jupiter is the largest planet."},
    {"_id": ObjectId(), "topic": "History", "text": "The Roman Empire shaped Western civilization."},
]

for d in docs:
    db.insert_one(collection, d)
```

---

### Step 2 – Embed Documents with the ZEmbedder CLI

`zembedder.py` has a CLI that:

* Finds docs **missing** `embedding.<field_name>`.
* Computes embeddings.
* Writes them back as `embedding.<field_name>.vectors` using dot-path helpers.

Embed a collection’s `text` field:

```bash
python -m zmongo_toolbag.zembedder \
  --db test \
  --collection zretriever_default_kb \
  --all \
  --text-field text
```

For a legal collection with multiple embedded fields:

```bash
# Embed citations -> embedding.citation
python zembedder.py --db test --collection legal_codex --all --text-field citation

# Embed case names -> embedding.case_name
python zembedder.py --db test --collection legal_codex --all --text-field case_name

# Embed long case text -> embedding.case_text
python zembedder.py --db test --collection legal_codex --all --text-field case_text
```

The CLI is **field-specific**: `--all` only processes docs that are missing that particular `embedding.<text_field>` entry. You can re-run for other fields without touching existing embeddings.

---

### Step 3 – Create a LocalVectorSearch over a Field

`LocalVectorSearch` is a small helper that:

* Loads all docs with `embedding.<field_key>` present.
* Extracts `embedding.<field_key>.vectors` (taking the first vector when not chunked).
* Builds an in-memory NumPy matrix and runs cosine similarity.

Example (searching on `text` embeddings):

```python
from .local_vector_search import LocalVectorSearch

vector_search = LocalVectorSearch(
    repository=db,                # ZMongo instance
    collection="zretriever_default_kb",
    embedding_field="embedding",  # top-level container
    field_key="text",             # embedding.text.vectors
    # vector_key defaults to "vectors"
)
```

For another field (e.g. `case_text`):

```python
case_text_search = LocalVectorSearch(
    repository=db,
    collection="legal_codex",
    embedding_field="embedding",
    field_key="case_text",        # embedding.case_text.vectors
)
```

---

### Step 4 – Use ZRetriever as a LangChain Retriever

`ZRetriever` implements `BaseRetriever` using the new `_get_relevant_documents` / `_aget_relevant_documents` API:

* Query is embedded via `ZEmbedder`.
* Vector search is done by `LocalVectorSearch`.
* Results are filtered by `similarity_threshold` and returned as `Document` objects.

Basic wiring:

```python
from .zretriever import ZRetriever
from .zembedder import ZEmbedder

db_client = ZMongo()
embedder = ZEmbedder(repository=db_client)

collection = "zretriever_default_kb"
embedding_field = "embedding"
content_field = "text"

vector_search = LocalVectorSearch(
    repository=db_client,
    collection=collection,
    embedding_field=embedding_field,
    field_key=content_field,  # embedding.text.vectors
)

retriever = ZRetriever(
    repository=db_client,
    embedder=embedder,
    vector_searcher=vector_search,
    collection_name=collection,
    embedding_field=embedding_field,
    content_field=content_field,
    top_k=5,
    similarity_threshold=0.75,
)
```

Synchronous query:

```python
query = "Which organelle provides energy in the cell?"
results = retriever.invoke(query)  # LangChain-style API

for i, doc in enumerate(results, start=1):
    print(f"\n--- Result {i} ---")
    print("Content:", doc.page_content)
    print("Metadata:", doc.metadata)
```

Asynchronous query:

```python
docs = await retriever.ainvoke("What shaped Western civilization?")
```

---

### Multiple Retrievers for Different Fields

Because embeddings are stored per field, you can easily build separate retrievers for different use cases:

```python
# Name-based search (short label)
name_search = LocalVectorSearch(
    repository=db_client,
    collection="legal_codex",
    embedding_field="embedding",
    field_key="case_name",          # embedding.case_name.vectors
)

name_retriever = ZRetriever(
    repository=db_client,
    embedder=embedder,
    vector_searcher=name_search,
    collection_name="legal_codex",
    embedding_field="embedding",
    content_field="case_name",
    top_k=5,
    similarity_threshold=0.7,
)

# Full-text search (long opinion text)
text_search = LocalVectorSearch(
    repository=db_client,
    collection="legal_codex",
    embedding_field="embedding",
    field_key="case_text",          # embedding.case_text.vectors
)

text_retriever = ZRetriever(
    repository=db_client,
    embedder=embedder,
    vector_searcher=text_search,
    collection_name="legal_codex",
    embedding_field="embedding",
    content_field="case_text",
    top_k=5,
    similarity_threshold=0.7,
)

docs_by_name = name_retriever.invoke("duty to disclose latent defects case")
docs_by_text = text_retriever.invoke("duty to disclose latent defects to residential purchaser")
```

---

### Built-In Demo

You can run a quick end-to-end demo (docs → embeddings → retrieval) with:

```bash
python zretriever.py
```

The demo:

1. Inserts three simple documents into `zretriever_default_kb`.
2. Embeds `text` with `ZEmbedder`.
3. Runs a query like *"Which organelle provides energy in the cell?"*.
4. Prints the top 2 results and metadata (including `retrieval_score`).
