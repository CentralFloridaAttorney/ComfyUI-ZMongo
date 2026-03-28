Here’s an updated `RUN_ME_ZEMBEDDER.md` that matches the new ZEmbedder CLI and behavior (including path-like keys and auto `embedding.<text_field>` fields). 

````markdown
# ZEmbedder – RUN ME

This file shows how to use `zembedder.py` with the new ZEmbedder system.

The CLI now supports:

- `--db` – MongoDB database name  
- `--collection` – collection name  
- `--all` – batch mode (process all docs **missing** the embedding field)  
- `--id` – single-record mode (by ObjectId)  
- `--text-field` – path-like key to the source text (e.g. `case_name`, `irac.raw_text`, `tags.issues.0`)  
- `--embedding-field` – optional path where embeddings are stored  
  - If omitted, ZEmbedder automatically uses:  
    `embedding.<text_field>`  
    - `case_name` → `embedding.case_name`  
    - `irac.raw_text` → `embedding.irac.raw_text`  
    - `tags.issues.0` → `embedding.tags.issues.0`  

ZEmbedder:

- Uses `DataProcessor.get_value` for path-like keys, so nested and “flattened” keys both work.
- Only calls the Gemini API when the embedding field is **missing**.
- Will **not** re-embed documents that already have the specified embedding field.

---

## 1. Batch mode – embed full case text (legal_codex.case_text)

Embeds `case_text` into `embedding.case_text` for all docs in `legal_codex`
that have `case_text` and **do not yet** have `embedding.case_text`:

```powershell
python zembedder.py --db test --collection legal_codex --all --text-field case_text
````

* Query used internally (conceptually):

  ```json
  {
    "case_text": { "$exists": true, "$ne": null },
    "embedding.case_text": { "$exists": false }
  }
  ```

* Safe to run multiple times: once a document has `embedding.case_text`, it will be skipped.

---

## 2. Batch mode – embed the primary issue tag (case_briefs.tags.issues.0)

Embeds the primary issue tag into `embedding.tags.issues.0` for all docs in `case_briefs`
that have `tags.issues.0` and **do not yet** have `embedding.tags.issues.0`:

```powershell
python zembedder.py --db test --collection case_briefs --all --text-field tags.issues.0
```

* Internal query (conceptually):

  ```json
  {
    "tags.issues.0": { "$exists": true, "$ne": null },
    "embedding.tags.issues.0": { "$exists": false }
  }
  ```

Again, you can re-run this command any time; only newly added docs (or docs where the embedding field is missing) will trigger new Gemini calls.

---

## 3. Batch mode – embed IRAC text (case_briefs.irac.raw_text)

Example for embedding the IRAC text into `embedding.irac.raw_text`:

```powershell
python zembedder.py --db test --collection case_briefs --all --text-field irac.raw_text
```

* Uses `DataProcessor.get_value(doc, "irac.raw_text")` to read the text.
* Embeddings are stored at `embedding.irac.raw_text`.

---

## 4. Single-record mode – embed one document by ObjectId

You can embed a single document using its `_id`.
ZEmbedder will skip the Gemini API call if the embedding field already exists.

### 4.1 Single record – case_name

```powershell
python zembedder.py --db test --collection case_briefs `
    --id 683d77ef7911d89bb8bd6d8f `
    --text-field case_name
```

* Reads `case_name`
* Writes to `embedding.case_name`
* If `embedding.case_name` already exists, it logs a “skipped (embedding already present)” message and **does not** call Gemini.

### 4.2 Single record – tags.issues.0

```powershell
python zembedder.py --db test --collection case_briefs `
    --id 683d77ef7911d89bb8bd6d9a `
    --text-field tags.issues.0
```

Embeds `tags.issues.0` into `embedding.tags.issues.0` for that one document only.

---

## 5. Overriding the embedding field (optional)

If you don’t want to use the default `embedding.<text_field>` pattern, you can explicitly set `--embedding-field`.

Examples:

### 5.1 Custom embedding field for case_name

```powershell
python zembedder.py --db test --collection case_briefs --all `
    --text-field case_name `
    --embedding-field embeddings.case_name_vector
```

Now embeddings go into `embeddings.case_name_vector` instead of `embedding.case_name`.

### 5.2 Custom embedding field for tags.issues.0

```powershell
python zembedder.py --db test --collection case_briefs --all --text-field tags.issues.0 --embedding-field embeddings.issue_primary
```

---

## 6. Safety: No re-embedding / no wasted API calls

For both single and batch modes:

* ZEmbedder **first checks** if the embedding field already exists (using `DataProcessor.get_value` on `embedding_field`).
* If present, it returns that data with `from_cache=True` and **does not** call Gemini.
* In batch mode, the Mongo query itself also filters out docs that already have the embedding field, so they aren’t even processed.

You can therefore safely:

* Run the same command multiple times.
* Add new docs later and re-run the same `--all` command; only new docs will be embedded.

```
```
