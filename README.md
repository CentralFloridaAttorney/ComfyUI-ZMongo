# ComfyUI-ZMongo

<p align="center">
  <strong>Database-backed workflow memory, document selection, and image persistence for ComfyUI.</strong>
</p>

<p align="center">
  <a href="#installation"><img alt="Install" src="https://img.shields.io/badge/install-custom__nodes-blue"></a>
  <a href="#nodes"><img alt="Nodes" src="https://img.shields.io/badge/nodes-ZMongo%2FAuth%20%7C%20Data%20%7C%20Image-green"></a>
  <a href="#license"><img alt="License" src="https://img.shields.io/badge/license-Apache--2.0-lightgrey"></a>
</p>

---

## What is ComfyUI-ZMongo?

**ComfyUI-ZMongo** is a custom node pack that lets ComfyUI workflows talk to a ZMongo-compatible backend API.

Use it to:

- log in to a ZMongo backend from ComfyUI (i.e. https://ztarot.app),
- list user collections,
- list documents in a collection,
- select the nth collection or document from list outputs,
- fetch document JSON,
- discover flattened dot-path fields,
- save generated images to Mongo-backed documents,
- reload saved images as ComfyUI `IMAGE` tensors,
- keep workflow state outside the ComfyUI graph.

This is useful for workflows that need **persistent memory**, **image archives**, **document-backed prompts**, **workflow state**, **batch frame storage**, or **database-driven routing**.

---

## Main Features

| Feature | Description |
|---|---|
| Login/session node | Creates a reusable `ZMONGO_SESSION` for downstream nodes. |
| Collection browser | Lists collections available to the authenticated user. |
| Document browser | Lists document IDs in a selected collection. |
| Nth-item selector | Selects one item from `collections_list`, `doc_ids_list`, or any list-like output. |
| JSON document loader | Loads a full document as JSON text. |
| Field-path discovery | Flattens a document and returns dot-path keys. |
| Image save/load | Saves ComfyUI images into documents and loads them back. |
| Batch image support | Saves and loads batches as document arrays or separate documents. |
| Refresh-safe behavior | Database-backed nodes re-run instead of silently using stale cached data. |
| Compact node labels | Shorter display names keep large graphs readable. |

---

## Installation

### Manual install with Git

Go to your ComfyUI `custom_nodes` folder:

```bash
cd /path/to/ComfyUI/custom_nodes
```

Clone the repository:

```bash
git clone https://github.com/CentralFloridaAttorney/ComfyUI-ZMongo.git
```

Install requirements using the same Python environment that runs ComfyUI:

```bash
cd ComfyUI-ZMongo
pip install -r requirements.txt
```

Restart ComfyUI.

---

### Linux example

```bash
cd ~/ComfyUI/custom_nodes
git clone https://github.com/centralfloridaattorney/ComfyUI-ZMongo.git
cd ComfyUI-ZMongo
source ~/ComfyUI/venv/bin/activate
pip install -r requirements.txt
```

Restart ComfyUI:

```bash
cd ~/ComfyUI
source ~/ComfyUI/venv/bin/activate
python3 main.py --listen 0.0.0.0 --port 50003
```

---

### Windows portable example

From `ComfyUI_windows_portable\ComfyUI\custom_nodes`:

```bat
git clone https://github.com/CentralFloridaAttorney/ComfyUI-ZMongo.git
cd ComfyUI-ZMongo
..\..\python_embeded\python.exe -m pip install -r requirements.txt
```

Restart ComfyUI.

---


## Backend Setup

ComfyUI-ZMongo does **not** connect directly to MongoDB. It talks to a backend API through `session_client.py`.

Backend URL:

```text
https://ztarot.app
```

The backend provides the routes for:

- login/logout,
- list collections,
- list documents,
- get document,
- create/update/delete document,
- upload image to field,
- view/download image field.
---

## Nodes

Nodes appear in these ComfyUI categories:

```text
ZMongo/Auth
ZMongo/Data
ZMongo/Image
```

### Auth Nodes

| Node | Purpose |
|---|---|
| **ZMongo Login** | Logs in and outputs a `ZMONGO_SESSION`. |
| **ZMongo Logout** | Logs out the current session. |

### Data Nodes

| Node | Purpose |
|---|---|
| **ZMongo List Collections** | Lists available collections for the authenticated user. |
| **ZMongo List Docs** | Lists document IDs from a selected collection. |
| **ZMongo Select Nth Item** | Selects one item from a list output. |
| **ZMongo Get Doc** | Loads one document by collection and document ID. |
| **ZMongo List Flattened Field Paths** | Lists dot-path keys from a document. |

### Image Nodes

| Node | Purpose |
|---|---|
| **ZMongo Display Image** | Loads one image from a document field. |
| **ZMongo Save Image** | Saves one image into a document field. |
| **ZMongo Save Images To Array** | Saves an image batch into an array field in one document. |
| **ZMongo Save Images As Documents** | Saves each image in a batch as a separate document. |
| **ZMongo Select Saved Path** | Selects one saved path from a batch save result. |
| **ZMongo Load Images From Array** | Loads an image batch from one document array. |
| **ZMongo Load Images From Documents** | Loads image documents into a ComfyUI image batch. |

---

## Quick Start Workflows

### 1. Login, list collections, select collection

```text
ZMongo Login
  session
    ↓
ZMongo List Collections
  list
    ↓
ZMongo Select Nth Item
  item
```

Set `index = 0` to select the first collection.

---

### 2. Select a document and load JSON

```text
ZMongo Login
  session
    ↓
ZMongo List Collections
  list
    ↓
ZMongo Select Nth Item
  selected collection
    ↓
ZMongo List Docs
  ids
    ↓
ZMongo Select Nth Item
  selected document_id
    ↓
ZMongo Get Doc
  json
```

Use this workflow to inspect saved prompts, metadata, image records, or workflow state.

---

### 3. Save one generated image

```text
Image-producing node
  image
    ↓
ZMongo Save Image
  refresh
    ↓
ZMongo List Docs.refresh
```

Recommended values:

```text
collection_name = images
field_path      = image_data
filename        = comfy_image.png
```

---

### 4. Save frames as separate documents

```text
IMAGE batch
  ↓
ZMongo Save Images As Documents
  refresh
  ↓
ZMongo Load Images From Documents
  images
  ↓
Video node
```

Recommended values:

```text
collection_name   = images
image_field_path  = image_data
doc_key_prefix    = frame
filename_prefix   = frame
```

---

### 5. Save frames to one document array

```text
IMAGE batch
  ↓
ZMongo Save Images To Array
  refresh
  ↓
ZMongo Load Images From Array
  images
  ↓
Video node
```

Recommended values:

```text
array_field_path = images
item_field_path  = image
```

This produces paths like:

```text
images.0.image
images.1.image
images.2.image
```

---

## Field Paths

ComfyUI-ZMongo uses dot-path strings to locate nested fields.

Examples:

```text
image_data
metadata.prompt
images.0.image
frames.12.preview
```

Rules:

1. Save nodes use the exact field path you enter.
2. Load/display nodes try the exact path first.
3. Load/display nodes may fall back to legacy `<field>.data` paths for older documents.
4. Use **ZMongo List Flattened Field Paths** to discover valid paths.

---

## Nodes

| Full name | Compact display |
|---|---|
| `ZMongo List Collections` | `List Collections` |
| `ZMongo List Docs` | `List Docs` |
| `ZMongo Select Nth Item` | `Select Nth` |
| `ZMongo Load Images From Documents` | `Load Images: Docs` |
| `ZMongo Save Images As Documents` | `Save Images: Docs` |
| `result_json` | `json` |
| `refresh_token` | `refresh` |
| `collections_list` | `list` |
| `doc_ids_list` | `ids` |

---

## Updating

From the node folder:

```bash
cd /path/to/ComfyUI/custom_nodes/ComfyUI-ZMongo
git pull
pip install -r requirements.txt
```

Restart ComfyUI after updating.

---

## Troubleshooting

### Nodes do not appear

Check that:

1. The folder is inside `ComfyUI/custom_nodes/`.
2. The folder contains `__init__.py`.
3. You restarted ComfyUI after installing.
4. Requirements installed into the same Python environment used by ComfyUI.
5. The file compiles.

```bash
cd /path/to/ComfyUI/custom_nodes/ComfyUI-ZMongo
python3 -m py_compile zmongo_session_nodes.py
```

Watch the ComfyUI terminal log for import errors.

---

### Login fails

Login failures usually mean the node cannot reach the backend or the backend rejected the credentials.

Check the `base_url` first.

For the live hosted backend, use:

```text
https://ztarot.app
```
---

### Collections or documents are empty

Possible causes:

- login failed,
- the authenticated user has no data yet,
- the backend is pointing to the wrong user silo,
- route prefixes changed in the backend but `session_client.py` still targets old routes.

---

### Wrong item selected

`ZMongo Select Nth Item` uses zero-based indexing:

```text
index = 0 -> first item
index = 1 -> second item
index = 2 -> third item
```

---

### Image display fails

Check:

1. `collection_name`,
2. `document_id`,
3. `field_path`,
4. whether the image is stored inline or behind a backend route.

Use **ZMongo List Flattened Field Paths** with `image_only = true` to find likely image fields.

---

### Video node rejects loaded frames

Many video nodes require all frames to have the same width and height.

Enable this option on image-loading nodes:

```text
resize_to_first = true
```

---

### Mongo document too large

MongoDB documents have a size limit. Do not store large image batches or videos directly inside one document.

Recommended production pattern:

```text
MongoDB document -> metadata + object-storage pointer
Object storage   -> actual image/video bytes
```

---

### Refresh behavior

Database-backed nodes depend on external state. This package uses an always-dirty and refresh-token pattern so ComfyUI does not silently reuse stale cached outputs after saves or updates.

Use refresh outputs after save/update nodes when a downstream node should reload data.

---

## Security

- Do not put raw MongoDB credentials inside ComfyUI workflows.
- Do not allow workflow JSON to choose arbitrary database names.
- Authenticate through the backend.
- Keep user data inside authenticated user silos.
- Keep auth, API-key, and token data in a separate auth database.
- Store large files in object storage instead of inline MongoDB documents.
- Never commit `.env`, `.secrets`, JWT keys, Cloudflare keys, or database passwords.

---

## Roadmap

- API-key login node for machine-only workflows.
- More JSON helper nodes for extracting nested values.
- Query builder node for document filtering.
- Better gallery/browser frontend widgets.
- Cloudflare R2/object-storage image persistence.
- Example workflows in `examples/`.
- Screenshots and short video tutorials.

---

## Repository Files

```text
ComfyUI-ZMongo/
├── zmongo_panel.js
├── zmongo_session_nodes.py
├── session_client.py
├── data_processor.py
├── requirements.txt
├── pyproject.toml
├── LICENSE
└── README.md
```

---

## License

ComfyUI-ZMongo is licensed under the Apache License 2.0.

See `LICENSE` for details.

---

## Credits

Built for ComfyUI workflows that need durable state, database-backed document selection, and reusable image/document persistence.