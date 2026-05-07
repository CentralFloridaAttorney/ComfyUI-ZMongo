# ComfyUI-ZMongo

<p align="center">
  <strong>API-key ZMongo document selection, metadata field discovery, and image persistence for ComfyUI.</strong>
</p>

<p align="center">
  <a href="#installation"><img alt="Install" src="https://img.shields.io/badge/install-custom__nodes-blue"></a>
  <a href="#nodes"><img alt="Nodes" src="https://img.shields.io/badge/nodes-ZMongo%2FAPI-green"></a>
  <a href="#license"><img alt="License" src="https://img.shields.io/badge/license-Apache--2.0-lightgrey"></a>
</p>

---

## What is ComfyUI-ZMongo?

**ComfyUI-ZMongo** is a ComfyUI custom node pack that lets workflows communicate with a ZMongo-compatible backend API.

Use it to:

- connect to a ZMongo backend with an API key,
- list user-silo collections,
- list document IDs in a collection,
- select one collection/document from ComfyUI list outputs,
- fetch and inspect document JSON,
- save generated images into ZMongo documents,
- display saved images back as ComfyUI `IMAGE` tensors,
- discover flattened dot-path keys for metadata and documents,
- save values into selected document fields,
- prepare for future fleet/agent routing once the Fleet backend routes are finalized.

This is useful for persistent workflow memory, image archives, metadata-driven prompts, document-backed workflows, batch frame storage, and database-driven ComfyUI routing.

---

## Current Architecture

The current node set uses **API-key authentication** and returns a `ZMONGO_API_SESSION`.

The older README described browser login/session nodes such as `ZMONGO_SESSION`, `ZMongo/Auth`, `ZMongo/Data`, and `ZMongo/Image`. The current workflow should use:

```text
ZMongo/API/00 Auth
ZMongo/API/01 Service
ZMongo/API/02 Collections
ZMongo/API/03 Docs
ZMongo/API/04 Images
ZMongo/API/05 Fleet        # experimental / not working yet
ZMongo/API/99 Helpers
```


> **Fleet status:** Fleet nodes are currently experimental and are not part of the reliable workflow yet. Use the API/session, document, image, metadata, save-value, and get-value nodes for current work.

Default live backend:

```text
base_url = https://ztarot.app
comfy_zmongo_prefix = /comfy-zmongo
fleet_prefix = /fleet
comfy_zmongo_fleet_prefix = /comfy-zmongo-fleet
```

---

## Main Features

| Feature | Node(s) | Description |
|---|---|---|
| API-key session | `00 API Key Session` | Creates a reusable `ZMONGO_API_SESSION`. |
| Health/whoami | `01 Health`, `01 Who Am I` | Verifies API and authenticated username silo. |
| Collection listing | `02 List Collections` | Lists collections available to the API key user. |
| Document listing | `03 List Docs` | Lists document IDs from a collection. |
| Select one list item | `99 Select Nth Item` | Selects one item from ComfyUI list outputs. |
| Document fetch | `03 Get Doc` | Loads one document by collection and document ID. |
| Query/count/update/delete | `03 Query Docs`, `03 Count Docs`, `03 Update Doc`, `03 Delete Doc` | Basic API document operations. |
| Save value | `03 Save Value` | Saves a value to a selected field path. |
| Get value | `03 Get Value` | Reads a value from a selected document by dot-path. |
| Image save | `04 Easy Save Image` | Saves an image as an inline ZMongo binary envelope. |
| Image display | `04 Display Image from ZMongo` | Loads image bytes from document JSON first, route fallback second. |
| Image debug | `04 Debug Image Document` | Reports image candidate fields and document shape. |
| Metadata paths | `04 Metadata Flattened Paths` | Outputs flattened dot-path keys from metadata or the whole document. |
| Fleet calls | `05 Fleet Status`, `05 Fleet Agents`, `05 Fleet Dispatch` | Experimental placeholders. Fleet is not working yet and should not be used in normal workflows. |
| JSON pick | `99 JSON Pick` | Extracts one value from a JSON string by dot path. |

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

### Linux example

```bash
cd /home/comfyuser/comfy_build/ComfyUI/custom_nodes
git clone https://github.com/CentralFloridaAttorney/ComfyUI-ZMongo.git
cd ComfyUI-ZMongo
source /home/comfyuser/comfy_build/venv/bin/activate
pip install -r requirements.txt
```

Restart ComfyUI:

```bash
cd /home/comfyuser/comfy_build/ComfyUI
source /home/comfyuser/comfy_build/venv/bin/activate
python3 main.py --listen 0.0.0.0 --port 50003
```

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

ComfyUI-ZMongo does not connect directly to MongoDB. It talks to your backend API.

Default values for the live backend:

```text
base_url = https://ztarot.app
comfy_zmongo_prefix = /comfy-zmongo
fleet_prefix = /fleet
comfy_zmongo_fleet_prefix = /comfy-zmongo-fleet
```

The API key should belong to the same username silo that owns the documents.

Important headers sent by the API session include:

```text
ZAI_API_KEY: <api key>
Authorization: Bearer <api key>
X-AGENT-USERNAME: <username>
```

---

## Nodes

### `ZMongo/API/00 Auth`

| Display name | Class | Purpose |
|---|---|---|
| `00 API Key Session` | `ZMongoApiKeySessionNode` | Creates the `ZMONGO_API_SESSION`. |
| `00 Close API Session` | `ZMongoApiCloseSessionNode` | Closes the HTTP session. |

### `ZMongo/API/01 Service`

| Display name | Class | Purpose |
|---|---|---|
| `01 Health` | `ZMongoApiHealthNode` | Checks backend health. |
| `01 Who Am I` | `ZMongoApiWhoamiNode` | Returns authenticated username and silo database info. |

### `ZMongo/API/02 Collections`

| Display name | Class | Purpose |
|---|---|---|
| `02 List Collections` | `ZMongoApiListCollectionsNode` | Outputs collections as JSON, list, and indexed text. |
| `02 Create Collection` | `ZMongoApiCreateCollectionNode` | Creates a collection. |
| `02 Delete Collection` | `ZMongoApiDeleteCollectionNode` | Deletes a collection after confirmation. |

### `ZMongo/API/03 Docs`

| Display name | Class | Purpose |
|---|---|---|
| `03 List Docs` | `ZMongoApiListDocsNode` | Lists document IDs in a collection. |
| `03 Get Doc` | `ZMongoApiGetDocNode` | Gets a document by collection and document ID. |
| `03 Query Docs` | `ZMongoApiQueryDocsNode` | Queries documents with JSON query/projection/sort. |
| `03 Count Docs` | `ZMongoApiCountDocsNode` | Counts matching documents. |
| `03 Create Doc` | `ZMongoApiCreateDocNode` | Creates a new document from JSON. |
| `03 Update Doc` | `ZMongoApiUpdateDocNode` | Updates a document or field path. |
| `03 Delete Doc` | `ZMongoApiDeleteDocNode` | Deletes a document by confirmed ID or query. |
| `03 Save Value` | `ZMongoApiSaveValueNode` | Saves a value to a field path in an existing or queried document. |
| `03 Get Value` | `ZMongoApiGetValueNode` | Reads a value from a selected document by dot-path. |

### `ZMongo/API/04 Images`

| Display name | Class | Purpose |
|---|---|---|
| `04 Display Image from ZMongo` | `ZMongoDisplayImageNode` | Loads one saved image from a ZMongo document. |
| `04 Easy Save Image` | `ZMongoApiEasySaveImageNode` | Saves a ComfyUI image to `image_data` or another field path. |
| `04 Debug Image Document` | `ZMongoApiDocumentImageDebugNode` | Inspects image candidates and document structure. |
| `04 Image Field Candidates` | `ZMongoApiImageFieldCandidatesNode` | Shows exact and legacy image field candidates. |
| `04 Metadata Flattened Paths` | `ZMongoApiMetadataFlattenedPathsNode` | Outputs flattened dot-path keys for metadata or the whole document. |

### `ZMongo/API/05 Fleet` — experimental / not working yet

Fleet nodes are present in the codebase as placeholders for future backend/fleet integration. Do not rely on them for production workflows yet. The current reliable workflow is API session → collection selection → document selection → image/value/metadata operations.

| Display name | Class | Current status |
|---|---|---|
| `05 Fleet Status` | `ZMongoApiFleetStatusNode` | Experimental. Route compatibility still needs to be finalized. |
| `05 Fleet Agents` | `ZMongoApiFleetAgentsNode` | Experimental. Route compatibility still needs to be finalized. |
| `05 Fleet Dispatch` | `ZMongoApiFleetDispatchNode` | Experimental. Dispatch contract still needs to be finalized. |

Planned Fleet work:

- confirm the backend route prefix, likely `/fleet` or `/api/fleet`;
- confirm request/response schemas for status, agents, and dispatch;
- add a `05 Fleet Probe` node before exposing dispatch nodes as reliable;
- return clear diagnostics when a route is missing or authentication fails;
- add example workflows only after the backend routes are stable.

### `ZMongo/API/99 Helpers`

| Display name | Class | Purpose |
|---|---|---|
| `99 Select Nth Item` | `ZMongoApiSelectNthItemNode` | Selects exactly one item from a ComfyUI list output. |
| `99 JSON Pick` | `ZMongoApiJsonPickNode` | Extracts a value from JSON by dot path. |

---

## Quick Start Workflows

### 1. Connect and verify the user silo

```text
00 API Key Session
  session
    ↓
01 Who Am I
```

Use `Who Am I` to verify:

```text
username
silo_db_name / db_name
success = true
```

---

### 2. Select a collection and document

```text
00 API Key Session
  session
    ↓
02 List Collections
  collections
    ↓
99 Select Nth Item
  item = selected collection
    ↓
03 List Docs
  ids
    ↓
99 Select Nth Item
  item = selected document_id
```

`99 Select Nth Item` uses zero-based indexing:

```text
index = 0 -> first item
index = 1 -> second item
index = 2 -> third item
```

---

### 3. Display an image from ZMongo

```text
selected collection
selected document_id
field_path = image_data
session
    ↓
04 Display Image from ZMongo
    ↓
PreviewImage
```

Recommended field path:

```text
image_data
```

Do not use this as the normal field path:

```text
image_data.data
```

`image_data.data` is an internal member of the binary envelope. The display node tries the public field first and only uses `.data` as a legacy fallback.

---

### 4. Easy save one image

```text
Load Image / generated IMAGE
  image
    ↓
04 Easy Save Image
```

Recommended values:

```text
collection_name = images
field_path      = image_data
filename        = comfy_image.png
metadata_json   = {}
```

Behavior:

- If `document_id` is connected and non-empty, the node updates that document only.
- If `document_id` is empty, the node creates a new document.
- Connected IDs like `(69fbd404309e0541c53fc0be)` are cleaned before saving.
- Existing-document updates use an explicit `_id` query and do not upsert new documents.

The saved image format is:

```json
{
  "__type__": "bytes",
  "encoding": "base64",
  "size_bytes": 12345,
  "data": "...",
  "filename": "comfy_image.png",
  "content_type": "image/png",
  "source": "comfyui",
  "storage_mode": "inline_zmongo_binary_envelope"
}
```

---

### 5. Discover flattened metadata/path keys

Use:

```text
04 Metadata Flattened Paths
```

Common settings:

#### Whole document

```text
metadata_field_path =
```

Leave blank to flatten the whole document.

#### Root metadata only

```text
metadata_field_path = metadata
```

#### Metadata inside an image envelope

```text
metadata_field_path = image_data.metadata
```

The `paths` output is a ComfyUI list output and can connect to:

```text
99 Select Nth Item
```

---

### 6. Save a metadata value to a selected document

Use:

```text
03 Save Value
```

Recommended workflow:

```text
selected collection  -> collection_name
selected document_id -> document_id
selected field path  -> field_path
value_json           -> value to save
```

Example settings:

```text
query_json = {}
document_id = 69fbd404309e0541c53fc0be
field_path = metadata.caption
value_json = "A saved caption"
parse_value_json = true
upsert_if_missing = false
```

When `document_id` is connected, the node should target:

```json
{"_id": "69fbd404309e0541c53fc0be"}
```

This avoids backend errors like:

```json
{
  "success": false,
  "message": "query or document_id is required.",
  "status_code": 400
}
```

---

### 7. Get a value from a selected document

Use:

```text
03 Get Value
```

Recommended workflow:

```text
selected collection  -> collection_name
selected document_id -> document_id
field_path           -> metadata.caption
```

Examples:

```text
metadata.caption
metadata.prompt
image_data.metadata.seed
image_data.metadata.prompt
```

Leave `field_path` blank to return the whole document as JSON text.

---

## Field Paths

ComfyUI-ZMongo uses dot-path strings to locate nested fields.

Examples:

```text
image_data
metadata.prompt
metadata.caption
image_data.metadata.seed
images.0.image
frames.12.preview
```

Rules:

1. Save nodes use the field path you enter.
2. Display/load nodes try the public field path first.
3. `image_data` is the normal image field.
4. `image_data.data` is usually an internal base64 member, not the preferred public field path.
5. Use `04 Metadata Flattened Paths` to discover document or metadata keys.
6. Use `04 Debug Image Document` to inspect image-specific fields.

---

## Image Storage Notes

### Inline ZMongo binary envelope

The easy image saver stores image bytes in a JSON-safe envelope:

```json
{
  "__type__": "bytes",
  "encoding": "base64",
  "size_bytes": 12345,
  "data": "..."
}
```

Display image behavior:

1. Fetches the document JSON.
2. Tries exact field path first, usually `image_data`.
3. Tries legacy `<field>.data` fallback.
4. Uses backend image route only after document decode fails.
5. Returns a readable diagnostic placeholder image if no image is found.

### Large files

MongoDB documents have a size limit. For large image batches or videos, prefer object storage or GridFS-style storage with metadata pointers.

Recommended production pattern:

```text
MongoDB document -> metadata + object-storage pointer
Object storage   -> actual image/video bytes
```

---

## Troubleshooting

### Nodes do not appear

Check:

1. The folder is inside `ComfyUI/custom_nodes/`.
2. The folder contains `__init__.py`.
3. You restarted ComfyUI after changing files.
4. Requirements were installed into the same Python environment used by ComfyUI.
5. The file compiles.

Example:

```bash
cd /home/comfyuser/comfy_build/ComfyUI/custom_nodes/ComfyUI-ZMongo
python3 -m py_compile nodes/zmongo_api_nodes.py
```

Watch the ComfyUI terminal log for import errors.

---

### API session fails

Check:

```text
base_url
zai_api_key
username
comfy_zmongo_prefix
verify_tls
```

Recommended live values:

```text
base_url = https://ztarot.app
comfy_zmongo_prefix = /comfy-zmongo
```

Use `01 Who Am I` to confirm the API key resolves to the expected username silo.

---

### Collections or documents are empty

Possible causes:

- API key is missing or invalid.
- Username does not match the silo that owns the documents.
- You selected the wrong collection index.
- The backend route prefix is wrong.
- The user has no documents in that collection.

---

### Select Nth outputs all items

The fixed `99 Select Nth Item` uses `INPUT_IS_LIST = True` so it receives the whole list and outputs exactly one item.

Use:

```text
02 List Collections.collections -> 99 Select Nth Item.items_list
03 List Docs.ids                -> 99 Select Nth Item.items_list
```

---

### Image displays a diagnostic placeholder

The display node no longer hides failures behind a black image. If no image is found, it returns a readable placeholder and JSON diagnostics.

Check:

```text
collection_name
document_id
field_path
username
api_key
base_url
comfy_zmongo_prefix
```

Try:

```text
04 Debug Image Document
04 Metadata Flattened Paths
```

Common field path:

```text
image_data
```

---

### Save Image creates a new document unexpectedly

Expected behavior:

- `document_id` present -> update existing document only.
- `document_id` empty -> create new document.

If a new document is created, inspect the `json` output and confirm `document_id` was non-empty after cleanup.

The node cleans values such as:

```text
(69fbd404309e0541c53fc0be)
('69fbd404309e0541c53fc0be',)
["69fbd404309e0541c53fc0be"]
```

into:

```text
69fbd404309e0541c53fc0be
```

---

### Save Value says `query or document_id is required`

Connect a selected document ID, or provide a non-empty query.

Recommended:

```text
document_id <- 99 Select Nth Item.item
query_json = {}
```

The fixed node converts the selected document ID into an explicit query:

```json
{"_id": "selected_document_id"}
```

---

### Metadata Flattened Paths returns `[]`

Check `metadata_field_path`:

```text
blank                 -> flatten whole document
metadata              -> flatten root metadata only
image_data.metadata   -> flatten image envelope metadata only
```

If the whole document still returns `[]`, use `03 Get Doc` to verify the selected document exists.

---

## Security

- Do not commit API keys inside workflow JSON.
- Rotate any API key accidentally pasted into a public issue, workflow, screenshot, or chat.
- Do not put raw MongoDB credentials inside ComfyUI workflows.
- Authenticate through the backend API.
- Keep user data inside authenticated user silos.
- Keep auth/API-key/token data in a separate auth database.
- Store large files in object storage instead of inline MongoDB documents.
- Never commit `.env`, `.secrets`, JWT keys, Cloudflare keys, database passwords, or R2 credentials.

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

## Suggested Repository Layout

```text
ComfyUI-ZMongo/
├── __init__.py
├── nodes/
│   ├── zmongo_api_nodes.py
│   ├── data_processor.py
│   └── session_client.py
├── web/
│   └── zmongo_panel.js
├── examples/
│   └── zmong_save_image_DEMO.json
├── requirements.txt
├── pyproject.toml
├── LICENSE
└── README.md
```

---

## Roadmap

- Add example workflows for save/display/metadata path selection.
- Add screenshots for API session, Save Image, Display Image, Metadata Flattened Paths, and Save Value.
- Add gallery/browser frontend widgets.
- Add optional object-storage/R2 image persistence nodes.
- Add query-builder nodes for common document filters.
- Add typed metadata picker nodes for image workflows.
- Re-enable Fleet documentation and example workflows after the backend Fleet routes are working.

---

## License

ComfyUI-ZMongo is licensed under the Apache License 2.0.

See `LICENSE` for details.

---

## Credits

Built for ComfyUI workflows that need durable state, database-backed document selection, reusable image/document persistence, and metadata-driven automation.