# ComfyUI-ZMongo

[![Python Version](https://img.shields.io/badge/python-3.10%2B-blue)](https://www.python.org/)
[![License](https://img.shields.io/badge/license-MIT-green)](LICENSE)

**ComfyUI-ZMongo** is a ComfyUI custom node package for saving, loading, browsing, and reusing workflow data through a ZMongo-style storage model.

The current project supports two practical modes:

1. **Hosted ZMongo API mode** through `https://businessprocessapplications.com`.
2. **Local File Store mode** for private testing and portable local workflows.

Both modes use the same user-facing workflow pattern: collections, document IDs, flattened dot-path fields, image fields, metadata fields, saved values, and reloadable workflow assets.

---

## Current Status

ComfyUI-ZMongo now focuses on a clean workflow-data layer for ComfyUI:

- Save generated or uploaded images to ZMongo-compatible documents.
- Load saved images back by `collection_name`, `document_id`, and `field_path`.
- Save and retrieve arbitrary prompt or metadata values.
- Browse collections and document IDs from inside ComfyUI.
- List flattened dot-path field names so users can select paths instead of guessing them.
- Save and reload image sequences for video workflows.
- Use either a hosted backend or local file-store backend with the same workflow structure.
- Avoid storing API keys in public workflows by using environment-variable based sessions.

---

## Important Security Rule

Do **not** type a real API key into a workflow that will be shared, published, or committed to Git.

ComfyUI saves widget values into workflow JSON. If a session node exposes an API key or username as a visible widget, those values can be serialized into the workflow file.

For public workflows, use the secure environment-variable session node:

```text
00 Secure Env API Session
```

Credentials should be supplied before ComfyUI starts:

```bash
export ZAI_API_KEY="your_real_api_key"
export ZTAROT_USERNAME="your_username"
```

Then start ComfyUI from the same shell.

Use the manual API key node only for private debugging:

```text
00 API Key Session
```

---

## Installation

From the ComfyUI custom nodes directory:

```bash
cd /home/comfyuser/comfy_build/ComfyUI/custom_nodes
git clone https://github.com/CentralFloridaAttorney/ComfyUI-ZMongo.git
cd ComfyUI-ZMongo
pip install -r requirements.txt
```

Restart ComfyUI after installation.

Typical ComfyUI path used during development:

```bash
cd /home/comfyuser/comfy_build/ComfyUI
python3 main.py
```

---

## Hosted API Configuration

Default hosted backend:

```text
https://businessprocessapplications.com
```

Default Comfy-ZMongo route prefix:

```text
/comfy-zmongo
```

The hosted API session sends requests to routes such as:

```text
/comfy-zmongo/api/health
/comfy-zmongo/api/whoami
/comfy-zmongo/api/collections
/comfy-zmongo/api/docs/<collection>
/comfy-zmongo/api/doc/<collection>/<document_id>
/comfy-zmongo/api/save-value
/comfy-zmongo/api/image/<collection>/<document_id>?field=<field_path>
```

---

## Local File Store Mode

Local File Store mode is intended for:

- Testing workflows without a hosted account.
- Building public demos without exposing backend credentials.
- Saving documents and image data locally using the same ZMongo-style collection/document/path structure.

Use:

```text
00 Local File Store Session
```

The local store follows the same conceptual model:

```text
collection_name -> document_id -> flattened dot-path field
```

This makes local workflows easy to migrate to hosted ZMongo later.

---

## Node Categories

| Category | Purpose | Main Nodes |
|---|---|---|
| `00 Auth` | Create local or hosted sessions | Local File Store Session, API Key Session, Secure Env API Session, Close API Session |
| `01 Service` | Backend checks | Health, Who Am I |
| `02 Collections` | Browse and manage collections | List Collections, Create Collection, Delete Collection |
| `03 Docs` | Work with documents and values | List Docs, Get Doc, Query Docs, Count Docs, Create Doc, Update Doc, Delete Doc, Save Value, Get Value |
| `04 Metadata / Paths` | Discover field paths | Metadata Flattened Paths, Select Nth Item |
| `04 Images` | Save and display images | Easy Save Image, Display Image, Debug Image Document |
| `04 Image Sequences / Video` | Save and reload video frames | Save Image Sequence, Load Image Sequence |
| `05 Gemini` | Optional prompt generation and structured responses | Gemini Chat, Gemini JSON, List Models, Count Tokens |

The exact node list depends on which modules are currently enabled in `nodes/__init__.py`.

---

## Core Concepts

### Collections

A collection is a logical bucket of documents. Common examples:

```text
images
image_sequences
prompts
workflow_presets
```

### Documents

A document is a single stored record. Most workflows select a document by `document_id`.

Example:

```text
69fbd588309e0541c53fc0c1
local_20260528_171929_b3c967eb
```

### Flattened Dot Paths

ComfyUI-ZMongo uses dot-separated paths to save and retrieve nested values.

Examples:

```text
image_data
prompt.positive
prompt.negative
metadata.seed
metadata.workflow_name
video.frames
image_data.preview.url
image_data.original.r2_key
```

Use the flattened path browser node to list available paths before selecting one.

---

## Common Workflows

### 1. Save and Load Image Demo

Purpose:

1. Generate or load an image.
2. Save it to a ZMongo-compatible document.
3. Return a `document_id`.
4. Load the same image back from storage.
5. Preview or save the verified image locally.

Typical values:

```text
collection_name: images
field_path: image_data
filename: zmongo_demo_generated.png
```

Recommended public workflow icon filename:

```text
zmongo_save_load_image_DEMO.png
```

Place it beside the workflow JSON:

```text
example_workflows/zmongo_save_load_image_DEMO.json
example_workflows/zmongo_save_load_image_DEMO.png
```

PNG is recommended because some ComfyUI workflow browsers do not reliably use `.jpg` thumbnails.

---

### 2. Get / Save Value Demo

Purpose:

1. List collections.
2. Select a collection.
3. List documents.
4. Select a document ID.
5. List flattened field paths.
6. Select a field path.
7. Save a prompt or metadata value.
8. Retrieve the value and verify its type.

Example value field:

```text
prompt.positive
```

Example saved value:

```text
A friendly robot slowly walks through a glowing futuristic garden, cinematic lighting, smooth forward camera movement, detailed environment, no text, no watermark.
```

---

### 3. Video Demo Workflow

The video demo shows how ZMongo can support image-to-video and frame persistence workflows.

Main flow:

1. Connect to ZMongo.
2. List collections and documents.
3. Save a start image.
4. Save prompt or metadata values.
5. Load the start image from ZMongo.
6. Generate an image-to-video result.
7. Extract video frames.
8. Save frames to an image sequence collection.
9. Load frames back from ZMongo.
10. Recreate and save an MP4 from loaded frames.

Typical image sequence values:

```text
collection_name: image_sequences
image_prefix: zmongo_video_demo_frame
fps: 16
limit: 120
```

Recommended public workflow icon filename:

```text
zmongo_video_DEMO.png
```

Place it beside the workflow JSON:

```text
example_workflows/zmongo_video_DEMO.json
example_workflows/zmongo_video_DEMO.png
```

---

## Example: Save an Image

Use an image save node with:

```text
collection_name: images
field_path: image_data
filename: comfy_image.png
document_id: <blank to create a new document, or existing ID to update>
doc_key: optional_stable_name
metadata_json: {}
```

Expected outputs:

```text
json
document_id
field_path
refresh
created_new_document
```

Connect the returned `document_id` into a display or verification node when possible.

---

## Example: Load an Image

Use an image display node with:

```text
collection_name: images
document_id: <selected or returned document_id>
field_path: image_data
cache: false
```

If loading fails, verify:

1. The collection name is correct.
2. The document ID exists.
3. The field path exists.
4. The selected session is the same backend used to save the image.
5. The user account has permission to access the document.

---

## Example: Save a Prompt Value

Use Save Value with:

```text
collection_name: images
document_id: <selected document_id>
field_path: prompt.positive
value_json: "A cinematic robot walks through a glowing futuristic garden."
parse_value_json: true
upsert_if_missing: false
parse_json_strings: true
normalize_for_storage: false
```

For plain text, either JSON-quote the text or connect a string node and let the node normalize it.

---

## Example: Get a Prompt Value

Use Get Value with:

```text
collection_name: images
document_id: <selected document_id>
field_path: prompt.positive
fallback: ""
cache: false
```

Expected outputs:

```text
json
value
exists
value_type
refresh
```

---

## Public Workflow Rules

For workflows intended for release:

- Do not include real usernames.
- Do not include real API keys.
- Prefer `00 Secure Env API Session` or `00 Local File Store Session`.
- Include clear markdown notes inside the workflow.
- Include a PNG thumbnail with the same base filename as the workflow JSON.
- Keep demo document IDs blank unless the workflow intentionally demonstrates selecting existing records.
- Use clear group titles such as `Connect`, `Select Collection`, `Save Image`, `Load Back`, `Save Metadata`, and `Verify`.

---

## Workflow Thumbnail Naming

For ComfyUI workflow galleries, use:

```text
example_workflows/<workflow_name>.json
example_workflows/<workflow_name>.png
```

Recommended:

```text
zmongo_save_load_image_DEMO.json
zmongo_save_load_image_DEMO.png
zmongo_get_save_value.json
zmongo_get_save_value.png
zmongo_video_DEMO.json
zmongo_video_DEMO.png
```

Avoid relying on `.jpg` thumbnails unless your workflow browser explicitly supports them.

---

## Troubleshooting

### Nodes do not appear

Check that `__init__.py` exports node mappings:

```python
from .nodes import NODE_CLASS_MAPPINGS, NODE_DISPLAY_NAME_MAPPINGS

WEB_DIRECTORY = "./js"

__all__ = [
    "NODE_CLASS_MAPPINGS",
    "NODE_DISPLAY_NAME_MAPPINGS",
    "WEB_DIRECTORY",
]
```

Restart ComfyUI after changing node files.

### API key appears in workflow JSON

Use `00 Secure Env API Session` instead of entering credentials into visible widgets.

Remove any saved secrets from existing workflows before publishing.

### Image loads from Mongo but not R2

Check the saved field structure. Common image field paths include:

```text
image_data
image_data.data
image_data.preview
image_data.original
image_data.preview.url
```

Use the flattened paths node to inspect the actual stored structure.

### Save Value fails with target document error

Make sure either:

```text
document_id is connected
```

or:

```json
{"_id": "DOCUMENT_ID_HERE"}
```

is supplied as `query_json`.

### Workflow icon does not show

Use `.png`, not `.jpg`, and match the base filename exactly:

```text
workflow_name.json
workflow_name.png
```

Linux filenames are case-sensitive.

---

## Development Notes

Primary development path used during testing:

```text
/home/comfyuser/comfy_build/ComfyUI/custom_nodes/ComfyUI-ZMongo
```

Typical copy/update command from a project checkout into ComfyUI:

```bash
rsync -av --delete \
  /home/comfyuser/PycharmProjects/ComfyUI-ZMongo/ \
  /home/comfyuser/comfy_build/ComfyUI/custom_nodes/ComfyUI-ZMongo/
```

Restart ComfyUI after updating Python node files.

---

## License

This project is licensed under the MIT License. See [LICENSE](LICENSE) for details.
