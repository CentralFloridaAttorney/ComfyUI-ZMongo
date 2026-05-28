# ComfyUI-ZMongo

[![Python Version](https://img.shields.io/badge/python-3.10%2B-blue)](https://www.python.org/)
[![License](https://img.shields.io/badge/license-MIT-green)](LICENSE)

[zmongo_get_save_value.json](example_workflows/zmongo_get_save_value.json)
**ComfyUI-ZMongo** is a ComfyUI custom node package for saving, loading, browsing, and reusing workflow data through a ZMongo-style storage model. 

The current project supports two practical modes:
1. **Hosted ZMongo API mode** through `https://businessprocessapplications.com`.
2. **Local File Store mode** for private testing and portable local workflows.

Both modes use the same user-facing workflow pattern: collections, document IDs, flattened dot-path fields, image fields, metadata fields, saved values, and reloadable workflow assets.

---

## Current Status & Features

ComfyUI-ZMongo focuses on a clean workflow-data layer for ComfyUI, now expanded with dynamic presets, document processing, and AI integrations:

* Save generated or uploaded images to ZMongo-compatible documents.
* Load saved images back by `collection_name`, `document_id`, and `field_path`.
* Save and retrieve arbitrary prompt or metadata values.
* Browse collections, list documents, and extract flattened dot-path field names directly inside ComfyUI.
* **Dynamic Presets**: Turn ComfyUI node settings into reusable workflow logic by saving node configurations and dynamically rebuilding output sockets.
* **Document Management & OCR**: Upload document files, create text documents, extract text, and queue documents for OCR processing.
* **Gemini AI Integration**: Built-in nodes for Gemini Chat, JSON generation, counting tokens, and saving AI responses directly to ZMongo documents.
* **Video & Image Sequences**: Save and reload image sequences or raw video files for continuous video workflows.
* **Fleet Dispatch**: Dispatch intents and payloads directly to fleet agents.
* **Custom UI Enhancements**: Features a dedicated ZMongo sidebar panel for login/registration and semantic socket colors (e.g., `ZMONGO_DOCUMENT_ID`) to prevent misconnections.

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
Then start ComfyUI from the same shell. Use the manual API key node only for private debugging.

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

---

## Node Categories

| Category | Purpose | Main Nodes |
|---|---|---|
| `00 Auth` | Create local or hosted sessions. | Local File Store Session, API Key Session, Close API Session. |
| `01 Service` | Backend API checks. | Health, Who Am I. |
| `02 Collections` | Browse and manage collections. | List Collections, Create Collection, Delete Collection. |
| `03 Docs` | Work with core documents and values. | List Docs, Get Doc, Query Docs, Count Docs, Create Doc, Update Doc, Delete Doc, Save Value, Get Value. |
| `04 Images` | Save, display, and discover image documents. | Easy Save Image, Display Image from ZMongo, Browse Collection Images, Debug Image Document, Image Field Candidates, Metadata Flattened Paths. |
| `04 Presets` | Create and hydrate dynamic presets. | Save Preset By Node ID, Load Preset, Dynamic Preset Outputs, Preset Debug Info. |
| `05 Fleet` | Interact with fleet agents and dispatch tasks. | Fleet Status, Fleet Agents, Fleet Dispatch. |
| `05 Gemini` | Generate AI prompts and structured responses. | Gemini Key Status, Save/Delete/Test Gemini API Key, Gemini Chat, Gemini JSON, List Models, Count Tokens, Prompt from ZMongo Doc, Chat and Save to ZMongo. |
| `06 Documents` | Upload files, create text docs, and run OCR. | Document File Browser, Upload Document File, Create Text Document, Get Document Text, Extract Document Text, Queue Document OCR, Document OCR Status. |
| `06 Video` | Save and load video data. | Save Video Frames, Load Video Frame, Save Video File, Video Frame Paths. |
| `07 Image Sequences` | Handle sequential image batches. | Save Image Sequence to ZMongo, Load Image Sequence from ZMongo. |
| `08 Presets/Convenience` | Quick access to standard presets. | KSampler Preset. |
| `09 Discovery` | Inspect ComfyUI node schemas. | Discover Workflow Node Settings, Discover Node Schema. |
| `99 Helpers` | Utility functions for lists and JSON. | Select Nth Item, JSON Pick. |

---

## Frontend UI Enhancements

* **ZMongo Sidebar Panel**: A built-in sidebar tab allows you to securely login or register an account directly from the ComfyUI interface.
* **Semantic Socket Colors**: To prevent accidental misconnections, custom socket types are visually color-coded (e.g., `ZMONGO_DOCUMENT_ID` is blue, `ZMONGO_FILE_PATH` is green, `ZMONGO_TEXT` is purple).
* **Dynamic Preset Widgets**: The `Dynamic Preset Outputs` node automatically updates its interface, adjusting text areas and hiding internal widgets to keep the workspace clean.

---

## Core Concepts

### Collections & Documents
A collection is a logical bucket of documents (e.g., `images`, `image_sequences`, `workflow_presets`). A document is a single stored record, selected by a unique `document_id`.

### Flattened Dot Paths
ComfyUI-ZMongo uses dot-separated paths to save and retrieve nested values. Examples include `image_data`, `prompt.positive`, and `video.frames`.

### Dynamic Presets
Instead of manually duplicating node settings, you can save the settings from any node (like a `KSampler`) as a named preset. The **Load Preset** and **Dynamic Preset Outputs** nodes will dynamically generate sockets matching the saved fields, allowing you to easily switch between "Draft", "Final Quality", or specific conditional styles.

---

## Common Workflows

### 1. Save and Load Image Demo
Demonstrates the complete save-and-verify loop by generating an image, saving it to ZMongo, and immediately loading it back using the returned `document_id`. 
* **Workflow File**: `zmongo_save_load_image.json` / `zmongo_save_load_image.jpg`.

### 2. Get / Save Value Demo
Guides you through selecting a collection, listing documents, finding a field path, and saving/retrieving prompt data (like `prompt.positive`).
* **Workflow File**: `zmongo_get_save_value.json` / `zmongo_get_save_value.jpg`.

### 3. Dynamic Preset Features
Shows how to use text parsing (e.g., searching for the word "many") to conditionally switch between different saved KSampler presets. 
* **Workflow File**: `zmongo_preset_features.json` / `zmongo_preset_features.jpg`.
[zmongo_preset_features.json](example_workflows/zmongo_preset_features.json)![zmongo_preset_features.jpg](example_workflows/zmongo_preset_features.jpg)
---

## Troubleshooting

### Authentication fails
Confirm your `username`, `zai_api_key`, and ensure the `comfy_zmongo_prefix` is set to `/comfy-zmongo`. If issues persist, use the ZMongo sidebar panel to re-authenticate and copy a fresh key.

### Field path save fails
Ensure you are using a writable path such as `prompt.positive` or `metadata.note`. ZMongo blocks writes to protected identity fields like `_id`.

### API key appears in workflow JSON
Use `00 Secure Env API Session` instead of entering credentials into visible widgets. Remove any saved secrets from existing workflows before publishing.

### Image loads from Mongo but not R2
Check the saved field structure. Use the **Metadata Flattened Paths** node to inspect the actual stored structure (e.g., `image_data.data` or `image_data.preview.url`).
