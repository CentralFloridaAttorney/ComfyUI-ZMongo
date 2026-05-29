# ComfyUI-ZMongo

[![Python Version](https://img.shields.io/badge/python-3.10%2B-blue)](https://www.python.org/)
[![License](https://img.shields.io/badge/license-MIT-green)](LICENSE)

**ComfyUI-ZMongo** is a custom node package for ComfyUI that integrates:

- **ZMongo database backend** (with hybrid Cloudflare R2 support)
- **Llama structured output nodes**
- **Gemini API nodes** (user-specific API key integration)

This package provides ready-to-use nodes for managing images, documents, and AI-powered chat/JSON generation in ComfyUI workflows.

---

## Table of Contents

1. [Installation](#installation)  
2. [Node Categories](#node-categories)  
3. [Usage Examples](#usage-examples)  
4. [Gemini & Llama Integration](#gemini--llama-integration)  
5. [Backend & Authentication](#backend--authentication)  
6. [Contributing](#contributing)  
7. [License](#license)  

---

## Installation

```bash
cd ~/ComfyUI/custom_nodes
git clone https://github.com/CentralFloridaAttorney/ComfyUI-ZMongo.git
cd ComfyUI-ZMongo
# Ensure your virtualenv is active
pip install -r requirements.txt
````

Make sure the backend (`BusinessProcessApplications`) is running:

```bash
export APP_BIND_PORT=50000
python3 backend/bpa_app.py
```

Set environment variables for Gemini and Llama models:

```bash
export LLAMA_MODEL_FILE=/home/user/resources/models/tinyllama.gguf
export GEMINI_API_KEY=<your_key>
```

---

## Node Categories

| Category       | Nodes                                                                                                               |
| -------------- | ------------------------------------------------------------------------------------------------------------------- |
| **00 Auth**    | Login, Register, Verify, WhoAmI                                                                                     |
| **01 Service** | List Collections, List Documents, Create/Update/Delete Document, Save Value                                         |
| **03 Docs**    | Query Documents, Count Documents, Get Single Document                                                               |
| **04 Images**  | Display Image, Save Image, Preview Image                                                                            |
| **05 Gemini**  | Key Status, Save/Delete/Test Gemini Key, Gemini Chat, Gemini JSON, List Models, Count Tokens, Chat & Save to ZMongo |
| **06 Llama**   | Llama Chat, Llama JSON, Llama Enum, Citation Verification                                                           |

---

## Usage Examples

### ZMongo Nodes

* **List Documents**

```json
{
  "collection": "images",
  "limit": 10,
  "skip": 0
}
```

* **Save Value to Document**

```json
{
  "collection": "images",
  "document_id": "69fbd588309e0541c53fc0c1",
  "field_path": "metadata.title",
  "value": "My Image Title",
  "upsert_if_missing": true
}
```

* **Get Image Preview**

```
collection: images
document_id: 69fbd588309e0541c53fc0c1
field_path: image_data
variant: preview
```

---

### Llama Nodes

* **Llama Chat Node**

  * Uses `/llama/api/chat` route
  * Input: `prompt`, optional `max_tokens`
  * Output: text + parsed JSON

* **Citation Verification Node**

  * Input: `legal_doc_path`, `cited_doc_paths`
  * Uses structured JSON output schema
  * Output: `is_quoted_correctly`, `misrepresentation_summary`, `overall_accuracy`, optional `correct_ruling`

---

### Gemini Nodes

* **Save Gemini API Key**

  * Saves key to `auth.users.integrations.gemini.api_key` (masked in UI)
* **Test Gemini Key**

  * Validates connectivity
* **Gemini Chat**

  * `/gemini/api/chat`
  * Sends prompt and returns response text
* **Gemini JSON**

  * `/gemini/api/json`
  * Returns structured JSON response
* **List Models**

  * Returns available Gemini model names
* **Count Tokens**

  * Returns estimated token usage for a prompt
* **Chat & Save to ZMongo**

  * Sends prompt, stores result in ZMongo collection

---

## Backend & Authentication

* **Authentication**

  * Supports `ZAI_API_KEY`, `Bearer JWT`, and browser session
  * All keys stored under `auth.users`
  * Tokens and usage limits enforced per user

* **Storage**

  * Hybrid MongoDB + Cloudflare R2 for images/assets
  * Tracks per-user usage in `/user/manager/settings` page

* **Routes**

  * `/comfy-zmongo/api/*` — ZMongo operations
  * `/llama/api/*` — Llama nodes
  * `/gemini/api/*` — Gemini nodes

---

## Settings Page Integration

* API keys for external AI models are managed under **API Management**
* Gemini key stored securely (cannot be read back in plaintext)
* Llama nodes use backend model path defined in `.env`

Example HTML snippet in `settings.html`:

```html
<input id="gemini_api_key" type="password" placeholder="Paste Gemini API key"/>
<button onclick="saveGeminiApiKey()">Save Gemini Key</button>
<button onclick="deleteGeminiApiKey()">Delete Gemini Key</button>
```

---

## Contributing

* Fork the repo and submit PRs for bug fixes or new nodes.
* Follow node naming convention: `ZMongo/XX Category`
* Add usage examples for any new node in README.

---

## License

This project is licensed under the MIT License. See [LICENSE](LICENSE) for details.

```

This fully integrates:

- Gemini node panel.
- Llama & Gemini route support.
- ComfyZMongo connection conventions.
- Usage examples ready for ComfyUI workflows.
```
