# Contributing to ComfyUI-ZMongo

First off, thank you for considering contributing to ComfyUI-ZMongo! It's people like you that make ComfyUI such a powerful and extensible community tool.

ComfyUI-ZMongo is designed to act as a **reusable parameter library and universal state engine** for ComfyUI, shifting the paradigm from wiring raw database fields to dynamically discovering schemas and hydrating typed node parameters.

The following is a set of guidelines for contributing to ComfyUI-ZMongo. These are mostly guidelines, not rules. Use your best judgment, and feel free to propose changes to this document in a pull request.

---

## 🚀 How Can I Contribute?

### 🐛 Reporting Bugs

Before creating bug reports, please check the existing issues as you might find out that you don't need to create one. When you are creating a bug report, please include as many details as possible:

* **A clear and descriptive title.**
* **ComfyUI Logs:** Paste the relevant backend terminal errors (especially `ImportError`, `AttributeError`, or `TemplateNotFound`).
* **Workflow JSON:** If a specific node sequence is failing, please attach the `.json` workflow file so we can reproduce the canvas state.
* **Session Type:** Specify whether you are using the `00 API Key Session` (hosted) or the `00 Local File Store Session`.

### 💡 Suggesting Enhancements

Enhancement suggestions are highly encouraged, particularly those involving **Node Presets**, **Dynamic Schema Discovery**, and **JavaScript UI improvements**.

* Provide a step-by-step description of the suggested enhancement.
* Explain why this enhancement would be useful to most ComfyUI-ZMongo users.
* If suggesting a new node, describe its `INPUT_TYPES`, `RETURN_TYPES`, and whether it acts as a primitive extractor or a dynamic frontend-assisted node.

### 🛠️ Pull Requests

1. Fork the repo and create your branch from `master`.
2. If you've added code that should be tested, add testing examples or workflows.
3. Ensure your code adheres to the project's architectural guidelines (see below).
4. Issue that pull request!

---

## 🏗️ Development & Architecture Guidelines

When writing code for ComfyUI-ZMongo, please adhere to the core architectural pillars of the project:

### 1. Separation of Concerns (Frontend vs. Backend)

* **Python Nodes (`/nodes/`):** Python nodes should handle data extraction, schema querying, and ComfyUI socket mapping. Do not force backend UI logic into the Python files.
* **JavaScript Extensions (`/web/js/`):** Use JavaScript hooks (like `onNodeCreated`) for dynamic UI mutations, such as spawning draggable output sockets or launching external pickers (e.g., Google Drive integration).

### 2. The ZMongo Parameter Library Model

ZMongo is built around node presets and field-path binding. When contributing new integrations:

* **Think globally:** "Load a saved configuration from ZMongo and apply it to any compatible ComfyUI node."
* **Use schemas:** Rely on ComfyUI's internal node registries and definitions (e.g., `INPUT_TYPES`) as the source of truth rather than hardcoding static parameter lists.

### 3. Session Parity (API vs. Local)

We maintain 100% plug-and-play parity between the Hosted API Session and the Local File Store Session.

* **Do not** branch node logic using hacky checks like `_is_local_file_store_session()`.
* Nodes should blindly pass data to the session object.
* Storage-specific logic (like Base64 binary envelope interception or local pointer resolution) must be handled inside the session wrappers (e.g., `LocalZMongoSession` in `generic_helpers.py`).

---

## 💻 Local Development Setup

To set up your local development environment:

1. Navigate to your ComfyUI custom nodes directory:
```bash
cd ComfyUI/custom_nodes/

```


2. Clone your fork of the repository:
```bash
git clone https://github.com/YOUR_USERNAME/ComfyUI-ZMongo.git
cd ComfyUI-ZMongo

```


3. Install the required dependencies via the editable install to ensure your IDE and Python environment sync properly:
```bash
pip install -e .

```


*(Note: If testing against specific older ComfyUI environments, be mindful of `numpy` 1.x vs 2.x compatibility).*
4. Restart your ComfyUI server. Clear your `__pycache__` directories if you encounter ghost imports.

---

## 📝 License

By contributing to ComfyUI-ZMongo, you agree that your contributions will be licensed under its **Apache-2.0** License.
