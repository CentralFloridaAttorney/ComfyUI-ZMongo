# 🚀 ComfyUI-ZMongo

Welcome to **ComfyUI-ZMongo**, an advanced framework that transforms ComfyUI into an integrated production ecosystem. By decoupling hardcoded values from your workflow graph and offloading them into a centralized database layer, ZMongo allows you to utilize your database as a reusable parameter library, prompt hub, and structured asset store.

Whether you want to synchronize complex generation parameters across multiple local worker rigs, store structured prompt configurations, or build automated document ingestion pipelines driven by Large Language Models (LLMs), ComfyUI-ZMongo seamlessly bridges your visual graphs with database state management.

---

## 📌 Table of Contents
* [🔑 00 Session / Auth](#-00-session--auth)
* [📁 01 Collections & 02 Docs](#-01-collections--02-docs)
* [📄 03 Documents](#-03-documents)
* [🖼️ 04 Image & 05 Images](#-04-image--05-images)
* [🧠 06 Gemini / AI API](#-06-gemini--ai-api)
* [📦 07 Text Agents](#-07-text-agents)
* [🎛️ 08 Presets](#-08-presets)
* [🎒 09 Content Packs](#-09-content-packs)
* [🛠️ Message of the Day (MOTD)](#-message-of-the-day-motd)
* [⚙️ Installation & Setup](#-installation--setup)

---

## 🔑 00 Session / Auth

Every enterprise data pipeline requires a flexible, authenticated entry point. The initialization section provides nodes that bridge local canvas variables to backend configurations using modern token infrastructure.

💡 **Added Benefit — Dual Operational Backends**: ZMongo custom nodes include a robust **Local File Store** system. If you don't supply a hosted base URL or authentication credentials, the database nodes can be operated using a Local Storage Node for saving and loading your data cleanly on your local hard drive. This provides immediate, zero-config persistence out of the box, with an effortless upgrade path to a hosted production cluster later on. Below is a recommended graph structure for a dual-operational setup:

[ ComfyUI Graph Grid ] ──► (00 API Key Session) ──► Secure Bearer Socket ──► Hosted Silo DB
        │
        └─► [ No Credentials Supplied ] ──► Built-In Local File Store ──► Local Disk Cache

### `00 API Key Session` (`ZMongoApiKeySessionNode`)
The primary cryptographic gateway for your database sessions. It manages persistence, tracks transaction logging, handles fallback switches for local-only storage, and securely flows your authenticated session downstream to all other operational nodes.
* **Inputs**:
  * `base_url`: The address of your backend operational cluster (e.g., `https://businessprocessapplications.com`). Leave blank or default to engage local-only mode.
  * `zai_api_key`: Your private security key fetched from your user management panel.
  * `username`: Your explicit account identity for logging and telemetry data isolating your personal silo storage.

### `00 Who Am I` (`ZMongoApiWhoAmINode`)
A network sanity-check block used to verify connection integrity. It pings the database server, validating active tokens, user access levels, and infrastructure connectivity statuses.

---

## 📁 01 Collections & 02 Docs

ZMongo groups data sets cleanly into distinct structural boundaries. These management tools provide full control over creating and managing collections (tables) and records directly within the node workspace.

### `01 Create Collection` / `01 Delete Collection`
Administrative schema nodes that allow you to stand up or drop custom dataset tables dynamically based on programmatic milestones or pipeline logic.

### `02 Create Document` / `02 Delete Document`
Saves or completely wipes out structured dictionary documents into target collections. Perfect for saving raw prompt structures, negative weights, and runtime configurations without typing a single line of raw code.

---

## 📄 03 Documents

The **03 Documents** suite provides local file scanning, document text parsing across various extensions, sequence selection, and asynchronous server-side OCR processing.

### 🔍 `03 Document File Browser` (`ZMongoDocumentFilePathBrowser`)
Scans a target machine folder with customized folder depth and extension filters. It formats discovered paths into array lists designed for batch streaming.
* **Supported Formats**: `.pdf`, `.docx`, `.txt`, `.md`, `.csv`, `.json`, `.log`, and `.rtf`.

### 📥 `03 Ingest Text PDF / DOCX` (`ZMongoDocumentIngestTextFile`)
Extracts text components from complex document files directly at the local system level. It maps extracted strings, line counts, and metadata configurations cleanly into document payloads.
* **Layout Engines**: Employs `PyMuPDF` for advanced PDF element mapping and `python-docx` for parsing tabular text configurations.

### 🔀 `03 Document Index Selector` (`ZMongoDocumentIndexSelector`)
A versatile index-expression compiler that slices and filters multi-document list records with precision.
* **Selection Syntax Examples**: 
  * `3` — Extracts index number 3.
  * `2-7` — Grabs an inclusive series of rows from index 2 to 7.
  * `0,2,5` — Filters out a specified custom set.
  * `*` or `all` — Passes the entire sequence uninterrupted.

### 👁️ `03 Queue Document OCR` / `06 Document OCR Status`
Schedules automated optical character recognition on the server backend for image-heavy documents or scanned media layers, outputting extraction tracking states and raw parsed text.

### 📑 Extended Document Node Registry:
* **`03 Upload Document File`**: Packages and uploads local asset payloads directly to server storage folders.
* **`03 Create Text Document`**: Instantiates a fresh document entry populated directly with string variables.
* **`03 List Documents`**: Returns structured collection metadata matching query arguments.
* **`03 Get Document Text`**: Retrieves target strings using intelligent structure fallbacks.
* **`03 Document Field Paths`**: Flattens deeply nested object trees into readable dot-notated list configurations.
* **`03 Get Document Value` / `06 Save Document Value`**: Targets deep properties directly to mutate or extract specific fields.

---

## 🖼️ 04 Image & 05 Images

These nodes turn your database into a centralized visual asset management layer, eliminating cluttered storage folders and preventing data loss when scaling across multiple machines.

* **`04 Save Image to Field`**: Intercepts active pixel grids directly from rendering nodes, packages them into optimized binary array objects, and saves them within targeted records.
* **`04 Load Image from Field`**: Resolves remote image pointers or binary fields and converts them into standard image tensors ready for immediate VAE decoding.
* **`05 Images Batch Upload`**: Collects multiple workflow output frames simultaneously and streams them directly into web platform asset galleries.

---

## 🧠 06 Gemini / AI API

Integrate advanced intelligence directly into your node graph to dynamically build prompt layouts, translate styles, and analyze imagery using state-of-the-art multimodal LLMs.

* **`06 Gemini Chat`**: Establishes conversational prompt logic paths, feeding text fields into deep models to dynamically create high-fidelity generation instructions.
* **`06 Gemini Flash`**: A lightweight inference option built for fast, low-latency processing tasks like parsing text or cleaning messy clip conditioning parameters.

---

## 📦 07 Text Agents (In-Development)

Turn your text strings into active logic directors. The text agents layer extracts context from your documents to dynamically branch your execution pipeline.

* **`07 Text Agent Router`**: Evaluates incoming text elements against complex logical conditions to automatically direct the active execution flow down different path branches based on string data.

---

## 🎛️ 08 Presets

The **08 Presets** architecture frees your workspace from rigid, tangled parameter connections. Instead of routing endless wires for seeds, steps, samplers, and CFG scales across a massive grid, you can group an entire node's state into a named database preset.

[ Target Node (e.g. KSampler ID: 14) ] ──► [ 08 Save Preset By Node ID ] ──► Unified DB Storage
                                                                                    │
[ Downstream Graph Mapping ] ◄── [ 08 Dynamic Preset Outputs ] ◄── [ 08 Load Preset ] ┘

### 💾 `08 Save Preset By Node ID` (`ZMongoSavePresetByNodeID`)
Inspects active node states at runtime using their unique canvas numeric ID. It captures every active input value, widget choice, slider property, and connection coordinate, saving them as a clean, standardized preset object.
* **Dynamic Connection Flattening**: If the targeted node gets its values from an upstream link, the save node automatically traces the connection chain back to its source, resolving and saving the actual literal values.

### 📂 `08 Load Preset` (`ZMongoLoadPreset`)
Retrieves stored configuration configurations. It can read parameters from local JSON files, hosted cloud databases, or direct text block values, formatting them into an active preset payload.

### 🔌 `08 Dynamic Preset Outputs` (`ZMongoDynamicPresetOutputs`)
An automated output pin factory that parses structural preset entries and exposes them through sequential wildcard sockets (`out_00`, `out_01`, etc.) to map properties directly to downstream inputs.

---

## 🎒 09 Content Packs

Content Packs expand on the preset system by bundling entire multi-node generation environments into single, highly transportable database collections. This allows you to store mixed production data—including images, structural strings, meta flags, and numeric generation configs—into Content Packs that can be easily shared across multiple projects.

* **`09 Export Content Pack`**: Packages active generation parameters, text items, and images into Downloadable Content Packs, which can be dropped into a canvas to rehydrate the saved content.
* **`09 Dynamic Content Pack Outputs`**: Automatically expands content pack array records back into sequential canvas links, providing a clean solution for building template-driven production pipelines.

---

## 🛠️ Message of the Day (MOTD)

* **`Message Of The Day (MOTD)`**: Connects to infrastructure alerts, updating your canvas with package change logs and cluster status announcements.

---

## ⚙️ Installation & Setup

To install the extension along with all advanced document extraction tools, clone the repository into your custom nodes directory and install dependencies inside your ComfyUI Python environment:

```bash
cd custom_nodes
git clone [https://github.com/CentralFloridaAttorney/ComfyUI-ZMongo.git](https://github.com/CentralFloridaAttorney/ComfyUI-ZMongo.git)
cd ComfyUI-ZMongo

# Install required processing libraries
pip install pymupdf python-docx requests python-dotenv