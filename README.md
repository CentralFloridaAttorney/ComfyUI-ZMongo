# ComfyUI-ZMongo

[![Overview](https://img.shields.io/badge/overview-doc-blue)](#overview)
[![Installation](https://img.shields.io/badge/installation-guide-green)](#installation)
[![Nodes](https://img.shields.io/badge/nodes-ZMongo%2FAPI-yellow)](#nodes)
[![License](https://img.shields.io/badge/license-Apache--2.0-lightgrey)](#license)

**BusinessProcessApplications.com** integrates a **ZMongo Data Feature** that allows ComfyUI workflows to store, retrieve, and display documents and images from a secure, isolated per-user cloud silo. 

Rather than requiring direct database exposure or managing raw connection strings, this suite routes securely through an API-driven session manager. It features automated metadata tree-flattening, atomic binary envelope management, and inline image serialization.

---

## Key Capabilities

* **Secure Per-User Silos:** Authenticate via API keys without exposing raw database credentials inside your graph workflows.
* **Atomic Binary Envelopes:** Automatically handles custom bytes-envelope wrappers (`__type__ = "bytes"`) to process base64 data structures seamlessly.
* **Automated Metadata Flattening:** Advanced dot-path key discovery dynamically flattens deeply-nested structures into clean, targetable addresses.
* **Robust Text Scalar Unwrapping:** Built-in safeguards peel punctuation artifacts, tuples, single-item lists, and quotation marks frequently introduced by complex graph link routing.
* **Visual Diagnostic Engine:** Node exceptions or missing field references generate readable, non-black diagnostic placeholder image streams directly in your UI for lightning-fast troubleshooting.

---

## Architecture & Configuration

### API-Session Model
All execution pathways route communication through a reusable, authenticated `ZMONGO_API_SESSION` connection client. 
* **Default Backend URL:** `https://businessprocessapplications.com`
* **ComfyUI-ZMongo Route Prefix:** `/comfy-zmongo`

### Categorized Route Structure
Nodes are strictly organized into predictable namespaces to streamline graph navigation:
```text
ZMongo/00 Auth          - Session lifecycle and endpoint initialization
ZMongo/01 Service       - Ecosystem connectivity verification and silo inspection
ZMongo/02 Collections   - Direct collection lifecycle operations (Sandbox management)
ZMongo/03 Docs          - Granular document CRUD, value mutations, and dot-path queries
ZMongo/04 Images        - Automated tensor serialization, rendering, and structural profiling
ZMongo/99 Helpers       - Scalar evaluation, item picking, and raw JSON parsing

```

---

## Installation

### Method 1: Via Pip (Recommended for Releases)

Install the node suite directly into your active ComfyUI Python dependencies:

```bash
pip install ComfyUI-ZMongo

```

### Method 2: Manual Git Setup

To track raw upstream development or maintain a local link context:

```bash
cd /path/to/ComfyUI/custom_nodes
git clone git@github.com:CentralFloridaAttorney/ComfyUI-ZMongo.git
cd ComfyUI-ZMongo
pip install -r requirements.txt

```

---

## Production Node Reference

### ZMongo/00 Auth

| Display Name | Class | Purpose |
| --- | --- | --- |
| **00 API Key Session** | `ZMongoApiKeySessionNode` | Spawns a reusable authenticated HTTP connection state using your platform API token. |
| **00 Close API Session** | `ZMongoApiCloseSessionNode` | Safely closes out and releases active requests connection pools. |

### ZMongo/01 Service

| Display Name | Class | Purpose |
| --- | --- | --- |
| **01 Health** | `ZMongoApiHealthNode` | Pings the backend service to verify API availability and health. |
| **01 Who Am I** | `ZMongoApiWhoamiNode` | Resolves active identity traits including your isolated user username silo and database context. |

### ZMongo/02 Collections

| Display Name | Class | Purpose |
| --- | --- | --- |
| **02 List Collections** | `ZMongoApiListCollectionsNode` | Pulls an array of all sandboxed collection namespaces bound to your account. |
| **02 Create Collection** | `ZMongoApiCreateCollectionNode` | Spins up a brand new collection bucket inside your cloud silo. |
| **02 Delete Collection** | `ZMongoApiDeleteCollectionNode` | Destroys an isolated collection using absolute string-matching verification. |

### ZMongo/03 Docs

| Display Name | Class | Purpose |
| --- | --- | --- |
| **03 List Docs** | `ZMongoApiListDocsNode` | Returns document identifiers existing within a specified namespace. |
| **03 Get Doc** | `ZMongoApiGetDocNode` | Retreives an entire raw JSON document structure via its object ID string. |
| **03 Query Docs** | `ZMongoApiQueryDocsNode` | Executes structured Mongo-flavored queries with pagination, projection, and sorting. |
| **03 Count Docs** | `ZMongoApiCountDocsNode` | Evaluates total records matching specified filter conditions without pulling raw records. |
| **03 Create Doc** | `ZMongoApiCreateDocNode` | Commits a clean structured JSON document into your designated collection storage. |
| **03 Update Doc** | `ZMongoApiUpdateDocNode` | Modifies an existing document using query matches or field mutations. |
| **03 Delete Doc** | `ZMongoApiDeleteDocNode` | Evicts dynamic document configurations matching targeting query filters. |
| **03 Get Value** | `ZMongoApiGetValueNode` | Targets and extracts a single deep property value via explicit dot-path formatting. |
| **03 Save Value** | `ZMongoApiSaveValueNode` | Directly mutates or inserts properties deeply nested within explicit path contexts. |

### ZMongo/04 Images

| Display Name | Class | Purpose |
| --- | --- | --- |
| **04 Display Image from ZMongo** | `ZMongoDisplayImageNode` | Decodes inline data frames or falls back to backend routing to display images. |
| **04 Easy Save Image** | `ZMongoApiEasySaveImageNode` | Encapsulates a ComfyUI tensor directly into an atomic binary envelope for transmission. |
| **04 Debug Image Document** | `ZMongoApiDocumentImageDebugNode` | Inspects document candidate structures and validates envelope contents. |
| **04 Image Field Candidates** | `ZMongoApiImageFieldCandidatesNode` | Evaluates field strings to score valid image mapping targets. |
| **04 Metadata Flattened Paths** | `ZMongoApiMetadataFlattenedPathsNode` | Unfolds data trees into flat dot-notated addressing indexes. |

### ZMongo/99 Helpers

| Display Name | Class | Purpose |
| --- | --- | --- |
| **99 Select Nth Item** | `ZMongoApiSelectNthItemNode` | Isolates a specific element from streaming output list configurations safely. |
| **99 JSON Pick** | `ZMongoApiJsonPickNode` | Utility to cleanly extract objects from stringified JSON blobs. |

---

## Core Graph Routing Blueprint Examples

* **Establish Session and Profile Validation:**
```text
[00 API Key Session] -> [01 Who Am I]

```


* **Iterate Sandboxed Storage Contexts:**
```text
[02 List Collections] -> [99 Select Nth Item] -> [03 List Docs] -> [99 Select Nth Item]

```


* **Persistent Media Writing Flow Rules:**
```text
[04 Easy Save Image]
* Providing a Document ID  -> Overwrites/Mutates that explicit index target.
* Leaving Document ID Empty -> Instantiates a fresh base layout file automatically.

```


* **Deep Schema Tree Inspection:**
```text
[04 Metadata Flattened Paths]
* Set metadata_field_path to ""                 -> Flattens complete target record.
* Set metadata_field_path to "metadata"         -> Limits parsing to base prompt structures.
* Set metadata_field_path to "image_data.metadata" -> Targets image generation tags exclusively.

```



---

## License

Distributed under the **Apache 2.0 License**. See accompanying `LICENSE` file for full authorization parameters.