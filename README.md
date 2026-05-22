# BusinessProcessApplications.com — ZMongo Integration

<p align="center">
  <strong>Business Process Applications: Practical automation, workflow intelligence, and database-backed tools for modern business operations.</strong>
</p>

<p align="center">
  <a href="#overview"><img alt="Overview" src="https://img.shields.io/badge/overview-doc-blue"></a>
  <a href="#installation"><img alt="Installation" src="https://img.shields.io/badge/installation-guide-green"></a>
  <a href="#nodes"><img alt="Nodes" src="https://img.shields.io/badge/nodes-ZMongo%2FAPI-yellow"></a>
  <a href="#license"><img alt="License" src="https://img.shields.io/badge/license-Apache--2.0-lightgrey"></a>
</p>

---

## Overview

**BusinessProcessApplications.com** integrates a **ZMongo Data Feature** that allows ComfyUI workflows to store, retrieve, and display documents and images from a per-user silo.

**Key capabilities:**

* API-key authentication for secure, per-user silo access.
* Dynamic collection and document listing.
* Document query, create, update, delete operations.
* Image persistence with inline binary envelopes or Cloudflare R2 as backend storage.
* Metadata flattening and dot-path key discovery.
* Fleet routing and task dispatching for distributed workflows.
* Live session token management and per-service usage tracking.

**Use cases:**

* Persistent workflow memory and automation.
* Image and metadata storage for AI/ComfyUI workflows.
* Database-driven prompt and document pipelines.
* Preview, debug, and route images inside ComfyUI without MongoDB direct access.

---

## Architecture

**API-key model:**

* All nodes connect through a reusable `ZMONGO_API_SESSION`.
* Default backend URL: `https://businessprocessapplications.com`.
* ComfyUI-ZMongo prefix: `/comfy-zmongo`.
* Fleet prefix: `/fleet`.
* ComfyUI-ZMongo-fleet prefix: `/comfy-zmongo-fleet`.

**Route structure:**

```text
ZMongo/00 Auth
ZMongo/01 Service
ZMongo/02 Collections
ZMongo/03 Docs
ZMongo/04 Images
ZMongo/05 Fleet
ZMongo/99 Helpers
```

**Security and best practices:**

* Do not embed API keys or credentials in workflows.
* Authenticate via backend API; ensure per-user silo isolation.
* Large assets should use object storage (Cloudflare R2) rather than inline JSON in MongoDB.

---

## Nodes

### 00 Auth

| Display Name         | Class                       | Purpose                                 |
| -------------------- | --------------------------- | --------------------------------------- |
| 00 API Key Session   | `ZMongoApiKeySessionNode`   | Creates reusable session for API calls. |
| 00 Close API Session | `ZMongoApiCloseSessionNode` | Closes the HTTP session.                |

### 01 Service

| Display Name | Class                 | Purpose                             |
| ------------ | --------------------- | ----------------------------------- |
| 01 Health    | `ZMongoApiHealthNode` | Verifies backend health.            |
| 01 Who Am I  | `ZMongoApiWhoamiNode` | Confirms username silo and DB info. |

### 02 Collections

| Display Name         | Class                           | Purpose                                          |
| -------------------- | ------------------------------- | ------------------------------------------------ |
| 02 List Collections  | `ZMongoApiListCollectionsNode`  | Lists collections available to the API key user. |
| 02 Create Collection | `ZMongoApiCreateCollectionNode` | Creates a new collection.                        |
| 02 Delete Collection | `ZMongoApiDeleteCollectionNode` | Deletes a collection after confirmation.         |

### 03 Docs

| Display Name  | Class                    | Purpose                                               |
| ------------- | ------------------------ | ----------------------------------------------------- |
| 03 List Docs  | `ZMongoApiListDocsNode`  | Lists document IDs in a collection.                   |
| 03 Get Doc    | `ZMongoApiGetDocNode`    | Loads a single document.                              |
| 03 Query Docs | `ZMongoApiQueryDocsNode` | Queries documents.                                    |
| 03 Count Docs | `ZMongoApiCountDocsNode` | Counts matching documents.                            |
| 03 Create Doc | `ZMongoApiCreateDocNode` | Creates a new document.                               |
| 03 Update Doc | `ZMongoApiUpdateDocNode` | Updates a document field or full JSON.                |
| 03 Delete Doc | `ZMongoApiDeleteDocNode` | Deletes a document by ID or query.                    |
| 03 Save Value | `ZMongoApiSaveValueNode` | Saves a value to a field path in a selected document. |
| 03 Get Value  | `ZMongoApiGetValueNode`  | Fetches one value from a document by dot-path.        |

### 04 Images

| Display Name                 | Class                                 | Purpose                                                |
| ---------------------------- | ------------------------------------- | ------------------------------------------------------ |
| 04 Display Image from ZMongo | `ZMongoDisplayImageNode`              | Shows image from a ZMongo document or fallback route.  |
| 04 Easy Save Image           | `ZMongoApiEasySaveImageNode`          | Saves a ComfyUI IMAGE tensor to ZMongo document.       |
| 04 Debug Image Document      | `ZMongoApiDocumentImageDebugNode`     | Outputs document image candidates and structure.       |
| 04 Image Field Candidates    | `ZMongoApiImageFieldCandidatesNode`   | Returns all valid image field paths for a document.    |
| 04 Metadata Flattened Paths  | `ZMongoApiMetadataFlattenedPathsNode` | Flattens dot-path keys for metadata or whole document. |

### 05 Fleet

| Display Name      | Class                        | Purpose                              |
| ----------------- | ---------------------------- | ------------------------------------ |
| 05 Fleet Status   | `ZMongoApiFleetStatusNode`   | Inspects fleet status.               |
| 05 Fleet Agents   | `ZMongoApiFleetAgentsNode`   | Lists all registered agents.         |
| 05 Fleet Dispatch | `ZMongoApiFleetDispatchNode` | Sends JSON payloads to fleet agents. |

### 99 Helpers

| Display Name       | Class                        | Purpose                                        |
| ------------------ | ---------------------------- | ---------------------------------------------- |
| 99 Select Nth Item | `ZMongoApiSelectNthItemNode` | Picks a single item from ComfyUI list outputs. |
| 99 JSON Pick       | `ZMongoApiJsonPickNode`      | Fetches a value from JSON by dot-path.         |

---

## Quick Workflows

**Connect and verify session:**

```text
00 API Key Session -> 01 Who Am I
```

**Select collection/document:**

```text
02 List Collections -> 99 Select Nth Item
03 List Docs        -> 99 Select Nth Item
```

**Display image:**

```text
04 Display Image from ZMongo
```

**Save image:**

```text
04 Easy Save Image
document_id connected → update
document_id empty     → create new document
```

**Flatten metadata paths:**

```text
04 Metadata Flattened Paths
metadata_field_path = ""  -> whole document
metadata_field_path = "metadata"  -> root metadata
metadata_field_path = "image_data.metadata" -> image metadata
```

**Save a value to a field:**

```text
03 Save Value
collection_name -> selected collection
document_id -> selected document
field_path -> target dot-path
value_json -> value to store
```

---

## Installation

```bash
# Navigate to ComfyUI custom_nodes folder
cd /path/to/ComfyUI/custom_nodes
git clone https://github.com/CentralFloridaAttorney/ComfyUI-ZMongo.git
cd ComfyUI-ZMongo
pip install -r requirements.txt
```

Restart ComfyUI.

---

## Notes

* ZMongo-ZComfyUI nodes use the **backend API**; no direct MongoDB connection is needed.
* Large images can be stored on **Cloudflare R2** or hybrid storage; previews are cached in MongoDB for fast display.
* Node failure will produce readable diagnostic images and JSON logs instead of black screens.
* Ensure session, API key, and document IDs are valid and connected.

---

## License

Apache 2.0 — see LICENSE file.

---

## Credits

Built for ComfyUI workflows needing durable state, document selection, image persistence, and metadata-driven automation.
