# ComfyUI-ZMongo

**A production-oriented ComfyUI custom node suite for turning ComfyUI into a real database-aware creative system.**

ComfyUI-ZMongo connects your workflows to **ZMongo-backed storage, retrieval, metadata, prompt management, and image persistence** so your graphs can do more than generate files—they can **remember, organize, version, search, and reuse structured content**.

---

## Why ComfyUI-ZMongo?

Most ComfyUI workflows end when an image is generated.

**ComfyUI-ZMongo starts there.**

This project gives ComfyUI a persistent memory layer and a practical data pipeline:

- Save prompts, settings, JSON payloads, and workflow state into ZMongo
- Store generated images inside documents as structured binary-safe payloads
- Load stored images back into ComfyUI as real `IMAGE` outputs
- Organize work by collection, document, `doc_key`, and field path
- Build reusable workflows that are **stateful**, **repeatable**, and **automation-friendly**
- Connect node graphs to a real backend instead of a folder full of disconnected files

If you want ComfyUI to behave less like a toy graph editor and more like a **creative operating system**, this is the missing layer.

---

## What makes this project special?

### 1. Real persistence for ComfyUI
ComfyUI-ZMongo lets you store structured data directly from node graphs into a database-backed system.  
That means your workflows can persist:

- prompts
- model settings
- metadata
- images
- derived outputs
- document records
- pipeline state

### 2. Image save and image load round-trips
This project is not just about saving text values.  
It includes nodes that let you:

- convert ComfyUI `IMAGE` outputs into JSON-safe storage payloads
- save those payloads into ZMongo documents
- retrieve them from ZMongo later
- decode them back into ComfyUI `IMAGE` and `MASK` outputs

That enables real workflow loops such as:

```text
Generate -> Save -> Search -> Reload -> Transform -> Save Again
```

### 3. Session-backed manager integration
The node suite is designed to work against the ZMongo manager routes using a robust session-backed flow.  
This matters in real deployments where authentication, route behavior, and browser-backed session state must work reliably.

### 4. Structured JSON-first design
Instead of scattering ad-hoc strings everywhere, ComfyUI-ZMongo standardizes structured outputs so your graphs can move real data between nodes in a consistent way.

### 5. Built for serious workflow authors
This project is aimed at people building:

- prompt libraries
- image archives
- retrieval-enhanced generation systems
- creative asset pipelines
- production automation workflows
- database-aware ComfyUI tools
- agentic or memory-backed visual systems

---

## Core capabilities

### ZMongo session manager nodes
The manager node set gives your workflow direct access to ZMongo-backed records and collections.

Typical operations include:

- connect/login
- list collections
- list documents
- get document
- find by `doc_key`
- create collection
- delete collection
- create document
- update field
- save value by query
- save value by `doc_key`
- delete document
- logout

### JSON utility nodes
ComfyUI-ZMongo includes JSON helper nodes so you can actually work with structured outputs cleanly inside graphs.

These helpers make it easier to:

- extract arrays or nested values
- select items by index
- select items by `_id`
- select items by `doc_key`
- enumerate field paths
- route structured data through larger workflows

### Image bridge nodes
The image bridge makes ComfyUI images database-friendly.

You can take a normal ComfyUI image and convert it into a structured JSON payload suitable for storage in ZMongo.

### Image loader nodes
The image loader does the reverse.

It reads image payloads back out of ZMongo and restores them as usable ComfyUI `IMAGE` outputs.

This is one of the most powerful features in the project because it turns the database into an active media source for graph execution.

---

## Why use these custom nodes instead of simpler file-based workflows?

### Because folders are not memory
Saving files to disk is easy.  
Finding the right one later, knowing what prompt created it, linking it to a project, associating it with metadata, and reusing it in a structured way is not.

ComfyUI-ZMongo solves that.

### Because image generation needs context
The best creative systems are not stateless.  
You want workflows that can remember:

- what was generated
- why it was generated
- which prompt version was used
- which collection it belongs to
- what downstream steps should happen next

### Because databases unlock automation
Once your results live in structured documents, you can:

- build galleries
- trigger follow-up workflows
- search by metadata
- group assets by project
- store prompt histories
- manage asset state across multiple runs
- integrate with external systems

### Because this is how serious pipelines scale
A production pipeline needs more than a save-image node.  
It needs structured storage, retrieval, indexing, and repeatability.

ComfyUI-ZMongo provides that foundation.

---

## Who should use ComfyUI-ZMongo?

You should use these custom nodes if you are:

- building a serious ComfyUI-based creative pipeline
- tired of losing workflow state between runs
- managing large prompt or image libraries
- creating tools for teams or repeated production tasks
- experimenting with memory-backed or agentic visual systems
- integrating ComfyUI with web backends, asset managers, or databases
- building a gallery, dashboard, archive, or content system around generated media

If your workflow needs **persistence, structure, retrieval, and reuse**, this project is for you.

---

## Example workflow patterns

### Prompt library workflow
Store prompts as documents, retrieve them by `doc_key`, select variants, and feed them into downstream generation nodes.

### Image archive workflow
Generate images, convert them to structured storage payloads, save them into documents, and later reload them by database key.

### Project asset workflow
Keep all outputs for a project in a single collection with organized field paths for prompts, thumbnails, finals, masks, and notes.

### Iterative refinement workflow
Save an image after each stage of editing so you can retrieve prior states and re-enter the graph from any point.

### Retrieval-enhanced visual workflow
Use ZMongo-stored prompts, metadata, or prior images as structured inputs to future generations.

---

## Design philosophy

ComfyUI-ZMongo is built around a few simple ideas:

### Persistent by default
A graph should be able to remember what it did.

### Structured over ad-hoc
JSON payloads and document fields scale better than disconnected strings and filenames.

### Backend-aware workflows
ComfyUI should be able to talk to real services, not just local folders.

### Round-trip media support
Saving media is not enough.  
You need to load it back into the graph and keep working.

### Professional workflow ergonomics
The node suite is designed to make database-backed workflows feel natural inside ComfyUI.

---

## Installation

Place the project inside your ComfyUI custom nodes directory.

```bash
ComfyUI/custom_nodes/ComfyUI-ZMongo
```

Restart ComfyUI after installation.

If your deployment uses a remote ZMongo-backed server, configure the connection/login nodes to point at your backend.

---

## Authentication model

ComfyUI-ZMongo is designed for real deployments and supports session-backed access patterns for manager routes.

In practical terms, that means the project is built to work with authenticated backend flows instead of assuming a simplistic local-only environment.

This is important when your ComfyUI setup is part of a wider system that includes:

- user accounts
- route protection
- session state
- web dashboards
- centralized storage

---

## Image storage model

Images are stored as structured, binary-safe JSON payloads rather than fragile raw blobs pushed through random text fields.

That allows:

- safe transport through nodes
- storage in document fields
- metadata attachment
- reliable reconstruction back into ComfyUI image tensors

This makes ComfyUI-ZMongo much more than a text/database connector.  
It becomes a true media-aware persistence layer.

---

## Why this project matters

There are many custom nodes that generate more things.

There are far fewer that help ComfyUI become a **durable creative platform**.

ComfyUI-ZMongo is valuable because it adds something deeper than another effect or model wrapper:

**it gives workflows memory, structure, and a backend.**

That changes what ComfyUI can be.

---

## Highlights

- Professional database-backed workflow integration
- Structured save/load behavior for documents and images
- JSON utility nodes for clean graph composition
- Session-aware backend connectivity
- Built for scalable, reusable, automation-friendly pipelines
- Ideal for production, experimentation, and serious tool-building

---

## Recommended use cases

- prompt repositories
- visual asset databases
- workflow state persistence
- iterative image editing pipelines
- image recall and regeneration systems
- gallery-backed ComfyUI installations
- creative backend integration
- multi-step generation systems with memory

---

## Contributing

Contributions are welcome, especially in these areas:

- additional save/load node patterns
- stronger collection/document browsing UX
- richer metadata helpers
- workflow templates
- backend-aware search nodes
- thumbnail and gallery support
- improved image/media document conventions

---

## Final pitch

If you only want to save files, there are simpler tools.

If you want ComfyUI to become a **persistent, structured, database-aware creative system**, use **ComfyUI-ZMongo**.

This project brings together:

- backend integration
- structured persistence
- image round-tripping
- reusable workflow data
- professional pipeline thinking

It is the node suite you use when your ComfyUI workflows need to **remember**, **organize**, and **scale**.

---

## ComfyUI-ZMongo at a glance

**Create. Store. Retrieve. Reuse. Scale.**

That is the difference.
