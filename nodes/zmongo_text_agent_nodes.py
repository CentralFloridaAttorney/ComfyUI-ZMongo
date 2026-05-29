
from __future__ import annotations

# pylint: disable=too-many-lines,too-many-locals,too-many-branches,too-many-statements,broad-exception-caught,line-too-long,missing-class-docstring,missing-function-docstring

import json
import re
import time
import uuid
from datetime import datetime, timezone
from typing import Any, Optional

from .generic_helpers import (
    AlwaysDirtyMixin,
    DEFAULT_COMFY_ZMONGO_PREFIX,
    DEFAULT_GEMINI_PREFIX,
    _dirty_token,
    _error_payload,
    _extract_text_from_gemini_payload,
    _json_text,
    _parse_json_object,
    _safe_get_by_path,
    _session_api_request,
    _success_payload,
)


TEXT_AGENT_CATEGORY = "ZMongo/06 Text Agents"
DEFAULT_MODEL = "gemini-2.5-flash"
DEFAULT_MEMORY_COLLECTION = "text_agent_memory"
DEFAULT_LEDGER_COLLECTION = "text_agent_ledger"
DEFAULT_CAPSULE_COLLECTION = "text_agent_capsules"
DEFAULT_CONTEXT_COLLECTION = "text_agent_context_packs"
DEFAULT_DOC_COLLECTION = "text_agent_documents"


# -----------------------------------------------------------------------------
# Internal helpers
# -----------------------------------------------------------------------------


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _clean(value: Any, default: str = "") -> str:
    text = str(value or "").strip()
    return text or default


def _safe_int(value: Any, default: int, minimum: int, maximum: int) -> int:
    try:
        number = int(value)
    except Exception:
        number = default
    return max(minimum, min(number, maximum))


def _safe_float(value: Any, default: float, minimum: float, maximum: float) -> float:
    try:
        number = float(value)
    except Exception:
        number = default
    return max(minimum, min(number, maximum))


def _csv_list(value: str) -> list[str]:
    return [part.strip() for part in (value or "").split(",") if part.strip()]


def _new_run_id(prefix: str = "text_agent") -> str:
    return f"{prefix}_{time.strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:10]}"


def _json_loads_any(value: Any, default: Any = None) -> Any:
    if value is None:
        return default
    if isinstance(value, (dict, list)):
        return value
    text = str(value or "").strip()
    if not text:
        return default
    try:
        return json.loads(text)
    except Exception:
        return default


def _parse_object(value: Any, default: Optional[dict[str, Any]] = None) -> dict[str, Any]:
    if isinstance(value, dict):
        return value
    parsed = _json_loads_any(value, default={})
    return parsed if isinstance(parsed, dict) else (default or {})


def _parse_schema(value: str) -> Optional[dict[str, Any]]:
    parsed = _json_loads_any(value, default=None)
    return parsed if isinstance(parsed, dict) and parsed else None


def _extract_json_from_text(text: str) -> Any:
    """Best-effort extraction of JSON from Gemini text responses."""
    raw = (text or "").strip()
    if not raw:
        return None

    # Remove common Markdown code fences.
    fenced = re.search(r"```(?:json)?\s*(.*?)```", raw, flags=re.IGNORECASE | re.DOTALL)
    if fenced:
        raw = fenced.group(1).strip()

    try:
        return json.loads(raw)
    except Exception:
        pass

    # Try first JSON object.
    start = raw.find("{")
    end = raw.rfind("}")
    if start >= 0 and end > start:
        try:
            return json.loads(raw[start : end + 1])
        except Exception:
            pass

    # Try first JSON array.
    start = raw.find("[")
    end = raw.rfind("]")
    if start >= 0 and end > start:
        try:
            return json.loads(raw[start : end + 1])
        except Exception:
            pass

    return None


def _payload_data(payload: Any) -> dict[str, Any]:
    if isinstance(payload, dict) and isinstance(payload.get("data"), dict):
        return payload["data"]
    return {}


def _extract_document_id(payload: Any) -> str:
    data = _payload_data(payload)
    candidates = (
        data.get("document_id"),
        data.get("_id"),
        data.get("id"),
        data.get("inserted_id"),
        data.get("upserted_id"),
        data.get("doc_id"),
    )
    for candidate in candidates:
        if candidate:
            return str(candidate)

    if isinstance(data.get("document"), dict):
        doc = data["document"]
        for key in ("_id", "id", "document_id"):
            if doc.get(key):
                return str(doc[key])

    return ""


def _extract_documents(payload: Any) -> list[dict[str, Any]]:
    data = _payload_data(payload)

    for key in ("documents", "docs", "items", "results", "records"):
        value = data.get(key)
        if isinstance(value, list):
            return [item for item in value if isinstance(item, dict)]

    value = data.get("document")
    if isinstance(value, dict):
        return [value]

    if isinstance(data, list):
        return [item for item in data if isinstance(item, dict)]

    return []


def _limit_text(text: str, max_chars: int) -> str:
    clean = str(text or "")
    if max_chars <= 0 or len(clean) <= max_chars:
        return clean
    return clean[: max_chars - 40].rstrip() + "\n...[truncated]"


def _get_by_paths(document: dict[str, Any], field_paths: list[str]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for path in field_paths:
        clean_path = path.strip().strip(".")
        if not clean_path:
            continue
        value = _safe_get_by_path(document, clean_path)
        if value is not None:
            result[clean_path] = value
    return result


def _stringify_record(record: dict[str, Any], field_paths: list[str], max_chars_per_record: int) -> str:
    doc_id = str(record.get("_id") or record.get("id") or record.get("document_id") or "")
    selected = _get_by_paths(record, field_paths) if field_paths else record
    if not selected:
        selected = record
    text = json.dumps(
        {"document_id": doc_id, "fields": selected},
        ensure_ascii=False,
        indent=2,
        default=str,
    )
    return _limit_text(text, max_chars_per_record)


def _extract_score(parsed: Any) -> float:
    if not isinstance(parsed, dict):
        return 0.0

    direct = parsed.get("score")
    if isinstance(direct, (int, float)):
        return float(direct)

    scores = parsed.get("scores")
    if isinstance(scores, dict):
        numeric = [float(v) for v in scores.values() if isinstance(v, (int, float))]
        if numeric:
            return round(sum(numeric) / len(numeric), 2)

    return 0.0


def _extract_best_prompt(parsed: Any, fallback: str = "") -> str:
    if isinstance(parsed, dict):
        for path in (
            "best.positive_prompt",
            "best.prompt",
            "selected.positive_prompt",
            "selected.prompt",
            "revised_prompt",
            "positive_prompt",
            "prompt",
        ):
            value = _safe_get_by_path(parsed, path)
            if isinstance(value, str) and value.strip():
                return value.strip()

        variants = parsed.get("variants")
        if isinstance(variants, list) and variants:
            first = variants[0]
            if isinstance(first, dict):
                for key in ("positive_prompt", "prompt", "text"):
                    if isinstance(first.get(key), str) and first[key].strip():
                        return first[key].strip()

    return fallback


def _extract_negative_prompt(parsed: Any) -> str:
    if isinstance(parsed, dict):
        for path in (
            "best.negative_prompt",
            "selected.negative_prompt",
            "negative_prompt",
        ):
            value = _safe_get_by_path(parsed, path)
            if isinstance(value, str):
                return value.strip()

        variants = parsed.get("variants")
        if isinstance(variants, list) and variants:
            first = variants[0]
            if isinstance(first, dict) and isinstance(first.get("negative_prompt"), str):
                return first["negative_prompt"].strip()
    return ""


def _agent_config(
    session: Any,
    *,
    project_name: str,
    memory_collection: str,
    ledger_collection: str,
    capsule_collection: str,
    context_collection: str,
    model: str,
    max_output_tokens: int,
    temperature: float,
    system_instruction: str,
    gemini_prefix: str,
    zmongo_prefix: str,
) -> dict[str, Any]:
    return {
        "session": session,
        "project_name": _clean(project_name, "default"),
        "memory_collection": _clean(memory_collection, DEFAULT_MEMORY_COLLECTION),
        "ledger_collection": _clean(ledger_collection, DEFAULT_LEDGER_COLLECTION),
        "capsule_collection": _clean(capsule_collection, DEFAULT_CAPSULE_COLLECTION),
        "context_collection": _clean(context_collection, DEFAULT_CONTEXT_COLLECTION),
        "model": _clean(model, DEFAULT_MODEL),
        "max_output_tokens": _safe_int(max_output_tokens, 2048, 1, 65536),
        "temperature": _safe_float(temperature, 0.4, 0.0, 2.0),
        "system_instruction": system_instruction or "",
        "gemini_prefix": _clean(gemini_prefix, DEFAULT_GEMINI_PREFIX),
        "zmongo_prefix": _clean(zmongo_prefix, DEFAULT_COMFY_ZMONGO_PREFIX),
    }


def _resolve_config(config: Any, session: Any = None) -> dict[str, Any]:
    if isinstance(config, dict):
        resolved = dict(config)
    else:
        resolved = {}
    if session is not None:
        resolved["session"] = session
    resolved.setdefault("project_name", "default")
    resolved.setdefault("memory_collection", DEFAULT_MEMORY_COLLECTION)
    resolved.setdefault("ledger_collection", DEFAULT_LEDGER_COLLECTION)
    resolved.setdefault("capsule_collection", DEFAULT_CAPSULE_COLLECTION)
    resolved.setdefault("context_collection", DEFAULT_CONTEXT_COLLECTION)
    resolved.setdefault("model", DEFAULT_MODEL)
    resolved.setdefault("max_output_tokens", 2048)
    resolved.setdefault("temperature", 0.4)
    resolved.setdefault("system_instruction", "")
    resolved.setdefault("gemini_prefix", DEFAULT_GEMINI_PREFIX)
    resolved.setdefault("zmongo_prefix", DEFAULT_COMFY_ZMONGO_PREFIX)
    return resolved


def _require_session(config: dict[str, Any]) -> Any:
    session = config.get("session")
    if session is None:
        raise ValueError("Missing ZMongo API session. Connect a ZMongo API Key Session node.")
    return session


def _query_docs(
    session: Any,
    *,
    collection: str,
    query: Optional[dict[str, Any]] = None,
    limit: int = 20,
    skip: int = 0,
    projection: Optional[dict[str, Any]] = None,
    sort: Optional[list[Any]] = None,
    zmongo_prefix: str = DEFAULT_COMFY_ZMONGO_PREFIX,
) -> dict[str, Any]:
    body: dict[str, Any] = {
        "collection": collection,
        "query": query or {},
        "many": True,
        "limit": _safe_int(limit, 20, 1, 500),
        "skip": max(0, int(skip or 0)),
        "cache": False,
    }
    if projection:
        body["projection"] = projection
    if sort:
        body["sort"] = sort
    return _session_api_request(
        session,
        "POST",
        "/api/query",
        json_body=body,
        gemini_prefix=zmongo_prefix,
    )


def _create_doc(
    session: Any,
    *,
    collection: str,
    document: dict[str, Any],
    zmongo_prefix: str = DEFAULT_COMFY_ZMONGO_PREFIX,
) -> dict[str, Any]:
    return _session_api_request(
        session,
        "POST",
        "/api/doc/create",
        json_body={"collection": collection, "document": document},
        gemini_prefix=zmongo_prefix,
    )


def _save_value(
    session: Any,
    *,
    collection: str,
    document_id: str,
    field_path: str,
    value: Any,
    upsert: bool = True,
    query: Optional[dict[str, Any]] = None,
    zmongo_prefix: str = DEFAULT_COMFY_ZMONGO_PREFIX,
) -> dict[str, Any]:
    body: dict[str, Any] = {
        "collection": collection,
        "query": query or {},
        "field_path": field_path,
        "value": value,
        "upsert_if_missing": bool(upsert),
        "parse_json_strings": False,
        "normalize_for_storage": False,
    }
    if document_id:
        body["document_id"] = document_id
    return _session_api_request(
        session,
        "POST",
        "/api/save-value",
        json_body=body,
        gemini_prefix=zmongo_prefix,
    )


def _gemini_chat(
    session: Any,
    *,
    prompt: str,
    model: str,
    max_output_tokens: int,
    temperature: float,
    system_instruction: str = "",
    response_schema: Optional[dict[str, Any]] = None,
    gemini_prefix: str = DEFAULT_GEMINI_PREFIX,
) -> dict[str, Any]:
    body: dict[str, Any] = {
        "prompt": prompt or "",
        "model": _clean(model, DEFAULT_MODEL),
        "max_output_tokens": _safe_int(max_output_tokens, 2048, 1, 65536),
        "temperature": _safe_float(temperature, 0.4, 0.0, 2.0),
    }
    if (system_instruction or "").strip():
        body["system_instruction"] = system_instruction.strip()
    if response_schema:
        body["response_schema"] = response_schema
        body["response_mime_type"] = "application/json"

    return _session_api_request(
        session,
        "POST",
        "/api/chat",
        json_body=body,
        gemini_prefix=gemini_prefix,
    )


def _save_agent_record(
    session: Any,
    *,
    collection: str,
    project_name: str,
    record_type: str,
    payload: dict[str, Any],
    zmongo_prefix: str,
) -> tuple[dict[str, Any], str]:
    doc = {
        "project_name": project_name,
        "record_type": record_type,
        "created_at": _utc_now(),
        "updated_at": _utc_now(),
        "payload": payload,
    }
    save_payload = _create_doc(
        session,
        collection=collection,
        document=doc,
        zmongo_prefix=zmongo_prefix,
    )
    return save_payload, _extract_document_id(save_payload)


def _context_records_text(records: list[dict[str, Any]], field_paths: list[str], max_chars: int) -> str:
    if not records:
        return ""
    max_per_record = max(500, int(max_chars / max(1, len(records))))
    chunks = []
    for index, record in enumerate(records, start=1):
        chunks.append(f"[Memory {index}]\n{_stringify_record(record, field_paths, max_per_record)}")
    return _limit_text("\n\n".join(chunks), max_chars)


def _json_schema_for_prompt_audit() -> dict[str, Any]:
    return {
        "type": "object",
        "properties": {
            "score": {"type": "number"},
            "scores": {"type": "object"},
            "strengths": {"type": "array", "items": {"type": "string"}},
            "weaknesses": {"type": "array", "items": {"type": "string"}},
            "recommendations": {"type": "array", "items": {"type": "string"}},
            "revised_prompt": {"type": "string"},
            "negative_prompt": {"type": "string"},
            "backend_paid_value": {"type": "string"},
        },
        "required": ["score", "scores", "strengths", "weaknesses", "recommendations", "revised_prompt"],
    }


def _json_schema_for_prompt_evolution() -> dict[str, Any]:
    return {
        "type": "object",
        "properties": {
            "best": {
                "type": "object",
                "properties": {
                    "index": {"type": "integer"},
                    "positive_prompt": {"type": "string"},
                    "negative_prompt": {"type": "string"},
                    "reason": {"type": "string"},
                    "score": {"type": "number"},
                },
            },
            "variants": {
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": {
                        "index": {"type": "integer"},
                        "positive_prompt": {"type": "string"},
                        "negative_prompt": {"type": "string"},
                        "reason": {"type": "string"},
                        "score": {"type": "number"},
                    },
                    "required": ["index", "positive_prompt", "reason", "score"],
                },
            },
        },
        "required": ["best", "variants"],
    }


# -----------------------------------------------------------------------------
# Nodes
# -----------------------------------------------------------------------------


class ZMongoTextAgentSessionNode(AlwaysDirtyMixin):
    @classmethod
    def INPUT_TYPES(cls):
        return {
            "required": {
                "session": ("ZMONGO_API_SESSION",),
                "project_name": ("STRING", {"default": "default", "multiline": False}),
                "model": ("STRING", {"default": DEFAULT_MODEL}),
                "max_output_tokens": ("INT", {"default": 2048, "min": 1, "max": 65536}),
                "temperature": ("FLOAT", {"default": 0.4, "min": 0.0, "max": 2.0, "step": 0.05}),
            },
            "optional": {
                "memory_collection": ("STRING", {"default": DEFAULT_MEMORY_COLLECTION}),
                "ledger_collection": ("STRING", {"default": DEFAULT_LEDGER_COLLECTION}),
                "capsule_collection": ("STRING", {"default": DEFAULT_CAPSULE_COLLECTION}),
                "context_collection": ("STRING", {"default": DEFAULT_CONTEXT_COLLECTION}),
                "system_instruction": ("STRING", {"default": "You are a precise ComfyUI prompt and workflow text agent. Return compact, useful output.", "multiline": True}),
                "gemini_prefix": ("STRING", {"default": DEFAULT_GEMINI_PREFIX}),
                "zmongo_prefix": ("STRING", {"default": DEFAULT_COMFY_ZMONGO_PREFIX}),
                "refresh_token": ("STRING", {"default": ""}),
            },
        }

    RETURN_TYPES = ("ZMONGO_TEXT_AGENT_CONFIG", "STRING", "STRING")
    RETURN_NAMES = ("text_agent_config", "project_name", "json")
    FUNCTION = "build"
    CATEGORY = TEXT_AGENT_CATEGORY

    def build(
        self,
        session,
        project_name: str,
        model: str,
        max_output_tokens: int,
        temperature: float,
        memory_collection: str = DEFAULT_MEMORY_COLLECTION,
        ledger_collection: str = DEFAULT_LEDGER_COLLECTION,
        capsule_collection: str = DEFAULT_CAPSULE_COLLECTION,
        context_collection: str = DEFAULT_CONTEXT_COLLECTION,
        system_instruction: str = "",
        gemini_prefix: str = DEFAULT_GEMINI_PREFIX,
        zmongo_prefix: str = DEFAULT_COMFY_ZMONGO_PREFIX,
        refresh_token: str = "",
    ):
        config = _agent_config(
            session,
            project_name=project_name,
            memory_collection=memory_collection,
            ledger_collection=ledger_collection,
            capsule_collection=capsule_collection,
            context_collection=context_collection,
            model=model,
            max_output_tokens=max_output_tokens,
            temperature=temperature,
            system_instruction=system_instruction,
            gemini_prefix=gemini_prefix,
            zmongo_prefix=zmongo_prefix,
        )
        public = {key: value for key, value in config.items() if key != "session"}
        public["refresh"] = _dirty_token("text_agent_session", refresh_token)
        return (config, config["project_name"], _json_text(_success_payload("Text agent session configured.", public)))


class ZMongoSavePromptMemoryNode(AlwaysDirtyMixin):
    @classmethod
    def INPUT_TYPES(cls):
        return {
            "required": {
                "text_agent_config": ("ZMONGO_TEXT_AGENT_CONFIG",),
                "memory_name": ("STRING", {"default": "new_memory"}),
                "text": ("STRING", {"default": "", "multiline": True}),
                "memory_type": ("STRING", {"default": "prompt"}),
            },
            "optional": {
                "tags_csv": ("STRING", {"default": "prompt,comfyui"}),
                "notes": ("STRING", {"default": "", "multiline": True}),
                "collection_override": ("STRING", {"default": ""}),
                "extra_json": ("STRING", {"default": "{}", "multiline": True}),
                "refresh_token": ("STRING", {"default": ""}),
            },
        }

    RETURN_TYPES = ("STRING", "STRING", "BOOLEAN", "STRING")
    RETURN_NAMES = ("json", "document_id", "success", "refresh")
    FUNCTION = "save_memory"
    CATEGORY = f"{TEXT_AGENT_CATEGORY}/Memory"

    def save_memory(
        self,
        text_agent_config,
        memory_name: str,
        text: str,
        memory_type: str,
        tags_csv: str = "",
        notes: str = "",
        collection_override: str = "",
        extra_json: str = "{}",
        refresh_token: str = "",
    ):
        refresh = _dirty_token("save_prompt_memory", refresh_token)
        try:
            config = _resolve_config(text_agent_config)
            session = _require_session(config)
            collection = _clean(collection_override, config["memory_collection"])
            extra = _parse_object(extra_json)

            document = {
                "project_name": config["project_name"],
                "memory_name": _clean(memory_name, "new_memory"),
                "memory_type": _clean(memory_type, "prompt"),
                "tags": _csv_list(tags_csv),
                "text": text or "",
                "notes": notes or "",
                "extra": extra,
                "created_at": _utc_now(),
                "updated_at": _utc_now(),
                "source": "ComfyUI-ZMongo Text Agents",
            }
            payload = _create_doc(session, collection=collection, document=document, zmongo_prefix=config["zmongo_prefix"])
            document_id = _extract_document_id(payload)
            return (_json_text(payload), document_id, bool(payload.get("success")), refresh)
        except Exception as exc:
            payload = _error_payload(str(exc), error_type=exc.__class__.__name__)
            return (_json_text(payload), "", False, refresh)


class ZMongoLoadMemoryCapsuleNode(AlwaysDirtyMixin):
    @classmethod
    def INPUT_TYPES(cls):
        return {
            "required": {
                "text_agent_config": ("ZMONGO_TEXT_AGENT_CONFIG",),
                "capsule_name": ("STRING", {"default": "default_style"}),
            },
            "optional": {
                "collection_override": ("STRING", {"default": ""}),
                "field_paths_csv": ("STRING", {"default": "capsule_text,text,summary,payload.context_pack"}),
                "max_chars": ("INT", {"default": 6000, "min": 500, "max": 64000}),
                "refresh_token": ("STRING", {"default": ""}),
            },
        }

    RETURN_TYPES = ("STRING", "STRING", "STRING", "BOOLEAN")
    RETURN_NAMES = ("capsule_context", "json", "document_id", "success")
    FUNCTION = "load_capsule"
    CATEGORY = f"{TEXT_AGENT_CATEGORY}/Memory"

    def load_capsule(
        self,
        text_agent_config,
        capsule_name: str,
        collection_override: str = "",
        field_paths_csv: str = "capsule_text,text,summary,payload.context_pack",
        max_chars: int = 6000,
        refresh_token: str = "",
    ):
        try:
            config = _resolve_config(text_agent_config)
            session = _require_session(config)
            collection = _clean(collection_override, config["capsule_collection"])
            query = {"project_name": config["project_name"], "capsule_name": _clean(capsule_name)}
            payload = _query_docs(
                session,
                collection=collection,
                query=query,
                limit=1,
                sort=[["updated_at", -1]],
                zmongo_prefix=config["zmongo_prefix"],
            )
            docs = _extract_documents(payload)
            field_paths = _csv_list(field_paths_csv)
            context = _context_records_text(docs, field_paths, _safe_int(max_chars, 6000, 500, 64000))
            document_id = str(docs[0].get("_id") or docs[0].get("document_id") or "") if docs else ""
            merged = _success_payload(
                "Memory capsule loaded." if docs else "No memory capsule matched the requested name.",
                {"query_payload": payload, "capsule_name": capsule_name, "document_id": document_id, "refresh": _dirty_token("load_capsule", refresh_token)},
            )
            merged["success"] = bool(docs) and bool(payload.get("success", True))
            return (context, _json_text(merged), document_id, bool(merged["success"]))
        except Exception as exc:
            payload = _error_payload(str(exc), error_type=exc.__class__.__name__)
            return ("", _json_text(payload), "", False)


class ZMongoBuildContextPackNode(AlwaysDirtyMixin):
    @classmethod
    def INPUT_TYPES(cls):
        return {
            "required": {
                "text_agent_config": ("ZMONGO_TEXT_AGENT_CONFIG",),
                "query_text": ("STRING", {"default": "", "multiline": True}),
                "source_collection": ("STRING", {"default": DEFAULT_MEMORY_COLLECTION}),
                "limit": ("INT", {"default": 8, "min": 1, "max": 50}),
            },
            "optional": {
                "field_paths_csv": ("STRING", {"default": "memory_name,memory_type,tags,text,summary,notes,payload"}),
                "max_source_chars": ("INT", {"default": 12000, "min": 1000, "max": 64000}),
                "save_context_pack": ("BOOLEAN", {"default": True}),
                "context_pack_name": ("STRING", {"default": "latest_context_pack"}),
                "extra_filter_json": ("STRING", {"default": "{}", "multiline": True}),
                "refresh_token": ("STRING", {"default": ""}),
            },
        }

    RETURN_TYPES = ("STRING", "STRING", "STRING", "BOOLEAN")
    RETURN_NAMES = ("context_pack", "json", "context_document_id", "success")
    FUNCTION = "build_context"
    CATEGORY = f"{TEXT_AGENT_CATEGORY}/Memory"

    def build_context(
        self,
        text_agent_config,
        query_text: str,
        source_collection: str,
        limit: int,
        field_paths_csv: str = "memory_name,memory_type,tags,text,summary,notes,payload",
        max_source_chars: int = 12000,
        save_context_pack: bool = True,
        context_pack_name: str = "latest_context_pack",
        extra_filter_json: str = "{}",
        refresh_token: str = "",
    ):
        try:
            config = _resolve_config(text_agent_config)
            session = _require_session(config)
            extra_filter = _parse_object(extra_filter_json)
            query = {"project_name": config["project_name"]}
            query.update(extra_filter)

            source_payload = _query_docs(
                session,
                collection=_clean(source_collection, config["memory_collection"]),
                query=query,
                limit=limit,
                sort=[["updated_at", -1]],
                zmongo_prefix=config["zmongo_prefix"],
            )
            records = _extract_documents(source_payload)
            field_paths = _csv_list(field_paths_csv)
            raw_context = _context_records_text(records, field_paths, _safe_int(max_source_chars, 12000, 1000, 64000))

            prompt = f"""
Build a compact ComfyUI prompt-memory context pack.

User query:
{query_text or "(no query provided)"}

Project:
{config["project_name"]}

Source records:
{raw_context}

Return useful context only. Include:
- reusable style constraints
- prompt fragments worth reusing
- project-specific warnings
- prior successful patterns
- exact memory/document references when available

Do not invent stored facts. If records are weak, say what is missing.
""".strip()

            gemini_payload = _gemini_chat(
                session,
                prompt=prompt,
                model=config["model"],
                max_output_tokens=config["max_output_tokens"],
                temperature=config["temperature"],
                system_instruction=config["system_instruction"],
                gemini_prefix=config["gemini_prefix"],
            )
            context_pack = _extract_text_from_gemini_payload(gemini_payload)
            context_document_id = ""

            save_payload: dict[str, Any] = {}
            if save_context_pack and gemini_payload.get("success"):
                save_payload, context_document_id = _save_agent_record(
                    session,
                    collection=config["context_collection"],
                    project_name=config["project_name"],
                    record_type="context_pack",
                    payload={
                        "context_pack_name": _clean(context_pack_name, "latest_context_pack"),
                        "query_text": query_text or "",
                        "source_collection": source_collection,
                        "source_document_ids": [str(doc.get("_id") or doc.get("document_id") or "") for doc in records],
                        "field_paths": field_paths,
                        "context_pack": context_pack,
                        "source_count": len(records),
                    },
                    zmongo_prefix=config["zmongo_prefix"],
                )

            merged = _success_payload(
                "Context pack built.",
                {
                    "source_payload": source_payload,
                    "gemini_payload": gemini_payload,
                    "save_payload": save_payload,
                    "source_count": len(records),
                    "context_document_id": context_document_id,
                    "refresh": _dirty_token("build_context_pack", refresh_token),
                },
            )
            merged["success"] = bool(gemini_payload.get("success")) and (not save_context_pack or bool(save_payload.get("success")))
            return (context_pack, _json_text(merged), context_document_id, bool(merged["success"]))
        except Exception as exc:
            payload = _error_payload(str(exc), error_type=exc.__class__.__name__)
            return ("", _json_text(payload), "", False)


class ZMongoPromptCriticNode(AlwaysDirtyMixin):
    @classmethod
    def INPUT_TYPES(cls):
        return {
            "required": {
                "text_agent_config": ("ZMONGO_TEXT_AGENT_CONFIG",),
                "prompt": ("STRING", {"default": "", "multiline": True}),
            },
            "optional": {
                "context_pack": ("STRING", {"default": "", "multiline": True}),
                "target_model": ("STRING", {"default": "ComfyUI image/video model"}),
                "rubric": ("STRING", {"default": "Score clarity, visual specificity, consistency, contradiction risk, and model fit.", "multiline": True}),
                "save_to_ledger": ("BOOLEAN", {"default": True}),
                "refresh_token": ("STRING", {"default": ""}),
            },
        }

    RETURN_TYPES = ("STRING", "STRING", "FLOAT", "BOOLEAN", "STRING")
    RETURN_NAMES = ("json", "revised_prompt", "score", "success", "refresh")
    FUNCTION = "critique"
    CATEGORY = f"{TEXT_AGENT_CATEGORY}/Prompt Agents"

    def critique(
        self,
        text_agent_config,
        prompt: str,
        context_pack: str = "",
        target_model: str = "ComfyUI image/video model",
        rubric: str = "",
        save_to_ledger: bool = True,
        refresh_token: str = "",
    ):
        refresh = _dirty_token("prompt_critic", refresh_token)
        try:
            config = _resolve_config(text_agent_config)
            session = _require_session(config)
            schema = _json_schema_for_prompt_audit()
            critic_prompt = f"""
Critique this ComfyUI generation prompt as a professional prompt engineer.

Target model:
{target_model}

Rubric:
{rubric or "Score clarity, visual specificity, consistency, contradiction risk, and model fit."}

Project context:
{context_pack or "(none)"}

Prompt:
{prompt or ""}

Return JSON only. The revised_prompt should be directly usable as a ComfyUI positive prompt.
""".strip()

            gemini_payload = _gemini_chat(
                session,
                prompt=critic_prompt,
                model=config["model"],
                max_output_tokens=config["max_output_tokens"],
                temperature=0.2,
                system_instruction=config["system_instruction"],
                response_schema=schema,
                gemini_prefix=config["gemini_prefix"],
            )
            text = _extract_text_from_gemini_payload(gemini_payload)
            parsed = _extract_json_from_text(text)
            if parsed is None:
                parsed = {"raw_text": text, "score": 0, "revised_prompt": prompt or ""}

            score = _extract_score(parsed)
            revised_prompt = _extract_best_prompt(parsed, prompt or "")

            save_payload: dict[str, Any] = {}
            ledger_id = ""
            if save_to_ledger and gemini_payload.get("success"):
                save_payload, ledger_id = _save_agent_record(
                    session,
                    collection=config["ledger_collection"],
                    project_name=config["project_name"],
                    record_type="prompt_critic",
                    payload={
                        "run_id": _new_run_id("critic"),
                        "original_prompt": prompt or "",
                        "context_pack": context_pack or "",
                        "target_model": target_model,
                        "rubric": rubric,
                        "audit": parsed,
                        "score": score,
                        "revised_prompt": revised_prompt,
                    },
                    zmongo_prefix=config["zmongo_prefix"],
                )

            merged = _success_payload(
                "Prompt critique completed.",
                {
                    "audit": parsed,
                    "score": score,
                    "revised_prompt": revised_prompt,
                    "gemini_payload": gemini_payload,
                    "save_payload": save_payload,
                    "ledger_id": ledger_id,
                    "refresh": refresh,
                },
            )
            merged["success"] = bool(gemini_payload.get("success")) and (not save_to_ledger or bool(save_payload.get("success")))
            return (_json_text(merged), revised_prompt, float(score), bool(merged["success"]), refresh)
        except Exception as exc:
            payload = _error_payload(str(exc), error_type=exc.__class__.__name__)
            return (_json_text(payload), prompt or "", 0.0, False, refresh)


class ZMongoPromptEvolverNode(AlwaysDirtyMixin):
    @classmethod
    def INPUT_TYPES(cls):
        return {
            "required": {
                "text_agent_config": ("ZMONGO_TEXT_AGENT_CONFIG",),
                "base_prompt": ("STRING", {"default": "", "multiline": True}),
                "variant_count": ("INT", {"default": 4, "min": 1, "max": 20}),
            },
            "optional": {
                "context_pack": ("STRING", {"default": "", "multiline": True}),
                "mutation_mode": ("STRING", {"default": "cinematic, specific, production-ready"}),
                "target_model": ("STRING", {"default": "ComfyUI image/video model"}),
                "save_to_ledger": ("BOOLEAN", {"default": True}),
                "refresh_token": ("STRING", {"default": ""}),
            },
        }

    RETURN_TYPES = ("STRING", "STRING", "STRING", "BOOLEAN", "STRING")
    RETURN_NAMES = ("best_positive_prompt", "best_negative_prompt", "json", "success", "refresh")
    FUNCTION = "evolve"
    CATEGORY = f"{TEXT_AGENT_CATEGORY}/Prompt Agents"

    def evolve(
        self,
        text_agent_config,
        base_prompt: str,
        variant_count: int,
        context_pack: str = "",
        mutation_mode: str = "cinematic, specific, production-ready",
        target_model: str = "ComfyUI image/video model",
        save_to_ledger: bool = True,
        refresh_token: str = "",
    ):
        refresh = _dirty_token("prompt_evolver", refresh_token)
        try:
            config = _resolve_config(text_agent_config)
            session = _require_session(config)
            count = _safe_int(variant_count, 4, 1, 20)

            evolve_prompt = f"""
Create {count} improved ComfyUI prompt variants and select the best one.

Target model:
{target_model}

Mutation mode:
{mutation_mode}

Project memory/context:
{context_pack or "(none)"}

Base prompt:
{base_prompt or ""}

Return JSON only. Each variant must include:
- index
- positive_prompt
- negative_prompt
- reason
- score

The best positive_prompt must be directly usable in ComfyUI.
""".strip()

            gemini_payload = _gemini_chat(
                session,
                prompt=evolve_prompt,
                model=config["model"],
                max_output_tokens=config["max_output_tokens"],
                temperature=max(0.2, float(config["temperature"])),
                system_instruction=config["system_instruction"],
                response_schema=_json_schema_for_prompt_evolution(),
                gemini_prefix=config["gemini_prefix"],
            )
            text = _extract_text_from_gemini_payload(gemini_payload)
            parsed = _extract_json_from_text(text)
            if parsed is None:
                parsed = {
                    "best": {"index": 1, "positive_prompt": base_prompt or "", "negative_prompt": "", "reason": "Gemini did not return parseable JSON.", "score": 0},
                    "variants": [],
                    "raw_text": text,
                }

            best_positive = _extract_best_prompt(parsed, base_prompt or "")
            best_negative = _extract_negative_prompt(parsed)

            save_payload: dict[str, Any] = {}
            ledger_id = ""
            if save_to_ledger and gemini_payload.get("success"):
                save_payload, ledger_id = _save_agent_record(
                    session,
                    collection=config["ledger_collection"],
                    project_name=config["project_name"],
                    record_type="prompt_evolution",
                    payload={
                        "run_id": _new_run_id("evolve"),
                        "base_prompt": base_prompt or "",
                        "context_pack": context_pack or "",
                        "mutation_mode": mutation_mode,
                        "target_model": target_model,
                        "variant_count": count,
                        "evolution": parsed,
                        "best_positive_prompt": best_positive,
                        "best_negative_prompt": best_negative,
                    },
                    zmongo_prefix=config["zmongo_prefix"],
                )

            merged = _success_payload(
                "Prompt evolution completed.",
                {
                    "evolution": parsed,
                    "best_positive_prompt": best_positive,
                    "best_negative_prompt": best_negative,
                    "gemini_payload": gemini_payload,
                    "save_payload": save_payload,
                    "ledger_id": ledger_id,
                    "refresh": refresh,
                },
            )
            merged["success"] = bool(gemini_payload.get("success")) and (not save_to_ledger or bool(save_payload.get("success")))
            return (best_positive, best_negative, _json_text(merged), bool(merged["success"]), refresh)
        except Exception as exc:
            payload = _error_payload(str(exc), error_type=exc.__class__.__name__)
            return (base_prompt or "", "", _json_text(payload), False, refresh)


class ZMongoBestVariantSelectorNode(AlwaysDirtyMixin):
    @classmethod
    def INPUT_TYPES(cls):
        return {
            "required": {
                "variants_json": ("STRING", {"default": "{}", "multiline": True}),
            },
            "optional": {
                "fallback_prompt": ("STRING", {"default": "", "multiline": True}),
                "refresh_token": ("STRING", {"default": ""}),
            },
        }

    RETURN_TYPES = ("STRING", "STRING", "FLOAT", "STRING")
    RETURN_NAMES = ("positive_prompt", "negative_prompt", "score", "json")
    FUNCTION = "select_best"
    CATEGORY = f"{TEXT_AGENT_CATEGORY}/Prompt Agents"

    def select_best(self, variants_json: str, fallback_prompt: str = "", refresh_token: str = ""):
        parsed = _extract_json_from_text(variants_json) or _parse_object(variants_json)
        # Accept either raw evolution object or full wrapper from Prompt Evolver.
        evolution = _safe_get_by_path(parsed, "data.evolution") if isinstance(parsed, dict) else None
        if isinstance(evolution, dict):
            parsed = evolution

        positive = _extract_best_prompt(parsed, fallback_prompt or "")
        negative = _extract_negative_prompt(parsed)
        score = _extract_score(_safe_get_by_path(parsed, "best") if isinstance(parsed, dict) else parsed)

        payload = _success_payload(
            "Best variant selected.",
            {"positive_prompt": positive, "negative_prompt": negative, "score": score, "refresh": _dirty_token("best_variant", refresh_token)},
        )
        return (positive, negative, float(score), _json_text(payload))


class ZMongoPromptProvenanceLedgerNode(AlwaysDirtyMixin):
    @classmethod
    def INPUT_TYPES(cls):
        return {
            "required": {
                "text_agent_config": ("ZMONGO_TEXT_AGENT_CONFIG",),
                "original_prompt": ("STRING", {"default": "", "multiline": True}),
                "final_prompt": ("STRING", {"default": "", "multiline": True}),
            },
            "optional": {
                "negative_prompt": ("STRING", {"default": "", "multiline": True}),
                "context_pack": ("STRING", {"default": "", "multiline": True}),
                "image_document_id": ("STRING", {"default": ""}),
                "workflow_id": ("STRING", {"default": ""}),
                "notes": ("STRING", {"default": "", "multiline": True}),
                "extra_json": ("STRING", {"default": "{}", "multiline": True}),
                "refresh_token": ("STRING", {"default": ""}),
            },
        }

    RETURN_TYPES = ("STRING", "STRING", "BOOLEAN", "STRING")
    RETURN_NAMES = ("json", "ledger_document_id", "success", "refresh")
    FUNCTION = "save_ledger"
    CATEGORY = f"{TEXT_AGENT_CATEGORY}/Provenance"

    def save_ledger(
        self,
        text_agent_config,
        original_prompt: str,
        final_prompt: str,
        negative_prompt: str = "",
        context_pack: str = "",
        image_document_id: str = "",
        workflow_id: str = "",
        notes: str = "",
        extra_json: str = "{}",
        refresh_token: str = "",
    ):
        refresh = _dirty_token("prompt_provenance", refresh_token)
        try:
            config = _resolve_config(text_agent_config)
            session = _require_session(config)
            payload = {
                "run_id": _new_run_id("provenance"),
                "original_prompt": original_prompt or "",
                "final_prompt": final_prompt or "",
                "negative_prompt": negative_prompt or "",
                "context_pack": context_pack or "",
                "image_document_id": image_document_id or "",
                "workflow_id": workflow_id or "",
                "notes": notes or "",
                "extra": _parse_object(extra_json),
            }
            save_payload, ledger_id = _save_agent_record(
                session,
                collection=config["ledger_collection"],
                project_name=config["project_name"],
                record_type="prompt_provenance",
                payload=payload,
                zmongo_prefix=config["zmongo_prefix"],
            )
            return (_json_text(save_payload), ledger_id, bool(save_payload.get("success")), refresh)
        except Exception as exc:
            payload = _error_payload(str(exc), error_type=exc.__class__.__name__)
            return (_json_text(payload), "", False, refresh)


class ZMongoFindSimilarSuccessfulPromptsNode(AlwaysDirtyMixin):
    @classmethod
    def INPUT_TYPES(cls):
        return {
            "required": {
                "text_agent_config": ("ZMONGO_TEXT_AGENT_CONFIG",),
                "query_prompt": ("STRING", {"default": "", "multiline": True}),
            },
            "optional": {
                "source_collection": ("STRING", {"default": DEFAULT_LEDGER_COLLECTION}),
                "limit": ("INT", {"default": 12, "min": 1, "max": 50}),
                "field_paths_csv": ("STRING", {"default": "payload.final_prompt,payload.best_positive_prompt,payload.evolution.best.positive_prompt,payload.score,payload.notes,record_type,created_at"}),
                "refresh_token": ("STRING", {"default": ""}),
            },
        }

    RETURN_TYPES = ("STRING", "STRING", "BOOLEAN")
    RETURN_NAMES = ("ranked_context", "json", "success")
    FUNCTION = "find_similar"
    CATEGORY = f"{TEXT_AGENT_CATEGORY}/Provenance"

    def find_similar(
        self,
        text_agent_config,
        query_prompt: str,
        source_collection: str = DEFAULT_LEDGER_COLLECTION,
        limit: int = 12,
        field_paths_csv: str = "payload.final_prompt,payload.best_positive_prompt,payload.evolution.best.positive_prompt,payload.score,payload.notes,record_type,created_at",
        refresh_token: str = "",
    ):
        try:
            config = _resolve_config(text_agent_config)
            session = _require_session(config)
            source_payload = _query_docs(
                session,
                collection=_clean(source_collection, config["ledger_collection"]),
                query={"project_name": config["project_name"]},
                limit=limit,
                sort=[["created_at", -1]],
                zmongo_prefix=config["zmongo_prefix"],
            )
            records = _extract_documents(source_payload)
            raw_context = _context_records_text(records, _csv_list(field_paths_csv), 16000)

            prompt = f"""
Rank the stored prompt records by usefulness for improving this new prompt.

New prompt:
{query_prompt}

Stored records:
{raw_context}

Return a compact ranked list. Include exact document references when present. Focus on successful reusable prompt fragments and warnings.
""".strip()

            gemini_payload = _gemini_chat(
                session,
                prompt=prompt,
                model=config["model"],
                max_output_tokens=config["max_output_tokens"],
                temperature=0.2,
                system_instruction=config["system_instruction"],
                gemini_prefix=config["gemini_prefix"],
            )
            ranked_context = _extract_text_from_gemini_payload(gemini_payload)
            merged = _success_payload(
                "Similar successful prompts ranked.",
                {
                    "source_payload": source_payload,
                    "gemini_payload": gemini_payload,
                    "source_count": len(records),
                    "refresh": _dirty_token("find_similar", refresh_token),
                },
            )
            merged["success"] = bool(gemini_payload.get("success"))
            return (ranked_context, _json_text(merged), bool(merged["success"]))
        except Exception as exc:
            payload = _error_payload(str(exc), error_type=exc.__class__.__name__)
            return ("", _json_text(payload), False)


class ZMongoCaptionPackGeneratorNode(AlwaysDirtyMixin):
    @classmethod
    def INPUT_TYPES(cls):
        return {
            "required": {
                "text_agent_config": ("ZMONGO_TEXT_AGENT_CONFIG",),
                "source_prompt": ("STRING", {"default": "", "multiline": True}),
            },
            "optional": {
                "context_pack": ("STRING", {"default": "", "multiline": True}),
                "platform": ("STRING", {"default": "YouTube Shorts"}),
                "tone": ("STRING", {"default": "clear, energetic, non-clickbait"}),
                "save_to_ledger": ("BOOLEAN", {"default": True}),
                "refresh_token": ("STRING", {"default": ""}),
            },
        }

    RETURN_TYPES = ("STRING", "STRING", "STRING", "BOOLEAN")
    RETURN_NAMES = ("caption_text", "json", "ledger_document_id", "success")
    FUNCTION = "caption_pack"
    CATEGORY = f"{TEXT_AGENT_CATEGORY}/Publishing"

    def caption_pack(
        self,
        text_agent_config,
        source_prompt: str,
        context_pack: str = "",
        platform: str = "YouTube Shorts",
        tone: str = "clear, energetic, non-clickbait",
        save_to_ledger: bool = True,
        refresh_token: str = "",
    ):
        try:
            config = _resolve_config(text_agent_config)
            session = _require_session(config)

            prompt = f"""
Create a publishing caption pack for a ComfyUI output.

Platform:
{platform}

Tone:
{tone}

Project context:
{context_pack or "(none)"}

Source prompt / image description:
{source_prompt or ""}

Return:
- title options
- short caption
- description
- hashtags/tags
- alt text
- one-line internal note about what memory should be saved
""".strip()

            gemini_payload = _gemini_chat(
                session,
                prompt=prompt,
                model=config["model"],
                max_output_tokens=config["max_output_tokens"],
                temperature=config["temperature"],
                system_instruction=config["system_instruction"],
                gemini_prefix=config["gemini_prefix"],
            )
            caption_text = _extract_text_from_gemini_payload(gemini_payload)
            save_payload: dict[str, Any] = {}
            ledger_id = ""

            if save_to_ledger and gemini_payload.get("success"):
                save_payload, ledger_id = _save_agent_record(
                    session,
                    collection=config["ledger_collection"],
                    project_name=config["project_name"],
                    record_type="caption_pack",
                    payload={
                        "run_id": _new_run_id("caption"),
                        "source_prompt": source_prompt or "",
                        "context_pack": context_pack or "",
                        "platform": platform,
                        "tone": tone,
                        "caption_pack": caption_text,
                    },
                    zmongo_prefix=config["zmongo_prefix"],
                )

            merged = _success_payload(
                "Caption pack generated.",
                {"gemini_payload": gemini_payload, "save_payload": save_payload, "ledger_id": ledger_id, "refresh": _dirty_token("caption_pack", refresh_token)},
            )
            merged["success"] = bool(gemini_payload.get("success")) and (not save_to_ledger or bool(save_payload.get("success")))
            return (caption_text, _json_text(merged), ledger_id, bool(merged["success"]))
        except Exception as exc:
            payload = _error_payload(str(exc), error_type=exc.__class__.__name__)
            return ("", _json_text(payload), "", False)


class ZMongoWorkflowDocumentationAgentNode(AlwaysDirtyMixin):
    @classmethod
    def INPUT_TYPES(cls):
        return {
            "required": {
                "text_agent_config": ("ZMONGO_TEXT_AGENT_CONFIG",),
                "workflow_name": ("STRING", {"default": "ComfyUI-ZMongo Workflow"}),
                "workflow_summary": ("STRING", {"default": "", "multiline": True}),
            },
            "optional": {
                "context_pack": ("STRING", {"default": "", "multiline": True}),
                "include_sections_csv": ("STRING", {"default": "overview,requirements,node setup,usage steps,troubleshooting,release notes"}),
                "save_to_collection": ("BOOLEAN", {"default": True}),
                "collection_override": ("STRING", {"default": DEFAULT_DOC_COLLECTION}),
                "refresh_token": ("STRING", {"default": ""}),
            },
        }

    RETURN_TYPES = ("STRING", "STRING", "STRING", "BOOLEAN")
    RETURN_NAMES = ("markdown", "json", "document_id", "success")
    FUNCTION = "write_docs"
    CATEGORY = f"{TEXT_AGENT_CATEGORY}/Publishing"

    def write_docs(
        self,
        text_agent_config,
        workflow_name: str,
        workflow_summary: str,
        context_pack: str = "",
        include_sections_csv: str = "overview,requirements,node setup,usage steps,troubleshooting,release notes",
        save_to_collection: bool = True,
        collection_override: str = DEFAULT_DOC_COLLECTION,
        refresh_token: str = "",
    ):
        try:
            config = _resolve_config(text_agent_config)
            session = _require_session(config)
            sections = _csv_list(include_sections_csv)

            prompt = f"""
Write professional Markdown documentation for a ComfyUI workflow.

Workflow name:
{workflow_name}

Requested sections:
{sections}

Workflow summary / raw notes:
{workflow_summary or ""}

Project context:
{context_pack or "(none)"}

Requirements:
- Use clear markdown headings.
- Include a setup checklist.
- Explain what the hosted backend does and why an API account is needed.
- Include troubleshooting for auth/session/API-key/context-pack failures.
- Do not include code unless necessary.
""".strip()

            gemini_payload = _gemini_chat(
                session,
                prompt=prompt,
                model=config["model"],
                max_output_tokens=config["max_output_tokens"],
                temperature=0.3,
                system_instruction=config["system_instruction"],
                gemini_prefix=config["gemini_prefix"],
            )
            markdown = _extract_text_from_gemini_payload(gemini_payload)
            save_payload: dict[str, Any] = {}
            document_id = ""

            if save_to_collection and gemini_payload.get("success"):
                save_payload, document_id = _save_agent_record(
                    session,
                    collection=_clean(collection_override, DEFAULT_DOC_COLLECTION),
                    project_name=config["project_name"],
                    record_type="workflow_documentation",
                    payload={
                        "workflow_name": workflow_name,
                        "workflow_summary": workflow_summary,
                        "context_pack": context_pack,
                        "sections": sections,
                        "markdown": markdown,
                    },
                    zmongo_prefix=config["zmongo_prefix"],
                )

            merged = _success_payload(
                "Workflow documentation generated.",
                {"gemini_payload": gemini_payload, "save_payload": save_payload, "document_id": document_id, "refresh": _dirty_token("workflow_docs", refresh_token)},
            )
            merged["success"] = bool(gemini_payload.get("success")) and (not save_to_collection or bool(save_payload.get("success")))
            return (markdown, _json_text(merged), document_id, bool(merged["success"]))
        except Exception as exc:
            payload = _error_payload(str(exc), error_type=exc.__class__.__name__)
            return ("", _json_text(payload), "", False)


# -----------------------------------------------------------------------------
# ComfyUI mappings
# -----------------------------------------------------------------------------


NODE_CLASS_MAPPINGS = {
    "ZMongoTextAgentSessionNode": ZMongoTextAgentSessionNode,
    "ZMongoSavePromptMemoryNode": ZMongoSavePromptMemoryNode,
    "ZMongoLoadMemoryCapsuleNode": ZMongoLoadMemoryCapsuleNode,
    "ZMongoBuildContextPackNode": ZMongoBuildContextPackNode,
    "ZMongoPromptCriticNode": ZMongoPromptCriticNode,
    "ZMongoPromptEvolverNode": ZMongoPromptEvolverNode,
    "ZMongoBestVariantSelectorNode": ZMongoBestVariantSelectorNode,
    "ZMongoPromptProvenanceLedgerNode": ZMongoPromptProvenanceLedgerNode,
    "ZMongoFindSimilarSuccessfulPromptsNode": ZMongoFindSimilarSuccessfulPromptsNode,
    "ZMongoCaptionPackGeneratorNode": ZMongoCaptionPackGeneratorNode,
    "ZMongoWorkflowDocumentationAgentNode": ZMongoWorkflowDocumentationAgentNode,
}

NODE_DISPLAY_NAME_MAPPINGS = {
    "ZMongoTextAgentSessionNode": "06 Text Agent Session",
    "ZMongoSavePromptMemoryNode": "06 Save Prompt Memory",
    "ZMongoLoadMemoryCapsuleNode": "06 Load Memory Capsule",
    "ZMongoBuildContextPackNode": "06 Build Context Pack",
    "ZMongoPromptCriticNode": "06 Prompt Critic",
    "ZMongoPromptEvolverNode": "06 Prompt Evolver",
    "ZMongoBestVariantSelectorNode": "06 Best Variant Selector",
    "ZMongoPromptProvenanceLedgerNode": "06 Save Prompt Provenance Ledger",
    "ZMongoFindSimilarSuccessfulPromptsNode": "06 Find Similar Successful Prompts",
    "ZMongoCaptionPackGeneratorNode": "06 Caption Pack Generator",
    "ZMongoWorkflowDocumentationAgentNode": "06 Workflow Documentation Agent",
}

__all__ = [
    "NODE_CLASS_MAPPINGS",
    "NODE_DISPLAY_NAME_MAPPINGS",
]
