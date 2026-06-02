from __future__ import annotations

import importlib
import json
import re
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple


# -----------------------------------------------------------------------------
# Constants
# -----------------------------------------------------------------------------

PRESET_SCHEMA_VERSION = "1.0.0"
PRESET_KIND = "zmongo_node_preset"

DEFAULT_COLLECTION_NAME = "workflow_presets"

STORAGE_AUTO = "auto"
STORAGE_LOCAL_FILE = "local_file"
STORAGE_ZMONGO_API = "zmongo_api"

MAX_DYNAMIC_OUTPUTS = 64

DYNAMIC_PRESET_NODE_CLASS = "ZMongoDynamicPresetOutputs"
LOAD_PRESET_NODE_CLASS = "ZMongoLoadPreset"
SAVE_PRESET_NODE_CLASS = "ZMongoSavePresetByNodeID"
API_STORAGE_FORMAT = "dot_path_fields_v3"

# -----------------------------------------------------------------------------
# Missing Helpers Implementation
# -----------------------------------------------------------------------------

def _json_dumps(obj: Any) -> str:
    try:
        return json.dumps(obj, indent=2, default=str)
    except Exception:
        return "{}"

def _parse_json(text: Any) -> dict[str, Any]:
    if isinstance(text, dict): return text
    try:
        return json.loads(str(text))
    except Exception:
        return {}

def _preset_path(collection_name: str, preset_name: str) -> Path:
    base_dir = Path(__file__).parent.parent / "zmongo_presets" / collection_name
    base_dir.mkdir(parents=True, exist_ok=True)
    return base_dir / f"{preset_name}.json"

def _payload_data(payload: Any) -> dict[str, Any]:
    if isinstance(payload, dict):
        data = payload.get("data")
        if isinstance(data, dict): return data
    return payload if isinstance(payload, dict) else {}

def _payload_success(payload: Any) -> bool:
    if isinstance(payload, dict):
        return bool(payload.get("success", True))
    return False

def _api_legacy_query_for_preset(preset_name: str) -> dict[str, Any]:
    return {"preset_name": str(preset_name or "").strip()}

def _preset_document_score(document: dict[str, Any], target_name: str) -> float:
    score = 0.0
    if document.get("schema_kind") == PRESET_KIND:
        score += 10
    if str(document.get("preset_name")).strip() == target_name:
        score += 10
    score += float(document.get("updated_at_unix", 0.0)) / 1e10
    return score

def _is_link_value(value: Any) -> bool:
    return isinstance(value, list) and len(value) == 2 and isinstance(value[0], (str, int)) and isinstance(value[1], int)

def _parse_link_value(value: Any) -> tuple[str, Optional[int]]:
    if _is_link_value(value):
        return str(value[0]), int(value[1])
    return "", None

def _normalize_node_id(node_id: Any) -> str:
    return str(node_id)

def _resolve_storage_backend(backend: str, session: Any = None) -> str:
    if backend == STORAGE_AUTO:
        return STORAGE_ZMONGO_API if session is not None else STORAGE_LOCAL_FILE
    return backend

def _node_result(result: tuple, ui_text: str) -> dict[str, Any]:
    return {"ui": {"text": [ui_text]}, "result": result}

# -----------------------------------------------------------------------------
# JSON / path helpers
# -----------------------------------------------------------------------------

def _normalize_bson_extended_json(value: Any) -> Any:
    """
    Convert Mongo Extended JSON wrappers into normal Python values.
    Handles ObjectId, number wrappers, dates, and nested values.
    """
    if isinstance(value, dict):
        keys = set(value.keys())

        if keys == {"$oid"}:
            return str(value["$oid"])

        if keys == {"$numberLong"}:
            try:
                return int(value["$numberLong"])
            except Exception:
                return str(value["$numberLong"])

        if keys == {"$numberInt"}:
            try:
                return int(value["$numberInt"])
            except Exception:
                return str(value["$numberInt"])

        if keys == {"$numberDouble"}:
            try:
                return float(value["$numberDouble"])
            except Exception:
                return str(value["$numberDouble"])

        if keys == {"$numberDecimal"}:
            try:
                return float(value["$numberDecimal"])
            except Exception:
                return str(value["$numberDecimal"])

        if keys == {"$date"}:
            return value["$date"]

        if keys == {"$binary"}:
            return value["$binary"]

        return {
            str(key): _normalize_bson_extended_json(child)
            for key, child in value.items()
        }

    if isinstance(value, list):
        return [_normalize_bson_extended_json(item) for item in value]

    return value


def _extract_documents(payload: Any) -> list[dict[str, Any]]:
    """
    Extract documents from all known ZMongo/SafeResult/API wrapper shapes.
    """
    payload = _normalize_bson_extended_json(payload)

    if not isinstance(payload, dict):
        return []

    candidates: list[Any] = []
    data = payload.get("data")

    if isinstance(data, dict):
        for key in ("documents", "docs", "results", "items"):
            value = data.get(key)
            if isinstance(value, list):
                candidates.extend(value)

        for key in ("document", "doc", "result"):
            value = data.get(key)
            if isinstance(value, dict):
                candidates.append(value)

        if (
            "_id" in data
            or "document_id" in data
            or "id" in data
            or "preset_fields_by_index" in data
            or "preset_payload" in data
            or "fields" in data
        ):
            candidates.append(data)

    elif isinstance(data, list):
        candidates.extend(data)

    for key in ("documents", "docs", "results", "items"):
        value = payload.get(key)
        if isinstance(value, list):
            candidates.extend(value)

    for key in ("document", "doc", "result"):
        value = payload.get(key)
        if isinstance(value, dict):
            candidates.append(value)

    if (
        "_id" in payload
        or "document_id" in payload
        or "id" in payload
        or "preset_fields_by_index" in payload
        or "preset_payload" in payload
        or "fields" in payload
    ):
        candidates.append(payload)

    documents: list[dict[str, Any]] = []
    seen: set[str] = set()

    for candidate in candidates:
        candidate = _normalize_bson_extended_json(candidate)

        if not isinstance(candidate, dict):
            continue

        identity = str(
            candidate.get("_id")
            or candidate.get("document_id")
            or candidate.get("id")
            or id(candidate)
        )

        if identity in seen:
            continue

        seen.add(identity)
        documents.append(candidate)

    return documents


def _extract_first_document(payload: Any) -> dict[str, Any]:
    docs = _extract_documents(payload)
    return docs[0] if docs else {}


def _document_id_from_document(document: Any) -> str:
    document = _normalize_bson_extended_json(document)

    if not isinstance(document, dict):
        return ""

    for key in ("_id", "document_id", "inserted_id", "id", "system_id", "uuid"):
        value = document.get(key)
        if value:
            return str(_normalize_bson_extended_json(value))

    return ""


def _document_id_from_payload(payload: Any) -> str:
    if not isinstance(payload, dict):
        return ""

    data = _payload_data(payload)

    for key in ("document_id", "inserted_id", "_id", "id", "system_id", "uuid"):
        value = data.get(key)
        if value:
            return str(value)

    document = _extract_first_document(payload)
    return _document_id_from_document(document)


def _api_field_slot(index: int) -> str:
    return f"f{int(index):03d}"


def _reconstruct_preset_payload_from_dot_path_document(document: Any) -> dict[str, Any]:
    """
    Reconstruct direct saved ZMongo dot-path preset documents.
    """
    document = _normalize_bson_extended_json(document)

    if not isinstance(document, dict):
        return {}

    fields_by_index = document.get("preset_fields_by_index")
    if not isinstance(fields_by_index, dict) or not fields_by_index:
        return {}

    try:
        field_count = int(document.get("field_count") or 0)
    except Exception:
        field_count = 0

    if field_count > 0:
        keys = [_api_field_slot(index) for index in range(field_count)]
    else:
        keys = sorted(str(key) for key in fields_by_index.keys())

    fields: list[dict[str, Any]] = []

    for key in keys:
        raw_field = fields_by_index.get(key)
        raw_field = _normalize_bson_extended_json(raw_field)

        if not isinstance(raw_field, dict):
            continue

        input_name = str(raw_field.get("input_name") or "").strip()
        if not input_name:
            continue

        value = _normalize_bson_extended_json(raw_field.get("value"))

        field: dict[str, Any] = {
            "input_name": input_name,
            "declared_type": raw_field.get("declared_type") or "UNKNOWN",
            "widget_kind": raw_field.get("widget_kind") or "unknown",
            "value": value,
            "value_python_type": raw_field.get("value_python_type") or type(value).__name__,
            "constraints": raw_field.get("constraints") if isinstance(raw_field.get("constraints"), dict) else {},
            "options_source": raw_field.get("options_source"),
            "options_snapshot": raw_field.get("options_snapshot") if isinstance(raw_field.get("options_snapshot"), list) else [],
            "source_node_class": raw_field.get("source_node_class") or document.get("source_node_class") or "",
            "is_link": bool(raw_field.get("is_link")),
            "link_target": raw_field.get("link_target"),
            "resolved_from_link": bool(raw_field.get("resolved_from_link", False)),
            "resolved_link_target": raw_field.get("resolved_link_target"),
        }

        fields.append(field)

    if not fields:
        return {}

    payload: dict[str, Any] = {
        "schema_kind": str(document.get("schema_kind") or PRESET_KIND),
        "schema_version": str(document.get("schema_version") or PRESET_SCHEMA_VERSION),
        "preset_name": str(document.get("preset_name") or ""),
        "collection_name": str(document.get("collection_name") or DEFAULT_COLLECTION_NAME),
        "source_node_id": str(document.get("source_node_id") or ""),
        "source_node_class": str(document.get("source_node_class") or ""),
        "save_mode": str(document.get("save_mode") or ""),
        "created_at_unix": document.get("created_at_unix") or document.get("updated_at_unix") or time.time(),
        "updated_at_unix": document.get("updated_at_unix") or document.get("created_at_unix") or time.time(),
        "field_count": len(fields),
        "fields": fields,
    }

    if isinstance(document.get("unresolved_links"), list):
        payload["unresolved_links"] = document.get("unresolved_links")

    if isinstance(document.get("unresolved_link_details"), list):
        payload["unresolved_link_details"] = document.get("unresolved_link_details")

    if document.get("warning"):
        payload["warning"] = document.get("warning")

    return payload


def _extract_preset_payload_from_document(document: Any) -> dict[str, Any]:
    """Universal preset extractor."""
    document = _normalize_bson_extended_json(document)

    if not isinstance(document, dict):
        return {}

    # 1. Direct preset payload.
    if isinstance(document.get("fields"), list):
        payload = dict(document)
        payload["field_count"] = len(payload.get("fields") or [])
        return payload

    # 2. New dot-path ZMongo document.
    reconstructed = _reconstruct_preset_payload_from_dot_path_document(document)
    if reconstructed:
        return reconstructed

    # 3. Legacy wrapped payload.
    payload = document.get("preset_payload")

    if isinstance(payload, str):
        parsed = _parse_json(payload)
        if isinstance(parsed.get("fields"), list):
            parsed["field_count"] = len(parsed.get("fields") or [])
            return parsed

    if isinstance(payload, dict):
        nested = _extract_preset_payload_from_document(payload)
        if nested:
            return nested

    # 4. Common API wrappers.
    for wrapper_key in ("document", "doc", "result", "data"):
        wrapped = document.get(wrapper_key)
        if isinstance(wrapped, dict):
            nested = _extract_preset_payload_from_document(wrapped)
            if nested:
                return nested

    # 5. Query/list wrappers.
    docs = _extract_documents(document)
    for doc in docs:
        nested = _extract_preset_payload_from_document(doc)
        if nested:
            return nested

    return {}


def _extract_preset_payload_from_json_text(value: Any) -> tuple[dict[str, Any], str, str]:
    if value is None:
        return {}, "", "No document_json provided."

    if isinstance(value, dict):
        parsed = value
    else:
        text = str(value or "").strip()
        if not text or text == "{}":
            return {}, "", "No document_json provided."
        try:
            parsed = json.loads(text)
        except Exception as exc:
            return {}, "", f"document_json was not valid JSON: {exc}"

    parsed = _normalize_bson_extended_json(parsed)
    document = _extract_first_document(parsed) or parsed
    document_id = _document_id_from_document(document) or _document_id_from_payload(parsed)
    payload = _extract_preset_payload_from_document(parsed)

    if not payload:
        return {}, document_id, "document_json did not contain reconstructable preset data."

    return payload, document_id, "Loaded preset from provided document_json."


# -----------------------------------------------------------------------------
# Local storage
# -----------------------------------------------------------------------------

def _save_preset_local(payload: Dict[str, Any], overwrite: bool) -> Tuple[bool, str, Path]:
    preset_name = str(payload.get("preset_name") or "")
    collection_name = str(payload.get("collection_name") or DEFAULT_COLLECTION_NAME)
    path = _preset_path(collection_name, preset_name)

    if path.exists() and not overwrite:
        return False, f"Preset {preset_name!r} already exists. Enable overwrite to replace it.", path

    path.write_text(_json_dumps(payload), encoding="utf-8")
    return True, f"Saved preset {preset_name!r} to local file {path}", path


def _load_preset_local(collection_name: str, preset_name: str) -> Tuple[Dict[str, Any], str, Path]:
    path = _preset_path(collection_name, preset_name)

    if not path.exists():
        return {}, f"Preset {preset_name!r} was not found at {path}", path

    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:
        return {}, f"Could not read preset {preset_name!r}: {exc}", path

    if not isinstance(payload, dict):
        return {}, f"Preset {preset_name!r} did not contain a JSON object.", path

    return payload, f"Loaded preset {preset_name!r} from local file {path}", path


# -----------------------------------------------------------------------------
# ZMongo API storage
# -----------------------------------------------------------------------------

def _api_query_for_preset(preset_name: str) -> dict[str, Any]:
    return {
        "schema_kind": PRESET_KIND,
        "preset_name": str(preset_name or "").strip(),
    }

def _api_top_level_preset_fields(payload: dict[str, Any]) -> dict[str, Any]:
    return {
        "schema_kind": PRESET_KIND,
        "schema_version": str(payload.get("schema_version") or PRESET_SCHEMA_VERSION),
        "preset_name": str(payload.get("preset_name") or "").strip(),
        "collection_name": str(payload.get("collection_name") or DEFAULT_COLLECTION_NAME),
        "source_node_id": str(payload.get("source_node_id") or ""),
        "source_node_class": str(payload.get("source_node_class") or ""),
        "save_mode": str(payload.get("save_mode") or ""),
        "field_count": int(payload.get("field_count") or 0),
        "updated_at_unix": time.time(),
        "api_storage_format": API_STORAGE_FORMAT,
    }


def _api_field_leafs(field: dict[str, Any]) -> dict[str, Any]:
    safe_field = dict(field or {})
    return {
        "input_name": safe_field.get("input_name"),
        "declared_type": safe_field.get("declared_type"),
        "widget_kind": safe_field.get("widget_kind"),
        "value": safe_field.get("value"),
        "value_python_type": safe_field.get("value_python_type"),
        "constraints": safe_field.get("constraints") if isinstance(safe_field.get("constraints"), dict) else {},
        "options_source": safe_field.get("options_source"),
        "options_snapshot": safe_field.get("options_snapshot") if isinstance(safe_field.get("options_snapshot"), list) else [],
        "source_node_class": safe_field.get("source_node_class"),
        "is_link": bool(safe_field.get("is_link")),
        "link_target": safe_field.get("link_target"),
        "resolved_from_link": bool(safe_field.get("resolved_from_link", False)),
        "resolved_link_target": safe_field.get("resolved_link_target"),
    }



def _session_api_request(
    *,
    session: Any,
    method: str,
    path: str,
    json_body: Optional[dict[str, Any]] = None,
    params: Optional[dict[str, Any]] = None,
) -> dict[str, Any]:
    """
    Call the real Comfy-ZMongo backend through ZMongoApiSession.request.

    The verified backend route prefix is:

        /comfy-zmongo/api

    ZMongoApiSession.request(prefix="/api", path="/...") is responsible for
    adding /comfy-zmongo before /api.
    """
    if session is None:
        return {
            "success": False,
            "message": "No ZMongo API session provided.",
            "data": {},
            "error": {"msg": "missing session"},
            "status_code": 400,
        }

    request_fn = getattr(session, "request", None)
    if not callable(request_fn):
        return {
            "success": False,
            "message": "ZMongo API session does not expose request().",
            "data": {
                "required_method": "request",
                "method": method,
                "path": path,
                "json_body": json_body or {},
                "params": params or {},
            },
            "error": {"msg": "session.request unavailable"},
            "status_code": 500,
        }

    api_prefix = getattr(session, "API_PREFIX", "/api") or "/api"

    try:
        response = request_fn(
            method,
            api_prefix,
            path,
            json_body=json_body,
            params=params,
        )
        return response if isinstance(response, dict) else {
            "success": False,
            "message": "API response was not a JSON object.",
            "data": {"raw_response": response},
            "error": {"msg": "non-dict response"},
            "status_code": 0,
        }
    except TypeError:
        try:
            response = request_fn(
                method,
                api_prefix,
                path,
                json_body=json_body,
            )
            return response if isinstance(response, dict) else {}
        except Exception as exc:
            return {
                "success": False,
                "message": f"API request failed: {exc}",
                "data": {
                    "method": method,
                    "path": path,
                    "json_body": json_body or {},
                    "params": params or {},
                },
                "error": {"type": exc.__class__.__name__, "msg": str(exc)},
                "status_code": 0,
            }
    except Exception as exc:
        return {
            "success": False,
            "message": f"API request failed: {exc}",
            "data": {
                "method": method,
                "path": path,
                "json_body": json_body or {},
                "params": params or {},
            },
            "error": {"type": exc.__class__.__name__, "msg": str(exc)},
            "status_code": 0,
        }


def _api_query_documents_direct(
    *,
    session: Any,
    collection_name: str,
    query: dict[str, Any],
    many: bool = True,
    limit: int = 50,
    skip: int = 0,
    sort: Any = None,
    cache: bool = False,
) -> dict[str, Any]:
    """
    Real backend query route.

    Verified in ComfyZMongoRoutes:
        POST /api/query
    mounted as:
        POST /comfy-zmongo/api/query
    """
    body: dict[str, Any] = {
        "collection": collection_name,
        "coll": collection_name,
        "query": query or {},
        "many": bool(many),
        "limit": int(limit or 50),
        "skip": int(skip or 0),
        "cache": bool(cache),
    }

    if sort is not None:
        body["sort"] = sort

    return _session_api_request(
        session=session,
        method="POST",
        path="/query",
        json_body=body,
    )


def _api_get_document_direct(
    *,
    session: Any,
    collection_name: str,
    document_id: str,
) -> dict[str, Any]:
    """
    Real backend single-document route.

    Verified in ComfyZMongoRoutes:
        GET /api/doc/<coll>/<doc_id>
    mounted as:
        GET /comfy-zmongo/api/doc/<coll>/<doc_id>
    """
    clean_collection = str(collection_name or "").strip()
    clean_id = str(document_id or "").strip()

    return _session_api_request(
        session=session,
        method="GET",
        path=f"/doc/{clean_collection}/{clean_id}",
        params={"cache": "false"},
    )


def _api_save_value(
    *,
    session: Any,
    collection_name: str,
    query: dict[str, Any],
    field_path: str,
    value: Any,
) -> tuple[bool, str, str, dict[str, Any]]:
    """
    Save one preset leaf through the verified real backend route.

    Verified in ComfyZMongoRoutes:
        POST /api/save-value

    Mounted URL:
        /comfy-zmongo/api/save-value
    """
    body = {
        "collection": collection_name,
        "coll": collection_name,
        "query": query or {},
        "field_path": field_path,
        "value": value,
        "upsert_if_missing": True,
        "upsert": True,
        "parse_json_strings": False,
        "normalize_for_storage": True,
    }

    response_dict = _session_api_request(
        session=session,
        method="POST",
        path="/save-value",
        json_body=body,
    )

    success = _payload_success(response_dict)
    document_id = _document_id_from_payload(response_dict)

    if not success:
        # Fallback for older local/session adapters that expose save_value()
        # but not the direct backend request route.
        save_value = getattr(session, "save_value", None)
        if callable(save_value):
            try:
                fallback_response = save_value(
                    collection=collection_name,
                    query=query,
                    document_id="",
                    field_path=field_path,
                    value=value,
                    upsert_if_missing=True,
                    parse_json_strings=False,
                    normalize_for_storage=True,
                )
                fallback_dict = fallback_response if isinstance(fallback_response, dict) else {}
                fallback_success = _payload_success(fallback_dict)
                fallback_document_id = _document_id_from_payload(fallback_dict)

                if fallback_success:
                    return (
                        True,
                        f"Saved {field_path} using session.save_value fallback.",
                        fallback_document_id or document_id,
                        fallback_dict,
                    )

                response_dict = fallback_dict or response_dict
                document_id = fallback_document_id or document_id

            except Exception:
                pass

    if not success:
        return (
            False,
            f"save-value returned failure at {field_path!r}: {response_dict}",
            document_id,
            response_dict,
        )

    return True, f"Saved {field_path}", document_id, response_dict


def _query_preset_api_document(
    *,
    session: Any,
    collection_name: str,
    preset_name: str,
) -> tuple[dict[str, Any], str, dict[str, Any]]:
    """
    Query presets using the verified real backend route:

        POST /comfy-zmongo/api/query

    Falls back to session.query_docs/list_docs only for older local adapters.
    """
    if session is None:
        return {}, "", {}

    collection = (collection_name or DEFAULT_COLLECTION_NAME).strip()
    cleaned_name = str(preset_name or "").strip()

    if not collection or not cleaned_name:
        return {}, "", {}

    queries = [
        _api_query_for_preset(cleaned_name),
        _api_legacy_query_for_preset(cleaned_name),
    ]

    sort_variants: list[Any] = [
        [["updated_at_unix", -1]],
        [("updated_at_unix", -1)],
        None,
    ]

    responses: list[dict[str, Any]] = []
    candidates: list[dict[str, Any]] = []
    last_response: dict[str, Any] = {}

    def add_response(response: Any) -> None:
        nonlocal last_response
        response_dict = _normalize_bson_extended_json(response if isinstance(response, dict) else {})
        if response_dict:
            responses.append(response_dict)
            last_response = response_dict

        for document in _extract_documents(response_dict):
            if not isinstance(document, dict):
                continue

            normalized_doc = _normalize_bson_extended_json(document)
            if not isinstance(normalized_doc, dict):
                continue

            doc_name = str(normalized_doc.get("preset_name") or "").strip()
            if doc_name and doc_name != cleaned_name:
                continue

            if (
                normalized_doc.get("schema_kind") == PRESET_KIND
                or doc_name == cleaned_name
                or "preset_fields_by_index" in normalized_doc
                or "preset_payload" in normalized_doc
                or "fields" in normalized_doc
            ):
                candidates.append(normalized_doc)

    for query in queries:
        for sort in sort_variants:
            response = _api_query_documents_direct(
                session=session,
                collection_name=collection,
                query=query,
                many=True,
                limit=50,
                skip=0,
                sort=sort,
                cache=False,
            )
            add_response(response)

            response = _api_query_documents_direct(
                session=session,
                collection_name=collection,
                query=query,
                many=False,
                limit=1,
                skip=0,
                sort=sort,
                cache=False,
            )
            add_response(response)

    # Fallback support for older adapters.
    query_docs = getattr(session, "query_docs", None)
    list_docs = getattr(session, "list_docs", None)

    for query in queries:
        if callable(query_docs):
            try:
                add_response(
                    query_docs(
                        collection=collection,
                        query=query,
                        document_id="",
                        many=True,
                        limit=25,
                        skip=0,
                        projection=None,
                        sort=[["updated_at_unix", -1]],
                        cache=False,
                    )
                )
            except TypeError:
                try:
                    add_response(
                        query_docs(
                            collection=collection,
                            query=query,
                            many=True,
                            limit=25,
                            skip=0,
                            cache=False,
                        )
                    )
                except Exception:
                    pass
            except Exception:
                pass

        if callable(list_docs):
            try:
                add_response(list_docs(collection=collection, query=query, limit=25, skip=0))
            except Exception:
                pass

    if candidates:
        candidates.sort(key=lambda doc: _preset_document_score(doc, cleaned_name), reverse=True)
        document = candidates[0]
        document_id = _document_id_from_document(document)
        merged_response = {
            "success": True,
            "message": f"Found preset {cleaned_name!r}.",
            "data": {
                "document": document,
                "candidate_count": len(candidates),
                "responses_checked": len(responses),
                "real_backend_routes": [
                    "POST /comfy-zmongo/api/query",
                    "POST /comfy-zmongo/api/save-value",
                ],
            },
            "error": None,
            "status_code": int((last_response or {}).get("status_code") or 200),
        }
        return document, document_id, merged_response

    return {}, "", last_response


def _reconstruct_preset_payload_from_api_document(document: Any) -> dict[str, Any]:
    if not isinstance(document, dict):
        return {}

    fields_by_index = document.get("preset_fields_by_index")
    if not isinstance(fields_by_index, dict) or not fields_by_index:
        return {}

    field_count = 0
    try:
        field_count = int(document.get("field_count") or 0)
    except Exception:
        field_count = 0

    keys = [_api_field_slot(i) for i in range(max(field_count, len(fields_by_index)))]

    for key in sorted(str(k) for k in fields_by_index.keys()):
        if key not in keys:
            keys.append(key)

    fields: list[dict[str, Any]] = []

    for key in keys:
        raw_field = fields_by_index.get(key)

        if not isinstance(raw_field, dict):
            continue

        input_name = str(raw_field.get("input_name") or "").strip()
        if not input_name:
            continue

        field = {
            "input_name": input_name,
            "declared_type": raw_field.get("declared_type") or "UNKNOWN",
            "widget_kind": raw_field.get("widget_kind") or "unknown",
            "value": raw_field.get("value"),
            "value_python_type": raw_field.get("value_python_type") or type(raw_field.get("value")).__name__,
            "constraints": raw_field.get("constraints") if isinstance(raw_field.get("constraints"), dict) else {},
            "options_source": raw_field.get("options_source"),
            "options_snapshot": raw_field.get("options_snapshot") if isinstance(raw_field.get("options_snapshot"), list) else [],
            "source_node_class": raw_field.get("source_node_class") or document.get("source_node_class") or "",
            "is_link": bool(raw_field.get("is_link")),
            "link_target": raw_field.get("link_target"),
        }

        if raw_field.get("resolved_from_link") is not None:
            field["resolved_from_link"] = bool(raw_field.get("resolved_from_link"))

        if raw_field.get("resolved_link_target") is not None:
            field["resolved_link_target"] = raw_field.get("resolved_link_target")

        fields.append(field)

    if not fields:
        return {}

    payload = {
        "schema_kind": str(document.get("schema_kind") or PRESET_KIND),
        "schema_version": str(document.get("schema_version") or PRESET_SCHEMA_VERSION),
        "preset_name": str(document.get("preset_name") or ""),
        "collection_name": str(document.get("collection_name") or DEFAULT_COLLECTION_NAME),
        "source_node_id": str(document.get("source_node_id") or ""),
        "source_node_class": str(document.get("source_node_class") or ""),
        "save_mode": str(document.get("save_mode") or ""),
        "created_at_unix": document.get("created_at_unix") or document.get("updated_at_unix") or time.time(),
        "field_count": len(fields),
        "fields": fields,
    }

    if document.get("warning"):
        payload["warning"] = document.get("warning")

    if isinstance(document.get("unresolved_links"), list):
        payload["unresolved_links"] = document.get("unresolved_links")

    if isinstance(document.get("unresolved_link_details"), list):
        payload["unresolved_link_details"] = document.get("unresolved_link_details")

    return payload


def _save_preset_api(
    *,
    session: Any,
    collection_name: str,
    payload: dict[str, Any],
    overwrite: bool,
) -> tuple[bool, str, str]:
    if session is None:
        return False, "No ZMongo API session provided.", ""

    preset_name = str(payload.get("preset_name") or "").strip()
    if not preset_name:
        return False, "preset_name is required.", ""

    collection = (collection_name or DEFAULT_COLLECTION_NAME).strip()
    query = _api_query_for_preset(preset_name)

    existing_doc, existing_id, existing_response = _query_preset_api_document(
        session=session,
        collection_name=collection,
        preset_name=preset_name,
    )

    if existing_doc and not overwrite:
        return (
            False,
            f"Preset {preset_name!r} already exists in ZMongo API. Enable overwrite to replace it.",
            existing_id,
        )

    fields = payload.get("fields") or []
    if not isinstance(fields, list):
        return False, "payload.fields must be a list.", existing_id

    failures: list[str] = []
    latest_document_id = existing_id

    for field_path, value in _api_top_level_preset_fields(payload).items():
        ok, message, document_id, raw_response = _api_save_value(
            session=session,
            collection_name=collection,
            query=query,
            field_path=field_path,
            value=value,
        )

        if document_id:
            latest_document_id = document_id

        if not ok:
            failures.append(message)

    ok, message, document_id, raw_response = _api_save_value(
        session=session,
        collection_name=collection,
        query=query,
        field_path="field_count",
        value=len(fields),
    )
    if document_id:
        latest_document_id = document_id
    if not ok:
        failures.append(message)

    for index, field in enumerate(fields):
        if not isinstance(field, dict):
            continue

        slot = _api_field_slot(index)
        leafs = _api_field_leafs(field)

        for leaf_name, value in leafs.items():
            field_path = f"preset_fields_by_index.{slot}.{leaf_name}"

            ok, message, document_id, raw_response = _api_save_value(
                session=session,
                collection_name=collection,
                query=query,
                field_path=field_path,
                value=value,
            )

            if document_id:
                latest_document_id = document_id

            if not ok:
                failures.append(message)

    if isinstance(payload.get("unresolved_links"), list):
        ok, message, document_id, raw_response = _api_save_value(
            session=session,
            collection_name=collection,
            query=query,
            field_path="unresolved_links",
            value=payload.get("unresolved_links"),
        )
        if document_id:
            latest_document_id = document_id
        if not ok:
            failures.append(message)

    if isinstance(payload.get("unresolved_link_details"), list):
        ok, message, document_id, raw_response = _api_save_value(
            session=session,
            collection_name=collection,
            query=query,
            field_path="unresolved_link_details",
            value=payload.get("unresolved_link_details"),
        )
        if document_id:
            latest_document_id = document_id
        if not ok:
            failures.append(message)

    if payload.get("warning"):
        ok, message, document_id, raw_response = _api_save_value(
            session=session,
            collection_name=collection,
            query=query,
            field_path="warning",
            value=payload.get("warning"),
        )
        if document_id:
            latest_document_id = document_id
        if not ok:
            failures.append(message)

    if failures:
        return (
            False,
            "API preset save had field_path write failures: " + " | ".join(failures[:12]),
            latest_document_id,
        )

    saved_doc, saved_id, verify_response = _query_preset_api_document(
        session=session,
        collection_name=collection,
        preset_name=preset_name,
    )

    reconstructed = _reconstruct_preset_payload_from_api_document(saved_doc)
    if not reconstructed or not isinstance(reconstructed.get("fields"), list):
        return (
            False,
            (
                "API preset save completed, but verification could not reconstruct fields. "
                f"verify_response={verify_response}"
            ),
            saved_id or latest_document_id,
        )

    if len(reconstructed.get("fields") or []) != len(fields):
        return (
            False,
            (
                f"API preset save verification field count mismatch. "
                f"expected={len(fields)} actual={len(reconstructed.get('fields') or [])}"
            ),
            saved_id or latest_document_id,
        )

    return (
        True,
        f"Saved preset {preset_name!r} to ZMongo API using dot-path field storage.",
        saved_id or latest_document_id,
    )


def _load_preset_api(
    *,
    session: Any,
    collection_name: str,
    preset_name: str,
    document_id: str = "",
) -> tuple[dict[str, Any], str, str]:
    if session is None:
        return {}, "No ZMongo API session provided.", ""

    collection = str(collection_name or DEFAULT_COLLECTION_NAME).strip()
    cleaned_name = str(preset_name or "").strip()
    cleaned_document_id = str(document_id or "").strip()

    if not collection:
        return {}, "collection_name is required.", ""

    if cleaned_document_id:
        response = _api_get_document_direct(
            session=session,
            collection_name=collection,
            document_id=cleaned_document_id,
        )

        response = _normalize_bson_extended_json(response)
        payload = _extract_preset_payload_from_document(response)

        if payload:
            return (
                payload,
                f"Loaded preset from real backend document_id={cleaned_document_id}.",
                cleaned_document_id,
            )

        # Fallback for older session implementations.
        get_doc = getattr(session, "get_doc", None)
        if callable(get_doc):
            try:
                fallback_response = get_doc(
                    collection=collection,
                    document_id=cleaned_document_id,
                    cache=False,
                )
                fallback_response = _normalize_bson_extended_json(fallback_response)
                payload = _extract_preset_payload_from_document(fallback_response)
                if payload:
                    return (
                        payload,
                        f"Loaded preset from session.get_doc fallback document_id={cleaned_document_id}.",
                        cleaned_document_id,
                    )
            except Exception:
                pass

        return (
            {},
            "API get document returned no reconstructable preset data. "
            + _json_dumps(response),
            cleaned_document_id,
        )

    if not cleaned_name:
        return {}, "preset_name or document_id is required.", ""

    document, selected_id, response = _query_preset_api_document(
        session=session,
        collection_name=collection,
        preset_name=cleaned_name,
    )

    if not document:
        return (
            {},
            "No reconstructable preset document was found by real backend query. "
            + _json_dumps(
                {
                    "collection": collection,
                    "preset_name": cleaned_name,
                    "response": response,
                }
            ),
            "",
        )

    payload = _extract_preset_payload_from_document(document)

    if not payload:
        return (
            {},
            "Preset document was found, but could not be reconstructed. "
            + _json_dumps(
                {
                    "collection": collection,
                    "preset_name": cleaned_name,
                    "document_id": selected_id,
                    "document_keys": sorted(str(key) for key in document.keys()),
                }
            ),
            selected_id,
        )

    return (
        payload,
        f"Loaded preset {cleaned_name!r} from real backend query; document_id={selected_id}.",
        selected_id,
    )

# -----------------------------------------------------------------------------
# ComfyUI node inspection helpers
# -----------------------------------------------------------------------------

def _load_comfy_node_class_mappings() -> Dict[str, Any]:
    try:
        comfy_nodes = importlib.import_module("nodes")
        mappings = getattr(comfy_nodes, "NODE_CLASS_MAPPINGS", {})
        return mappings if isinstance(mappings, dict) else {}
    except Exception:
        return {}


def _input_groups_from_schema(input_types: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
    return {
        "required": dict(input_types.get("required") or {}),
        "optional": dict(input_types.get("optional") or {}),
        "hidden": dict(input_types.get("hidden") or {}),
    }


def _is_combo_declaration(type_decl: Any) -> bool:
    return isinstance(type_decl, (list, tuple)) and not isinstance(type_decl, str)


def _serialize_options(options: Any, limit: int = 1000) -> List[Any]:
    if options is None:
        return []

    if isinstance(options, (list, tuple)):
        if len(options) == 1 and isinstance(options[0], (list, tuple)):
            options = options[0]

        out: List[Any] = []
        for item in list(options)[:limit]:
            try:
                json.dumps(item)
                out.append(item)
            except Exception:
                out.append(str(item))
        return out

    return []


def _normalize_input_declaration(
    input_name: str,
    declaration: Any,
    current_value: Any,
    source_node_class: str,
) -> Dict[str, Any]:
    declared_type = "UNKNOWN"
    widget_kind = "unknown"
    options_snapshot: List[Any] = []
    constraints: Dict[str, Any] = {}

    raw_type = None
    raw_options: Dict[str, Any] = {}

    if isinstance(declaration, tuple):
        raw_type = declaration[0] if len(declaration) >= 1 else None
        if len(declaration) >= 2 and isinstance(declaration[1], dict):
            raw_options = declaration[1]
    elif isinstance(declaration, list):
        raw_type = declaration
    elif isinstance(declaration, str):
        raw_type = declaration
    else:
        raw_type = declaration

    if _is_combo_declaration(raw_type):
        declared_type = "COMBO"
        widget_kind = "dropdown"
        options_snapshot = _serialize_options(raw_type)
    else:
        declared_type = str(raw_type)
        if declared_type in {"INT", "FLOAT"}:
            widget_kind = "number"
        elif declared_type == "BOOLEAN":
            widget_kind = "boolean"
        elif declared_type == "STRING":
            widget_kind = "text"
        else:
            widget_kind = "socket"

    if isinstance(raw_options, dict):
        for key in (
            "default",
            "min",
            "max",
            "step",
            "round",
            "multiline",
            "dynamicPrompts",
            "control_after_generate",
            "advanced",
            "forceInput",
            "defaultInput",
            "tooltip",
            "image_upload",
            "audio_upload",
            "video_upload",
            "file_upload",
            "image_folder",
            "remote",
            "multi_select",
            "options",
        ):
            if key in raw_options:
                value = raw_options.get(key)
                try:
                    json.dumps(value)
                    constraints[key] = value
                except Exception:
                    constraints[key] = str(value)

        if declared_type != "COMBO" and "options" in raw_options:
            declared_type = "COMBO"
            widget_kind = "dropdown"
            options_snapshot = _serialize_options(raw_options.get("options"))

    options_source = None
    if source_node_class in {"KSampler", "KSamplerAdvanced"} and input_name == "sampler_name":
        options_source = "comfy.samplers.KSampler.SAMPLERS"
    elif source_node_class in {"KSampler", "KSamplerAdvanced"} and input_name == "scheduler":
        options_source = "comfy.samplers.KSampler.SCHEDULERS"
    elif input_name.endswith("_name"):
        options_source = "runtime_dropdown"

    is_link = _is_link_value(current_value)

    return {
        "input_name": input_name,
        "declared_type": declared_type,
        "widget_kind": widget_kind,
        "value": current_value,
        "value_python_type": type(current_value).__name__,
        "constraints": constraints,
        "options_source": options_source,
        "options_snapshot": options_snapshot,
        "source_node_class": source_node_class,
        "is_link": is_link,
        "link_target": current_value if is_link else None,
    }


def _extract_prompt_node(prompt: Any, target_node_id: str) -> Tuple[Optional[Dict[str, Any]], str]:
    if not isinstance(prompt, dict):
        return None, "Hidden PROMPT was not available or was not a dictionary."

    target_node_id = _normalize_node_id(target_node_id)

    if target_node_id in prompt:
        node = prompt.get(target_node_id)
        return node if isinstance(node, dict) else None, ""

    for key, value in prompt.items():
        if _normalize_node_id(key) == target_node_id:
            return value if isinstance(value, dict) else None, ""

    available = ", ".join(str(k) for k in sorted(prompt.keys(), key=lambda x: str(x))[:50])
    return None, f"Node id {target_node_id!r} was not found in prompt. Available ids: {available}"


def _prompt_node_by_id(prompt: Any, node_id: Any) -> Dict[str, Any]:
    if not isinstance(prompt, dict):
        return {}

    normalized = _normalize_node_id(node_id)

    if normalized in prompt and isinstance(prompt[normalized], dict):
        return prompt[normalized]

    for key, value in prompt.items():
        if _normalize_node_id(key) == normalized and isinstance(value, dict):
            return value

    return {}


# -----------------------------------------------------------------------------
# Dynamic-loader link resolution
# -----------------------------------------------------------------------------

def _load_preset_from_prompt_loader_node(
    *,
    loader_node: Dict[str, Any],
    session: Any = None,
) -> Dict[str, Any]:
    inputs = loader_node.get("inputs") or {}
    if not isinstance(inputs, dict):
        return {}

    preset_name = str(inputs.get("preset_name") or "").strip()
    collection_name = str(inputs.get("collection_name") or DEFAULT_COLLECTION_NAME).strip()
    storage_backend = str(inputs.get("storage_backend") or STORAGE_AUTO).strip()

    if not preset_name:
        return {}

    if storage_backend in {STORAGE_AUTO, STORAGE_LOCAL_FILE}:
        local_payload, _, _ = _load_preset_local(collection_name, preset_name)
        if local_payload:
            return local_payload

    if session is not None and storage_backend in {STORAGE_AUTO, STORAGE_ZMONGO_API}:
        api_payload, _, _ = _load_preset_api(
            session=session,
            collection_name=collection_name,
            preset_name=preset_name,
        )
        if api_payload:
            return api_payload

    return {}


def _load_preset_from_prompt_save_node(
    *,
    save_node: Dict[str, Any],
    session: Any = None,
) -> Dict[str, Any]:
    inputs = save_node.get("inputs") or {}
    if not isinstance(inputs, dict):
        return {}

    preset_name = str(inputs.get("preset_name") or "").strip()
    collection_name = str(inputs.get("collection_name") or DEFAULT_COLLECTION_NAME).strip()
    storage_backend = str(inputs.get("storage_backend") or STORAGE_AUTO).strip()

    if not preset_name:
        return {}

    if storage_backend in {STORAGE_AUTO, STORAGE_LOCAL_FILE}:
        local_payload, _, _ = _load_preset_local(collection_name, preset_name)
        if local_payload:
            return local_payload

    if session is not None and storage_backend in {STORAGE_AUTO, STORAGE_ZMONGO_API}:
        api_payload, _, _ = _load_preset_api(
            session=session,
            collection_name=collection_name,
            preset_name=preset_name,
        )
        if api_payload:
            return api_payload

    return {}


def _extract_dynamic_preset_payload_from_prompt_node(
    *,
    prompt: Any,
    dynamic_node: Dict[str, Any],
    session: Any = None,
) -> Dict[str, Any]:
    inputs = dynamic_node.get("inputs") or {}
    if not isinstance(inputs, dict):
        return {}

    preset_json_input = inputs.get("preset_json")

    direct_payload = _parse_json(preset_json_input)
    if direct_payload.get("fields"):
        return direct_payload

    source_node_id, _source_output_index = _parse_link_value(preset_json_input)
    if not source_node_id:
        return {}

    source_node = _prompt_node_by_id(prompt, source_node_id)
    source_class = str(source_node.get("class_type") or "")

    if source_class == LOAD_PRESET_NODE_CLASS:
        return _load_preset_from_prompt_loader_node(
            loader_node=source_node,
            session=session,
        )

    if source_class == SAVE_PRESET_NODE_CLASS:
        return _load_preset_from_prompt_save_node(
            save_node=source_node,
            session=session,
        )

    return {}


def _link_source_node_info(
    *,
    prompt: Any,
    link_value: Any,
) -> tuple[str, Optional[int], str, Dict[str, Any]]:
    """
    Return source node information for a ComfyUI link value.

    ComfyUI prompt inputs represent links as [source_node_id, output_index].
    This helper keeps link detection centralized so preset saving can distinguish
    ordinary graph wiring from ZMongo dynamic preset value links.
    """
    source_node_id, output_index = _parse_link_value(link_value)
    if not source_node_id or output_index is None:
        return "", None, "", {}

    source_node = _prompt_node_by_id(prompt, source_node_id)
    if not source_node:
        return source_node_id, output_index, "", {}

    source_class = str(source_node.get("class_type") or "")
    return source_node_id, output_index, source_class, source_node


def _resolve_dynamic_loader_field_from_link(
    *,
    prompt: Any,
    link_value: Any,
    target_input_name: str,
    session: Any = None,
) -> Dict[str, Any]:
    source_node_id, output_index, source_class, source_node = _link_source_node_info(
        prompt=prompt,
        link_value=link_value,
    )

    if not source_node_id or output_index is None or not source_node:
        return {}

    if source_class != DYNAMIC_PRESET_NODE_CLASS:
        return {}

    preset_payload = _extract_dynamic_preset_payload_from_prompt_node(
        prompt=prompt,
        dynamic_node=source_node,
        session=session,
    )

    fields = preset_payload.get("fields") or []
    if not isinstance(fields, list):
        return {}

    target_name = str(target_input_name or "").strip()

    # First choice: exact output-index match AND exact field-name match.
    # This is the only safe index-based resolution because dynamic outputs are
    # positional but target inputs are named.
    if 0 <= output_index < len(fields) and isinstance(fields[output_index], dict):
        candidate = fields[output_index]
        candidate_name = str(candidate.get("input_name") or "").strip()
        if candidate_name == target_name:
            return candidate

    # Second choice: exact field-name match anywhere in the loaded preset.
    # This survives field-order changes while still avoiding wrong-value saves.
    for field in fields:
        if not isinstance(field, dict):
            continue
        field_name = str(field.get("input_name") or "").strip()
        if field_name == target_name:
            return field

    # Deliberately do not fall back to returning fields[output_index].
    # Saving an indexed field with a mismatched name silently stores the wrong
    # preset value and is harder to diagnose than an unresolved-link warning.
    return {}


def _merge_resolved_link_field(
    *,
    base_field: Dict[str, Any],
    resolved_field: Dict[str, Any],
    target_input_name: str,
    link_value: Any,
) -> Dict[str, Any]:
    merged = dict(base_field)

    merged["input_name"] = target_input_name
    merged["value"] = resolved_field.get("value")
    merged["value_python_type"] = type(resolved_field.get("value")).__name__
    merged["is_link"] = False
    merged["link_target"] = None
    merged["resolved_from_link"] = True
    merged["resolved_link_target"] = link_value

    if resolved_field.get("declared_type"):
        merged["declared_type"] = resolved_field.get("declared_type")
    if resolved_field.get("widget_kind"):
        merged["widget_kind"] = resolved_field.get("widget_kind")
    if isinstance(resolved_field.get("options_snapshot"), list):
        merged["options_snapshot"] = resolved_field.get("options_snapshot")
    if resolved_field.get("options_source"):
        merged["options_source"] = resolved_field.get("options_source")

    return merged


def _build_preset_payload(
    *,
    preset_name: str,
    collection_name: str,
    target_node_id: str,
    prompt: Any,
    save_mode: str,
    session: Any = None,
) -> Tuple[Dict[str, Any], str]:
    target_node_id = _normalize_node_id(target_node_id)
    prompt_node, error = _extract_prompt_node(prompt, target_node_id)

    if error:
        return {}, error

    if not prompt_node:
        return {}, f"Node id {target_node_id!r} was found but node data was empty."

    source_node_class = str(prompt_node.get("class_type") or "")
    current_inputs = prompt_node.get("inputs") or {}

    if not source_node_class:
        return {}, f"Node id {target_node_id!r} has no class_type."

    if not isinstance(current_inputs, dict):
        return {}, f"Node id {target_node_id!r} has no usable inputs dictionary."

    node_class_mappings = _load_comfy_node_class_mappings()
    node_cls = node_class_mappings.get(source_node_class)

    if node_cls is None:
        return {}, f"Could not find ComfyUI node class for {source_node_class!r}."

    try:
        input_types = node_cls.INPUT_TYPES()
    except Exception as exc:
        return {}, f"Could not inspect INPUT_TYPES for {source_node_class!r}: {exc}"

    input_groups = _input_groups_from_schema(input_types)
    declarations: Dict[str, Any] = {}
    declarations.update(input_groups["required"])
    declarations.update(input_groups["optional"])

    fields: List[Dict[str, Any]] = []
    unresolved_links: List[str] = []
    unresolved_link_details: List[Dict[str, Any]] = []

    for input_name, current_value in current_inputs.items():
        if input_name not in declarations:
            continue

        declaration = declarations[input_name]
        base_field = _normalize_input_declaration(
            input_name=input_name,
            declaration=declaration,
            current_value=current_value,
            source_node_class=source_node_class,
        )

        is_link = bool(base_field.get("is_link"))

        if is_link:
            source_node_id, output_index, link_source_class, _source_node = _link_source_node_info(
                prompt=prompt,
                link_value=current_value,
            )
            is_dynamic_loader_link = link_source_class == DYNAMIC_PRESET_NODE_CLASS

            resolved_field = _resolve_dynamic_loader_field_from_link(
                prompt=prompt,
                link_value=current_value,
                target_input_name=input_name,
                session=session,
            )

            if resolved_field:
                merged_field = _merge_resolved_link_field(
                    base_field=base_field,
                    resolved_field=resolved_field,
                    target_input_name=input_name,
                    link_value=current_value,
                )
                merged_field["resolved_source_node_id"] = source_node_id
                merged_field["resolved_source_output_index"] = output_index
                merged_field["resolved_source_node_class"] = link_source_class
                merged_field["resolved_source_input_name"] = str(resolved_field.get("input_name") or "")

                if save_mode in {"widgets_only", "widgets_and_inputs"}:
                    fields.append(merged_field)
                elif save_mode == "inputs_only":
                    fields.append(base_field)

                continue

            unresolved_links.append(input_name)
            unresolved_link_details.append(
                {
                    "input_name": input_name,
                    "link_value": current_value,
                    "source_node_id": source_node_id,
                    "source_output_index": output_index,
                    "source_node_class": link_source_class,
                    "dynamic_loader_link": is_dynamic_loader_link,
                    "reason": (
                        "Dynamic preset link could not be resolved to an underlying field value."
                        if is_dynamic_loader_link
                        else "Ordinary ComfyUI runtime link; no serializable widget value available."
                    ),
                }
            )

            # A dynamic preset output is a value-provider link.  If resolution
            # fails, do not store the ComfyUI graph link as the preset value in
            # the normal save modes; that is the bug this branch prevents.
            if is_dynamic_loader_link and save_mode in {"widgets_only", "widgets_and_inputs"}:
                continue

            if save_mode == "widgets_only":
                continue

            if save_mode in {"inputs_only", "widgets_and_inputs"}:
                fields.append(base_field)

            continue

        if save_mode == "inputs_only":
            continue

        fields.append(base_field)

    payload = {
        "schema_kind": PRESET_KIND,
        "schema_version": PRESET_SCHEMA_VERSION,
        "preset_name": preset_name,
        "collection_name": collection_name,
        "source_node_id": target_node_id,
        "source_node_class": source_node_class,
        "save_mode": save_mode,
        "created_at_unix": time.time(),
        "field_count": len(fields),
        "fields": fields,
    }

    if unresolved_links:
        payload["unresolved_links"] = unresolved_links
        payload["unresolved_link_details"] = unresolved_link_details
        payload["warning"] = (
            "Some linked inputs could not be resolved. Dynamic preset output "
            "links are not saved as raw ComfyUI link arrays in widgets_only or "
            "widgets_and_inputs mode; they must resolve to underlying preset values."
        )

    return payload, ""


# -----------------------------------------------------------------------------
# Hydration helpers
# -----------------------------------------------------------------------------

def _coerce_field_value(field: dict[str, Any], strict_combo_validation: bool = True) -> Any:
    declared_type = str(field.get("declared_type") or "")
    value = _normalize_bson_extended_json(field.get("value"))

    if declared_type == "INT":
        try:
            return int(value)
        except Exception:
            return 0

    if declared_type == "FLOAT":
        try:
            return float(value)
        except Exception:
            return 0.0

    if declared_type == "BOOLEAN":
        if isinstance(value, str):
            return value.strip().lower() in {
                "true",
                "1",
                "yes",
                "on",
                "enable",
                "enabled",
            }
        return bool(value)

    if declared_type == "COMBO":
        value = "" if value is None else str(value)

        if strict_combo_validation:
            options = field.get("options_snapshot") or []
            if options and value not in options:
                print(
                    f"[ComfyUI-ZMongo] Warning: combo value {value!r} "
                    f"for {field.get('input_name')!r} is not in saved options."
                )

        return value

    if declared_type == "STRING":
        return "" if value is None else str(value)

    return value


# -----------------------------------------------------------------------------
# Nodes
# -----------------------------------------------------------------------------

class ZMongoSavePresetByNodeID:
    @classmethod
    def INPUT_TYPES(cls):
        return {
            "required": {
                "preset_name": (
                    "STRING",
                    {
                        "default": "main_sampler",
                        "tooltip": "Name used to save this node preset.",
                    },
                ),
                "target_node_id": (
                    "STRING",
                    {
                        "default": "1",
                        "tooltip": "The ComfyUI node id number to inspect and save.",
                    },
                ),
                "collection_name": (
                    "STRING",
                    {
                        "default": DEFAULT_COLLECTION_NAME,
                        "tooltip": "Local preset folder or ZMongo collection name.",
                    },
                ),
                "storage_backend": (
                    [STORAGE_AUTO, STORAGE_LOCAL_FILE, STORAGE_ZMONGO_API],
                    {
                        "default": STORAGE_AUTO,
                        "tooltip": "auto uses ZMongo API when session is connected, otherwise local file.",
                    },
                ),
                "overwrite": (
                    "BOOLEAN",
                    {
                        "default": True,
                        "tooltip": "Replace an existing preset with the same name.",
                    },
                ),
                "save_mode": (
                    ["widgets_only", "inputs_only", "widgets_and_inputs"],
                    {
                        "default": "widgets_and_inputs",
                        "tooltip": "widgets_and_inputs is safest when the target node is already connected to dynamic preset outputs.",
                    },
                ),
            },
            "optional": {
                "session": (
                    "ZMONGO_API_SESSION",
                    {
                        "forceInput": True,
                        "tooltip": "Optional API session. Required for zmongo_api storage.",
                    },
                ),
            },
            "hidden": {
                "prompt": "PROMPT",
                "extra_pnginfo": "EXTRA_PNGINFO",
                "unique_id": "UNIQUE_ID",
            },
        }

    RETURN_TYPES = ("STRING", "STRING", "INT", "STRING", "STRING", "STRING")
    RETURN_NAMES = (
        "preset_json",
        "preset_name",
        "saved_count",
        "storage_backend_used",
        "document_id",
        "status",
    )
    FUNCTION = "save_preset"
    CATEGORY = "ZMongo/04 Presets"

    def save_preset(
        self,
        preset_name: str,
        target_node_id: str,
        collection_name: str = DEFAULT_COLLECTION_NAME,
        storage_backend: str = STORAGE_AUTO,
        overwrite: bool = True,
        save_mode: str = "widgets_and_inputs",
        session: Any = None,
        prompt: Optional[Dict[str, Any]] = None,
        extra_pnginfo: Optional[Dict[str, Any]] = None,
        unique_id: Optional[str] = None,
    ):
        payload, error = _build_preset_payload(
            preset_name=preset_name,
            collection_name=collection_name,
            target_node_id=target_node_id,
            prompt=prompt,
            save_mode=save_mode,
            session=session,
        )

        if error:
            result = ("{}", preset_name, 0, "", "", f"ERROR: {error}")
            return _node_result(result, "{}")

        backend_used = _resolve_storage_backend(storage_backend, session=session)
        saved_count = int(payload.get("field_count") or 0)
        preset_json = _json_dumps(payload)

        if backend_used == STORAGE_ZMONGO_API:
            ok, message, document_id = _save_preset_api(
                session=session,
                collection_name=collection_name,
                payload=payload,
                overwrite=overwrite,
            )

            if not ok:
                result = (
                    preset_json,
                    preset_name,
                    0,
                    backend_used,
                    document_id,
                    f"ERROR: {message}",
                )
                return _node_result(result, preset_json)

            result = (
                preset_json,
                preset_name,
                saved_count,
                backend_used,
                document_id,
                message,
            )
            return _node_result(result, preset_json)

        ok, message, path = _save_preset_local(payload, overwrite=overwrite)
        if not ok:
            result = (
                preset_json,
                preset_name,
                0,
                backend_used,
                "",
                f"ERROR: {message}",
            )
            return _node_result(result, preset_json)

        result = (
            preset_json,
            preset_name,
            saved_count,
            backend_used,
            "",
            message,
        )
        return _node_result(result, preset_json)


class ZMongoLoadPreset:
    """
    Load a preset by name, document id, or already-loaded ZMongo document JSON.
    """
    @classmethod
    def INPUT_TYPES(cls):
        return {
            "required": {
                "preset_name": (
                    "STRING",
                    {
                        "default": "main_sampler",
                        "tooltip": "Preset name to load when document_json/document_id is not supplied.",
                    },
                ),
                "collection_name": (
                    "STRING",
                    {
                        "default": DEFAULT_COLLECTION_NAME,
                        "tooltip": "Local preset folder or ZMongo collection name.",
                    },
                ),
                "storage_backend": (
                    [STORAGE_AUTO, STORAGE_LOCAL_FILE, STORAGE_ZMONGO_API],
                    {
                        "default": STORAGE_AUTO,
                        "tooltip": "auto uses ZMongo API when session is connected, otherwise local file.",
                    },
                ),
            },
            "optional": {
                "session": (
                    "ZMONGO_API_SESSION",
                    {
                        "forceInput": True,
                        "tooltip": "Optional API session. Required for zmongo_api storage.",
                    },
                ),
                "document_id": (
                    "STRING",
                    {
                        "default": "",
                        "forceInput": True,
                        "tooltip": "Optional direct ZMongo document id. If supplied, this loads that document instead of querying by preset_name.",
                    },
                ),
                "document_json": (
                    "STRING",
                    {
                        "default": "",
                        "multiline": True,
                        "forceInput": True,
                        "tooltip": "Optional JSON from 03 Get Doc. If supplied, this is reconstructed directly into preset_json.",
                    },
                ),
            },
        }

    RETURN_TYPES = ("STRING", "STRING", "STRING", "STRING", "INT", "STRING", "STRING", "STRING")
    RETURN_NAMES = (
        "preset_json",
        "preset_name",
        "source_node_id",
        "source_node_class",
        "field_count",
        "storage_backend_used",
        "document_id",
        "status",
    )
    FUNCTION = "load_preset"
    CATEGORY = "ZMongo/04 Presets"

    def _return_payload(
        self,
        *,
        payload: dict[str, Any],
        preset_name: str,
        backend_used: str,
        document_id: str,
        message: str,
    ):
        payload = _normalize_bson_extended_json(payload)

        fields = payload.get("fields") if isinstance(payload, dict) else None
        if not isinstance(fields, list):
            fields = []

        payload["field_count"] = len(fields)

        preset_json = _json_dumps(payload)

        result = (
            preset_json,
            str(payload.get("preset_name") or preset_name or ""),
            str(payload.get("source_node_id") or ""),
            str(payload.get("source_node_class") or ""),
            int(payload.get("field_count") or 0),
            backend_used,
            document_id or "",
            message,
        )
        return _node_result(result, preset_json)

    def _return_error(
        self,
        *,
        preset_name: str,
        backend_used: str,
        document_id: str = "",
        message: str,
    ):
        result = (
            "{}",
            preset_name or "",
            "",
            "",
            0,
            backend_used,
            document_id or "",
            f"ERROR: {message}",
        )
        return _node_result(result, "{}")

    def load_preset(
        self,
        preset_name: str,
        collection_name: str = DEFAULT_COLLECTION_NAME,
        storage_backend: str = STORAGE_AUTO,
        session: Any = None,
        document_id: str = "",
        document_json: str = "",
    ):
        backend_used = _resolve_storage_backend(storage_backend, session=session)
        clean_preset_name = str(preset_name or "").strip()
        clean_document_id = str(document_id or "").strip()

        # --------------------------------------------------------------
        # 1. Highest priority: already-loaded document JSON from 03 Get Doc.
        # --------------------------------------------------------------
        payload, parsed_document_id, message = _extract_preset_payload_from_json_text(document_json)

        if payload:
            return self._return_payload(
                payload=payload,
                preset_name=clean_preset_name,
                backend_used="document_json",
                document_id=parsed_document_id or clean_document_id,
                message=message,
            )

        # --------------------------------------------------------------
        # 2. API backend: load by document_id if supplied, otherwise query by preset_name.
        # --------------------------------------------------------------
        if backend_used == STORAGE_ZMONGO_API:
            payload, message, loaded_document_id = _load_preset_api(
                session=session,
                collection_name=collection_name,
                preset_name=clean_preset_name,
                document_id=clean_document_id,
            )

            if not payload:
                return self._return_error(
                    preset_name=clean_preset_name,
                    backend_used=backend_used,
                    document_id=loaded_document_id or clean_document_id,
                    message=message,
                )

            return self._return_payload(
                payload=payload,
                preset_name=clean_preset_name,
                backend_used=backend_used,
                document_id=loaded_document_id or clean_document_id,
                message=message,
            )

        # --------------------------------------------------------------
        # 3. Local backend.
        # --------------------------------------------------------------
        payload, message, _path = _load_preset_local(collection_name, clean_preset_name)

        if not payload:
            return self._return_error(
                preset_name=clean_preset_name,
                backend_used=backend_used,
                message=message,
            )

        payload = _extract_preset_payload_from_document(payload) or payload

        return self._return_payload(
            payload=payload,
            preset_name=clean_preset_name,
            backend_used=backend_used,
            document_id="",
            message=message,
        )


class ZMongoDynamicPresetOutputs:
    MAX_OUTPUTS = MAX_DYNAMIC_OUTPUTS

    @classmethod
    def INPUT_TYPES(cls):
        return {
            "required": {
                "preset_json": (
                    "STRING",
                    {
                        "default": "{}",
                        "multiline": True,
                        "tooltip": "Preset JSON from ZMongo Load Preset.",
                    },
                ),
                "strict_combo_validation": (
                    "BOOLEAN",
                    {
                        "default": True,
                        "tooltip": "Validate dropdown values against the saved option snapshot.",
                    },
                ),
            }
        }

    RETURN_TYPES = tuple(["*"] * MAX_DYNAMIC_OUTPUTS)
    RETURN_NAMES = tuple([f"out_{i:02d}" for i in range(MAX_DYNAMIC_OUTPUTS)])
    FUNCTION = "hydrate"
    CATEGORY = "ZMongo/04 Presets"

    def hydrate(
        self,
        preset_json: str,
        strict_combo_validation: bool = True,
    ):
        payload = _parse_json(preset_json)
        output_values = [None] * self.MAX_OUTPUTS

        if not payload:
            return tuple(output_values)

        fields = payload.get("fields") or []
        if not isinstance(fields, list):
            return tuple(output_values)

        for index, field in enumerate(fields[: self.MAX_OUTPUTS]):
            if not isinstance(field, dict):
                continue

            output_values[index] = _coerce_field_value(
                field,
                strict_combo_validation=bool(strict_combo_validation),
            )

        return tuple(output_values)


class ZMongoPresetDebugInfo:
    @classmethod
    def INPUT_TYPES(cls):
        return {
            "required": {
                "preset_json": (
                    "STRING",
                    {
                        "default": "{}",
                        "multiline": True,
                    },
                ),
            }
        }

    RETURN_TYPES = ("STRING", "STRING", "STRING", "INT", "STRING")
    RETURN_NAMES = ("preset_name", "source_node_id", "source_node_class", "field_count", "summary_json")
    FUNCTION = "debug"
    CATEGORY = "ZMongo/04 Presets"

    def debug(self, preset_json: str):
        payload = _parse_json(preset_json)

        if not payload:
            return ("", "", "", 0, _json_dumps({"success": False, "message": "Invalid preset JSON."}))

        fields = payload.get("fields") or []
        field_names = []

        if isinstance(fields, list):
            for field in fields:
                if isinstance(field, dict):
                    field_names.append(
                        {
                            "input_name": field.get("input_name"),
                            "declared_type": field.get("declared_type"),
                            "widget_kind": field.get("widget_kind"),
                            "value": field.get("value"),
                            "resolved_from_link": field.get("resolved_from_link", False),
                            "resolved_link_target": field.get("resolved_link_target"),
                            "resolved_source_node_id": field.get("resolved_source_node_id"),
                            "resolved_source_output_index": field.get("resolved_source_output_index"),
                            "resolved_source_node_class": field.get("resolved_source_node_class"),
                            "resolved_source_input_name": field.get("resolved_source_input_name"),
                            "is_link": field.get("is_link", False),
                            "link_target": field.get("link_target"),
                        }
                    )

        summary = {
            "success": True,
            "preset_name": payload.get("preset_name"),
            "source_node_id": payload.get("source_node_id"),
            "source_node_class": payload.get("source_node_class"),
            "field_count": len(field_names),
            "fields": field_names,
            "unresolved_links": payload.get("unresolved_links", []),
            "unresolved_link_details": payload.get("unresolved_link_details", []),
            "warning": payload.get("warning", ""),
        }

        return (
            str(payload.get("preset_name") or ""),
            str(payload.get("source_node_id") or ""),
            str(payload.get("source_node_class") or ""),
            len(field_names),
            _json_dumps(summary),
        )


# -----------------------------------------------------------------------------
# ComfyUI mappings
# -----------------------------------------------------------------------------

NODE_CLASS_MAPPINGS = {
    "ZMongoSavePresetByNodeID": ZMongoSavePresetByNodeID,
    "ZMongoLoadPreset": ZMongoLoadPreset,
    "ZMongoDynamicPresetOutputs": ZMongoDynamicPresetOutputs,
    "ZMongoPresetDebugInfo": ZMongoPresetDebugInfo,
}

NODE_DISPLAY_NAME_MAPPINGS = {
    "ZMongoSavePresetByNodeID": "04 Save Preset By Node ID",
    "ZMongoLoadPreset": "04 Load Preset",
    "ZMongoDynamicPresetOutputs": "04 Dynamic Preset Outputs",
    "ZMongoPresetDebugInfo": "04 Preset Debug Info",
}

__all__ = [
    "NODE_CLASS_MAPPINGS",
    "NODE_DISPLAY_NAME_MAPPINGS",
]