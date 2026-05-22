from __future__ import annotations

import inspect
import json
import os
import time
import uuid
from pathlib import Path
from typing import Any, Dict, Optional, Tuple

import requests
from dotenv import load_dotenv

# -----------------------------------------------------------------------------
# Environment
# -----------------------------------------------------------------------------

ENV_PATH1 = Path.home() / ".resources" / ".env"
ENV_PATH2 = Path.home() / ".resources" / ".secrets"
load_dotenv(ENV_PATH1)
load_dotenv(ENV_PATH2)

DEFAULT_BASE_URL = os.getenv("ZTAROT_BASE_URL", os.getenv("BPA_BASE_URL", "https://businessprocessapplications.com")).rstrip("/")
DEFAULT_TIMEOUT = int(os.getenv("ZTAROT_TIMEOUT_SECONDS", os.getenv("BPA_TIMEOUT_SECONDS", "30")))
DEFAULT_GEMINI_PREFIX = os.getenv("GEMINI_API_PREFIX", "/gemini").strip().rstrip("/") or "/gemini"


# -----------------------------------------------------------------------------
# Generic helpers compatible with the existing ComfyUI-ZMongo node style
# -----------------------------------------------------------------------------


def _dirty_token(*parts: Any) -> str:
    prefix = ":".join(str(part) for part in parts if part is not None)
    return f"{prefix}:{time.time_ns()}:{uuid.uuid4().hex}"


def _json_text(value: Any) -> str:
    return json.dumps(value, indent=2, ensure_ascii=False, default=str)


def _error_payload(message: str, *, status_code: int = 0, data: Optional[dict[str, Any]] = None) -> dict[str, Any]:
    return {
        "success": False,
        "message": message,
        "data": data or {},
        "error": {"msg": message},
        "status_code": status_code,
    }


def _success_payload(message: str, data: Optional[dict[str, Any]] = None, *, status_code: int = 200) -> dict[str, Any]:
    return {
        "success": True,
        "message": message,
        "data": data or {},
        "error": None,
        "status_code": status_code,
    }


def _ensure_payload_dict(payload: Any) -> dict[str, Any]:
    if isinstance(payload, dict):
        result = dict(payload)
    else:
        result = {
            "success": False,
            "message": "Payload was not a JSON object.",
            "data": payload,
            "error": {"msg": f"Unexpected payload type: {type(payload).__name__}"},
        }
    result.setdefault("success", False)
    result.setdefault("message", "OK" if result.get("success") else "")
    result.setdefault("data", {})
    result.setdefault("error", None)
    result.setdefault("status_code", 0)
    return result


def _normalize_base_url(raw_base_url: str) -> str:
    base = (raw_base_url or DEFAULT_BASE_URL).strip().rstrip("/")
    suffixes = (
        "/api/comfy-zmongo",
        "/comfy_zmongo",
        "/comfy-zmongo",
        "/gemini",
        "/user/manager/api",
        "/user/manager",
        "/user/login",
        "/user/dashboard",
        "/user/settings",
        "/user/profile",
        "/user",
    )
    changed = True
    while changed:
        changed = False
        for suffix in suffixes:
            if base.endswith(suffix):
                base = base[: -len(suffix)].rstrip("/")
                changed = True
    return base or DEFAULT_BASE_URL


def _clean_prefix(value: str, default: str = DEFAULT_GEMINI_PREFIX) -> str:
    cleaned = (value or default).strip().rstrip("/")
    if not cleaned.startswith("/"):
        cleaned = f"/{cleaned}"
    return cleaned or default


def _parse_json_object(value: str, field_name: str = "json") -> dict[str, Any]:
    text = (value or "").strip()
    if not text:
        return {}
    parsed = json.loads(text)
    if not isinstance(parsed, dict):
        raise ValueError(f"{field_name} must be a JSON object.")
    return parsed


def _parse_any_json(value: str, parse_json: bool = True) -> Any:
    if not parse_json:
        return value
    text = (value or "").strip()
    if not text:
        return ""
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        return value


def _as_bool(value: Any, default: bool = False) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes", "y", "on"}
    return default


def _safe_get_by_path(obj: Any, path: str, default: Any = None) -> Any:
    if not path:
        return obj
    current = obj
    for part in path.split("."):
        if part == "":
            continue
        if isinstance(current, dict):
            if part in current:
                current = current[part]
            else:
                return default
        elif isinstance(current, list) and part.isdigit():
            index = int(part)
            if 0 <= index < len(current):
                current = current[index]
            else:
                return default
        else:
            return default
    return current


def _extract_text_from_gemini_payload(payload: dict[str, Any]) -> str:
    data = payload.get("data", {}) if isinstance(payload, dict) else {}
    if isinstance(data, dict):
        for key in ("text", "response_text", "output", "content"):
            value = data.get(key)
            if value is not None:
                return str(value)

        # Fallback for wrapped SDK responses.
        response = data.get("response") or data.get("raw")
        if isinstance(response, dict):
            for key in ("text", "output", "content"):
                value = response.get(key)
                if value is not None:
                    return str(value)

    return ""


def _extract_models_from_payload(payload: dict[str, Any]) -> list[str]:
    data = payload.get("data", {}) if isinstance(payload, dict) else {}
    candidates: Any = []
    if isinstance(data, dict):
        candidates = data.get("models") or data.get("items") or data.get("results") or []
    elif isinstance(data, list):
        candidates = data

    models: list[str] = []
    if isinstance(candidates, list):
        for item in candidates:
            if isinstance(item, str):
                models.append(item)
            elif isinstance(item, dict):
                name = item.get("name") or item.get("model") or item.get("id")
                if name:
                    models.append(str(name))
    return models


class AlwaysDirtyMixin:
    @classmethod
    def IS_CHANGED(cls, *args, **kwargs):
        return _dirty_token(cls.__name__)


# -----------------------------------------------------------------------------
# Session adapter
# -----------------------------------------------------------------------------


def _session_api_request(
    session: Any,
    method: str,
    path: str,
    *,
    json_body: Optional[dict[str, Any]] = None,
    params: Optional[dict[str, Any]] = None,
    gemini_prefix: str = DEFAULT_GEMINI_PREFIX,
) -> dict[str, Any]:
    """
    Call the Gemini route through the existing ZMongo session object.

    Supports both known ComfyUI-ZMongo connection client shapes:
      1. ZMongoApiSession.request(method, prefix, path, json_body=...)
      2. ZTarotManagerSessionClient.request(method, path, json_body=...)

    Falls back to the underlying requests.Session when needed, preserving the
    API key headers and TLS/timeout values from the existing connection node.
    """
    if session is None:
        return _error_payload("No ZMongo API session provided.")

    prefix = _clean_prefix(gemini_prefix, DEFAULT_GEMINI_PREFIX)
    clean_path = path if path.startswith("/") else f"/{path}"

    request_method = getattr(session, "request", None)
    if callable(request_method):
        # Newer API session shape from nodes/zmongo_api_nodes.py:
        # request(method, prefix, path, json_body=...)
        try:
            return _ensure_payload_dict(
                request_method(
                    method.upper(),
                    prefix,
                    clean_path,
                    json_body=json_body,
                    params=params,
                )
            )
        except TypeError:
            pass

        try:
            kwargs: dict[str, Any] = {"json_body": json_body}
            # Older client does not always support params.
            if params:
                query = "&".join(f"{key}={value}" for key, value in params.items() if value is not None)
                old_path = f"{prefix}{clean_path}?{query}" if query else f"{prefix}{clean_path}"
            else:
                old_path = f"{prefix}{clean_path}"
            return _ensure_payload_dict(request_method(method.upper(), old_path, **kwargs))
        except TypeError:
            pass
        except Exception as exc:
            return _error_payload(f"Gemini route request failed through session.request: {exc}")

    # Direct requests fallback using connection metadata from the existing session.
    base_url = _normalize_base_url(str(getattr(session, "base_url", DEFAULT_BASE_URL)))
    timeout = int(getattr(session, "timeout", DEFAULT_TIMEOUT) or DEFAULT_TIMEOUT)
    verify_tls = bool(getattr(session, "verify_tls", True))
    url = f"{base_url}{prefix}{clean_path}"

    headers = {
        "Accept": "application/json",
        "Content-Type": "application/json",
        "User-Agent": "comfyui-zmongo-gemini-nodes/1.0",
    }

    api_key = str(getattr(session, "zai_api_key", "") or getattr(session, "api_key", "") or "").strip()
    username = str(getattr(session, "username", "") or "").strip()
    if api_key:
        headers["ZAI_API_KEY"] = api_key
    if username:
        headers["ZAI_USER"] = username

    bearer = str(getattr(session, "bearer_token", "") or getattr(session, "jwt_token", "") or "").strip()
    if bearer and "Authorization" not in headers:
        headers["Authorization"] = f"Bearer {bearer}"

    requests_session = getattr(session, "session", None)
    if not isinstance(requests_session, requests.Session):
        requests_session = requests.Session()

    try:
        response = requests_session.request(
            method=method.upper(),
            url=url,
            headers=headers,
            json=json_body,
            params=params,
            timeout=timeout,
            verify=verify_tls,
        )
        try:
            payload = response.json()
        except ValueError:
            payload = {
                "success": response.ok,
                "message": response.reason or ("OK" if response.ok else "Request failed"),
                "data": {},
                "error": None if response.ok else {"msg": response.text[:1200]},
                "raw_text": response.text,
            }
        payload = _ensure_payload_dict(payload)
        payload["status_code"] = response.status_code
        return payload
    except requests.RequestException as exc:
        return _error_payload(f"Gemini route request failed: {exc}")


def _session_get_doc(session: Any, collection_name: str, document_id: str, cache: bool = False) -> dict[str, Any]:
    get_doc = getattr(session, "get_doc", None)
    if callable(get_doc):
        try:
            return _ensure_payload_dict(
                get_doc(
                    collection=(collection_name or "").strip(),
                    document_id=(document_id or "").strip(),
                    cache=bool(cache),
                )
            )
        except TypeError:
            pass
        try:
            return _ensure_payload_dict(get_doc((collection_name or "").strip(), (document_id or "").strip()))
        except Exception as exc:
            return _error_payload(f"get_doc failed: {exc}")

    return _session_api_request(
        session,
        "GET",
        f"/api/doc/{collection_name}/{document_id}",
        gemini_prefix="/comfy-zmongo",
    )


def _session_save_value(
    session: Any,
    *,
    collection_name: str,
    document_id: str,
    query: Optional[dict[str, Any]],
    field_path: str,
    value: Any,
    upsert: bool,
) -> dict[str, Any]:
    save_value_by_query = getattr(session, "save_value_by_query", None)
    if callable(save_value_by_query):
        try:
            return _ensure_payload_dict(
                save_value_by_query(
                    collection_name=collection_name,
                    query=query or ({"_id": document_id} if document_id else {}),
                    field_path=field_path,
                    value=value,
                    upsert_if_missing=upsert,
                )
            )
        except Exception as exc:
            return _error_payload(f"save_value_by_query failed: {exc}")

    body = {
        "collection": collection_name,
        "field_path": field_path,
        "value": value,
        "upsert_if_missing": bool(upsert),
    }
    if document_id:
        body["document_id"] = document_id
    elif query:
        body["query"] = query
    else:
        return _error_payload("Save requires document_id or query_json.")

    # Prefer the existing Comfy-ZMongo API route. It is the canonical API used by
    # the current custom node system.
    return _session_api_request(session, "POST", "/api/save-value", json_body=body, gemini_prefix="/comfy-zmongo")


def _extract_document(payload: dict[str, Any]) -> dict[str, Any]:
    data = payload.get("data", {}) if isinstance(payload, dict) else {}
    if isinstance(data, dict):
        document = data.get("document")
        if isinstance(document, dict):
            return document
        if "_id" in data:
            return data
    return {}


# -----------------------------------------------------------------------------
# Gemini route nodes
# -----------------------------------------------------------------------------


class GeminiApiKeyStatusNode(AlwaysDirtyMixin):
    @classmethod
    def INPUT_TYPES(cls):
        return {
            "required": {"session": ("ZMONGO_API_SESSION",)},
            "optional": {
                "gemini_prefix": ("STRING", {"default": DEFAULT_GEMINI_PREFIX}),
                "refresh_token": ("STRING", {"default": ""}),
            },
        }

    RETURN_TYPES = ("STRING", "BOOLEAN", "STRING")
    RETURN_NAMES = ("json", "has_key", "masked_key")
    FUNCTION = "key_status"
    CATEGORY = "ZMongo/05 Gemini"

    def key_status(self, session, gemini_prefix: str = DEFAULT_GEMINI_PREFIX, refresh_token: str = ""):
        payload = _session_api_request(session, "GET", "/api/key/status", gemini_prefix=gemini_prefix)
        data = payload.get("data", {}) if isinstance(payload, dict) else {}
        has_key = _as_bool(data.get("has_key") if isinstance(data, dict) else False)
        masked = str(data.get("masked_key") or data.get("key_preview") or "") if isinstance(data, dict) else ""
        return (_json_text(payload), has_key, masked)


class GeminiSaveApiKeyNode(AlwaysDirtyMixin):
    @classmethod
    def INPUT_TYPES(cls):
        return {
            "required": {
                "session": ("ZMONGO_API_SESSION",),
                "gemini_api_key": ("STRING", {"default": "", "multiline": False}),
            },
            "optional": {"gemini_prefix": ("STRING", {"default": DEFAULT_GEMINI_PREFIX})},
        }

    RETURN_TYPES = ("STRING", "BOOLEAN", "STRING")
    RETURN_NAMES = ("json", "success", "masked_key")
    FUNCTION = "save_key"
    CATEGORY = "ZMongo/05 Gemini"

    def save_key(self, session, gemini_api_key: str, gemini_prefix: str = DEFAULT_GEMINI_PREFIX):
        body = {"gemini_api_key": (gemini_api_key or "").strip()}
        payload = _session_api_request(session, "POST", "/api/key/save", json_body=body, gemini_prefix=gemini_prefix)
        data = payload.get("data", {}) if isinstance(payload, dict) else {}
        masked = str(data.get("masked_key") or data.get("key_preview") or "") if isinstance(data, dict) else ""
        return (_json_text(payload), bool(payload.get("success")), masked)


class GeminiDeleteApiKeyNode(AlwaysDirtyMixin):
    @classmethod
    def INPUT_TYPES(cls):
        return {
            "required": {"session": ("ZMONGO_API_SESSION",)},
            "optional": {"gemini_prefix": ("STRING", {"default": DEFAULT_GEMINI_PREFIX})},
        }

    RETURN_TYPES = ("STRING", "BOOLEAN", "STRING")
    RETURN_NAMES = ("json", "success", "refresh")
    FUNCTION = "delete_key"
    CATEGORY = "ZMongo/05 Gemini"

    def delete_key(self, session, gemini_prefix: str = DEFAULT_GEMINI_PREFIX):
        token = _dirty_token("gemini_delete_key")
        payload = _session_api_request(session, "POST", "/api/key/delete", json_body={}, gemini_prefix=gemini_prefix)
        return (_json_text(payload), bool(payload.get("success")), token)


class GeminiTestApiKeyNode(AlwaysDirtyMixin):
    @classmethod
    def INPUT_TYPES(cls):
        return {
            "required": {"session": ("ZMONGO_API_SESSION",)},
            "optional": {
                "gemini_prefix": ("STRING", {"default": DEFAULT_GEMINI_PREFIX}),
                "model": ("STRING", {"default": "gemini-2.5-flash"}),
                "refresh_token": ("STRING", {"default": ""}),
            },
        }

    RETURN_TYPES = ("STRING", "BOOLEAN", "STRING")
    RETURN_NAMES = ("json", "success", "text")
    FUNCTION = "test_key"
    CATEGORY = "ZMongo/05 Gemini"

    def test_key(self, session, gemini_prefix: str = DEFAULT_GEMINI_PREFIX, model: str = "gemini-2.5-flash", refresh_token: str = ""):
        payload = _session_api_request(
            session,
            "POST",
            "/api/key/test",
            json_body={"model": (model or "gemini-2.5-flash").strip()},
            gemini_prefix=gemini_prefix,
        )
        return (_json_text(payload), bool(payload.get("success")), _extract_text_from_gemini_payload(payload))


class GeminiChatNode(AlwaysDirtyMixin):
    @classmethod
    def INPUT_TYPES(cls):
        return {
            "required": {
                "session": ("ZMONGO_API_SESSION",),
                "prompt": ("STRING", {"default": "", "multiline": True}),
                "model": ("STRING", {"default": "gemini-2.5-flash"}),
                "max_output_tokens": ("INT", {"default": 1024, "min": 1, "max": 65536}),
                "temperature": ("FLOAT", {"default": 0.7, "min": 0.0, "max": 2.0, "step": 0.05}),
            },
            "optional": {
                "system_instruction": ("STRING", {"default": "", "multiline": True}),
                "gemini_prefix": ("STRING", {"default": DEFAULT_GEMINI_PREFIX}),
                "refresh_token": ("STRING", {"default": ""}),
            },
        }

    RETURN_TYPES = ("STRING", "STRING", "BOOLEAN")
    RETURN_NAMES = ("json", "text", "success")
    FUNCTION = "chat"
    CATEGORY = "ZMongo/05 Gemini"

    def chat(
        self,
        session,
        prompt: str,
        model: str,
        max_output_tokens: int,
        temperature: float,
        system_instruction: str = "",
        gemini_prefix: str = DEFAULT_GEMINI_PREFIX,
        refresh_token: str = "",
    ):
        body = {
            "prompt": prompt or "",
            "model": (model or "gemini-2.5-flash").strip(),
            "max_output_tokens": int(max_output_tokens),
            "temperature": float(temperature),
        }
        if (system_instruction or "").strip():
            body["system_instruction"] = system_instruction
        payload = _session_api_request(session, "POST", "/api/chat", json_body=body, gemini_prefix=gemini_prefix)
        return (_json_text(payload), _extract_text_from_gemini_payload(payload), bool(payload.get("success")))


class GeminiJsonNode(AlwaysDirtyMixin):
    @classmethod
    def INPUT_TYPES(cls):
        return {
            "required": {
                "session": ("ZMONGO_API_SESSION",),
                "prompt": ("STRING", {"default": "Return a JSON object.", "multiline": True}),
                "schema_json": ("STRING", {"default": "{}", "multiline": True}),
                "model": ("STRING", {"default": "gemini-2.5-flash"}),
                "max_output_tokens": ("INT", {"default": 2048, "min": 1, "max": 65536}),
                "temperature": ("FLOAT", {"default": 0.2, "min": 0.0, "max": 2.0, "step": 0.05}),
            },
            "optional": {
                "system_instruction": ("STRING", {"default": "", "multiline": True}),
                "gemini_prefix": ("STRING", {"default": DEFAULT_GEMINI_PREFIX}),
                "refresh_token": ("STRING", {"default": ""}),
            },
        }

    RETURN_TYPES = ("STRING", "STRING", "BOOLEAN")
    RETURN_NAMES = ("json", "parsed_or_text", "success")
    FUNCTION = "generate_json"
    CATEGORY = "ZMongo/05 Gemini"

    def generate_json(
        self,
        session,
        prompt: str,
        schema_json: str,
        model: str,
        max_output_tokens: int,
        temperature: float,
        system_instruction: str = "",
        gemini_prefix: str = DEFAULT_GEMINI_PREFIX,
        refresh_token: str = "",
    ):
        try:
            schema = _parse_json_object(schema_json, "schema_json")
            body = {
                "prompt": prompt or "",
                "schema": schema,
                "schema_json": schema,
                "model": (model or "gemini-2.5-flash").strip(),
                "max_output_tokens": int(max_output_tokens),
                "temperature": float(temperature),
            }
            if (system_instruction or "").strip():
                body["system_instruction"] = system_instruction
            payload = _session_api_request(session, "POST", "/api/json", json_body=body, gemini_prefix=gemini_prefix)
            data = payload.get("data", {}) if isinstance(payload, dict) else {}
            parsed = data.get("parsed") or data.get("json") or data.get("object") if isinstance(data, dict) else None
            result_text = _json_text(parsed) if parsed is not None else _extract_text_from_gemini_payload(payload)
            return (_json_text(payload), result_text, bool(payload.get("success")))
        except Exception as exc:
            payload = _error_payload(str(exc))
            return (_json_text(payload), "", False)


class GeminiListModelsNode(AlwaysDirtyMixin):
    @classmethod
    def INPUT_TYPES(cls):
        return {
            "required": {"session": ("ZMONGO_API_SESSION",)},
            "optional": {
                "gemini_prefix": ("STRING", {"default": DEFAULT_GEMINI_PREFIX}),
                "refresh_token": ("STRING", {"default": ""}),
            },
        }

    RETURN_TYPES = ("STRING", "*", "STRING")
    RETURN_NAMES = ("json", "models", "indexed")
    OUTPUT_IS_LIST = (False, True, False)
    FUNCTION = "list_models"
    CATEGORY = "ZMongo/05 Gemini"

    def list_models(self, session, gemini_prefix: str = DEFAULT_GEMINI_PREFIX, refresh_token: str = ""):
        payload = _session_api_request(session, "GET", "/api/models", gemini_prefix=gemini_prefix)
        models = _extract_models_from_payload(payload)
        indexed = _json_text([f"{index}: {value}" for index, value in enumerate(models)])
        return (_json_text(payload), models, indexed)


class GeminiCountTokensNode(AlwaysDirtyMixin):
    @classmethod
    def INPUT_TYPES(cls):
        return {
            "required": {
                "session": ("ZMONGO_API_SESSION",),
                "prompt": ("STRING", {"default": "", "multiline": True}),
                "model": ("STRING", {"default": "gemini-2.5-flash"}),
            },
            "optional": {
                "gemini_prefix": ("STRING", {"default": DEFAULT_GEMINI_PREFIX}),
                "refresh_token": ("STRING", {"default": ""}),
            },
        }

    RETURN_TYPES = ("STRING", "INT", "BOOLEAN")
    RETURN_NAMES = ("json", "token_count", "success")
    FUNCTION = "count_tokens"
    CATEGORY = "ZMongo/05 Gemini"

    def count_tokens(self, session, prompt: str, model: str = "gemini-2.5-flash", gemini_prefix: str = DEFAULT_GEMINI_PREFIX, refresh_token: str = ""):
        payload = _session_api_request(
            session,
            "POST",
            "/api/count-tokens",
            json_body={"prompt": prompt or "", "model": (model or "gemini-2.5-flash").strip()},
            gemini_prefix=gemini_prefix,
        )
        data = payload.get("data", {}) if isinstance(payload, dict) else {}
        count = 0
        if isinstance(data, dict):
            for key in ("total_tokens", "token_count", "tokens"):
                if key in data:
                    try:
                        count = int(data.get(key) or 0)
                    except Exception:
                        count = 0
                    break
        return (_json_text(payload), count, bool(payload.get("success")))


class GeminiPromptFromZMongoDocNode(AlwaysDirtyMixin):
    """
    Load a document through the existing ZMongo API session, extract a dot-path,
    prepend/append prompt text, and send the result to Gemini.
    """

    @classmethod
    def INPUT_TYPES(cls):
        return {
            "required": {
                "session": ("ZMONGO_API_SESSION",),
                "collection_name": ("STRING", {"default": ""}),
                "document_id": ("STRING", {"default": ""}),
                "field_path": ("STRING", {"default": "text"}),
                "prompt_prefix": ("STRING", {"default": "Analyze the following content:\n\n", "multiline": True}),
                "prompt_suffix": ("STRING", {"default": "", "multiline": True}),
                "model": ("STRING", {"default": "gemini-2.5-flash"}),
                "max_output_tokens": ("INT", {"default": 2048, "min": 1, "max": 65536}),
                "temperature": ("FLOAT", {"default": 0.4, "min": 0.0, "max": 2.0, "step": 0.05}),
            },
            "optional": {
                "system_instruction": ("STRING", {"default": "", "multiline": True}),
                "cache": ("BOOLEAN", {"default": False}),
                "gemini_prefix": ("STRING", {"default": DEFAULT_GEMINI_PREFIX}),
                "refresh_token": ("STRING", {"default": ""}),
            },
        }

    RETURN_TYPES = ("STRING", "STRING", "STRING", "BOOLEAN")
    RETURN_NAMES = ("json", "source_text", "gemini_text", "success")
    FUNCTION = "prompt_from_doc"
    CATEGORY = "ZMongo/05 Gemini"

    def prompt_from_doc(
        self,
        session,
        collection_name: str,
        document_id: str,
        field_path: str,
        prompt_prefix: str,
        prompt_suffix: str,
        model: str,
        max_output_tokens: int,
        temperature: float,
        system_instruction: str = "",
        cache: bool = False,
        gemini_prefix: str = DEFAULT_GEMINI_PREFIX,
        refresh_token: str = "",
    ):
        try:
            doc_payload = _session_get_doc(session, collection_name, document_id, cache=cache)
            document = _extract_document(doc_payload)
            if not document:
                payload = _error_payload("Document not found or did not contain a document object.", data={"doc_payload": doc_payload})
                return (_json_text(payload), "", "", False)

            source_value = _safe_get_by_path(document, (field_path or "").strip(), default="")
            source_text = source_value if isinstance(source_value, str) else _json_text(source_value)
            prompt = f"{prompt_prefix or ''}{source_text}{prompt_suffix or ''}"

            body = {
                "prompt": prompt,
                "model": (model or "gemini-2.5-flash").strip(),
                "max_output_tokens": int(max_output_tokens),
                "temperature": float(temperature),
            }
            if (system_instruction or "").strip():
                body["system_instruction"] = system_instruction

            gemini_payload = _session_api_request(session, "POST", "/api/chat", json_body=body, gemini_prefix=gemini_prefix)
            merged_payload = _success_payload(
                "Gemini prompt from ZMongo document completed." if gemini_payload.get("success") else "Gemini prompt from ZMongo document failed.",
                {
                    "source": {
                        "collection_name": collection_name,
                        "document_id": document_id,
                        "field_path": field_path,
                    },
                    "doc_payload": doc_payload,
                    "gemini_payload": gemini_payload,
                },
                status_code=int(gemini_payload.get("status_code") or 200),
            )
            merged_payload["success"] = bool(gemini_payload.get("success"))
            gemini_text = _extract_text_from_gemini_payload(gemini_payload)
            return (_json_text(merged_payload), source_text, gemini_text, bool(gemini_payload.get("success")))
        except Exception as exc:
            payload = _error_payload(str(exc))
            return (_json_text(payload), "", "", False)


class GeminiChatAndSaveToZMongoNode(AlwaysDirtyMixin):
    """
    Send a prompt to Gemini and save the returned text into a ZMongo document
    using the same authenticated ZMongo API session.
    """

    @classmethod
    def INPUT_TYPES(cls):
        return {
            "required": {
                "session": ("ZMONGO_API_SESSION",),
                "prompt": ("STRING", {"default": "", "multiline": True}),
                "collection_name": ("STRING", {"default": ""}),
                "document_id": ("STRING", {"default": ""}),
                "query_json": ("STRING", {"default": "{}", "multiline": True}),
                "target_field_path": ("STRING", {"default": "gemini.response"}),
                "model": ("STRING", {"default": "gemini-2.5-flash"}),
                "max_output_tokens": ("INT", {"default": 2048, "min": 1, "max": 65536}),
                "temperature": ("FLOAT", {"default": 0.5, "min": 0.0, "max": 2.0, "step": 0.05}),
                "upsert": ("BOOLEAN", {"default": True}),
            },
            "optional": {
                "system_instruction": ("STRING", {"default": "", "multiline": True}),
                "save_full_payload": ("BOOLEAN", {"default": False}),
                "gemini_prefix": ("STRING", {"default": DEFAULT_GEMINI_PREFIX}),
            },
        }

    RETURN_TYPES = ("STRING", "STRING", "STRING", "BOOLEAN")
    RETURN_NAMES = ("json", "gemini_text", "refresh", "success")
    FUNCTION = "chat_and_save"
    CATEGORY = "ZMongo/05 Gemini"

    def chat_and_save(
        self,
        session,
        prompt: str,
        collection_name: str,
        document_id: str,
        query_json: str,
        target_field_path: str,
        model: str,
        max_output_tokens: int,
        temperature: float,
        upsert: bool,
        system_instruction: str = "",
        save_full_payload: bool = False,
        gemini_prefix: str = DEFAULT_GEMINI_PREFIX,
    ):
        token = _dirty_token("gemini_chat_save", collection_name, document_id, target_field_path)
        try:
            query = _parse_json_object(query_json, "query_json")
            body = {
                "prompt": prompt or "",
                "model": (model or "gemini-2.5-flash").strip(),
                "max_output_tokens": int(max_output_tokens),
                "temperature": float(temperature),
            }
            if (system_instruction or "").strip():
                body["system_instruction"] = system_instruction

            gemini_payload = _session_api_request(session, "POST", "/api/chat", json_body=body, gemini_prefix=gemini_prefix)
            gemini_text = _extract_text_from_gemini_payload(gemini_payload)
            if not gemini_payload.get("success"):
                return (_json_text(gemini_payload), gemini_text, token, False)

            value_to_save: Any = gemini_payload if save_full_payload else gemini_text
            save_payload = _session_save_value(
                session,
                collection_name=(collection_name or "").strip(),
                document_id=(document_id or "").strip(),
                query=query,
                field_path=(target_field_path or "gemini.response").strip(),
                value=value_to_save,
                upsert=bool(upsert),
            )

            merged = _success_payload(
                "Gemini response generated and save attempted.",
                {"gemini_payload": gemini_payload, "save_payload": save_payload},
                status_code=int(save_payload.get("status_code") or 200),
            )
            merged["success"] = bool(gemini_payload.get("success")) and bool(save_payload.get("success"))
            if not merged["success"]:
                merged["message"] = "Gemini response generated, but ZMongo save failed."
            return (_json_text(merged), gemini_text, token, bool(merged["success"]))
        except Exception as exc:
            payload = _error_payload(str(exc))
            return (_json_text(payload), "", token, False)


# -----------------------------------------------------------------------------
# ComfyUI mappings
# -----------------------------------------------------------------------------

NODE_CLASS_MAPPINGS = {
    "GeminiApiKeyStatusNode": GeminiApiKeyStatusNode,
    "GeminiSaveApiKeyNode": GeminiSaveApiKeyNode,
    "GeminiDeleteApiKeyNode": GeminiDeleteApiKeyNode,
    "GeminiTestApiKeyNode": GeminiTestApiKeyNode,
    "GeminiChatNode": GeminiChatNode,
    "GeminiJsonNode": GeminiJsonNode,
    "GeminiListModelsNode": GeminiListModelsNode,
    "GeminiCountTokensNode": GeminiCountTokensNode,
    "GeminiPromptFromZMongoDocNode": GeminiPromptFromZMongoDocNode,
    "GeminiChatAndSaveToZMongoNode": GeminiChatAndSaveToZMongoNode,
}

NODE_DISPLAY_NAME_MAPPINGS = {
    "GeminiApiKeyStatusNode": "05 Gemini Key Status",
    "GeminiSaveApiKeyNode": "05 Save Gemini API Key",
    "GeminiDeleteApiKeyNode": "05 Delete Gemini API Key",
    "GeminiTestApiKeyNode": "05 Test Gemini API Key",
    "GeminiChatNode": "05 Gemini Chat",
    "GeminiJsonNode": "05 Gemini JSON",
    "GeminiListModelsNode": "05 Gemini List Models",
    "GeminiCountTokensNode": "05 Gemini Count Tokens",
    "GeminiPromptFromZMongoDocNode": "05 Gemini Prompt from ZMongo Doc",
    "GeminiChatAndSaveToZMongoNode": "05 Gemini Chat and Save to ZMongo",
}

__all__ = [
    "NODE_CLASS_MAPPINGS",
    "NODE_DISPLAY_NAME_MAPPINGS",
]
