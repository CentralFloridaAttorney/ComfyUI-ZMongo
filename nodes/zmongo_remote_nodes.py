import json
import logging
from typing import Any, Dict, Optional, Tuple
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

from zmongo_toolbag.data_processor import DataProcessor
from zmongo_toolbag.safe_result import SafeResult

logger = logging.getLogger(__name__)

# Simple in-process session cache for the current ComfyUI runtime.
# Keyed by base_url|username
REMOTE_SESSION_CACHE: Dict[str, str] = {}


def _safe_json(value: Any) -> str:
    try:
        if isinstance(value, SafeResult):
            return value.to_json(indent=2)
        return DataProcessor.to_json(value, indent=2)
    except Exception as exc:
        return json.dumps({"error": f"Serialization failed: {exc}"}, indent=2)


def _normalize_base_url(base_url: str) -> str:
    return str(base_url or "").strip().rstrip("/")


def _cache_key(base_url: str, username: str) -> str:
    return f"{_normalize_base_url(base_url)}|{str(username or '').strip()}"


def _parse_scalar_or_json(raw: Any, *, parse_json: bool) -> Any:
    if not parse_json:
        return raw

    if isinstance(raw, str):
        text = raw.strip()
        if not text:
            return ""
        try:
            return json.loads(text)
        except Exception:
            return raw

    return raw


def _parse_json_object(raw: str, field_name: str, default: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    text = (raw or "").strip()
    if not text:
        return default or {}

    try:
        parsed = json.loads(text)
    except Exception as exc:
        raise ValueError(f"{field_name} is not valid JSON: {exc}") from exc

    if not isinstance(parsed, dict):
        raise ValueError(f"{field_name} must be a JSON object")

    return parsed


def _http_json(
    url: str,
    *,
    method: str = "GET",
    payload: Optional[Dict[str, Any]] = None,
    bearer_token: str = "",
    timeout_seconds: float = 30.0,
) -> Tuple[int, Dict[str, Any]]:
    headers = {
        "Accept": "application/json",
    }

    body: Optional[bytes] = None
    if payload is not None:
        body = json.dumps(payload).encode("utf-8")
        headers["Content-Type"] = "application/json"

    token = str(bearer_token or "").strip()
    if token:
        headers["Authorization"] = f"Bearer {token}"

    request = Request(
        url=url,
        data=body,
        headers=headers,
        method=method.upper(),
    )

    try:
        with urlopen(request, timeout=float(timeout_seconds)) as response:
            status = int(getattr(response, "status", 200))
            raw = response.read().decode("utf-8", errors="replace")
            data = json.loads(raw) if raw.strip() else {}
            if not isinstance(data, dict):
                data = {"data": data}
            return status, data

    except HTTPError as exc:
        raw = exc.read().decode("utf-8", errors="replace")
        try:
            data = json.loads(raw) if raw.strip() else {}
        except Exception:
            data = {"error": raw or str(exc)}
        if not isinstance(data, dict):
            data = {"data": data}
        return int(exc.code), data

    except URLError as exc:
        raise ConnectionError(f"Request failed: {exc}") from exc


class ZMongoRemoteLoginNode:
    """
    Log into ztarot.app (or another compatible server) and return a bearer token.

    Outputs:
    - token
    - status_json
    - username
    """

    CATEGORY = "ZMongo/Remote"
    FUNCTION = "login"
    RETURN_TYPES = ("STRING", "STRING", "STRING")
    RETURN_NAMES = ("token", "status_json", "username")

    @classmethod
    def INPUT_TYPES(cls):
        return {
            "required": {
                "base_url": ("STRING", {"default": "https://ztarot.app"}),
                "username": ("STRING", {"default": ""}),
                "password": ("STRING", {"default": "", "multiline": False}),
                "timeout_seconds": ("FLOAT", {"default": 30.0, "min": 1.0, "max": 300.0}),
                "store_in_runtime_cache": ("BOOLEAN", {"default": True}),
            }
        }

    def login(
        self,
        base_url: str,
        username: str,
        password: str,
        timeout_seconds: float,
        store_in_runtime_cache: bool,
    ):
        try:
            normalized_base = _normalize_base_url(base_url)
            normalized_username = str(username or "").strip()

            if not normalized_base:
                raise ValueError("base_url is required")
            if not normalized_username:
                raise ValueError("username is required")
            if not str(password or ""):
                raise ValueError("password is required")

            status, data = _http_json(
                f"{normalized_base}/api/auth/login",
                method="POST",
                payload={
                    "username": normalized_username,
                    "password": password,
                },
                timeout_seconds=timeout_seconds,
            )

            token = str(data.get("token") or "").strip()
            if status >= 400 or not token:
                message = data.get("error") or data.get("message") or f"HTTP {status}"
                failure = {
                    "success": False,
                    "status_code": status,
                    "error": message,
                    "base_url": normalized_base,
                    "username": normalized_username,
                }
                return ("", _safe_json(failure), normalized_username)

            if store_in_runtime_cache:
                REMOTE_SESSION_CACHE[_cache_key(normalized_base, normalized_username)] = token

            payload = {
                "success": True,
                "status_code": status,
                "base_url": normalized_base,
                "username": data.get("username", normalized_username),
                "user_id": data.get("user_id"),
                "expires_in": data.get("expires_in"),
                "token_cached": bool(store_in_runtime_cache),
            }
            return (token, _safe_json(payload), str(data.get("username", normalized_username)))

        except Exception as exc:
            logger.exception("ZMongoRemoteLoginNode failure")
            failure = SafeResult.from_exception(exc, operation="remote_login")
            return ("", failure.to_json(indent=2), str(username or ""))


class ZMongoRemoteSaveValueNode:
    """
    Save workflow output to the remote ZMongo manager API.

    Authentication:
    - If bearer_token input is non-empty, it is used.
    - Otherwise, if username is provided, a cached token from ZMongoRemoteLoginNode
      is used for the given base_url|username pair.

    Supports:
    - create new document
    - update by document_id
    - update by query_json
    - dot-path save using field_path
    """

    CATEGORY = "ZMongo/Remote"
    FUNCTION = "save_value"
    RETURN_TYPES = ("STRING", "BOOLEAN", "STRING")
    RETURN_NAMES = ("result_json", "success", "resolved_token")

    @classmethod
    def INPUT_TYPES(cls):
        return {
            "required": {
                "base_url": ("STRING", {"default": "https://ztarot.app"}),
                "collection_name": ("STRING", {"default": ""}),
                "value_to_save": ("STRING", {"default": "", "multiline": True}),
                "parse_value_as_json": ("BOOLEAN", {"default": True}),
                "upsert_if_missing": ("BOOLEAN", {"default": True}),
                "timeout_seconds": ("FLOAT", {"default": 30.0, "min": 1.0, "max": 300.0}),
            },
            "optional": {
                "bearer_token": ("STRING", {"default": "", "forceInput": True}),
                "username": ("STRING", {"default": "", "forceInput": True}),
                "document_id": ("STRING", {"default": ""}),
                "query_json": ("STRING", {"default": "{}", "multiline": True}),
                "field_path": ("STRING", {"default": ""}),
                "metadata_json": ("STRING", {"default": "{}", "multiline": True}),
            },
        }

    def save_value(
        self,
        base_url: str,
        collection_name: str,
        value_to_save: str,
        parse_value_as_json: bool,
        upsert_if_missing: bool,
        timeout_seconds: float,
        bearer_token: str = "",
        username: str = "",
        document_id: str = "",
        query_json: str = "{}",
        field_path: str = "",
        metadata_json: str = "{}",
    ):
        try:
            normalized_base = _normalize_base_url(base_url)
            normalized_collection = str(collection_name or "").strip()
            normalized_username = str(username or "").strip()

            if not normalized_base:
                raise ValueError("base_url is required")
            if not normalized_collection:
                raise ValueError("collection_name is required")

            token = str(bearer_token or "").strip()
            if not token and normalized_username:
                token = REMOTE_SESSION_CACHE.get(_cache_key(normalized_base, normalized_username), "")

            if not token:
                raise ValueError(
                    "No bearer token provided. Use bearer_token input or log in first with ZMongo Remote Login."
                )

            parsed_value = _parse_scalar_or_json(value_to_save, parse_json=parse_value_as_json)
            query = _parse_json_object(query_json, "query_json", default={})
            metadata = _parse_json_object(metadata_json, "metadata_json", default={})

            payload: Dict[str, Any] = {
                "collection": normalized_collection,
                "value": parsed_value,
                "upsert_if_missing": bool(upsert_if_missing),
                "metadata": metadata,
            }

            normalized_field_path = str(field_path or "").strip()
            if normalized_field_path:
                payload["field_path"] = normalized_field_path

            normalized_document_id = str(document_id or "").strip()
            if normalized_document_id:
                payload["document_id"] = normalized_document_id
            elif query:
                payload["query"] = query

            status, data = _http_json(
                f"{normalized_base}/user/manager/api/save-value",
                method="POST",
                payload=payload,
                bearer_token=token,
                timeout_seconds=timeout_seconds,
            )

            success = status < 400 and not data.get("error")
            result_payload = {
                "success": success,
                "status_code": status,
                "base_url": normalized_base,
                "collection_name": normalized_collection,
                "document_id": normalized_document_id,
                "query_used": DataProcessor.to_json_compatible(query),
                "field_path": normalized_field_path,
                "response": data,
            }

            return (_safe_json(result_payload), bool(success), token)

        except Exception as exc:
            logger.exception("ZMongoRemoteSaveValueNode failure")
            failure = SafeResult.from_exception(exc, operation="remote_save_value")
            return (failure.to_json(indent=2), False, str(bearer_token or ""))


NODE_CLASS_MAPPINGS = {
    "ZMongoRemoteLoginNode": ZMongoRemoteLoginNode,
    "ZMongoRemoteSaveValueNode": ZMongoRemoteSaveValueNode,
}

NODE_DISPLAY_NAME_MAPPINGS = {
    "ZMongoRemoteLoginNode": "ZMongo Remote Login",
    "ZMongoRemoteSaveValueNode": "ZMongo Remote Save Value",
}