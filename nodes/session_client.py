from __future__ import annotations

import json
import os
import urllib.parse
from pathlib import Path
from typing import Any, Dict, List, Optional

import requests
from dotenv import load_dotenv

ENV_PATH1 = Path.home() / ".resources" / ".env"
ENV_PATH2 = Path.home() / ".resources" / ".secrets"
load_dotenv(ENV_PATH1)
load_dotenv(ENV_PATH2)

DEFAULT_BASE_URL = os.getenv("ZTAROT_BASE_URL", "https://ztarot.app").rstrip("/")
DEFAULT_TIMEOUT = int(os.getenv("ZTAROT_TIMEOUT_SECONDS", "30"))


def json_dumps(value: Any) -> str:
    return json.dumps(value, indent=2, sort_keys=True, ensure_ascii=False, default=str)


def parse_json_maybe(value: str, parse_json: bool) -> Any:
    value = value or ""
    if not parse_json:
        return value
    stripped = value.strip()
    if stripped == "":
        return ""
    try:
        return json.loads(stripped)
    except json.JSONDecodeError:
        return value


def parse_object_json(value: str, default: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    stripped = (value or "").strip()
    if not stripped:
        return default or {}
    parsed = json.loads(stripped)
    if not isinstance(parsed, dict):
        raise ValueError("Expected a JSON object")
    return parsed


def parse_any_json(value: str, default: Any = None) -> Any:
    stripped = (value or "").strip()
    if not stripped:
        return default
    return json.loads(stripped)


def flatten_paths(data: Any, parent_key: str = "", sep: str = ".") -> List[str]:
    flat_keys: List[str] = []
    if isinstance(data, dict) and data.get("__type__") == "bytes":
        if parent_key:
            flat_keys.append(parent_key)
        return flat_keys
    if isinstance(data, list):
        for index, value in enumerate(data):
            new_key = f"{parent_key}{sep}{index}" if parent_key else str(index)
            flat_keys.extend(flatten_paths(value, new_key, sep))
        return flat_keys
    if isinstance(data, dict):
        for key, value in data.items():
            new_key = f"{parent_key}{sep}{key}" if parent_key else str(key)
            flat_keys.extend(flatten_paths(value, new_key, sep))
        return flat_keys
    if parent_key:
        flat_keys.append(parent_key)
    return flat_keys


def safe_get_by_path(data: Any, field_path: str, sep: str = ".") -> Any:
    current = data
    for part in (field_path or "").split(sep):
        if part == "":
            continue
        if isinstance(current, dict):
            if part not in current:
                raise KeyError(f"Missing key: {part}")
            current = current[part]
            continue
        if isinstance(current, list):
            index = int(part)
            current = current[index]
            continue
        raise KeyError(f"Cannot descend into non-container at {part!r}")
    return current


def normalize_base_url(raw_base_url: str) -> str:
    base = (raw_base_url or DEFAULT_BASE_URL).strip().rstrip("/")
    suffixes = (
        "/user/manager",
        "/user/api-manager",
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


def ensure_payload_dict(payload: Any) -> Dict[str, Any]:
    if isinstance(payload, dict):
        result = dict(payload)
    else:
        result = {
            "success": False,
            "message": "Payload was not an object",
            "data": {},
            "error": {"msg": f"Unexpected payload type: {type(payload).__name__}"},
        }
    if "success" not in result:
        result["success"] = False
    if "status_code" not in result:
        result["status_code"] = 0
    if "message" not in result:
        result["message"] = "OK" if result.get("success") else ""
    if "error" not in result:
        result["error"] = None
    if result.get("data") is None:
        result["data"] = {}
    return result


def extract_data(payload: Dict[str, Any]) -> Any:
    payload = ensure_payload_dict(payload)
    return payload.get("data", {})


def extract_document(payload: Dict[str, Any]) -> Dict[str, Any]:
    data = extract_data(payload)
    if isinstance(data, dict):
        document = data.get("document")
        if isinstance(document, dict):
            return document
        if "_id" in data:
            return data
    return {}


def extract_primary_json_value(payload_or_value: Any) -> Any:
    if isinstance(payload_or_value, dict) and "data" in payload_or_value:
        payload = ensure_payload_dict(payload_or_value)
        data = payload.get("data", {})
        if isinstance(data, dict):
            for key in ("documents", "collections", "results", "items"):
                if key in data:
                    return data.get(key) or []
            if "document" in data:
                return data.get("document") or {}
        return data
    return payload_or_value


def summarize_for_text(value: Any) -> str:
    if isinstance(value, list):
        lines: List[str] = []
        for index, item in enumerate(value):
            if isinstance(item, dict):
                if "_id" in item and "doc_key" in item:
                    lines.append(f"{index}: {item.get('_id')} | {item.get('doc_key')}")
                elif "_id" in item:
                    lines.append(f"{index}: {item.get('_id')}")
                elif "doc_key" in item:
                    lines.append(f"{index}: {item.get('doc_key')}")
                else:
                    lines.append(f"{index}: {json.dumps(item, ensure_ascii=False, default=str)}")
            else:
                lines.append(f"{index}: {item}")
        return "\n".join(lines)
    if isinstance(value, dict):
        return json_dumps(value)
    if value is None:
        return ""
    return str(value)


def extract_doc_ids(payload_or_value: Any) -> List[str]:
    value = extract_primary_json_value(payload_or_value)
    if isinstance(value, list):
        return [str(item["_id"]) for item in value if isinstance(item, dict) and "_id" in item]
    if isinstance(value, dict) and "_id" in value:
        return [str(value["_id"])]
    return []


def find_doc_by_key(payload_or_value: Any, doc_key: str) -> Optional[Dict[str, Any]]:
    value = extract_primary_json_value(payload_or_value)
    if isinstance(value, list):
        for doc in value:
            if isinstance(doc, dict) and doc.get("doc_key") == doc_key:
                return doc
    if isinstance(value, dict) and value.get("doc_key") == doc_key:
        return value
    return None


def extract_field_paths_from_value(payload_or_value: Any) -> List[str]:
    value = extract_primary_json_value(payload_or_value)
    keys = set()
    if isinstance(value, list):
        for item in value:
            keys.update(flatten_paths(item))
    else:
        keys.update(flatten_paths(value))
    return sorted(keys)


class ZTarotManagerSessionClient:
    def __init__(
        self,
        base_url: Optional[str] = None,
        username: Optional[str] = None,
        password: Optional[str] = None,
        timeout: int = DEFAULT_TIMEOUT,
        verify_tls: bool = True,
        refresh_session_each_request: bool = True,
    ) -> None:
        self.base_url = normalize_base_url(base_url or DEFAULT_BASE_URL)
        self.username = (username or os.getenv("ZTAROT_USERNAME", "")).strip()
        self.password = (password or os.getenv("ZTAROT_PASSWORD", "")).strip()
        self.timeout = max(1, int(timeout))
        self.verify_tls = bool(verify_tls)
        self.refresh_session_each_request = bool(refresh_session_each_request)
        self.session = requests.Session()
        self.is_authenticated = False

    def _require_credentials(self) -> None:
        if not self.username or not self.password:
            raise RuntimeError(
                "ZTAROT_USERNAME and ZTAROT_PASSWORD are required in "
                "~/.resources/.env or ~/.resources/.secrets, or must be supplied "
                "to the connect node."
            )

    def _browser_headers(self, content_type: Optional[str] = None) -> Dict[str, str]:
        headers = {
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "User-Agent": "ztarot-session-client/3.0",
            "Origin": self.base_url,
            "Referer": f"{self.base_url}/user/login",
        }
        if content_type:
            headers["Content-Type"] = content_type
        return headers

    def _manager_headers(self) -> Dict[str, str]:
        return {
            "Accept": "application/json",
            "Content-Type": "application/json",
            "User-Agent": "ztarot-session-client/3.0",
            "Origin": self.base_url,
            "Referer": f"{self.base_url}/user/manager/",
        }

    def login(self, force: bool = False) -> Dict[str, Any]:
        if self.is_authenticated and not force:
            return {
                "success": True,
                "message": "Already authenticated",
                "data": {
                    "cookies": self.session.cookies.get_dict(),
                    "effective_base_url": self.base_url,
                    "refresh_session_each_request": self.refresh_session_each_request,
                },
                "status_code": 200,
                "error": None,
            }

        self._require_credentials()
        login_url = f"{self.base_url}/user/login"

        self.session.get(
            login_url,
            headers=self._browser_headers(),
            timeout=self.timeout,
            verify=self.verify_tls,
            allow_redirects=True,
        )

        response = self.session.post(
            login_url,
            headers=self._browser_headers("application/x-www-form-urlencoded"),
            data={"username": self.username, "password": self.password},
            timeout=self.timeout,
            verify=self.verify_tls,
            allow_redirects=False,
        )

        if response.status_code not in (302, 303):
            snippet = response.text[:1200]
            reason = ""
            lowered = snippet.lower()
            if "<title>z-mongo // login</title>" in lowered:
                reason = (
                    "\nLikely causes:\n"
                    f"- wrong credentials for username {self.username!r}\n"
                    f"- wrong base_url input; effective base_url is {self.base_url!r} and login_url is {login_url!r}\n"
                    "- reusing an old workflow value like https://ztarot.app/user/manager instead of the site root"
                )
            raise RuntimeError(
                f"Login failed with HTTP {response.status_code}\n"
                f"effective_base_url={self.base_url}\n"
                f"login_url={login_url}\n"
                f"{snippet}{reason}"
            )

        cookies = self.session.cookies.get_dict()
        if "session" not in cookies:
            raise RuntimeError(
                f"Login did not return a session cookie. Cookies: {list(cookies.keys())}"
            )

        location = response.headers.get("Location") or "/user/dashboard"
        if location.startswith("/"):
            location = f"{self.base_url}{location}"
        self.session.get(
            location,
            headers=self._browser_headers(),
            timeout=self.timeout,
            verify=self.verify_tls,
            allow_redirects=True,
        )

        self.is_authenticated = True
        return {
            "success": True,
            "message": "Authenticated",
            "data": {
                "cookies": self.session.cookies.get_dict(),
                "effective_base_url": self.base_url,
                "login_url": login_url,
                "refresh_session_each_request": self.refresh_session_each_request,
            },
            "status_code": 200,
            "error": None,
        }

    def close(self) -> None:
        self.session.close()
        self.is_authenticated = False

    def ensure_authenticated(self) -> None:
        if not self.is_authenticated:
            self.login()

    def _normalize_payload(self, response: requests.Response) -> Dict[str, Any]:
        try:
            payload = response.json()
            if not isinstance(payload, dict):
                payload = {"success": response.ok, "data": payload}
        except ValueError:
            payload = {
                "success": False,
                "message": "Non-JSON response",
                "data": {},
                "error": {"msg": "Response was not JSON"},
                "raw_text": response.text,
            }
        payload = ensure_payload_dict(payload)
        payload["status_code"] = response.status_code
        if payload.get("message") == "" and response.ok:
            payload["message"] = "OK"
        return payload

    def request(self, method: str, path: str, *, json_body: Optional[Dict[str, Any]] = None, allow_reauth: bool = True) -> Dict[str, Any]:
        if self.refresh_session_each_request:
            self.login(force=True)
        else:
            self.ensure_authenticated()

        response = self.session.request(
            method=method,
            url=f"{self.base_url}{path}",
            headers=self._manager_headers(),
            json=json_body,
            timeout=self.timeout,
            verify=self.verify_tls,
            allow_redirects=True,
        )
        payload = self._normalize_payload(response)

        if allow_reauth and response.status_code in (401, 403):
            self.login(force=True)
            response = self.session.request(
                method=method,
                url=f"{self.base_url}{path}",
                headers=self._manager_headers(),
                json=json_body,
                timeout=self.timeout,
                verify=self.verify_tls,
                allow_redirects=True,
            )
            payload = self._normalize_payload(response)
        return payload

    def list_collections(self) -> Dict[str, Any]:
        return self.request("GET", "/user/manager/api/collections")

    def list_docs(self, collection_name: str, limit: int = 50, skip: int = 0) -> Dict[str, Any]:
        quoted = urllib.parse.quote(collection_name, safe="")
        query = f"?limit={max(1, min(int(limit), 200))}&skip={max(0, int(skip))}"
        return self.request("GET", f"/user/manager/api/docs/{quoted}{query}")

    def get_doc(self, collection_name: str, document_id: str) -> Dict[str, Any]:
        quoted_coll = urllib.parse.quote(collection_name, safe="")
        quoted_doc = urllib.parse.quote(document_id, safe="")
        return self.request("GET", f"/user/manager/api/doc/{quoted_coll}/{quoted_doc}")

    def create_collection(self, collection_name: str) -> Dict[str, Any]:
        return self.request("POST", "/user/manager/api/collection/create", json_body={"name": collection_name})

    def delete_collection(self, collection_name: str) -> Dict[str, Any]:
        return self.request("POST", "/user/manager/api/collection/delete", json_body={"name": collection_name})

    def create_doc(self, collection_name: str, document: Dict[str, Any]) -> Dict[str, Any]:
        return self.request("POST", "/user/manager/api/create", json_body={"collection": collection_name, "document": document})

    def update_field(self, collection_name: str, document_id: str, field_path: str, value: Any) -> Dict[str, Any]:
        return self.request("POST", "/user/manager/api/update", json_body={"collection": collection_name, "id": document_id, "key": field_path, "value": value})

    def save_value_by_query(self, *, collection_name: str, query: Dict[str, Any], field_path: str, value: Any, upsert_if_missing: bool = True) -> Dict[str, Any]:
        return self.request(
            "POST",
            "/user/manager/api/save-value",
            json_body={
                "collection": collection_name,
                "query": query,
                "field_path": field_path,
                "value": value,
                "upsert_if_missing": bool(upsert_if_missing),
            },
        )

    def save_value_by_doc_key(self, *, collection_name: str, doc_key: str, field_path: str, value: Any, upsert_if_missing: bool = True) -> Dict[str, Any]:
        return self.save_value_by_query(
            collection_name=collection_name,
            query={"doc_key": doc_key},
            field_path=field_path,
            value=value,
            upsert_if_missing=upsert_if_missing,
        )

    def delete_doc(self, collection_name: str, document_id: str) -> Dict[str, Any]:
        return self.request("POST", "/user/manager/api/delete", json_body={"collection": collection_name, "id": document_id})

    def logout(self) -> Dict[str, Any]:
        try:
            response = self.session.get(
                f"{self.base_url}/user/logout",
                headers=self._browser_headers(),
                timeout=self.timeout,
                verify=self.verify_tls,
                allow_redirects=True,
            )
            return self._normalize_payload(response)
        finally:
            self.close()
