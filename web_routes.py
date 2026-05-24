from __future__ import annotations

import os
from typing import Any, Dict, Optional

import requests
from aiohttp import web

try:
    from server import PromptServer
except Exception as exc:
    PromptServer = None
    print(f"[ComfyUI-ZMongo] Could not import PromptServer: {exc}")


DEFAULT_BASE_URL = os.getenv("BPA_BASE_URL", "https://businessprocessapplications.com").rstrip("/")
DEFAULT_PREFIX = os.getenv("COMFY_ZMONGO_API_PREFIX", "/comfy-zmongo").rstrip("/")
DEFAULT_TIMEOUT = int(os.getenv("BPA_TIMEOUT_SECONDS", "30"))


def _normalize_base_url(value: str) -> str:
    base = (value or DEFAULT_BASE_URL).strip().rstrip("/")
    suffixes = (
        "/api/comfy-zmongo",
        "/comfy_zmongo",
        "/comfy-zmongo",
        "/user/manager/api",
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


def _cors_headers() -> Dict[str, str]:
    return {
        "Cache-Control": "no-store",
        "Access-Control-Allow-Origin": "*",
        "Access-Control-Allow-Methods": "GET,POST,OPTIONS",
        "Access-Control-Allow-Headers": "Content-Type,Authorization,ZAI_API_KEY,ZAI_USER,X-ZAI-User,X-Username",
    }


def _json_response(payload: Any, status: int = 200):
    if not isinstance(payload, dict):
        payload = {
            "success": False,
            "message": "Payload was not a JSON object.",
            "data": payload,
            "error": {"msg": f"Unexpected payload type: {type(payload).__name__}"},
            "status_code": status,
        }

    payload.setdefault("success", status < 400)
    payload.setdefault("message", "OK" if status < 400 else "Request failed.")
    payload.setdefault("data", {})
    payload.setdefault("error", None)
    payload.setdefault("status_code", status)

    return web.json_response(payload, status=status, headers=_cors_headers())


async def _read_json(request: web.Request) -> Dict[str, Any]:
    if request.method.upper() == "OPTIONS":
        return {}

    try:
        payload = await request.json()
    except Exception:
        payload = {}

    return payload if isinstance(payload, dict) else {}


def _backend_headers(api_key: str = "", username: str = "") -> Dict[str, str]:
    headers = {
        "Accept": "application/json",
        "Content-Type": "application/json",
        "User-Agent": "comfyui-zmongo-panel-proxy/1.1",
    }

    clean_key = (api_key or "").strip()
    clean_user = (username or "").strip()

    if clean_key:
        headers["ZAI_API_KEY"] = clean_key
        headers["Authorization"] = f"Bearer {clean_key}"

    if clean_user:
        headers["ZAI_USER"] = clean_user
        headers["X-ZAI-User"] = clean_user
        headers["X-Username"] = clean_user

    return headers


def _request_backend(
    *,
    method: str,
    base_url: str,
    path: str,
    json_body: Optional[dict[str, Any]] = None,
    api_key: str = "",
    username: str = "",
    timeout: int = DEFAULT_TIMEOUT,
) -> dict[str, Any]:
    clean_base = _normalize_base_url(base_url)
    clean_path = path if path.startswith("/") else f"/{path}"
    url = f"{clean_base}{DEFAULT_PREFIX}{clean_path}"

    try:
        response = requests.request(
            method=method.upper(),
            url=url,
            headers=_backend_headers(api_key=api_key, username=username),
            json=json_body if method.upper() != "GET" else None,
            timeout=max(1, int(timeout or DEFAULT_TIMEOUT)),
            verify=True,
            allow_redirects=True,
        )

        try:
            payload = response.json()
        except Exception:
            payload = {
                "success": response.ok,
                "message": response.reason or ("OK" if response.ok else "Request failed"),
                "data": {},
                "error": None if response.ok else {"msg": response.text[:2000]},
                "raw_text": response.text[:4000],
            }

        if not isinstance(payload, dict):
            payload = {
                "success": response.ok,
                "message": response.reason or ("OK" if response.ok else "Request failed"),
                "data": payload,
                "error": None if response.ok else {"msg": response.text[:1200]},
            }

        payload.setdefault("success", response.ok)
        payload.setdefault("message", response.reason or ("OK" if response.ok else "Request failed"))
        payload.setdefault("data", {})
        payload.setdefault("error", None if response.ok else {"msg": response.text[:1200]})
        payload["status_code"] = response.status_code
        payload["_proxy"] = {
            "url": url,
            "method": method.upper(),
            "backend_status_code": response.status_code,
        }
        return payload

    except requests.RequestException as exc:
        return {
            "success": False,
            "message": f"Backend request failed: {exc}",
            "data": {
                "base_url": clean_base,
                "prefix": DEFAULT_PREFIX,
                "path": clean_path,
                "url": url,
            },
            "error": {
                "type": exc.__class__.__name__,
                "msg": str(exc),
            },
            "status_code": 0,
        }


def _extract_base_url(payload: dict[str, Any]) -> str:
    return _normalize_base_url(str(payload.get("base_url") or DEFAULT_BASE_URL))


def _extract_username(payload: dict[str, Any]) -> str:
    return str(payload.get("username") or "").strip()


def _extract_api_key(payload: dict[str, Any]) -> str:
    return str(
        payload.get("api_key")
        or payload.get("zai_api_key")
        or payload.get("ZAI_API_KEY")
        or ""
    ).strip()


async def _options_ok(request: web.Request):
    return _json_response(
        {
            "success": True,
            "message": "OPTIONS OK",
            "data": {"method": request.method, "path": request.path},
            "error": None,
            "status_code": 200,
        },
        status=200,
    )


async def zmongo_panel_health(request: web.Request):
    if request.method.upper() == "OPTIONS":
        return await _options_ok(request)

    if request.method.upper() == "POST":
        body = await _read_json(request)
        base_url = _extract_base_url(body)
    else:
        base_url = _normalize_base_url(request.query.get("base_url") or DEFAULT_BASE_URL)

    payload = _request_backend(
        method="GET",
        base_url=base_url,
        path="/api/health",
    )
    return _json_response(payload, status=200)


async def zmongo_panel_register(request: web.Request):
    if request.method.upper() == "OPTIONS":
        return await _options_ok(request)

    body = await _read_json(request)
    base_url = _extract_base_url(body)

    payload = _request_backend(
        method="POST",
        base_url=base_url,
        path="/api/panel/register",
        json_body=body,
    )
    return _json_response(payload, status=200)


async def zmongo_panel_login(request: web.Request):
    if request.method.upper() == "OPTIONS":
        return await _options_ok(request)

    body = await _read_json(request)
    base_url = _extract_base_url(body)

    payload = _request_backend(
        method="POST",
        base_url=base_url,
        path="/api/panel/login",
        json_body=body,
    )
    return _json_response(payload, status=200)


async def zmongo_panel_api_key(request: web.Request):
    if request.method.upper() == "OPTIONS":
        return await _options_ok(request)

    body = await _read_json(request)
    base_url = _extract_base_url(body)

    payload = _request_backend(
        method="POST",
        base_url=base_url,
        path="/api/panel/api-key",
        json_body=body,
    )
    return _json_response(payload, status=200)


async def zmongo_panel_whoami(request: web.Request):
    if request.method.upper() == "OPTIONS":
        return await _options_ok(request)

    body = await _read_json(request)
    base_url = _extract_base_url(body)
    username = _extract_username(body)
    api_key = _extract_api_key(body)

    payload = _request_backend(
        method="GET",
        base_url=base_url,
        path="/api/whoami",
        api_key=api_key,
        username=username,
    )
    return _json_response(payload, status=200)


async def zmongo_panel_routes(request: web.Request):
    return _json_response(
        {
            "success": True,
            "message": "ComfyUI-ZMongo local proxy routes are installed.",
            "data": {
                "base_url": DEFAULT_BASE_URL,
                "prefix": DEFAULT_PREFIX,
                "routes": [
                    "GET|POST|OPTIONS /zmongo-panel/health",
                    "POST|OPTIONS /zmongo-panel/register",
                    "POST|OPTIONS /zmongo-panel/login",
                    "POST|OPTIONS /zmongo-panel/api-key",
                    "POST|OPTIONS /zmongo-panel/whoami",
                    "GET /zmongo-panel/routes",
                ],
            },
            "error": None,
            "status_code": 200,
        },
        status=200,
    )


def _register_route(method: str, path: str, handler):
    try:
        PromptServer.instance.routes.add_route(method, path, handler)
        print(f"[ComfyUI-ZMongo] Registered {method} {path}")
    except RuntimeError as exc:
        # Duplicate route after hot reload. Not fatal.
        print(f"[ComfyUI-ZMongo] Route already registered or failed: {method} {path}: {exc}")
    except Exception as exc:
        print(f"[ComfyUI-ZMongo] Failed registering {method} {path}: {exc}")


if PromptServer is not None:
    for method in ("GET", "POST", "OPTIONS"):
        _register_route(method, "/zmongo-panel/health", zmongo_panel_health)

    for method in ("POST", "OPTIONS"):
        _register_route(method, "/zmongo-panel/register", zmongo_panel_register)
        _register_route(method, "/zmongo-panel/login", zmongo_panel_login)
        _register_route(method, "/zmongo-panel/api-key", zmongo_panel_api_key)
        _register_route(method, "/zmongo-panel/whoami", zmongo_panel_whoami)

    _register_route("GET", "/zmongo-panel/routes", zmongo_panel_routes)

    print("[ComfyUI-ZMongo] Local panel proxy routes installed at /zmongo-panel/*")
