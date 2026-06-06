from __future__ import annotations

import json
from typing import Any

from aiohttp import web

try:
    from server import PromptServer
except Exception:  # pragma: no cover
    PromptServer = None


_EMPTY_CSS = """
/* ComfyUI-ZMongo user.css fallback.
   Intentionally empty; prevents harmless ComfyUI userdata 404s. */
""".strip()

_EMPTY_TEMPLATES: dict[str, Any] = {
    "templates": [],
    "workflows": [],
}

_EMPTY_USERDATA_LIST: list[dict[str, Any]] = []


def _json_response(data: Any, *, status: int = 200) -> web.Response:
    return web.Response(
        text=json.dumps(data, ensure_ascii=False),
        status=status,
        content_type="application/json",
        headers={
            "Cache-Control": "no-store",
            "Access-Control-Allow-Origin": "*",
        },
    )


def _text_response(text: str, *, content_type: str, status: int = 200) -> web.Response:
    return web.Response(
        text=text,
        status=status,
        content_type=content_type,
        headers={
            "Cache-Control": "no-store",
            "Access-Control-Allow-Origin": "*",
        },
    )


async def user_css_fallback(request: web.Request) -> web.Response:
    return _text_response(_EMPTY_CSS + "\n", content_type="text/css")


async def comfy_templates_fallback(request: web.Request) -> web.Response:
    return _json_response(_EMPTY_TEMPLATES)


async def userdata_index_fallback(request: web.Request) -> web.Response:
    """
    Handles ComfyUI frontend probes such as:

        /api/userdata?dir=subgraphs&recurse=true&split=false&full_info=true

    Returning an empty list is safer than 404.
    """
    return _json_response(_EMPTY_USERDATA_LIST)


async def userdata_file_fallback(request: web.Request) -> web.Response:
    """
    Conservative fallback for known ComfyUI userdata files.
    Unknown files still return 404 so real missing assets are not hidden.
    """
    filename = (request.match_info.get("filename") or "").strip().lstrip("/")

    if filename == "user.css":
        return await user_css_fallback(request)

    if filename == "comfy.templates.json":
        return await comfy_templates_fallback(request)

    return _json_response(
        {
            "success": False,
            "error": "userdata_not_found",
            "path": filename,
        },
        status=404,
    )


def register_zmongo_comfy_userdata_fallbacks() -> None:
    """
    Register narrow fallback routes for ComfyUI frontend userdata probes.

    Routes added:
        GET /api/userdata
        GET /api/userdata/{filename:.*}

    These are compatibility shims only. They do not expose ZMongo CRUD routes.
    """
    if PromptServer is None:
        print("[ZMongo] PromptServer unavailable; userdata fallback routes not registered.")
        return

    instance = getattr(PromptServer, "instance", None)
    routes = getattr(instance, "routes", None)

    if routes is None:
        print("[ZMongo] PromptServer routes unavailable; userdata fallback routes not registered.")
        return

    if getattr(instance, "_zmongo_userdata_fallback_registered", False):
        return

    routes.get("/api/userdata")(userdata_index_fallback)
    routes.get("/api/userdata/{filename:.*}")(userdata_file_fallback)

    setattr(instance, "_zmongo_userdata_fallback_registered", True)
    print("[ZMongo] ComfyUI userdata fallback routes registered.")