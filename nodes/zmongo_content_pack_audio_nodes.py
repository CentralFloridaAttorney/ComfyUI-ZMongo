from __future__ import annotations

"""
ComfyUI-ZMongo audio helpers for Content Packs.

This module adds a node that extracts an audio asset from a normalized ZMongo
Content Pack, portable Content Pack envelope, or compatible JSON dict/string,
and saves the audio file to the user's ComfyUI output folder or another allowed
local folder.

Primary node:
    ZMongoContentPackSaveAudioFileV3

Alias:
    ZMongoContentPackGetAudioFileV3
"""

import base64
import binascii
import inspect
import json
import mimetypes
import os
import re
import shutil
from pathlib import Path
from typing import Any, Optional

try:
    import folder_paths  # type: ignore
except Exception:  # pragma: no cover - ComfyUI import is runtime-only
    folder_paths = None  # type: ignore


CONTENT_PACK_TYPE = "ZMONGO_CONTENT_PACK"
AUDIO_SESSION_TYPE = "ZMONGO_API_SESSION"


_AUDIO_EXT_BY_CONTENT_TYPE = {
    "audio/wav": ".wav",
    "audio/x-wav": ".wav",
    "audio/wave": ".wav",
    "audio/mpeg": ".mp3",
    "audio/mp3": ".mp3",
    "audio/ogg": ".ogg",
    "audio/flac": ".flac",
    "audio/aac": ".aac",
    "audio/x-aac": ".aac",
    "audio/mp4": ".m4a",
    "audio/x-m4a": ".m4a",
    "audio/webm": ".webm",
    "video/mp4": ".mp4",  # useful when an uploaded mp4 contains the source audio
}


_DATA_URI_RE = re.compile(r"^data:(?P<mime>[^;,]+)?(?P<params>(?:;[^,]*)*),(?P<data>.*)$", re.DOTALL)


def _safe_str(value: Any, default: str = "") -> str:
    if value is None:
        return default
    try:
        return str(value)
    except Exception:
        return default


def _json_dumps(value: Any) -> str:
    try:
        return json.dumps(value, ensure_ascii=False, indent=2, default=str)
    except Exception:
        return json.dumps({"error": "failed to serialize field info"})


def _safe_filename(value: Any, fallback: str = "audio") -> str:
    text = _safe_str(value).strip() or fallback
    text = text.replace("\\", "/").split("/")[-1]
    text = re.sub(r"[^A-Za-z0-9_. -]+", "_", text).strip(" ._-")
    return text or fallback


def _safe_stem(value: Any, fallback: str = "audio") -> str:
    name = _safe_filename(value, fallback)
    stem = Path(name).stem if "." in name else name
    stem = re.sub(r"[^A-Za-z0-9_.-]+", "_", stem).strip("._-")
    return stem or fallback


def _default_output_dir() -> Path:
    if folder_paths is not None:
        try:
            return Path(folder_paths.get_output_directory()).resolve()
        except Exception:
            pass
    return (Path.cwd() / "output").resolve()


def _resolve_output_dir(output_dir: str, allow_absolute_path: bool) -> Path:
    base = _default_output_dir()
    raw = _safe_str(output_dir).strip()
    if not raw:
        target = base / "zmongo_content_pack_audio"
    else:
        path = Path(os.path.expanduser(raw))
        if path.is_absolute():
            target = path if allow_absolute_path else base / "zmongo_content_pack_audio" / path.name
        else:
            target = base / path
    target.mkdir(parents=True, exist_ok=True)
    return target.resolve()


def _guess_extension(content_type: str = "", filename: str = "", fallback: str = ".wav") -> str:
    filename = _safe_str(filename)
    suffix = Path(filename).suffix.lower()
    if suffix and re.match(r"^\.[a-z0-9]{1,8}$", suffix):
        return suffix
    ctype = _safe_str(content_type).split(";")[0].strip().lower()
    if ctype in _AUDIO_EXT_BY_CONTENT_TYPE:
        return _AUDIO_EXT_BY_CONTENT_TYPE[ctype]
    guessed = mimetypes.guess_extension(ctype) if ctype else None
    if guessed:
        return guessed
    return fallback


def _unique_path(path: Path) -> Path:
    if not path.exists():
        return path
    stem = path.stem
    suffix = path.suffix
    parent = path.parent
    for idx in range(1, 100000):
        candidate = parent / f"{stem}_{idx:03d}{suffix}"
        if not candidate.exists():
            return candidate
    raise RuntimeError(f"Could not create unique filename for {path}")


def _parse_json_maybe(value: Any) -> Any:
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return {}
        try:
            return json.loads(text)
        except Exception:
            return value
    return value


def _unwrap_content_pack(value: Any) -> Any:
    value = _parse_json_maybe(value)
    if not isinstance(value, dict):
        return value

    # Portable V3 envelopes usually wrap the normalized content pack.
    for key in ("content_pack", "pack", "contentPack"):
        inner = value.get(key)
        if isinstance(inner, dict) and ("fields" in inner or "manifest" in inner or "schema_kind" in inner):
            return inner

    # Some exports use a payload wrapper.
    payload = value.get("payload")
    if isinstance(payload, dict) and "fields" in payload:
        return payload

    return value


def _field_list(content_pack: Any) -> list[dict[str, Any]]:
    pack = _unwrap_content_pack(content_pack)
    if isinstance(pack, dict):
        fields = pack.get("fields")
        if isinstance(fields, list):
            return [f for f in fields if isinstance(f, dict)]

        manifest = pack.get("manifest")
        if isinstance(manifest, dict) and isinstance(manifest.get("fields"), list):
            return [f for f in manifest["fields"] if isinstance(f, dict)]

        payload = pack.get("payload")
        if isinstance(payload, dict) and isinstance(payload.get("fields"), list):
            return [f for f in payload["fields"] if isinstance(f, dict)]

    return []


def _field_key_text(field: dict[str, Any], key: str) -> str:
    return _safe_str(field.get(key)).strip()


def _looks_like_audio_field(field: dict[str, Any]) -> bool:
    comfy_type = _field_key_text(field, "comfy_type").upper()
    json_type = _field_key_text(field, "json_type").lower()
    content_type = _field_key_text(field, "content_type").lower()
    asset_type = _field_key_text(field, "asset_type").lower()
    storage = _field_key_text(field, "storage").lower()
    alias = _field_key_text(field, "alias").lower()
    source_path = _field_key_text(field, "source_path").lower()

    if comfy_type == "AUDIO":
        return True
    if content_type.startswith("audio/"):
        return True
    if asset_type in {"audio", "audio_asset", "media_audio"}:
        return True
    if "audio" in json_type or "audio" in storage:
        return True
    if "audio" in alias or "audio" in source_path:
        return True

    value = field.get("value")
    if isinstance(value, str) and value.strip().startswith("data:audio/"):
        return True
    if isinstance(value, dict):
        ctype = _safe_str(value.get("content_type") or value.get("mime_type")).lower()
        atype = _safe_str(value.get("asset_type") or value.get("type")).lower()
        if ctype.startswith("audio/") or atype in {"audio", "audio_asset", "media_audio"}:
            return True
        for key in ("audio", "audio_data", "audio_file", "wav", "mp3", "base64", "b64"):
            if key in value:
                return True

    return False


def _find_audio_field(content_pack: Any, field_alias: str) -> tuple[Optional[dict[str, Any]], str]:
    fields = _field_list(content_pack)
    wanted = _safe_str(field_alias).strip()
    wanted_lower = wanted.lower()

    if wanted:
        for field in fields:
            candidates = [
                _field_key_text(field, "alias"),
                _field_key_text(field, "name"),
                _field_key_text(field, "label"),
                _field_key_text(field, "source_path"),
            ]
            for candidate in candidates:
                cand_lower = candidate.lower()
                if cand_lower == wanted_lower or cand_lower.endswith("." + wanted_lower):
                    return field, "alias_match"

    # Common aliases for content-pack video/audio workflows.
    for common in ("audio", "audio_file", "sound", "soundtrack", "voice", "music"):
        for field in fields:
            if _field_key_text(field, "alias").lower() == common:
                return field, "common_alias"

    for field in fields:
        if _looks_like_audio_field(field):
            return field, "first_audio_field"

    return None, "not_found"


def _decode_data_uri(value: str) -> tuple[bytes, str]:
    match = _DATA_URI_RE.match(value.strip())
    if not match:
        raise ValueError("not a data URI")
    content_type = (match.group("mime") or "application/octet-stream").strip()
    params = (match.group("params") or "").lower()
    data_text = match.group("data") or ""
    if ";base64" in params:
        return base64.b64decode(data_text, validate=False), content_type
    return data_text.encode("utf-8"), content_type


def _decode_base64_text(value: str) -> bytes:
    text = re.sub(r"\s+", "", value.strip())
    try:
        return base64.b64decode(text, validate=True)
    except binascii.Error:
        # Some encoders omit padding or include lenient characters.
        padding = "=" * ((4 - len(text) % 4) % 4)
        return base64.b64decode(text + padding, validate=False)


def _read_local_asset(path_text: str, asset_root: str = "") -> Optional[bytes]:
    if not path_text:
        return None
    path = Path(os.path.expanduser(path_text))
    candidates: list[Path] = []
    if path.is_absolute():
        candidates.append(path)
    else:
        root = _safe_str(asset_root).strip()
        if root:
            candidates.append(Path(os.path.expanduser(root)) / path)
        candidates.append(Path.cwd() / path)
        try:
            candidates.append(_default_output_dir() / path)
        except Exception:
            pass
    for candidate in candidates:
        try:
            resolved = candidate.resolve()
            if resolved.is_file():
                return resolved.read_bytes()
        except Exception:
            continue
    return None


def _extract_from_value(value: Any, *, asset_root: str = "") -> tuple[Optional[bytes], str, str, str]:
    """Return bytes, content_type, filename, source_kind."""
    if value is None:
        return None, "", "", "none"

    if isinstance(value, bytes):
        return value, "application/octet-stream", "", "bytes"

    if isinstance(value, str):
        text = value.strip()
        if text.startswith("data:"):
            data, ctype = _decode_data_uri(text)
            return data, ctype, "", "data_uri"

        local_bytes = _read_local_asset(text, asset_root=asset_root)
        if local_bytes is not None:
            return local_bytes, "", Path(text).name, "local_path"

        if len(text) > 64:
            return _decode_base64_text(text), "application/octet-stream", "", "base64"

        return None, "", "", "unsupported_string"

    if isinstance(value, dict):
        content_type = _safe_str(value.get("content_type") or value.get("mime_type") or value.get("type"))
        filename = _safe_str(value.get("filename") or value.get("name") or value.get("file_name"))

        # Data URI or base64 fields.
        for key in ("data_uri", "uri", "audio_data", "audio", "data", "base64", "b64", "content", "value"):
            if key in value:
                try:
                    data, ctype, fname, source = _extract_from_value(value.get(key), asset_root=asset_root)
                    if data is not None:
                        return data, ctype or content_type, filename or fname, f"dict.{key}.{source}"
                except Exception:
                    continue

        # Local or ZIP asset paths.
        for key in ("local_path", "file_path", "path", "relative_path", "asset_path"):
            path_text = _safe_str(value.get(key))
            if path_text:
                data = _read_local_asset(path_text, asset_root=asset_root)
                if data is not None:
                    return data, content_type, filename or Path(path_text).name, f"dict.{key}.local_asset"

    return None, "", "", "unsupported_value"


def _extract_from_session(
    session: Any,
    field: dict[str, Any],
    *,
    master_key_hex: str = "",
) -> tuple[Optional[bytes], str, str, str]:
    """
    Best-effort support for future/host-specific ZMongo API session objects.
    The node supports inline and local assets without session. For backend asset
    refs, it tries common method names if they exist.
    """
    if session is None:
        return None, "", "", "no_session"

    asset_ref = field.get("asset_ref") if isinstance(field.get("asset_ref"), dict) else {}
    original = field.get("original") if isinstance(field.get("original"), dict) else {}

    collection = _safe_str(
        field.get("asset_collection")
        or asset_ref.get("asset_collection")
        or original.get("asset_collection")
        or asset_ref.get("collection")
        or original.get("collection")
    )
    document_id = _safe_str(
        field.get("document_id")
        or field.get("file_id")
        or asset_ref.get("document_id")
        or asset_ref.get("file_id")
        or original.get("document_id")
        or original.get("file_id")
    )
    field_path = _safe_str(
        field.get("field_path")
        or field.get("source_path")
        or asset_ref.get("field_path")
        or original.get("field_path")
    )

    method_names = (
        "fetch_audio_field",
        "fetch_file_field",
        "fetch_media_field",
        "fetch_blob_field",
        "fetch_asset_bytes",
        "fetch_field_bytes",
    )

    for method_name in method_names:
        method = getattr(session, method_name, None)
        if not callable(method):
            continue

        attempts = [
            (collection, document_id, field_path, master_key_hex),
            (collection, document_id, field_path),
            (document_id, field_path, master_key_hex),
            (document_id, field_path),
            (document_id,),
        ]
        for args in attempts:
            args = tuple(arg for arg in args if arg != "")
            try:
                result = method(*args)
                if inspect.isawaitable(result):
                    continue
                data, ctype, fname, source = _normalize_session_result(result)
                if data is not None:
                    return data, ctype, fname, f"session.{method_name}.{source}"
            except TypeError:
                continue
            except Exception:
                continue

    return None, "", "", "session_fetch_failed"


def _normalize_session_result(result: Any) -> tuple[Optional[bytes], str, str, str]:
    if result is None:
        return None, "", "", "none"
    if isinstance(result, bytes):
        return result, "application/octet-stream", "", "bytes"
    if isinstance(result, str):
        return _extract_from_value(result)
    if isinstance(result, dict):
        # Common response wrappers.
        content_type = _safe_str(result.get("content_type") or result.get("mime_type"))
        filename = _safe_str(result.get("filename") or result.get("name"))
        for key in ("bytes", "data", "content", "base64", "b64", "data_uri", "value", "file"):
            if key in result:
                data, ctype, fname, source = _extract_from_value(result.get(key))
                if data is not None:
                    return data, ctype or content_type, filename or fname, f"dict.{key}.{source}"
    return None, "", "", "unsupported_session_result"


def _content_pack_name(content_pack: Any) -> str:
    pack = _unwrap_content_pack(content_pack)
    if isinstance(pack, dict):
        return _safe_str(
            pack.get("content_pack_name")
            or pack.get("name")
            or pack.get("title")
            or pack.get("public_slug")
            or "content_pack"
        )
    return "content_pack"


class ZMongoContentPackSaveAudioFileV3:
    """Save an AUDIO field from a ZMongo Content Pack to a local file."""

    CATEGORY = "ZMongo/09 Content Packs/Get"
    FUNCTION = "save_audio"
    RETURN_TYPES = ("STRING", "STRING", "BOOLEAN")
    RETURN_NAMES = ("audio_path", "field_info_json", "saved")
    OUTPUT_NODE = True

    @classmethod
    def INPUT_TYPES(cls):
        return {
            "required": {
                "content_pack": (CONTENT_PACK_TYPE,),
                "field_alias": ("STRING", {"default": "audio", "multiline": False}),
                "output_dir": ("STRING", {"default": "zmongo_content_pack_audio", "multiline": False}),
                "filename": ("STRING", {"default": "", "multiline": False}),
                "overwrite": ("BOOLEAN", {"default": False}),
            },
            "optional": {
                "session": (AUDIO_SESSION_TYPE,),
                "master_key_hex": ("STRING", {"default": "", "multiline": False}),
                "asset_root": ("STRING", {"default": "", "multiline": False}),
                "allow_absolute_path": ("BOOLEAN", {"default": False}),
            },
        }

    def save_audio(
        self,
        content_pack: Any,
        field_alias: str = "audio",
        output_dir: str = "zmongo_content_pack_audio",
        filename: str = "",
        overwrite: bool = False,
        session: Any = None,
        master_key_hex: str = "",
        asset_root: str = "",
        allow_absolute_path: bool = False,
        **kwargs: Any,
    ):
        field, match_reason = _find_audio_field(content_pack, field_alias)
        info: dict[str, Any] = {
            "requested_alias": field_alias,
            "match_reason": match_reason,
            "saved": False,
        }

        if field is None:
            info["error"] = "No audio field was found in the content pack."
            return ("", _json_dumps(info), False)

        info.update(
            {
                "alias": field.get("alias", ""),
                "label": field.get("label", ""),
                "source_path": field.get("source_path", ""),
                "comfy_type": field.get("comfy_type", ""),
                "json_type": field.get("json_type", ""),
                "storage": field.get("storage", ""),
            }
        )

        content_type = _safe_str(field.get("content_type") or field.get("mime_type"))
        source_filename = _safe_str(field.get("filename") or field.get("name") or field.get("file_name"))

        try:
            data, ctype, fname, source_kind = _extract_from_value(field.get("value"), asset_root=asset_root)
        except Exception as exc:
            data, ctype, fname, source_kind = None, "", "", f"value_decode_failed:{exc}"

        if data is None:
            # Try field-level dict forms and asset refs.
            try:
                data, ctype, fname, source_kind = _extract_from_value(field, asset_root=asset_root)
            except Exception as exc:
                data, ctype, fname, source_kind = None, "", "", f"field_decode_failed:{exc}"

        if data is None:
            data, ctype, fname, source_kind = _extract_from_session(
                session,
                field,
                master_key_hex=master_key_hex,
            )

        content_type = ctype or content_type or "application/octet-stream"
        source_filename = fname or source_filename
        info["source_kind"] = source_kind
        info["content_type"] = content_type

        if data is None:
            info["error"] = (
                "Audio data was not available inline and could not be resolved from asset_root or session. "
                "For ZIP exports, set asset_root to the extracted ZIP folder. For backend asset refs, connect a valid ZMongo session."
            )
            return ("", _json_dumps(info), False)

        output_path = _resolve_output_dir(output_dir, bool(allow_absolute_path))
        ext = _guess_extension(content_type, source_filename, fallback=".wav")

        requested_filename = _safe_filename(filename, "")
        if requested_filename:
            final_name = requested_filename
            if not Path(final_name).suffix:
                final_name += ext
        elif source_filename:
            final_name = _safe_filename(source_filename)
            if not Path(final_name).suffix:
                final_name += ext
        else:
            pack_stem = _safe_stem(_content_pack_name(content_pack), "content_pack")
            alias_stem = _safe_stem(field.get("alias") or field_alias or "audio", "audio")
            final_name = f"{pack_stem}_{alias_stem}{ext}"

        target = (output_path / final_name).resolve()
        if not bool(overwrite):
            target = _unique_path(target)

        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(data)

        info.update(
            {
                "saved": True,
                "audio_path": str(target),
                "size_bytes": len(data),
                "filename": target.name,
                "output_dir": str(target.parent),
            }
        )

        print(f"[ZMongoContentPackSaveAudioFileV3] Saved audio field {info.get('alias')!r} to {target}")
        return (str(target), _json_dumps(info), True)


# Alias class. The display name can say "Get" but the behavior is intentionally
# save-to-file because ComfyUI's native AUDIO type is not stable across all video
# node packs.
class ZMongoContentPackGetAudioFileV3(ZMongoContentPackSaveAudioFileV3):
    pass


NODE_CLASS_MAPPINGS = {
    "ZMongoContentPackSaveAudioFileV3": ZMongoContentPackSaveAudioFileV3,
    "ZMongoContentPackGetAudioFileV3": ZMongoContentPackGetAudioFileV3,
}

NODE_DISPLAY_NAME_MAPPINGS = {
    "ZMongoContentPackSaveAudioFileV3": "ZMongo Content Pack Save Audio File V3",
    "ZMongoContentPackGetAudioFileV3": "ZMongo Content Pack Get Audio File V3",
}
