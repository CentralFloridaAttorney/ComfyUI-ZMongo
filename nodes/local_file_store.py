"""
ComfyUI-ZMongo Local File Store

Lightweight local persistence backend for ComfyUI-ZMongo.

This is intentionally NOT local MongoDB.
It stores JSON documents and image files on disk while preserving:
- collections
- documents
- document_id
- flattened key paths
- save_value / get_value
- save_image / load_image
"""

from __future__ import annotations

import base64
import binascii
import copy
import hashlib
import json
import mimetypes
import os
import re
import tempfile
import threading
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Union


DEFAULT_LOCAL_MAX_VALUE_BYTES = int(os.getenv("ZMONGO_LOCAL_MAX_VALUE_BYTES", str(9 * 10 * 10 * 256 * 1024)))
DEFAULT_LOCAL_MAX_DOCUMENT_BYTES = int(os.getenv("ZMONGO_LOCAL_MAX_DOCUMENT_BYTES", str(9 *10 * 10 * 1024 * 1024)))
DEFAULT_LOCAL_MAX_IMAGE_BYTES = int(os.getenv("ZMONGO_LOCAL_MAX_IMAGE_BYTES", str(9 * 10 * 10 * 10 * 1024 * 1024)))

print(DEFAULT_LOCAL_MAX_IMAGE_BYTES)
print(DEFAULT_LOCAL_MAX_DOCUMENT_BYTES)
print(DEFAULT_LOCAL_MAX_VALUE_BYTES)

class DataProcessor:
    @staticmethod
    def to_json_compatible(data: Any, _seen: Optional[set] = None) -> Any:
        if _seen is None:
            _seen = set()

        if data is None or isinstance(data, (bool, int, str)):
            return data

        if isinstance(data, float):
            if data != data:
                return "NaN"
            if data == float("inf"):
                return "Infinity"
            if data == float("-inf"):
                return "-Infinity"
            return data

        if isinstance(data, datetime):
            return data.isoformat()

        if isinstance(data, (bytes, bytearray, memoryview)):
            raw = bytes(data)
            try:
                return raw.decode("utf-8")
            except Exception:
                return {
                    "__type__": "bytes",
                    "encoding": "base64",
                    "size_bytes": len(raw),
                    "data": base64.b64encode(raw).decode("ascii"),
                }

        if isinstance(data, dict):
            obj_id = id(data)
            if obj_id in _seen:
                return {"__circular_reference__": "dict"}
            _seen.add(obj_id)
            return {
                str(k): DataProcessor.to_json_compatible(v, _seen.copy())
                for k, v in data.items()
            }

        if isinstance(data, (list, tuple, set)):
            obj_id = id(data)
            if obj_id in _seen:
                return {"__circular_reference__": type(data).__name__}
            _seen.add(obj_id)
            return [
                DataProcessor.to_json_compatible(v, _seen.copy())
                for v in list(data)
            ]

        if hasattr(data, "model_dump") and callable(data.model_dump):
            try:
                return DataProcessor.to_json_compatible(data.model_dump(), _seen.copy())
            except Exception:
                pass

        if hasattr(data, "dict") and callable(data.dict):
            try:
                return DataProcessor.to_json_compatible(data.dict(), _seen.copy())
            except Exception:
                pass

        if hasattr(data, "__dict__"):
            try:
                return DataProcessor.to_json_compatible(
                    {k: v for k, v in vars(data).items() if not k.startswith("_")},
                    _seen.copy(),
                )
            except Exception:
                pass

        return str(data)

    @staticmethod
    def flatten_json(data: Any, parent_key: str = "", sep: str = ".") -> Dict[str, Any]:
        flat: Dict[str, Any] = {}

        if isinstance(data, dict) and data.get("__type__") == "bytes":
            if parent_key:
                flat[parent_key] = data
            return flat

        if isinstance(data, dict):
            for key, value in data.items():
                new_key = f"{parent_key}{sep}{key}" if parent_key else str(key)
                flat.update(DataProcessor.flatten_json(value, new_key, sep))
            return flat

        if isinstance(data, list):
            for index, value in enumerate(data):
                new_key = f"{parent_key}{sep}{index}" if parent_key else str(index)
                flat.update(DataProcessor.flatten_json(value, new_key, sep))
            return flat

        if parent_key:
            flat[parent_key] = data

        return flat

    @staticmethod
    def flatten_dict(data: Dict[str, Any], sep: str = ".") -> Dict[str, Any]:
        return DataProcessor.flatten_json(data, sep=sep)

    @staticmethod
    def get_value(data: Union[Dict[str, Any], List[Any]], key: str) -> Any:
        if not key:
            return data

        value: Any = data
        for part in key.split("."):
            if isinstance(value, dict):
                value = value.get(part)
            elif isinstance(value, list) and part.isdigit():
                index = int(part)
                value = value[index] if 0 <= index < len(value) else None
            else:
                return None

            if value is None:
                return None

        return value

    @staticmethod
    def set_value(data: Union[Dict[str, Any], List[Any]], key: str, val: Any) -> bool:
        if not isinstance(data, (dict, list)) or not key:
            return False

        parts = key.split(".")
        target: Any = data

        for part in parts[:-1]:
            if isinstance(target, dict):
                next_target = target.get(part)
                if next_target is None:
                    next_target = {}
                    target[part] = next_target
                target = next_target
            elif isinstance(target, list) and part.isdigit():
                index = int(part)
                if not (0 <= index < len(target)):
                    return False
                target = target[index]
            else:
                return False

        last = parts[-1]

        if isinstance(target, dict):
            target[last] = val
            return True

        if isinstance(target, list) and last.isdigit():
            index = int(last)
            if 0 <= index < len(target):
                target[index] = val
                return True

        return False


@dataclass
class SafeResult:
    success: bool
    data: Optional[Any] = None
    message: str = "Success"
    error: Optional[Any] = None
    status_code: int = 200

    @classmethod
    def ok(
        cls,
        data: Optional[Any] = None,
        message: str = "OK",
        status_code: int = 200,
    ) -> "SafeResult":
        return cls(
            success=True,
            data=DataProcessor.to_json_compatible(data),
            message=message,
            status_code=status_code,
        )

    @classmethod
    def fail(
        cls,
        error: Any = "Error",
        data: Optional[Any] = None,
        status_code: int = 400,
        message: Optional[str] = None,
    ) -> "SafeResult":
        return cls(
            success=False,
            data=DataProcessor.to_json_compatible(data),
            message=message or str(error),
            error=DataProcessor.to_json_compatible(error),
            status_code=status_code,
        )

    def to_dict(self) -> Dict[str, Any]:
        return {
            "success": self.success,
            "data": self.data,
            "message": self.message,
            "error": self.error,
            "status_code": self.status_code,
        }

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), indent=2, ensure_ascii=False)


class LocalFileStore:
    DOCUMENT_SUFFIX = ".document.json"
    STORAGE_BACKEND = "local_file_store"
    max_value_bytes = DEFAULT_LOCAL_MAX_VALUE_BYTES
    max_document_bytes = DEFAULT_LOCAL_MAX_DOCUMENT_BYTES
    max_image_bytes = DEFAULT_LOCAL_MAX_IMAGE_BYTES

    @staticmethod
    def _json_size_bytes(value: Any) -> int:
        return len(json.dumps(DataProcessor.to_json_compatible(value), ensure_ascii=False, default=str).encode("utf-8"))

    def _limit_data(self, kind: str, size_bytes: int, max_bytes: int, extra: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        data = {
            "storage_backend": self.STORAGE_BACKEND,
            "kind": kind,
            "size_bytes": int(size_bytes),
            "max_bytes": int(max_bytes),
            "recommended_backend": "https://businessprocessapplications.com",
            "recommended_pipeline": "Use hosted ZMongo for large documents/files; backend ZEmbedder.py handles chunking and embeddings.",
        }
        if extra:
            data.update(extra)
        return data

    def _limit_result(self, kind: str, size_bytes: int, max_bytes: int, extra: Optional[Dict[str, Any]] = None) -> SafeResult:
        return SafeResult.fail(
            error={"error_type": "LocalStorageItemTooLarge", **self._limit_data(kind, size_bytes, max_bytes, extra)},
            data=self._limit_data(kind, size_bytes, max_bytes, extra),
            status_code=413,
            message=f"Local File Store {kind} is too large: {size_bytes} bytes exceeds {max_bytes} bytes.",
        )

    def _limit_info(self) -> Dict[str, Any]:
        return {
            "local_max_value_bytes": int(self.max_value_bytes),
            "local_max_document_bytes": int(self.max_document_bytes),
            "local_max_image_bytes": int(self.max_image_bytes),
            "large_document_backend": "https://businessprocessapplications.com",
            "large_document_chunker": "ZEmbedder.py",
        }

    def __init__(self, root_dir: Optional[Union[str, Path]] = None) -> None:
        if root_dir is None:
            root_dir = Path(__file__).resolve().parent / "local_store"

        self.root_dir = Path(root_dir).expanduser().resolve()
        self.collections_dir = self.root_dir / "collections"
        self.manifest_path = self.root_dir / "manifest.json"
        self._lock = threading.RLock()

        self.collections_dir.mkdir(parents=True, exist_ok=True)
        self._ensure_manifest()

    @staticmethod
    def _now() -> str:
        return datetime.now(timezone.utc).isoformat()

    @staticmethod
    def _new_id() -> str:
        stamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        return f"local_{stamp}_{uuid.uuid4().hex[:8]}"

    @staticmethod
    def _safe_name(value: Any, default: str = "item") -> str:
        text = str(value or default).strip()
        text = re.sub(r"[^a-zA-Z0-9._-]+", "_", text)
        text = text.strip("._-")
        return text or default

    @staticmethod
    def _safe_collection_name(collection: str) -> str:
        name = LocalFileStore._safe_name(collection, "default")
        if name in {".", ".."}:
            return "default"
        return name

    @staticmethod
    def _safe_field_name(field_path: Optional[str]) -> str:
        if not field_path:
            return "asset"
        return LocalFileStore._safe_name(field_path.replace(".", "_"), "asset")

    @staticmethod
    def _json_loads_maybe(value: Any) -> Any:
        if not isinstance(value, str):
            return value

        stripped = value.strip()
        if not stripped:
            return value

        if not (
            stripped.startswith("{")
            or stripped.startswith("[")
            or stripped in {"true", "false", "null"}
            or re.fullmatch(r"-?\d+(\.\d+)?", stripped)
        ):
            return value

        try:
            return json.loads(stripped)
        except Exception:
            return value

    @staticmethod
    def _file_mime(filename: str) -> str:
        return mimetypes.guess_type(filename)[0] or "application/octet-stream"

    @staticmethod
    def _extension_from_filename(filename: str, default: str = ".bin") -> str:
        suffix = Path(filename).suffix
        return suffix if suffix else default

    def _collection_dir(self, collection: str) -> Path:
        coll = self._safe_collection_name(collection)
        path = self.collections_dir / coll
        path.mkdir(parents=True, exist_ok=True)
        return path

    def _document_path(self, collection: str, document_id: str) -> Path:
        coll_dir = self._collection_dir(collection)
        safe_id = self._safe_name(document_id, self._new_id())
        return coll_dir / f"{safe_id}{self.DOCUMENT_SUFFIX}"

    def _relative(self, path: Path) -> str:
        return path.resolve().relative_to(self.root_dir).as_posix()

    def _absolute_from_relative(self, path: str) -> Path:
        candidate = (self.root_dir / path).resolve()
        root = self.root_dir.resolve()
        if root not in candidate.parents and candidate != root:
            raise ValueError(f"Path escapes local store root: {path}")
        return candidate

    def _ensure_manifest(self) -> None:
        if self.manifest_path.exists():
            return

        manifest = {
            "storage_backend": self.STORAGE_BACKEND,
            "version": 1,
            "created_at": self._now(),
            "updated_at": self._now(),
            "root_dir": str(self.root_dir),
            "description": "ComfyUI-ZMongo Local File Store",
            "limits": self._limit_info(),
        }
        self._write_json(self.manifest_path, manifest)

    def _touch_manifest(self) -> None:
        manifest = self._read_json(self.manifest_path, default={})
        manifest["updated_at"] = self._now()
        manifest["root_dir"] = str(self.root_dir)
        self._write_json(self.manifest_path, manifest)

    @staticmethod
    def _read_json(path: Path, default: Optional[Any] = None) -> Any:
        if not path.exists():
            return copy.deepcopy(default)
        with path.open("r", encoding="utf-8") as f:
            return json.load(f)

    @staticmethod
    def _write_json(path: Path, data: Any) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        safe_data = DataProcessor.to_json_compatible(data)

        fd, tmp_name = tempfile.mkstemp(
            prefix=f".{path.name}.",
            suffix=".tmp",
            dir=str(path.parent),
            text=True,
        )

        try:
            with os.fdopen(fd, "w", encoding="utf-8") as f:
                json.dump(safe_data, f, indent=2, ensure_ascii=False)
                f.write("\n")
            os.replace(tmp_name, path)
        finally:
            if os.path.exists(tmp_name):
                os.unlink(tmp_name)

    def _iter_document_paths(self, collection: str) -> Iterable[Path]:
        coll_dir = self._collection_dir(collection)
        yield from sorted(coll_dir.glob(f"*{self.DOCUMENT_SUFFIX}"))

    def _read_document_path(self, path: Path) -> Dict[str, Any]:
        doc = self._read_json(path, default={})
        return doc if isinstance(doc, dict) else {}

    def _find_document_path(
        self,
        collection: str,
        *,
        document_id: Optional[str] = None,
        query: Optional[Dict[str, Any]] = None,
    ) -> Optional[Path]:
        if document_id:
            direct = self._document_path(collection, document_id)
            if direct.exists():
                return direct

            for path in self._iter_document_paths(collection):
                doc = self._read_document_path(path)
                if str(doc.get("_id")) == str(document_id):
                    return path

            return None

        if query:
            for path in self._iter_document_paths(collection):
                doc = self._read_document_path(path)
                if self._matches_query(doc, query):
                    return path

        return None

    def _matches_query(self, doc: Dict[str, Any], query: Dict[str, Any]) -> bool:
        if not query:
            return True

        for key, expected in query.items():
            actual = DataProcessor.get_value(doc, key)
            if actual != expected and str(actual) != str(expected):
                return False

        return True

    def _base_document(
        self,
        collection: str,
        document_id: Optional[str] = None,
        initial: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        now = self._now()
        doc = dict(initial or {})
        doc.setdefault("_id", document_id or self._new_id())
        doc.setdefault("collection", self._safe_collection_name(collection))
        doc.setdefault("created_at", now)
        doc["updated_at"] = now
        doc["storage_backend"] = self.STORAGE_BACKEND
        return doc

    def ping(self) -> SafeResult:
        try:
            with self._lock:
                self._ensure_manifest()
                return SafeResult.ok(
                    {
                        "ok": True,
                        "storage_backend": self.STORAGE_BACKEND,
                        "root_dir": str(self.root_dir),
                        "manifest": str(self.manifest_path),
                        **self._limit_info(),
                    }
                )
        except Exception as exc:
            return self._fail(exc, "ping")

    def list_collections(self) -> SafeResult:
        try:
            with self._lock:
                names = [
                    p.name
                    for p in sorted(self.collections_dir.iterdir())
                    if p.is_dir()
                ]
                return SafeResult.ok(
                    {
                        "collections": names,
                        "count": len(names),
                        "storage_backend": self.STORAGE_BACKEND,
                    }
                )
        except Exception as exc:
            return self._fail(exc, "list_collections")

    def create_collection(self, collection: str) -> SafeResult:
        try:
            with self._lock:
                coll = self._safe_collection_name(collection)
                path = self._collection_dir(coll)
                self._touch_manifest()
                return SafeResult.ok(
                    {
                        "collection": coll,
                        "path": str(path),
                        "storage_backend": self.STORAGE_BACKEND,
                    }
                )
        except Exception as exc:
            return self._fail(exc, "create_collection")

    def list_documents(
        self,
        collection: str,
        query: Optional[Dict[str, Any]] = None,
        *,
        limit: int = 100,
        include_documents: bool = True,
    ) -> SafeResult:
        try:
            with self._lock:
                coll = self._safe_collection_name(collection)
                docs: List[Dict[str, Any]] = []

                for path in self._iter_document_paths(coll):
                    doc = self._read_document_path(path)
                    if not self._matches_query(doc, query or {}):
                        continue

                    item = doc if include_documents else {
                        "_id": doc.get("_id"),
                        "collection": doc.get("collection", coll),
                        "created_at": doc.get("created_at"),
                        "updated_at": doc.get("updated_at"),
                        "document_path": self._relative(path),
                    }
                    docs.append(item)

                    if len(docs) >= max(0, int(limit)):
                        break

                return SafeResult.ok(
                    {
                        "documents": docs,
                        "count": len(docs),
                        "collection": coll,
                        "query": query or {},
                        "storage_backend": self.STORAGE_BACKEND,
                    }
                )
        except Exception as exc:
            return self._fail(exc, "list_documents")

    def save_document(
        self,
        collection: str,
        document: Dict[str, Any],
        *,
        document_id: Optional[str] = None,
        upsert: bool = True,
    ) -> SafeResult:
        try:
            with self._lock:
                coll = self._safe_collection_name(collection)

                if not isinstance(document, dict):
                    return SafeResult.fail(
                        error={"error_type": "ValueError", "error": "document must be a dict"},
                        message="document must be a dict",
                        status_code=400,
                    )

                safe_doc = DataProcessor.to_json_compatible(document)
                safe_doc_size = self._json_size_bytes(safe_doc)
                if safe_doc_size > self.max_document_bytes:
                    return self._limit_result("document", safe_doc_size, self.max_document_bytes, {"collection": coll})
                target_id = document_id or safe_doc.get("_id")

                existing_path = self._find_document_path(coll, document_id=target_id) if target_id else None

                if existing_path and existing_path.exists():
                    existing_doc = self._read_document_path(existing_path)
                    created_at = existing_doc.get("created_at") or self._now()
                    final_doc = dict(existing_doc)
                    final_doc.update(safe_doc)
                    final_doc["_id"] = existing_doc.get("_id") or target_id
                    final_doc["created_at"] = created_at
                    final_doc["updated_at"] = self._now()
                    final_doc["collection"] = coll
                    final_doc["storage_backend"] = self.STORAGE_BACKEND
                    path = existing_path
                    operation = "updated_existing"
                else:
                    if target_id is None:
                        target_id = self._new_id()

                    if not upsert:
                        return SafeResult.fail(
                            error={"error_type": "NotFound", "error": "document not found and upsert is False"},
                            message="document not found and upsert is False",
                            status_code=404,
                        )

                    final_doc = self._base_document(coll, document_id=str(target_id), initial=safe_doc)
                    path = self._document_path(coll, str(final_doc["_id"]))
                    operation = "inserted_new"

                self._write_json(path, final_doc)
                self._touch_manifest()

                return SafeResult.ok(
                    {
                        "operation": operation,
                        "document_id": final_doc["_id"],
                        "collection": coll,
                        "document_path": self._relative(path),
                        "document": final_doc,
                        "storage_backend": self.STORAGE_BACKEND,
                    }
                )
        except Exception as exc:
            return self._fail(exc, "save_document")

    def load_document(
        self,
        collection: str,
        *,
        document_id: Optional[str] = None,
        query: Optional[Dict[str, Any]] = None,
    ) -> SafeResult:
        try:
            with self._lock:
                coll = self._safe_collection_name(collection)

                path = self._find_document_path(
                    coll,
                    document_id=document_id,
                    query=query,
                )

                if not path:
                    return SafeResult.fail(
                        error={
                            "error_type": "NotFound",
                            "collection": coll,
                            "document_id": document_id,
                            "query": query or {},
                        },
                        message="Document not found in Local File Store",
                        status_code=404,
                    )

                doc = self._read_document_path(path)

                return SafeResult.ok(
                    {
                        "document": doc,
                        "document_id": doc.get("_id"),
                        "collection": coll,
                        "document_path": self._relative(path),
                        "storage_backend": self.STORAGE_BACKEND,
                    }
                )
        except Exception as exc:
            return self._fail(exc, "load_document")

    def delete_document(
        self,
        collection: str,
        *,
        document_id: Optional[str] = None,
        query: Optional[Dict[str, Any]] = None,
        delete_assets: bool = False,
    ) -> SafeResult:
        try:
            with self._lock:
                coll = self._safe_collection_name(collection)
                path = self._find_document_path(coll, document_id=document_id, query=query)

                if not path:
                    return SafeResult.fail(
                        error={"error_type": "NotFound", "collection": coll},
                        message="Document not found",
                        status_code=404,
                    )

                doc = self._read_document_path(path)
                deleted_assets: List[str] = []

                if delete_assets:
                    flat = DataProcessor.flatten_json(doc)
                    for key, value in flat.items():
                        if key.endswith("local_path") and isinstance(value, str):
                            try:
                                asset_path = self._absolute_from_relative(value)
                                if asset_path.exists():
                                    asset_path.unlink()
                                    deleted_assets.append(value)
                            except Exception:
                                pass

                path.unlink()
                self._touch_manifest()

                return SafeResult.ok(
                    {
                        "deleted_count": 1,
                        "document_id": doc.get("_id"),
                        "collection": coll,
                        "deleted_assets": deleted_assets,
                        "storage_backend": self.STORAGE_BACKEND,
                    }
                )
        except Exception as exc:
            return self._fail(exc, "delete_document")

    def save_value(
        self,
        collection: str,
        value: Any,
        *,
        document_id: Optional[str] = None,
        query: Optional[Dict[str, Any]] = None,
        field_path: Optional[str] = None,
        upsert: bool = True,
        parse_json_strings: bool = True,
    ) -> SafeResult:
        try:
            with self._lock:
                coll = self._safe_collection_name(collection)

                parsed_value = self._json_loads_maybe(value) if parse_json_strings else value
                parsed_value = DataProcessor.to_json_compatible(parsed_value)
                parsed_value_size = self._json_size_bytes(parsed_value)
                if parsed_value_size > self.max_value_bytes:
                    return self._limit_result("value", parsed_value_size, self.max_value_bytes, {
                        "collection": coll,
                        "document_id": document_id,
                        "field_path": field_path,
                    })

                path = self._find_document_path(
                    coll,
                    document_id=document_id,
                    query=query,
                )

                if path:
                    doc = self._read_document_path(path)
                    operation = "updated_existing"
                else:
                    if not upsert:
                        return SafeResult.fail(
                            error={
                                "error_type": "ValueError",
                                "error": "query or document_id is required, or upsert must be True",
                            },
                            message="Cannot determine target document",
                            status_code=400,
                        )

                    initial: Dict[str, Any] = {}

                    if query:
                        for k, v in query.items():
                            if not k.startswith("$"):
                                DataProcessor.set_value(initial, k, v)

                    doc = self._base_document(coll, document_id=document_id, initial=initial)
                    path = self._document_path(coll, str(doc["_id"]))
                    operation = "inserted_new"

                if field_path and str(field_path).strip():
                    ok = DataProcessor.set_value(doc, str(field_path).strip(), parsed_value)
                    if not ok:
                        return SafeResult.fail(
                            error={
                                "error_type": "ValueError",
                                "error": f"Could not set field_path: {field_path}",
                            },
                            message=f"Could not set field_path: {field_path}",
                            status_code=400,
                        )
                elif isinstance(parsed_value, dict):
                    doc.update(parsed_value)
                else:
                    doc["value"] = parsed_value

                doc["updated_at"] = self._now()
                doc["collection"] = coll
                doc["storage_backend"] = self.STORAGE_BACKEND

                doc_size = self._json_size_bytes(doc)
                if doc_size > self.max_document_bytes:
                    return self._limit_result("document", doc_size, self.max_document_bytes, {
                        "collection": coll,
                        "document_id": doc.get("_id"),
                        "field_path": field_path,
                    })

                self._write_json(path, doc)
                self._touch_manifest()

                return SafeResult.ok(
                    {
                        "operation": operation,
                        "document_id": doc.get("_id"),
                        "collection": coll,
                        "field_path": field_path,
                        "saved_value": parsed_value,
                        "document": doc,
                        "document_path": self._relative(path),
                        "storage_backend": self.STORAGE_BACKEND,
                    }
                )
        except Exception as exc:
            return self._fail(exc, "save_value")

    def get_value(
        self,
        collection: str,
        *,
        field_path: Optional[str] = None,
        document_id: Optional[str] = None,
        query: Optional[Dict[str, Any]] = None,
        default: Any = None,
    ) -> SafeResult:
        try:
            with self._lock:
                loaded = self.load_document(
                    collection,
                    document_id=document_id,
                    query=query,
                )

                if not loaded.success:
                    return loaded

                doc = loaded.data.get("document") if isinstance(loaded.data, dict) else None

                if not isinstance(doc, dict):
                    return SafeResult.fail(
                        error={"error_type": "ValueError", "error": "Loaded document is not a dict"},
                        message="Loaded document is not a dict",
                        status_code=500,
                    )

                value = DataProcessor.get_value(doc, field_path or "") if field_path else doc

                if value is None:
                    value = default

                return SafeResult.ok(
                    {
                        "value": value,
                        "field_path": field_path,
                        "document_id": doc.get("_id"),
                        "collection": self._safe_collection_name(collection),
                        "storage_backend": self.STORAGE_BACKEND,
                    }
                )
        except Exception as exc:
            return self._fail(exc, "get_value")

    def list_flattened_key_paths(
        self,
        collection: str,
        *,
        document_id: Optional[str] = None,
        query: Optional[Dict[str, Any]] = None,
        include_values: bool = False,
    ) -> SafeResult:
        try:
            with self._lock:
                loaded = self.load_document(
                    collection,
                    document_id=document_id,
                    query=query,
                )

                if not loaded.success:
                    return loaded

                doc = loaded.data.get("document") if isinstance(loaded.data, dict) else None

                if not isinstance(doc, dict):
                    return SafeResult.fail(
                        error={"error_type": "ValueError", "error": "Loaded document is not a dict"},
                        message="Loaded document is not a dict",
                        status_code=500,
                    )

                flat = DataProcessor.flatten_json(doc)
                paths = sorted(flat.keys())

                data: Dict[str, Any] = {
                    "paths": paths,
                    "count": len(paths),
                    "document_id": doc.get("_id"),
                    "collection": self._safe_collection_name(collection),
                    "storage_backend": self.STORAGE_BACKEND,
                }

                if include_values:
                    data["flat"] = flat

                return SafeResult.ok(data)
        except Exception as exc:
            return self._fail(exc, "list_flattened_key_paths")

    def save_image(
        self,
        collection: str,
        image: Any,
        *,
        filename: str = "comfy_image.png",
        document_id: Optional[str] = None,
        query: Optional[Dict[str, Any]] = None,
        field_path: str = "image_data",
        metadata: Optional[Dict[str, Any]] = None,
        upsert: bool = True,
    ) -> SafeResult:
        try:
            with self._lock:
                coll = self._safe_collection_name(collection)
                image_bytes = self._coerce_image_bytes(image)
                if len(image_bytes) > self.max_image_bytes:
                    return self._limit_result("image", len(image_bytes), self.max_image_bytes, {"collection": coll, "filename": filename})
                safe_filename = self._safe_name(filename, "comfy_image.png")

                path = self._find_document_path(
                    coll,
                    document_id=document_id,
                    query=query,
                )

                if path:
                    doc = self._read_document_path(path)
                    operation = "updated_existing"
                else:
                    if not upsert:
                        return SafeResult.fail(
                            error={
                                "error_type": "ValueError",
                                "error": "query or document_id is required when upsert is False",
                            },
                            message="Cannot determine target document",
                            status_code=400,
                        )

                    initial: Dict[str, Any] = {}

                    if query:
                        for k, v in query.items():
                            if not k.startswith("$"):
                                DataProcessor.set_value(initial, k, v)

                    doc = self._base_document(coll, document_id=document_id, initial=initial)
                    path = self._document_path(coll, str(doc["_id"]))
                    operation = "inserted_new"

                document_id_final = str(doc["_id"])
                field_token = self._safe_field_name(field_path)
                ext = self._extension_from_filename(safe_filename, ".png")
                asset_name = f"{document_id_final}_{field_token}_{uuid.uuid4().hex[:8]}{ext}"
                asset_path = self._collection_dir(coll) / asset_name
                asset_path.write_bytes(image_bytes)

                sha256 = hashlib.sha256(image_bytes).hexdigest()

                image_record = {
                    "storage_policy": "local_file",
                    "local_path": self._relative(asset_path),
                    "filename": safe_filename,
                    "size_bytes": len(image_bytes),
                    "sha256": sha256,
                    "content_type": self._file_mime(safe_filename),
                    "saved_at": self._now(),
                }

                if metadata:
                    image_record["metadata"] = DataProcessor.to_json_compatible(metadata)

                ok = DataProcessor.set_value(doc, field_path, image_record)

                if not ok:
                    return SafeResult.fail(
                        error={
                            "error_type": "ValueError",
                            "error": f"Could not set image field_path: {field_path}",
                        },
                        message=f"Could not set image field_path: {field_path}",
                        status_code=400,
                    )

                doc["updated_at"] = self._now()
                doc["collection"] = coll
                doc["storage_backend"] = self.STORAGE_BACKEND

                self._write_json(path, doc)
                self._touch_manifest()

                return SafeResult.ok(
                    {
                        "operation": operation,
                        "document_id": document_id_final,
                        "collection": coll,
                        "field_path": field_path,
                        "filename": safe_filename,
                        "size_bytes": len(image_bytes),
                        "local_path": image_record["local_path"],
                        "image_data": image_record,
                        "document": doc,
                        "document_path": self._relative(path),
                        "storage_backend": self.STORAGE_BACKEND,
                    }
                )
        except Exception as exc:
            return self._fail(exc, "save_image")

    def load_image(
        self,
        collection: str,
        *,
        document_id: Optional[str] = None,
        query: Optional[Dict[str, Any]] = None,
        field_path: str = "image_data",
        as_base64: bool = False,
    ) -> SafeResult:
        try:
            with self._lock:
                loaded = self.load_document(
                    collection,
                    document_id=document_id,
                    query=query,
                )

                if not loaded.success:
                    return loaded

                doc = loaded.data.get("document") if isinstance(loaded.data, dict) else None

                if not isinstance(doc, dict):
                    return SafeResult.fail(
                        error={"error_type": "ValueError", "error": "Loaded document is not a dict"},
                        message="Loaded document is not a dict",
                        status_code=500,
                    )

                image_record = DataProcessor.get_value(doc, field_path)

                if not isinstance(image_record, dict):
                    return SafeResult.fail(
                        error={
                            "error_type": "NotFound",
                            "field_path": field_path,
                            "document_id": doc.get("_id"),
                        },
                        message="Image field_path not found",
                        status_code=404,
                    )

                local_path = image_record.get("local_path")

                if not local_path:
                    return SafeResult.fail(
                        error={
                            "error_type": "ValueError",
                            "error": f"No local_path at {field_path}",
                        },
                        message=f"No local_path at {field_path}",
                        status_code=404,
                    )

                asset_path = self._absolute_from_relative(local_path)

                if not asset_path.exists():
                    return SafeResult.fail(
                        error={
                            "error_type": "FileNotFoundError",
                            "local_path": local_path,
                            "absolute_path": str(asset_path),
                        },
                        message="Local image file not found",
                        status_code=404,
                    )

                image_bytes = asset_path.read_bytes()

                payload: Dict[str, Any] = {
                    "document_id": doc.get("_id"),
                    "collection": self._safe_collection_name(collection),
                    "field_path": field_path,
                    "filename": image_record.get("filename") or asset_path.name,
                    "local_path": local_path,
                    "absolute_path": str(asset_path),
                    "size_bytes": len(image_bytes),
                    "content_type": image_record.get("content_type") or self._file_mime(asset_path.name),
                    "storage_backend": self.STORAGE_BACKEND,
                }

                if as_base64:
                    payload["base64"] = base64.b64encode(image_bytes).decode("ascii")
                    payload["data_uri"] = f"{payload['content_type']};base64,{payload['base64']}"
                else:
                    payload["bytes"] = image_bytes

                return SafeResult.ok(payload)
        except Exception as exc:
            return self._fail(exc, "load_image")

    @staticmethod
    def select_nth_item(items: Any, index: int = 0, default: Any = None) -> SafeResult:
        try:
            if isinstance(items, dict):
                if "documents" in items and isinstance(items["documents"], list):
                    seq = items["documents"]
                elif "collections" in items and isinstance(items["collections"], list):
                    seq = items["collections"]
                elif "paths" in items and isinstance(items["paths"], list):
                    seq = items["paths"]
                else:
                    seq = list(items.values())
            elif isinstance(items, (list, tuple)):
                seq = list(items)
            else:
                seq = []

            idx = int(index)
            value = seq[idx] if 0 <= idx < len(seq) else default

            return SafeResult.ok(
                {
                    "index": idx,
                    "value": value,
                    "count": len(seq),
                    "found": 0 <= idx < len(seq),
                }
            )
        except Exception as exc:
            return SafeResult.fail(
                error={"error_type": type(exc).__name__, "error": str(exc)},
                message=str(exc),
                status_code=500,
            )

    def get_storage_summary(self) -> SafeResult:
        try:
            with self._lock:
                file_count = 0
                total_bytes = 0
                collection_count = 0

                for coll_dir in self.collections_dir.iterdir():
                    if not coll_dir.is_dir():
                        continue

                    collection_count += 1

                    for path in coll_dir.rglob("*"):
                        if path.is_file():
                            file_count += 1
                            total_bytes += path.stat().st_size

                return SafeResult.ok(
                    {
                        "storage_backend": self.STORAGE_BACKEND,
                        "root_dir": str(self.root_dir),
                        "collections": collection_count,
                        "files": file_count,
                        "total_bytes": total_bytes,
                        "total_mb": round(total_bytes / (1024 * 1024), 4),
                        **self._limit_info(),
                    }
                )
        except Exception as exc:
            return self._fail(exc, "get_storage_summary")

    def _coerce_image_bytes(self, image: Any) -> bytes:
        if isinstance(image, (bytes, bytearray, memoryview)):
            return bytes(image)

        if isinstance(image, Path):
            return image.expanduser().read_bytes()

        if isinstance(image, str):
            text = image.strip()

            if text.startswith("data:") and "," in text:
                _, b64 = text.split(",", 1)
                return base64.b64decode(b64)

            candidate = Path(text).expanduser()

            if candidate.exists() and candidate.is_file():
                return candidate.read_bytes()

            cleaned = text

            if cleaned.startswith("```"):
                cleaned = re.sub(r"^```[a-zA-Z0-9_-]*\s*", "", cleaned)
                cleaned = re.sub(r"\s*```$", "", cleaned)

            if len(cleaned) > 64 and " " not in cleaned:
                try:
                    return base64.b64decode(cleaned, validate=True)
                except binascii.Error:
                    pass

            return cleaned.encode("utf-8")

        try:
            from PIL import Image
            import io

            if isinstance(image, Image.Image):
                buf = io.BytesIO()
                image.save(buf, format="PNG")
                return buf.getvalue()
        except Exception:
            pass

        try:
            import numpy as np
            from PIL import Image
            import io

            if isinstance(image, np.ndarray):
                arr = image

                if arr.dtype != np.uint8:
                    if arr.max() <= 1.0:
                        arr = (arr * 255).clip(0, 255).astype(np.uint8)
                    else:
                        arr = arr.clip(0, 255).astype(np.uint8)

                pil = Image.fromarray(arr)
                buf = io.BytesIO()
                pil.save(buf, format="PNG")
                return buf.getvalue()
        except Exception:
            pass

        try:
            import torch
            import numpy as np
            from PIL import Image
            import io

            if isinstance(image, torch.Tensor):
                tensor = image.detach().cpu()

                if tensor.ndim == 4:
                    tensor = tensor[0]

                if tensor.ndim == 3 and tensor.shape[0] in (1, 3, 4):
                    tensor = tensor.permute(1, 2, 0)

                arr = tensor.numpy()

                if arr.max() <= 1.0:
                    arr = (arr * 255).clip(0, 255).astype(np.uint8)
                else:
                    arr = arr.clip(0, 255).astype(np.uint8)

                if arr.ndim == 3 and arr.shape[2] == 1:
                    arr = arr[:, :, 0]

                pil = Image.fromarray(arr)
                buf = io.BytesIO()
                pil.save(buf, format="PNG")
                return buf.getvalue()
        except Exception:
            pass

        safe = DataProcessor.to_json_compatible(image)
        return json.dumps(safe, ensure_ascii=False).encode("utf-8")

    @staticmethod
    def _fail(exc: Exception, operation: str) -> SafeResult:
        return SafeResult.fail(
            error={
                "error_type": type(exc).__name__,
                "error": str(exc),
                "operation": operation,
            },
            message=str(exc),
            status_code=500,
        )


if __name__ == "__main__":
    store = LocalFileStore()

    print("PING")
    print(store.ping().to_json())

    print("\nSAVE DOCUMENT")
    saved = store.save_document(
        "images",
        {
            "metadata": {
                "prompt": {
                    "positive": "cat astronaut",
                    "negative": "blurry",
                },
                "seed": 42,
            }
        },
    )
    print(saved.to_json())

    doc_id = saved.data["document_id"]

    print("\nSAVE VALUE")
    print(
        store.save_value(
            "images",
            "orange tabby in a space helmet",
            document_id=doc_id,
            field_path="metadata.prompt.positive",
        ).to_json()
    )

    print("\nGET VALUE")
    print(
        store.get_value(
            "images",
            document_id=doc_id,
            field_path="metadata.prompt.positive",
        ).to_json()
    )

    print("\nSAVE IMAGE")
    fake_png = b"\x89PNG\r\n\x1a\n" + os.urandom(128)
    print(
        store.save_image(
            "images",
            fake_png,
            document_id=doc_id,
            filename="comfy_image.png",
            field_path="image_data",
            metadata={"source": "smoke_test"},
        ).to_json()
    )

    print("\nFLATTENED PATHS")
    print(
        store.list_flattened_key_paths(
            "images",
            document_id=doc_id,
            include_values=False,
        ).to_json()
    )

    print("\nLIST DOCUMENTS")
    print(store.list_documents("images", include_documents=False).to_json())

    print("\nSUMMARY")
    print(store.get_storage_summary().to_json())