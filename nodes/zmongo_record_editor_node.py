import json
import logging
from typing import Any, Dict, List, Tuple

from bson import ObjectId

from .zmongo_toolbag.zmongo import ZMongo
from .zmongo_toolbag.data_processor import DataProcessor

logger = logging.getLogger(__name__)


class ZMongoRecordEditorNode:
    CATEGORY = "ZMongo"
    FUNCTION = "get_record"
    RETURN_TYPES = ("STRING", "STRING", "STRING", "STRING")
    RETURN_NAMES = (
        "record_json",
        "record_id",
        "save_status",
        "flattened_field_names_json",
    )

    @classmethod
    def _get_zmongo(cls) -> ZMongo:
        return ZMongo()

    @staticmethod
    def _clean_record_id(value: Any) -> str:
        raw = "" if value is None else str(value)
        cleaned = raw.strip()

        if len(cleaned) >= 2 and cleaned[0] == cleaned[-1] and cleaned[0] in {"'", '"'}:
            cleaned = cleaned[1:-1].strip()

        return cleaned

    @classmethod
    def _build_id_candidates(cls, record_id: Any) -> List[Any]:
        cleaned = cls._clean_record_id(record_id)
        if not cleaned:
            return []

        candidates: List[Any] = [cleaned]
        if ObjectId.is_valid(cleaned):
            candidates.insert(0, ObjectId(cleaned))
        return candidates

    @staticmethod
    def _stringify_value(value: Any) -> str:
        if value is None:
            return ""
        if isinstance(value, str):
            return value
        try:
            return json.dumps(
                DataProcessor.to_json_compatible(value),
                ensure_ascii=False,
                indent=2,
                default=str,
            )
        except Exception:
            return str(value)

    @classmethod
    def get_collection_names(cls) -> List[str]:
        zmongo = None
        try:
            zmongo = cls._get_zmongo()
            result = zmongo.run_sync(zmongo.list_collections_async)

            if not result or not getattr(result, "success", False):
                logger.warning(
                    "Could not load collection names: %s",
                    getattr(result, "error", None),
                )
                return ["<no_collections_found>"]

            data = result.data or {}
            if not isinstance(data, dict):
                return ["<no_collections_found>"]

            names = data.get("collections", [])
            if not isinstance(names, list):
                return ["<no_collections_found>"]

            names = [str(name) for name in names if str(name).strip()]
            return names or ["<no_collections_found>"]

        except Exception as exc:
            logger.exception("Error loading collection names: %s", exc)
            return ["<mongo_error>"]
        finally:
            if zmongo:
                try:
                    zmongo.close()
                except Exception:
                    pass

    @classmethod
    def fetch_record(
        cls,
        collection_name: str,
        record_id: str,
        fallback_record_json: str = "",
    ) -> Dict[str, Any]:
        zmongo = None
        try:
            clean_id = cls._clean_record_id(record_id)

            if clean_id:
                if not collection_name or collection_name.startswith("<"):
                    return {}

                zmongo = cls._get_zmongo()

                for candidate_id in cls._build_id_candidates(clean_id):
                    result = zmongo.find_one(collection_name, {"_id": candidate_id})

                    if result and getattr(result, "success", False):
                        doc = result.original() if hasattr(result, "original") else result.data
                        if isinstance(doc, dict) and doc:
                            return doc

                logger.warning(
                    "Could not fetch record '%s' from '%s' using string/ObjectId lookup.",
                    clean_id,
                    collection_name,
                )
                return {}

            if fallback_record_json and str(fallback_record_json).strip():
                parsed = json.loads(fallback_record_json)
                return parsed if isinstance(parsed, dict) else {}

            return {}

        except Exception as exc:
            logger.exception(
                "Error fetching record for collection='%s', record_id='%s': %s",
                collection_name,
                record_id,
                exc,
            )
            return {}
        finally:
            if zmongo:
                try:
                    zmongo.close()
                except Exception:
                    pass

    @classmethod
    def get_flattened_record_pairs(
        cls,
        collection_name: str,
        record_id: str,
        fallback_record_json: str = "",
    ) -> List[Tuple[str, str]]:
        record = cls.fetch_record(collection_name, record_id, fallback_record_json)
        if not record:
            return []

        flat = DataProcessor.flatten_json(record)
        if not isinstance(flat, dict):
            return []

        return [
            (str(path), cls._stringify_value(value))
            for path, value in sorted(flat.items(), key=lambda item: str(item[0]))
        ]

    @classmethod
    def INPUT_TYPES(cls) -> Dict[str, Any]:
        collections = cls.get_collection_names()
        default_collection = collections[0] if collections else "<no_collections_found>"

        return {
            "required": {
                "collection_name": (collections, {"default": default_collection}),
                "record_id": ("STRING", {"default": "", "multiline": False}),
            },
            "optional": {
                "selected_record_json": ("STRING", {"default": "", "forceInput": True}),
                "save_status_in": ("STRING", {"default": "", "forceInput": True}),
            },
        }

    @classmethod
    def VALIDATE_INPUTS(
        cls,
        collection_name: str,
        record_id: str,
        selected_record_json: str = "",
        save_status_in: str = "",
    ):
        return True

    @classmethod
    def IS_CHANGED(
        cls,
        collection_name: str,
        record_id: str,
        selected_record_json: str = "",
        save_status_in: str = "",
    ):
        return f"{collection_name}|{record_id}|{selected_record_json}|{save_status_in}"

    def get_record(
        self,
        collection_name: str,
        record_id: str,
        selected_record_json: str = "",
        save_status_in: str = "",
    ):
        record = self.fetch_record(
            collection_name=collection_name,
            record_id=record_id,
            fallback_record_json=selected_record_json,
        )

        if not record:
            return (
                "{}",
                self._clean_record_id(record_id),
                str(save_status_in or ""),
                json.dumps([], indent=2),
            )

        resolved_record_id = str(record.get("_id", self._clean_record_id(record_id)))
        flattened_paths = [
            path
            for path, _ in self.get_flattened_record_pairs(
                collection_name=collection_name,
                record_id=resolved_record_id,
                fallback_record_json=selected_record_json,
            )
        ]

        return (
            json.dumps(
                DataProcessor.to_json_compatible(record),
                ensure_ascii=False,
                indent=2,
                default=str,
            ),
            resolved_record_id,
            str(save_status_in or ""),
            json.dumps(flattened_paths, ensure_ascii=False, indent=2),
        )


NODE_CLASS_MAPPINGS = {
    "ZMongoRecordEditorNode": ZMongoRecordEditorNode,
}

NODE_DISPLAY_NAME_MAPPINGS = {
    "ZMongoRecordEditorNode": "📝 ZMongo Record Editor",
}