import json
import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from ..zmongo_toolbag.data_processor import DataProcessor
from ..zmongo_toolbag.safe_result import SafeResult
from ..zmongo_toolbag.zmongo import ZMongo

logger = logging.getLogger(__name__)


def _safe_json(obj: Any) -> str:
    try:
        if isinstance(obj, SafeResult):
            return obj.to_json(indent=2)
        return DataProcessor.to_json(obj, indent=2)
    except Exception as exc:
        return json.dumps({"error": f"Serialization failed: {exc}"}, indent=2)


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _extract_document_from_result(result: SafeResult) -> Optional[Dict[str, Any]]:
    if not isinstance(result, SafeResult) or not result.success:
        return None

    data = result.data
    if isinstance(data, dict):
        document = data.get("document")
        if isinstance(document, dict):
            return document

    original = result.original()
    if isinstance(original, dict):
        document = original.get("document")
        if isinstance(document, dict):
            return document

    return None


class ZMongoChatTurnNode:
    """
    Stateful Mongo-backed chat helper for Comfy text-generation workflows.

    Modes:
    - prepare_turn: load/create thread, append current user message in-memory,
      and output a formatted prompt for TextGenerate.
    - commit_turn: load/create thread, append user + assistant messages,
      and persist the updated conversation document.
    """

    CATEGORY = "ZMongo/Workflow"
    FUNCTION = "run"
    RETURN_TYPES = ("STRING", "STRING", "STRING", "STRING", "INT", "STRING")
    RETURN_NAMES = (
        "prompt_text",
        "thread_id",
        "conversation_json",
        "history_text",
        "turn_count",
        "status_json",
    )

    MODES = ["prepare_turn", "commit_turn"]

    @classmethod
    def INPUT_TYPES(cls) -> Dict[str, Any]:
        return {
            "required": {
                "zmongo": ("ZMONGO_CONNECTION",),
                "collection_name": ("STRING", {"default": "chat_threads"}),
                "mode": (cls.MODES, {"default": "prepare_turn"}),
                "thread_id": ("STRING", {"default": "thread_001"}),
                "user_message": ("STRING", {"default": "Hello", "multiline": True}),
                "assistant_message": ("STRING", {"default": "", "multiline": True}),
                "system_prompt": ("STRING", {"default": "You are a helpful assistant.", "multiline": True}),
                "history_limit": ("INT", {"default": 8, "min": 1, "max": 200, "step": 1}),
                "create_if_missing": ("BOOLEAN", {"default": True}),
                "include_system_prompt": ("BOOLEAN", {"default": True}),
                "assistant_prefix": ("STRING", {"default": "Assistant:"}),
            }
        }

    @classmethod
    def IS_CHANGED(cls, **kwargs):
        """
        Generic change detector so subclasses can rename inputs
        (for example thread_id -> campaign_id) without causing
        signature mismatch warnings.
        """
        try:
            parts = []
            for key in sorted(kwargs.keys()):
                value = kwargs[key]
                if key == "zmongo":
                    parts.append(f"{key}={id(value)}")
                else:
                    parts.append(f"{key}={value}")
            return "|".join(parts)
        except Exception:
            return float("NaN")

    @staticmethod
    def _blank_thread(thread_id: str, system_prompt: str) -> Dict[str, Any]:
        now = _utc_now_iso()
        return {
            "_id": thread_id,
            "title": thread_id,
            "system_prompt": system_prompt,
            "messages": [],
            "created_at": now,
            "updated_at": now,
            "turn_count": 0,
            "status": "active",
        }

    def _load_or_create_thread(
        self,
        zmongo: ZMongo,
        collection_name: str,
        thread_id: str,
        system_prompt: str,
        create_if_missing: bool,
    ) -> SafeResult:
        result = zmongo.find_one(collection_name, {"_id": thread_id}, cache=False)
        document = _extract_document_from_result(result)

        if document is not None:
            if not isinstance(document.get("messages"), list):
                document["messages"] = []
            if not document.get("system_prompt"):
                document["system_prompt"] = system_prompt
            return SafeResult.ok(document)

        if not create_if_missing:
            return SafeResult.fail(
                error=f"Thread '{thread_id}' not found and create_if_missing is False.",
                status_code=404,
            )

        return SafeResult.ok(self._blank_thread(thread_id, system_prompt))

    @staticmethod
    def _normalize_messages(messages: Any) -> List[Dict[str, str]]:
        out: List[Dict[str, str]] = []
        if not isinstance(messages, list):
            return out

        for item in messages:
            if not isinstance(item, dict):
                continue
            role = str(item.get("role", "")).strip()
            content = str(item.get("content", "")).strip()
            if not role or not content:
                continue
            out.append({"role": role, "content": content})
        return out

    @staticmethod
    def _render_history_text(
        messages: List[Dict[str, str]],
        history_limit: int,
    ) -> str:
        trimmed = messages[-max(1, int(history_limit)) :]
        lines: List[str] = []
        for msg in trimmed:
            role = str(msg.get("role", "")).strip().capitalize() or "User"
            content = str(msg.get("content", "")).strip()
            lines.append(f"{role}: {content}")
        return "\n".join(lines)

    def _build_prompt(
        self,
        *,
        system_prompt: str,
        include_system_prompt: bool,
        messages: List[Dict[str, str]],
        user_message: str,
        history_limit: int,
        assistant_prefix: str,
    ) -> str:
        lines: List[str] = []

        if include_system_prompt and str(system_prompt or "").strip():
            lines.append(f"System: {system_prompt.strip()}")
            lines.append("")

        history_text = self._render_history_text(messages, history_limit=history_limit)
        if history_text:
            lines.append(history_text)
            lines.append("")

        lines.append(f"User: {str(user_message or '').strip()}")
        lines.append(str(assistant_prefix or "Assistant:").strip())

        return "\n".join(lines).strip()

    def _commit_thread(
        self,
        zmongo: ZMongo,
        collection_name: str,
        thread: Dict[str, Any],
    ) -> SafeResult:
        thread_to_save = dict(thread)
        thread_id = str(thread_to_save.get("_id", "")).strip()
        if not thread_id:
            return SafeResult.fail("Cannot save thread without _id")

        thread_to_save["updated_at"] = _utc_now_iso()
        thread_to_save["turn_count"] = len(
            [m for m in thread_to_save.get("messages", []) if isinstance(m, dict) and m.get("role") == "assistant"]
        )

        save_data = dict(thread_to_save)
        save_data.pop("_id", None)

        return zmongo.insert_or_update(
            collection_name,
            {"_id": thread_id},
            save_data,
        )

    def run(
        self,
        zmongo: ZMongo,
        collection_name: str,
        mode: str,
        thread_id: str,
        user_message: str,
        assistant_message: str,
        system_prompt: str,
        history_limit: int,
        create_if_missing: bool,
        include_system_prompt: bool,
        assistant_prefix: str,
    ):
        if zmongo is None:
            failure = SafeResult.fail("No ZMongo connection provided")
            return ("", thread_id, "{}", "", 0, failure.to_json(indent=2))

        try:
            collection_name = str(collection_name or "").strip()
            thread_id = str(thread_id or "").strip()
            user_message = str(user_message or "").strip()
            assistant_message = str(assistant_message or "").strip()
            system_prompt = str(system_prompt or "").strip()
            assistant_prefix = str(assistant_prefix or "Assistant:").strip()
            history_limit = max(1, int(history_limit))

            if not collection_name:
                raise ValueError("collection_name is required")
            if not thread_id:
                raise ValueError("thread_id is required")

            thread_res = self._load_or_create_thread(
                zmongo=zmongo,
                collection_name=collection_name,
                thread_id=thread_id,
                system_prompt=system_prompt,
                create_if_missing=create_if_missing,
            )
            if not thread_res.success or not isinstance(thread_res.data, dict):
                return ("", thread_id, "{}", "", 0, thread_res.to_json(indent=2))

            thread = dict(thread_res.data)
            thread["messages"] = self._normalize_messages(thread.get("messages", []))

            if mode == "prepare_turn":
                if not user_message:
                    return (
                        "",
                        thread_id,
                        _safe_json(thread),
                        self._render_history_text(thread["messages"], history_limit),
                        len(thread["messages"]) // 2,
                        _safe_json(
                            {
                                "success": False,
                                "mode": mode,
                                "message": "user_message is required for prepare_turn",
                                "thread_id": thread_id,
                                "collection_name": collection_name,
                            }
                        ),
                    )

                prompt_text = self._build_prompt(
                    system_prompt=thread.get("system_prompt", system_prompt),
                    include_system_prompt=include_system_prompt,
                    messages=thread["messages"],
                    user_message=user_message,
                    history_limit=history_limit,
                    assistant_prefix=assistant_prefix,
                )

                history_text = self._render_history_text(thread["messages"], history_limit)
                status_payload = {
                    "success": True,
                    "mode": mode,
                    "thread_id": thread_id,
                    "collection_name": collection_name,
                    "message_count": len(thread["messages"]),
                    "turn_count": len([m for m in thread["messages"] if m.get("role") == "assistant"]),
                    "created_if_missing": thread_res.original().get("created_at") == thread.get("created_at"),
                }

                return (
                    prompt_text,
                    thread_id,
                    _safe_json(thread),
                    history_text,
                    len([m for m in thread["messages"] if m.get("role") == "assistant"]),
                    _safe_json(status_payload),
                )

            if mode == "commit_turn":
                if not user_message:
                    raise ValueError("user_message is required for commit_turn")
                if not assistant_message:
                    raise ValueError("assistant_message is required for commit_turn")

                thread["system_prompt"] = thread.get("system_prompt") or system_prompt
                thread["messages"].append({"role": "user", "content": user_message})
                thread["messages"].append({"role": "assistant", "content": assistant_message})

                save_res = self._commit_thread(
                    zmongo=zmongo,
                    collection_name=collection_name,
                    thread=thread,
                )

                status_payload = {
                    "success": bool(save_res.success),
                    "mode": mode,
                    "thread_id": thread_id,
                    "collection_name": collection_name,
                    "message_count": len(thread["messages"]),
                    "turn_count": len([m for m in thread["messages"] if m.get("role") == "assistant"]),
                    "save_result": save_res.to_dict(),
                }

                return (
                    "",
                    thread_id,
                    _safe_json(thread),
                    self._render_history_text(thread["messages"], history_limit),
                    len([m for m in thread["messages"] if m.get("role") == "assistant"]),
                    _safe_json(status_payload),
                )

            raise ValueError(f"Unsupported mode: {mode}")

        except Exception as exc:
            logger.exception("ZMongoChatTurnNode failure")
            failure = SafeResult.from_exception(exc, operation="chat_turn")
            return ("", thread_id, "{}", "", 0, failure.to_json(indent=2))


NODE_CLASS_MAPPINGS = {
    "ZMongoChatTurnNode": ZMongoChatTurnNode,
}

NODE_DISPLAY_NAME_MAPPINGS = {
    "ZMongoChatTurnNode": "ZMongo Chat Turn",
}