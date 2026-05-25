from __future__ import annotations

from typing import Any

from .generic_helpers import AlwaysDirtyMixin, DEFAULT_GEMINI_PREFIX, _session_api_request, _as_bool, _json_text, \
    _parse_json_object, _extract_text_from_gemini_payload, _dirty_token, _error_payload, _extract_models_from_payload, \
    _session_get_doc, _extract_document, _safe_get_by_path, _success_payload, _session_save_value


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
