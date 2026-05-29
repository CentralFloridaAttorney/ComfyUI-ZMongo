# -----------------------------------------------------------------------------
# ComfyUI-ZMongo Helper Nodes - Production Version
# Fully backward-compatible with workflows
# -----------------------------------------------------------------------------
from datetime import datetime
from .generic_helpers import AlwaysDirtyMixin

# ------------------------------
# Select Nth Item Node
# ------------------------------
class ZMongoApiSelectNthItemNode(AlwaysDirtyMixin):
    FUNCTION = "select_nth_item"

    @classmethod
    def INPUT_TYPES(cls):
        return {
            "required": {
                "items_list": ("*",),
                "index": ("INT", {"default": 0}),
                "selection": ("STRING", {"default": ""}),
                "mode": ("STRING", {"default": "auto"}),  # auto, single, range, series, all
            },
            "optional": {
                "include_end": ("BOOLEAN", {"default": True}),
                "dedupe": ("BOOLEAN", {"default": True}),
            },
        }

    RETURN_TYPES = ("*", "*", "*", "INT", "STRING")
    RETURN_NAMES = ("item", "selected_items", "selected_json", "count", "status")
    CATEGORY = "ZMongo/99 Helpers"

    def select_nth_item(self, items_list, index=0, selection="", mode="auto", include_end=True, dedupe=True):
        if not items_list:
            return None, [], "[]", 0, "Empty items list"
        try:
            if mode == "auto" or mode == "single":
                sel_index = max(0, min(index, len(items_list)-1))
                selected_items = [items_list[sel_index]]
            elif mode == "all" or selection.strip() == "*":
                selected_items = items_list[:]
            else:
                selected_items = []
                for part in selection.split(","):
                    if "-" in part:
                        start, end = map(int, part.split("-"))
                        if not include_end:
                            end -= 1
                        selected_items.extend(items_list[start:end+1])
                    else:
                        selected_items.append(items_list[int(part)])
            if dedupe:
                seen = set()
                selected_items = [x for x in selected_items if not (x in seen or seen.add(x))]
            count = len(selected_items)
            return selected_items[0], selected_items, str(selected_items), count, f"Selected {count} items"
        except Exception as e:
            return None, [], "[]", 0, f"Error selecting items: {e}"


# ------------------------------
# Record Loop Manager Node
# ------------------------------
class ZMongoRecordLoopManagerNode(AlwaysDirtyMixin):
    FUNCTION = "loop_record"

    @classmethod
    def INPUT_TYPES(cls):
        return {
            "required": {
                "items_list": ("*",),
                "state_name": ("STRING", {"default": "default_loop"}),
                "advance_on_execute": ("BOOLEAN", {"default": True}),
                "reset_state": ("BOOLEAN", {"default": False}),
            },
            "optional": {
                "index": ("INT", {"default": 0}),
                "wrap": ("BOOLEAN", {"default": False}),
            },
        }

    RETURN_TYPES = ("*", "INT", "INT", "STRING")
    RETURN_NAMES = ("item", "index", "next_index", "progress_text")
    CATEGORY = "ZMongo/99 Helpers"

    def loop_record(self, items_list, state_name="default_loop", index=0, advance_on_execute=True, reset_state=False, wrap=False):
        total = len(items_list)
        if not total:
            return None, 0, 0, "Empty list"

        current_index = index if not reset_state else 0
        if current_index >= total:
            return None, current_index, current_index, "Loop complete"

        item = items_list[current_index]

        next_index = current_index + 1 if advance_on_execute else current_index
        if next_index >= total and wrap:
            next_index = 0

        progress_text = f"Item {current_index+1}/{total}: {item}"

        return item, current_index, next_index, progress_text


# ------------------------------
# Document Chunk Loop Manager Node
# ------------------------------
class ZMongoDocumentChunkLoopManagerNode(AlwaysDirtyMixin):
    FUNCTION = "loop_chunk"

    @classmethod
    def INPUT_TYPES(cls):
        return {
            "required": {
                "session": ("ZMONGO_API_SESSION",),
                "document_id": ("STRING",),
                "document_id_link": ("*",),  # backward compatibility
                "collection_name": ("STRING", {"default": "text_documents"}),
                "collection_name_link": ("*",),
                "chunk_field_root": ("STRING", {"default": "document_text.chunks"}),
                "index": ("INT", {"default": 0}),
                "advance_on_execute": ("BOOLEAN", {"default": True}),
                "reset_state": ("BOOLEAN", {"default": False}),
                "state_name": ("STRING", {"default": "default_chunk_loop"}),
            },
            "optional": {
                "dynamic_chunk_size": ("INT", {"default": 6000}),
                "dynamic_chunk_overlap": ("INT", {"default": 300}),
                "max_chunk_chars": ("INT", {"default": 12000}),
                "fallback_to_raw_text": ("BOOLEAN", {"default": True}),
                "raw_text_field_path": ("STRING", {"default": "document_text.raw_text"}),
            }
        }

    RETURN_TYPES = ("*", "INT", "INT", "STRING", "STRING", "STRING", "STRING", "STRING", "STRING", "STRING", "STRING", "STRING", "STRING", "STRING", "STRING")
    RETURN_NAMES = (
        "prompt",
        "chunk_text",
        "document_id_link",
        "result_field_path",
        "chunk_index",
        "chunk_count",
        "done",
        "next_index",
        "visible_index",
        "visible_next_index",
        "state_json",
        "status",
        "progress_text",
        "timestamp",
        "source_path",
    )
    CATEGORY = "ZMongo/99 Helpers"

    def loop_chunk(
        self,
        session,
        document_id,
        document_id_link=None,
        collection_name="text_documents",
        collection_name_link=None,
        chunk_field_root="document_text.chunks",
        index=0,
        advance_on_execute=True,
        reset_state=False,
        state_name="default_chunk_loop",
        dynamic_chunk_size=6000,
        dynamic_chunk_overlap=300,
        max_chunk_chars=12000,
        fallback_to_raw_text=True,
        raw_text_field_path="document_text.raw_text",
    ):
        chunks_payload = session.get_value(
            collection_name=collection_name,
            document_id=document_id,
            field_path=chunk_field_root,
            fallback=[]
        )
        chunks_list = chunks_payload or []
        total_chunks = len(chunks_list)
        current_index = index if not reset_state else 0

        if current_index >= total_chunks:
            return "", "", "", "", current_index, total_chunks, True, current_index, current_index, current_index, {}, "All chunks processed", "", ""

        chunk_text_field_path = f"{chunk_field_root}.{current_index}.text"
        chunk_text = session.get_value(
            collection_name=collection_name,
            document_id=document_id,
            field_path=chunk_text_field_path,
            fallback="" if fallback_to_raw_text else None
        )
        if not chunk_text and fallback_to_raw_text:
            chunk_text = session.get_value(
                collection_name=collection_name,
                document_id=document_id,
                field_path=raw_text_field_path,
                fallback=""
            )

        progress_text = f"Processing chunk {current_index+1}/{total_chunks} of document {document_id}"
        next_index = current_index + 1 if advance_on_execute else current_index

        timestamp = datetime.utcnow().isoformat() + "Z"
        return (
            "",  # prompt
            chunk_text,
            document_id_link,
            f"analysis.gemini.chunk_results.{current_index}.text",
            current_index,
            total_chunks,
            False,
            next_index,
            current_index,
            next_index,
            {"current_index": current_index, "next_index": next_index},
            "Chunk ready",
            progress_text,
            timestamp,
            chunk_text_field_path
        )


# ------------------------------
# JSON Pick Node
# ------------------------------
class ZMongoApiJsonPickNode(AlwaysDirtyMixin):
    FUNCTION = "json_pick"

    @classmethod
    def INPUT_TYPES(cls):
        return {
            "required": {
                "json": ("STRING",),
                "key": ("STRING", {"default": ""}),
            }
        }

    RETURN_TYPES = ("*",)
    RETURN_NAMES = ("value",)
    CATEGORY = "ZMongo/99 Helpers"

    def json_pick(self, json, key=""):
        import json as pyjson
        try:
            obj = pyjson.loads(json)
            return obj.get(key, None)
        except Exception:
            return None


# ------------------------------
# Node Class Mappings
# ------------------------------
NODE_CLASS_MAPPINGS = {
    "ZMongoApiSelectNthItemNode": ZMongoApiSelectNthItemNode,
    "ZMongoRecordLoopManagerNode": ZMongoRecordLoopManagerNode,
    "ZMongoDocumentChunkLoopManagerNode": ZMongoDocumentChunkLoopManagerNode,
    "ZMongoApiJsonPickNode": ZMongoApiJsonPickNode,
}

NODE_DISPLAY_NAME_MAPPINGS = {
    "ZMongoApiSelectNthItemNode": "99 Select Nth Item",
    "ZMongoRecordLoopManagerNode": "99 Record Loop Manager",
    "ZMongoDocumentChunkLoopManagerNode": "99 Document Chunk Loop Manager",
    "ZMongoApiJsonPickNode": "99 JSON Pick",
}