import logging

from .zmongo_chat_nodes import ZMongoChatTurnNode

logger = logging.getLogger(__name__)


class ZMongoDungeonMasterNode(ZMongoChatTurnNode):
    """
    A specialized ComfyUI node for D&D storytelling.

    Design:
    - Reuses the incoming ZMongo connection without mutating it.
    - Treats collection_name as explicit workflow state.
    - Makes collection_name a real socket input so it can be wired/driven.
    - Passes collection_name into the underlying chat logic.
    - Returns collection_name as a passthrough output for downstream nodes.
    """

    CATEGORY = "ZMongo/Adventure"
    FUNCTION = "play"

    RETURN_TYPES = (
        "STRING",
        "STRING",
        "STRING",
        "STRING",
        "INT",
        "STRING",
        "ZMONGO_CONNECTION",
        "STRING",
    )

    RETURN_NAMES = (
        "dm_prompt",
        "campaign_id",
        "campaign_data_json",
        "adventure_log",
        "turn_count",
        "status_json",
        "zmongo",
        "collection_name",
    )

    @classmethod
    def INPUT_TYPES(cls):
        return {
            "required": {
                "zmongo": ("ZMONGO_CONNECTION",),
                "collection_name": (
                    "STRING",
                    {
                        "default": "dnd_campaigns",
                        "forceInput": True,
                    },
                ),
                "mode": (cls.MODES, {"default": "prepare_turn"}),
                "campaign_id": ("STRING", {"default": "chronicles_of_valoria"}),
                "player_action": ("STRING", {"default": "I search the room.", "multiline": True}),
                "dm_response": ("STRING", {"default": "", "multiline": True}),
                "dm_persona": (
                    "STRING",
                    {
                        "default": (
                            "You are a legendary Dungeon Master. Describe scenes with rich detail, "
                            "manage game mechanics, and always ask 'What do you do?'"
                        ),
                        "multiline": True,
                    },
                ),
                "history_limit": ("INT", {"default": 12, "min": 1, "max": 100, "step": 1}),
                "include_dm_persona": ("BOOLEAN", {"default": True}),
            },
            "optional": {
                "dm_prefix": ("STRING", {"default": "Dungeon Master:"}),
            },
        }

    @classmethod
    def IS_CHANGED(cls, **kwargs):
        return super().IS_CHANGED(**kwargs)

    def play(self, **kwargs):
        zmongo = kwargs.get("zmongo")
        collection_name = str(kwargs.get("collection_name") or "").strip()

        if zmongo is None:
            raise ValueError("zmongo connection is required")
        if not collection_name:
            raise ValueError("collection_name is required")

        mapped_kwargs = {
            "zmongo": zmongo,
            "collection_name": collection_name,
            "mode": kwargs.get("mode"),
            "thread_id": kwargs.get("campaign_id"),
            "user_message": kwargs.get("player_action"),
            "assistant_message": kwargs.get("dm_response"),
            "system_prompt": kwargs.get("dm_persona"),
            "history_limit": kwargs.get("history_limit"),
            "create_if_missing": True,
            "include_system_prompt": kwargs.get("include_dm_persona"),
            "assistant_prefix": kwargs.get("dm_prefix", "Dungeon Master:"),
        }

        result = self.run(**mapped_kwargs)

        if not isinstance(result, tuple):
            raise TypeError("Expected tuple result from ZMongoChatTurnNode.run()")

        return (*result, zmongo, collection_name)


NODE_CLASS_MAPPINGS = {
    "ZMongoDungeonMasterNode": ZMongoDungeonMasterNode,
}

NODE_DISPLAY_NAME_MAPPINGS = {
    "ZMongoDungeonMasterNode": "ZMongo Dungeon Master",
}