import logging
from .zmongo_chat_nodes import ZMongoChatTurnNode
from ..zmongo_toolbag.zmongo import ZMongo

logger = logging.getLogger(__name__)


class ZMongoDungeonMasterNode(ZMongoChatTurnNode):
    """
    A specialized ComfyUI node for D&D Storytelling.
    It wraps the stateful ZMongo chat logic to provide a 'Dungeon Master'
    interface for consistent, memory-backed adventures.
    """

    CATEGORY = "ZMongo/Adventure"
    FUNCTION = "play"

    # We specialize the return names for a thematic experience
    RETURN_NAMES = (
        "dm_prompt",  # The formatted text for your LLM
        "campaign_id",  # Pass-through for the thread ID
        "campaign_data_json",  # Full record for inspection
        "adventure_log",  # Formatted text history
        "turn_count",  # Number of completed exchanges
        "status_json",  # Operation metadata
    )

    @classmethod
    def INPUT_TYPES(cls):
        """
        D&D specific inputs that map to the underlying ZMongo chat system.
        """
        return {
            "required": {
                "zmongo": ("ZMONGO_CONNECTION",),
                "collection_name": ("STRING", {"default": "dnd_campaigns"}),
                "mode": (cls.MODES, {"default": "prepare_turn"}),
                "campaign_id": ("STRING", {"default": "chronicles_of_valoria"}),
                "player_action": ("STRING", {"default": "I search the room.", "multiline": True}),
                "dm_response": ("STRING", {"default": "", "multiline": True}),
                "dm_persona": ("STRING", {
                    "default": (
                        "You are a legendary Dungeon Master. Describe scenes with rich detail, "
                        "manage game mechanics, and always ask 'What do you do?'"
                    ),
                    "multiline": True
                }),
                "history_limit": ("INT", {"default": 12, "min": 1, "max": 100, "step": 1}),
                "include_dm_persona": ("BOOLEAN", {"default": True}),
            },
            "optional": {
                "dm_prefix": ("STRING", {"default": "Dungeon Master:"}),
            }
        }

    def play(self, **kwargs):
        """
        Converts D&D adventure parameters into the standard ZMongo chat
        workflow and executes the turn logic.
        """
        # Map the 'Adventure' themed UI inputs back to the 'Chat' logic engine
        mapped_kwargs = {
            "zmongo": kwargs.get("zmongo"),
            "collection_name": kwargs.get("collection_name"),
            "mode": kwargs.get("mode"),
            "thread_id": kwargs.get("campaign_id"),
            "user_message": kwargs.get("player_action"),
            "assistant_message": kwargs.get("dm_response"),
            "system_prompt": kwargs.get("dm_persona"),
            "history_limit": kwargs.get("history_limit"),
            "create_if_missing": True,  # Always ensure a new campaign starts on a blank canvas
            "include_system_prompt": kwargs.get("include_dm_persona"),
            "assistant_prefix": kwargs.get("dm_prefix", "Dungeon Master:"),
        }

        # Reuse the established 'run' logic from ZMongoChatTurnNode
        return self.run(**mapped_kwargs)


NODE_CLASS_MAPPINGS = {
    "ZMongoDungeonMasterNode": ZMongoDungeonMasterNode,
}

NODE_DISPLAY_NAME_MAPPINGS = {
    "ZMongoDungeonMasterNode": "ZMongo Dungeon Master",
}