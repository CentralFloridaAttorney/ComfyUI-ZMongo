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

import json
import logging
import random
from datetime import datetime, timezone
from typing import Any, Dict, List

from ..zmongo_toolbag.data_processor import DataProcessor
from ..zmongo_toolbag.safe_result import SafeResult

logger = logging.getLogger(__name__)


def _safe_json(value: Any) -> str:
    try:
        if isinstance(value, SafeResult):
            return value.to_json(indent=2)
        return DataProcessor.to_json(value, indent=2)
    except Exception as exc:
        return json.dumps({"error": f"Serialization failed: {exc}"}, indent=2)


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


class ZMongoEncounterPromptBuilderNode:
    """
    Build a single D&D encounter-generation prompt from structured UI inputs.

    Workflow intent:
    - Use dropdowns for common encounter dimensions
    - Use toggles for optional prompt sections
    - Use text fields for creative control
    - Pass zmongo and collection_name through for downstream persistence or chat
    - Emit structured JSON for optional saving with ZMongoSaveValueNode
    """

    CATEGORY = "ZMongo/Adventure"
    FUNCTION = "build_encounter_prompt"

    TERRAIN_TYPES = [
        "forest",
        "swamp",
        "dungeon",
        "crypt",
        "cavern",
        "mountain pass",
        "roadside",
        "ruins",
        "village",
        "city alley",
        "castle",
        "sewers",
        "coastline",
        "desert",
        "arctic",
        "plains",
        "feywild",
        "shadowfell",
    ]

    ENCOUNTER_TYPES = [
        "combat",
        "social",
        "exploration",
        "puzzle",
        "trap",
        "ambush",
        "chase",
        "boss fight",
        "mixed encounter",
    ]

    DIFFICULTY_LEVELS = [
        "trivial",
        "easy",
        "medium",
        "hard",
        "deadly",
    ]

    PARTY_TIERS = [
        "tier 1 (levels 1-4)",
        "tier 2 (levels 5-10)",
        "tier 3 (levels 11-16)",
        "tier 4 (levels 17-20)",
    ]

    MOODS = [
        "grim",
        "mysterious",
        "heroic",
        "horror",
        "tense",
        "wondrous",
        "tragic",
        "comic relief",
        "political intrigue",
        "survival",
    ]

    OBJECTIVE_TYPES = [
        "defeat the threat",
        "survive and escape",
        "protect an NPC",
        "recover an item",
        "solve the situation peacefully",
        "delay the enemy",
        "capture a target",
        "escort someone safely",
        "investigate the scene",
    ]

    RETURN_TYPES = (
        "STRING",              # prompt_text
        "STRING",              # encounter_title
        "STRING",              # encounter_record_json
        "STRING",              # summary_text
        "ZMONGO_CONNECTION",   # zmongo
        "STRING",              # collection_name
        "STRING",              # query_json
    )

    RETURN_NAMES = (
        "prompt_text",
        "encounter_title",
        "encounter_record_json",
        "summary_text",
        "zmongo",
        "collection_name",
        "query_json",
    )

    @classmethod
    def INPUT_TYPES(cls) -> Dict[str, Any]:
        return {
            "required": {
                "zmongo": ("ZMONGO_CONNECTION",),
                "collection_name": ("STRING", {"default": "dnd_encounters", "forceInput": True}),
                "campaign_id": ("STRING", {"default": "chronicles_of_valoria"}),
                "encounter_slug": ("STRING", {"default": "encounter_001"}),
                "party_tier": (cls.PARTY_TIERS, {"default": cls.PARTY_TIERS[0]}),
                "party_size": ("INT", {"default": 4, "min": 1, "max": 10, "step": 1}),
                "encounter_type": (cls.ENCOUNTER_TYPES, {"default": "mixed encounter"}),
                "difficulty": (cls.DIFFICULTY_LEVELS, {"default": "medium"}),
                "terrain": (cls.TERRAIN_TYPES, {"default": "forest"}),
                "mood": (cls.MOODS, {"default": "mysterious"}),
                "primary_objective": (cls.OBJECTIVE_TYPES, {"default": "investigate the scene"}),
                "location_name": ("STRING", {"default": "The Shattered Ford"}),
                "enemy_or_focus": ("STRING", {"default": "bandits led by a hedge mage"}),
                "encounter_hook": ("STRING", {"default": "The party finds signs of a recent struggle.", "multiline": True}),
                "special_twist": ("STRING", {"default": "One apparent enemy may be trying to warn the party.", "multiline": True}),
                "constraints": ("STRING", {"default": "", "multiline": True}),
                "style_notes": ("STRING", {"default": "Use vivid but table-usable detail.", "multiline": True}),
                "include_read_aloud_text": ("BOOLEAN", {"default": True}),
                "include_tactical_notes": ("BOOLEAN", {"default": True}),
                "include_scaling_notes": ("BOOLEAN", {"default": True}),
                "include_treasure": ("BOOLEAN", {"default": True}),
                "include_roleplay_hooks": ("BOOLEAN", {"default": True}),
                "include_followup_hooks": ("BOOLEAN", {"default": True}),
                "include_structured_output_request": ("BOOLEAN", {"default": True}),
            },
            "optional": {
                "npc_names_json": ("STRING", {"default": "[]", "multiline": True}),
                "must_include_json": ("STRING", {"default": "[]", "multiline": True}),
                "forbidden_elements_json": ("STRING", {"default": "[]", "multiline": True}),
                "seed_hint": ("INT", {"default": 0, "min": 0, "max": 999999999}),
            },
        }

    @classmethod
    def IS_CHANGED(cls, **kwargs):
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
    def _parse_json_list(raw: str, field_name: str) -> List[str]:
        text = str(raw or "").strip()
        if not text:
            return []
        try:
            parsed = json.loads(text)
        except Exception as exc:
            raise ValueError(f"{field_name} is not valid JSON: {exc}") from exc

        if not isinstance(parsed, list):
            raise ValueError(f"{field_name} must be a JSON list")

        return [str(item).strip() for item in parsed if str(item).strip()]

    @staticmethod
    def _choose_seed(seed_hint: int, encounter_slug: str) -> int:
        if int(seed_hint or 0) > 0:
            return int(seed_hint)
        random.seed(f"{encounter_slug}|{_utc_now_iso()[:10]}")
        return random.randint(1000, 999999)

    @staticmethod
    def _build_title(location_name: str, encounter_type: str, enemy_or_focus: str) -> str:
        location = str(location_name or "").strip() or "Unknown Location"
        focus = str(enemy_or_focus or "").strip() or "Unknown Threat"
        kind = str(encounter_type or "").strip().title()
        return f"{kind} at {location}: {focus}"

    def build_encounter_prompt(
        self,
        zmongo,
        collection_name: str,
        campaign_id: str,
        encounter_slug: str,
        party_tier: str,
        party_size: int,
        encounter_type: str,
        difficulty: str,
        terrain: str,
        mood: str,
        primary_objective: str,
        location_name: str,
        enemy_or_focus: str,
        encounter_hook: str,
        special_twist: str,
        constraints: str,
        style_notes: str,
        include_read_aloud_text: bool,
        include_tactical_notes: bool,
        include_scaling_notes: bool,
        include_treasure: bool,
        include_roleplay_hooks: bool,
        include_followup_hooks: bool,
        include_structured_output_request: bool,
        npc_names_json: str = "[]",
        must_include_json: str = "[]",
        forbidden_elements_json: str = "[]",
        seed_hint: int = 0,
    ):
        if zmongo is None:
            failure = SafeResult.fail("No ZMongo connection provided")
            return ("", "", "{}", failure.message, None, str(collection_name or ""), "{}")

        try:
            collection_name = str(collection_name or "").strip()
            campaign_id = str(campaign_id or "").strip()
            encounter_slug = str(encounter_slug or "").strip()
            location_name = str(location_name or "").strip()
            enemy_or_focus = str(enemy_or_focus or "").strip()
            encounter_hook = str(encounter_hook or "").strip()
            special_twist = str(special_twist or "").strip()
            constraints = str(constraints or "").strip()
            style_notes = str(style_notes or "").strip()

            if not collection_name:
                raise ValueError("collection_name is required")
            if not campaign_id:
                raise ValueError("campaign_id is required")
            if not encounter_slug:
                raise ValueError("encounter_slug is required")

            npc_names = self._parse_json_list(npc_names_json, "npc_names_json")
            must_include = self._parse_json_list(must_include_json, "must_include_json")
            forbidden_elements = self._parse_json_list(forbidden_elements_json, "forbidden_elements_json")

            seed_value = self._choose_seed(seed_hint, encounter_slug)
            encounter_title = self._build_title(location_name, encounter_type, enemy_or_focus)
            encounter_id = f"{campaign_id}:{encounter_slug}"

            section_requests: List[str] = [
                "Create one tabletop-ready Dungeons & Dragons encounter.",
                f"Campaign ID: {campaign_id}",
                f"Encounter ID: {encounter_id}",
                f"Encounter title: {encounter_title}",
                f"Party tier: {party_tier}",
                f"Party size: {int(party_size)}",
                f"Encounter type: {encounter_type}",
                f"Difficulty target: {difficulty}",
                f"Terrain: {terrain}",
                f"Mood: {mood}",
                f"Primary objective: {primary_objective}",
                f"Location: {location_name}",
                f"Primary enemy/focus: {enemy_or_focus}",
                f"Encounter hook: {encounter_hook}",
                f"Special twist: {special_twist}",
                f"Randomization seed hint: {seed_value}",
            ]

            if constraints:
                section_requests.append(f"Constraints: {constraints}")

            if style_notes:
                section_requests.append(f"Style notes: {style_notes}")

            if npc_names:
                section_requests.append("Important NPC names to consider: " + ", ".join(npc_names))

            if must_include:
                section_requests.append("The encounter must include: " + "; ".join(must_include))

            if forbidden_elements:
                section_requests.append("Do not include: " + "; ".join(forbidden_elements))

            output_requests: List[str] = [
                "Design the encounter so it is immediately usable by a dungeon master at the table.",
                "Keep the scenario specific, concrete, and internally consistent.",
                "Make the stakes clear and tie the encounter to the stated objective.",
            ]

            if include_read_aloud_text:
                output_requests.append("Include a short boxed read-aloud description for the opening scene.")

            if include_tactical_notes:
                output_requests.append("Include tactical notes describing terrain usage, monster behavior, and pacing.")

            if include_scaling_notes:
                output_requests.append("Include scaling notes for making the encounter easier or harder.")

            if include_treasure:
                output_requests.append("Include treasure, reward, or meaningful consequence.")

            if include_roleplay_hooks:
                output_requests.append("Include roleplay hooks, negotiation angles, or social complications where appropriate.")

            if include_followup_hooks:
                output_requests.append("Include two or three follow-up hooks that can lead into later encounters.")

            if include_structured_output_request:
                output_requests.append(
                    "Format the response with these headings: Title, Setup, Read-Aloud Text, Creatures or Factions, "
                    "Environment, Objective, Twist, Running the Encounter, Tactical Notes, Scaling, Rewards, Follow-Up Hooks."
                )

            prompt_lines = []
            prompt_lines.extend(section_requests)
            prompt_lines.append("")
            prompt_lines.append("Output requirements:")
            prompt_lines.extend(f"- {item}" for item in output_requests)

            prompt_text = "\n".join(prompt_lines).strip()

            encounter_record = {
                "_id": encounter_id,
                "type": "dnd_encounter_prompt_request",
                "campaign_id": campaign_id,
                "encounter_slug": encounter_slug,
                "encounter_title": encounter_title,
                "collection_name": collection_name,
                "created_at": _utc_now_iso(),
                "seed_hint": seed_value,
                "inputs": {
                    "party_tier": party_tier,
                    "party_size": int(party_size),
                    "encounter_type": encounter_type,
                    "difficulty": difficulty,
                    "terrain": terrain,
                    "mood": mood,
                    "primary_objective": primary_objective,
                    "location_name": location_name,
                    "enemy_or_focus": enemy_or_focus,
                    "encounter_hook": encounter_hook,
                    "special_twist": special_twist,
                    "constraints": constraints,
                    "style_notes": style_notes,
                    "npc_names": npc_names,
                    "must_include": must_include,
                    "forbidden_elements": forbidden_elements,
                },
                "options": {
                    "include_read_aloud_text": bool(include_read_aloud_text),
                    "include_tactical_notes": bool(include_tactical_notes),
                    "include_scaling_notes": bool(include_scaling_notes),
                    "include_treasure": bool(include_treasure),
                    "include_roleplay_hooks": bool(include_roleplay_hooks),
                    "include_followup_hooks": bool(include_followup_hooks),
                    "include_structured_output_request": bool(include_structured_output_request),
                },
                "prompt_text": prompt_text,
                "status": "draft",
            }

            query_json = _safe_json({"_id": encounter_id})

            summary_text = (
                f"{encounter_title}\n"
                f"Type: {encounter_type} | Difficulty: {difficulty} | Terrain: {terrain} | Mood: {mood}\n"
                f"Objective: {primary_objective}\n"
                f"Collection: {collection_name}"
            )

            return (
                prompt_text,
                encounter_title,
                _safe_json(encounter_record),
                summary_text,
                zmongo,
                collection_name,
                query_json,
            )

        except Exception as exc:
            logger.exception("ZMongoEncounterPromptBuilderNode failure")
            failure = SafeResult.from_exception(exc, operation="build_encounter_prompt")
            return (
                "",
                "",
                "{}",
                failure.message,
                zmongo,
                str(collection_name or ""),
                "{}",
            )


NODE_CLASS_MAPPINGS = {
    "ZMongoDungeonMasterNode": ZMongoDungeonMasterNode,
    "ZMongoEncounterPromptBuilderNode": ZMongoEncounterPromptBuilderNode,
}

NODE_DISPLAY_NAME_MAPPINGS = {
    "ZMongoDungeonMasterNode": "ZMongo Dungeon Master",
    "ZMongoEncounterPromptBuilderNode": "ZMongo Encounter Prompt Builder",
}