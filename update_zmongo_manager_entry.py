from __future__ import annotations

import json
from pathlib import Path
from typing import Any


MANAGER_LIST_PATH = Path("custom-node-list.json")

ZMONGO_ENTRY: dict[str, Any] = {
    "author": "CentralFloridaAttorney",
    "title": "ComfyUI-ZMongo",
    "reference": "https://github.com/CentralFloridaAttorney/ComfyUI-ZMongo",
    "files": [
        "https://github.com/CentralFloridaAttorney/ComfyUI-ZMongo"
    ],
    "install_type": "git-clone",
    "description": (
        "ComfyUI-ZMongo provides API-key authenticated ZMongo persistence nodes "
        "for secure cloud and local workflow storage, documents, images, presets, "
        "Gemini workflows, dot-path saves, and dynamic workflow data loading."
    ),
}


def load_manager_list(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        raise FileNotFoundError(f"Could not find {path.resolve()}")

    data = json.loads(path.read_text(encoding="utf-8"))

    if not isinstance(data, list):
        raise TypeError(
            f"{path} must be a JSON array. "
            "Do not replace the full Manager custom-node-list.json with a single object."
        )

    for index, item in enumerate(data):
        if not isinstance(item, dict):
            raise TypeError(f"Entry at index {index} is not a JSON object.")

    return data


def is_zmongo_entry(item: dict[str, Any]) -> bool:
    return (
        item.get("title") == "ComfyUI-ZMongo"
        or item.get("reference") == "https://github.com/CentralFloridaAttorney/ComfyUI-ZMongo"
        or "CentralFloridaAttorney/ComfyUI-ZMongo" in str(item.get("files", ""))
    )


def update_zmongo_entry(data: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], bool, int]:
    updated: list[dict[str, Any]] = []
    found = False
    replaced_count = 0

    for item in data:
        if is_zmongo_entry(item):
            if not found:
                updated.append(ZMONGO_ENTRY)
                found = True
            replaced_count += 1
        else:
            updated.append(item)

    if not found:
        updated.append(ZMONGO_ENTRY)

    return updated, found, replaced_count


def write_manager_list(path: Path, data: list[dict[str, Any]]) -> None:
    backup_path = path.with_suffix(path.suffix + ".bak_before_zmongo_update")

    if not backup_path.exists():
        backup_path.write_text(path.read_text(encoding="utf-8"), encoding="utf-8")

    path.write_text(
        json.dumps(data, indent=2, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )


def main() -> None:
    data = load_manager_list(MANAGER_LIST_PATH)
    updated_data, found, replaced_count = update_zmongo_entry(data)

    write_manager_list(MANAGER_LIST_PATH, updated_data)

    # Validate final JSON immediately after writing.
    validated = json.loads(MANAGER_LIST_PATH.read_text(encoding="utf-8"))
    if not isinstance(validated, list):
        raise TypeError("Final custom-node-list.json is not a JSON array.")

    final_matches = [item for item in validated if is_zmongo_entry(item)]

    if len(final_matches) != 1:
        raise RuntimeError(
            f"Expected exactly one ComfyUI-ZMongo entry, found {len(final_matches)}."
        )

    print("Updated custom-node-list.json successfully.")
    print(f"Existing entry found: {found}")
    print(f"Entries replaced/removed: {replaced_count}")
    print("Final ComfyUI-ZMongo entry:")
    print(json.dumps(final_matches[0], indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()