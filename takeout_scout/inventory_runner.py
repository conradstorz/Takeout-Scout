"""Run Takeout_Inventory as a separate program.

Inventory performs the deep, cross-archive pass that Scout's per-archive scan
cannot: on a real export, 71.7% of photos have their sidecar in a different
archive than the media file.

It is invoked as a subprocess and never imported. That is a licence boundary,
not a style preference - Scout is GPL-3.0-or-later, Inventory is
AGPL-3.0-or-later, and importing it would make a combined work, pulling the
AGPL's network clause onto a program that serves a web UI. Running it as a
separate program communicating through argv and files keeps the two at arm's
length. It is also Inventory's own designed interface: a PEP 723 script meant
to be run with `uv run`.
"""
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

# Scout's repository root: takeout_scout/inventory_runner.py -> repo root.
# A module-level constant so tests can point it somewhere harmless.
SCOUT_ROOT = Path(__file__).resolve().parent.parent

SCRIPT_NAME = "takeout_inventory.py"
SIBLING_DIR = "Takeout_Inventory"


@dataclass(frozen=True)
class InventoryTool:
    """A located takeout_inventory.py, and how it was found.

    `source` is reported to the user so they can tell a guess from a choice:
    "sibling" (found beside Scout), "remembered" (a path they gave earlier).
    """

    script: Path
    source: str


def find_inventory(remembered: str | None = None) -> InventoryTool | None:
    """Locate takeout_inventory.py, or None if it is not present.

    Order: an explicit remembered path, then the sibling repository. Returns
    None rather than raising, because Inventory being absent is an ordinary
    state - Scout works without it, and the deep-pass offer simply does not
    appear.
    """
    if remembered:
        candidate = Path(remembered).expanduser()
        if candidate.is_file():
            return InventoryTool(candidate.resolve(), "remembered")

    sibling = SCOUT_ROOT.parent / SIBLING_DIR / SCRIPT_NAME
    if sibling.is_file():
        return InventoryTool(sibling.resolve(), "sibling")

    return None
