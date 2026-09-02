"""Scout must never import Takeout_Inventory.

Scout is GPL-3.0-or-later. Takeout_Inventory is AGPL-3.0-or-later. Importing
it would make the two a combined work in one process, and AGPL section 13 -
the network clause - would reach a program that serves a web UI. Running it as
a subprocess keeps them separate programs communicating through argv and a
file, which is where the licence boundary sits today.

This is the constraint whose violation would be least visible: the code would
work perfectly and only the licence would be wrong. So it is tested.
"""
from pathlib import Path

PACKAGE = Path(__file__).resolve().parent.parent / "takeout_scout"

FORBIDDEN = ("import takeout_inventory", "from takeout_inventory")


def test_no_module_imports_takeout_inventory() -> None:
    offenders = []
    for path in sorted(PACKAGE.rglob("*.py")):
        text = path.read_text(encoding="utf-8")
        for lineno, line in enumerate(text.splitlines(), start=1):
            code = line.split("#", 1)[0]
            if any(marker in code for marker in FORBIDDEN):
                offenders.append(f"{path.name}:{lineno}: {line.strip()}")

    assert not offenders, (
        "Scout (GPL-3) must not import Takeout_Inventory (AGPL-3); "
        "run it as a subprocess instead:\n  " + "\n  ".join(offenders))


def test_the_guard_scans_real_files() -> None:
    """Without this, an empty file list would make the guard pass forever."""
    modules = list(PACKAGE.rglob("*.py"))
    assert len(modules) >= 10, f"expected the package, found {modules}"
