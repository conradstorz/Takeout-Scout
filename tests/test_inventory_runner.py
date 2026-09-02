"""Tests for takeout_scout.inventory_runner.

Scout runs Takeout_Inventory as a separate program, never as an import -
Scout is GPL-3 and Inventory is AGPL-3, and importing would make a combined
work. These tests cover locating it and building its command line.
"""
from pathlib import Path

import pytest

from takeout_scout.inventory_runner import InventoryTool, find_inventory


def _make_inventory(root: Path) -> Path:
    """Create a stand-in takeout_inventory.py inside a sibling repo layout."""
    sibling = root / "Takeout_Inventory"
    sibling.mkdir(parents=True, exist_ok=True)
    script = sibling / "takeout_inventory.py"
    script.write_text("# stand-in\n", encoding="utf-8")
    return script


class TestFindInventory:
    def test_finds_the_sibling_repository(self, tmp_path, monkeypatch):
        """The common case: Takeout_Inventory sits beside Takeout-Scout."""
        script = _make_inventory(tmp_path)
        scout_root = tmp_path / "Takeout-Scout"
        scout_root.mkdir()
        monkeypatch.setattr("takeout_scout.inventory_runner.SCOUT_ROOT", scout_root)

        found = find_inventory()

        assert found is not None
        assert found.script == script
        assert found.source == "sibling"

    def test_returns_none_when_absent(self, tmp_path, monkeypatch):
        """A missing Inventory is an ordinary state, not an error."""
        scout_root = tmp_path / "Takeout-Scout"
        scout_root.mkdir()
        monkeypatch.setattr("takeout_scout.inventory_runner.SCOUT_ROOT", scout_root)

        assert find_inventory() is None

    def test_remembered_path_wins_over_sibling(self, tmp_path, monkeypatch):
        """An explicit choice beats the guess."""
        _make_inventory(tmp_path)
        scout_root = tmp_path / "Takeout-Scout"
        scout_root.mkdir()
        monkeypatch.setattr("takeout_scout.inventory_runner.SCOUT_ROOT", scout_root)

        elsewhere = tmp_path / "elsewhere"
        elsewhere.mkdir()
        chosen = elsewhere / "takeout_inventory.py"
        chosen.write_text("# stand-in\n", encoding="utf-8")

        found = find_inventory(remembered=str(chosen))

        assert found.script == chosen
        assert found.source == "remembered"

    def test_stale_remembered_path_falls_through(self, tmp_path, monkeypatch):
        """A path that no longer exists must not raise, and must not win."""
        script = _make_inventory(tmp_path)
        scout_root = tmp_path / "Takeout-Scout"
        scout_root.mkdir()
        monkeypatch.setattr("takeout_scout.inventory_runner.SCOUT_ROOT", scout_root)

        found = find_inventory(remembered=str(tmp_path / "gone" / "takeout_inventory.py"))

        assert found is not None
        assert found.script == script
        assert found.source == "sibling"

    def test_returns_an_absolute_path(self, tmp_path, monkeypatch):
        """The path is handed to a subprocess; a relative one is a trap."""
        _make_inventory(tmp_path)
        scout_root = tmp_path / "Takeout-Scout"
        scout_root.mkdir()
        monkeypatch.setattr("takeout_scout.inventory_runner.SCOUT_ROOT", scout_root)

        assert find_inventory().script.is_absolute()


import sys

from takeout_scout.inventory_runner import (
    InventoryFailed,
    deep_pass_commands,
    run_streaming,
)


class TestDeepPassCommands:
    """These argv strings are a contract with another repository's CLI.

    If Inventory renames a flag, this is what notices. Asserted literally on
    purpose - a test that rebuilt the expected value from the same code under
    test would notice nothing.
    """

    def test_two_phases_in_order(self, tmp_path):
        tool = InventoryTool(tmp_path / "takeout_inventory.py", "sibling")
        phases = [name for name, _ in deep_pass_commands(tool, tmp_path / "export")]
        assert phases == ["scan", "index"]

    def test_scan_command(self, tmp_path):
        script = tmp_path / "takeout_inventory.py"
        export = tmp_path / "export"
        tool = InventoryTool(script, "sibling")

        name, argv = deep_pass_commands(tool, export)[0]

        assert name == "scan"
        assert argv == ["uv", "run", str(script), "scan", "--takeout", str(export)]

    def test_index_command_writes_beside_the_export(self, tmp_path):
        script = tmp_path / "takeout_inventory.py"
        export = tmp_path / "export"
        tool = InventoryTool(script, "sibling")

        name, argv = deep_pass_commands(tool, export)[1]

        assert name == "index"
        assert argv == [
            "uv", "run", str(script), "index",
            "--out-sqlite", str(export / "takeout-index.sqlite"),
            "--out-json", str(export / "takeout-index.json"),
        ]


class TestRunStreaming:
    def test_yields_output_lines_as_they_arrive(self, tmp_path):
        cmd = [sys.executable, "-c", "print('one'); print('two')"]
        assert list(run_streaming(cmd, tmp_path)) == ["one", "two"]

    def test_raises_with_the_exit_code_on_failure(self, tmp_path):
        cmd = [sys.executable, "-c", "print('context'); raise SystemExit(4)"]

        with pytest.raises(InventoryFailed) as exc:
            list(run_streaming(cmd, tmp_path))

        assert exc.value.returncode == 4
        assert "context" in exc.value.tail

    def test_tail_is_bounded(self, tmp_path):
        """A failing run must not paste ten thousand lines into the page."""
        cmd = [sys.executable, "-c",
               "[print(i) for i in range(500)]; raise SystemExit(1)"]

        with pytest.raises(InventoryFailed) as exc:
            list(run_streaming(cmd, tmp_path))

        assert len(exc.value.tail) <= 20

    def test_undecodable_output_does_not_crash(self, tmp_path):
        """Takeout filenames contain every script on earth."""
        cmd = [sys.executable, "-c",
               "import sys; sys.stdout.buffer.write(b'caf\\xe9\\n')"]
        lines = list(run_streaming(cmd, tmp_path))
        assert len(lines) == 1
