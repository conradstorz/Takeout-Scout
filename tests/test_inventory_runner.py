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
