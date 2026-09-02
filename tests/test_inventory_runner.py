"""Tests for takeout_scout.inventory_runner.

Scout runs Takeout_Inventory as a separate program, never as an import -
Scout is GPL-3 and Inventory is AGPL-3, and importing would make a combined
work. These tests cover locating it and building its command line.
"""
from pathlib import Path

import pytest

from takeout_scout.inventory_runner import InventoryTool, find_inventory, export_dir_for


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

    def test_malformed_remembered_path_falls_through(self, tmp_path, monkeypatch):
        """A saved path can be anything a user typed.

        expanduser() raises RuntimeError on POSIX for an unknown ~user, and
        find_inventory is called on every Streamlit rerun with a remembered
        string pulled straight from state/inventory_path.json - a malformed
        value must fall through to the sibling lookup rather than crash the
        page.

        Simulated with monkeypatch rather than a literal null-byte string
        (e.g. "bad\\x00path.py"): on Python 3.13's pathlib, Path.is_file()
        already has its own `except ValueError: return False`, so the
        ValueError a null byte raises never reaches find_inventory's own
        try/except at all here - a null-byte test would pass identically
        whether or not this fix exists, proving nothing. Monkeypatching
        expanduser() to raise directly exercises the guard this fix adds,
        and fails without it regardless of platform or pathlib version.
        """
        script = _make_inventory(tmp_path)
        scout_root = tmp_path / "Takeout-Scout"
        scout_root.mkdir()
        monkeypatch.setattr("takeout_scout.inventory_runner.SCOUT_ROOT", scout_root)

        def _raise_runtime_error(self):
            raise RuntimeError("Can't determine home directory")
        monkeypatch.setattr(Path, "expanduser", _raise_runtime_error)

        found = find_inventory(remembered="~nouser/x.py")

        assert found is not None
        assert found.script == script
        assert found.source == "sibling"


import sys
import time

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

    def test_abandoning_the_generator_kills_the_child(self, tmp_path):
        """A Streamlit rerun mid-scan must not leave uv running detached."""
        marker = tmp_path / "still_alive.txt"
        child = (
            "import sys, time, pathlib\n"
            "print('started', flush=True)\n"
            "time.sleep(5)\n"
            f"pathlib.Path(r'{marker}').write_text('x')\n"
        )
        stream = run_streaming([sys.executable, "-c", child], tmp_path)

        assert next(stream) == "started"
        stream.close()          # what GeneratorExit does on abandonment

        # Wait past the child's full 5s sleep. A shorter wait can't tell a
        # killed child from a merely-not-finished-yet one: the marker
        # wouldn't exist within 1.5s either way, so that duration proves
        # nothing. Only outlasting the sleep makes the assertion meaningful.
        time.sleep(6)
        assert not marker.exists(), "child survived abandonment of the generator"

    def test_failure_carries_the_phase_it_was_given(self, tmp_path):
        cmd = [sys.executable, "-c", "raise SystemExit(2)"]

        with pytest.raises(InventoryFailed) as exc:
            list(run_streaming(cmd, tmp_path, phase="index"))

        assert exc.value.phase == "index"


class TestExportDirFor:
    def test_archive_yields_its_parent(self, tmp_path):
        archive = tmp_path / "takeout-001.zip"
        archive.write_bytes(b"not really a zip")
        assert export_dir_for(str(archive)) == str(tmp_path)

    def test_directory_yields_itself(self, tmp_path):
        """A directory scan's path IS the export dir; .parent would be wrong."""
        export = tmp_path / "Takeout"
        export.mkdir()
        assert export_dir_for(str(export)) == str(export)

    def test_missing_path_is_treated_as_a_file(self, tmp_path):
        """A path that no longer exists is not a directory, so use its parent."""
        assert export_dir_for(str(tmp_path / "gone.zip")) == str(tmp_path)
