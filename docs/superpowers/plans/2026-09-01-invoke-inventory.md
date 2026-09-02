# Invoking Takeout_Inventory Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Let a person run Scout's quick scan in the browser, then invoke `Takeout_Inventory` as a subprocess for the deep cross-archive pass, and read the resulting index back as a work list of what needs repairing.

**Architecture:** Three new pure modules under `takeout_scout/` — subprocess mechanics, read-only index reading, and work-list derivation — plus three touchpoints in `app.py`. Inventory is never imported, only executed, because Scout is GPL-3 and Inventory is AGPL-3.

**Tech Stack:** Python 3.10+, `sqlite3` and `subprocess` from the standard library, pytest, `uv`. **No new third-party dependencies.**

**Spec:** [`../specs/2026-09-01-invoke-inventory-design.md`](../specs/2026-09-01-invoke-inventory-design.md)

## Global Constraints

- Repository: `D:\Users\Conrad\Documents\programming\Takeout-Scout`. **Create a branch `feat/invoke-inventory` from `main` before Task 1.** Do not commit to `main`.
- Run Python only through `uv`: `uv run pytest`, `uv run python`. Never `pip install`, never `python -m venv`, never activate a venv.
- Do not chain shell commands with `&&`. Issue them separately.
- **Never `import takeout_inventory`, never vendor or copy any part of it.** Scout is GPL-3.0-or-later; Inventory is AGPL-3.0-or-later. Importing would create a combined work and pull AGPL §13 — the network clause — onto a program that serves a web UI. Task 5 adds a test enforcing this.
- Add no third-party dependencies. `sqlite3`, `subprocess`, `shutil` and `pathlib` are all standard library.
- Every new module under `takeout_scout/` starts with `from __future__ import annotations` after its docstring — all twelve existing modules do.
- Scout must remain fully usable with Inventory absent. A missing Inventory is an ordinary state, never an error.
- Scout must never write to Inventory's index. Open it read-only.
- Do not tidy or delete untracked files (`logs/`, `state/`, `takeouts_discovered/`, `discoveries_index.json`, `.pytest_cache/`, `__pycache__/`, `.venv/`).
- The suite is currently **218 passing**. All 218 must still pass, plus everything you add.
- Do not pin a total test count in any test or assertion — the doc guard parametrizes over Markdown files present on disk.

## Facts verified against Inventory's source

Do not re-derive these; they were read from `takeout_inventory.py` on 2026-09-01.

| Fact | Value |
| --- | --- |
| `INDEX_SCHEMA_VERSION` | `1` |
| `index_meta` keys written | `schema_version`, `tool_version` |
| `scan` arguments | `--takeout <dir>`, `--cache`, `--workers` |
| `index` arguments | `--out-sqlite`, `--out-json` |
| Confidence values | `own`, `related`, `none` |
| Pair rules | `exact`, `collision`, `no-ext-title`, `truncated-46`, `bracket-swap`, `numbered-dup`, `edited`, `edited+numbered`, `edited+no-ext`, `cross-extension`, `cross-directory`, `orphan`, `ambiguous` |
| `media` columns | `id, archive, path, area, folder, name, ext, size, actual_type, sidecar_id, rule, confidence` |
| `sidecar` columns | `id, archive, path, name, role, title, taken_at, lat, lon, device, trashed, archived, from_partner, parse_error` |

---

## File Structure

| File | Change | Responsibility |
| --- | --- | --- |
| `takeout_scout/inventory_runner.py` | create | Locate `takeout_inventory.py`; build argv; stream a subprocess. No Streamlit, no sqlite. |
| `takeout_scout/index_reader.py` | create | Open the index read-only, verify schema, expose rows. No Streamlit, no subprocess. |
| `takeout_scout/worklist.py` | create | Turn index rows into findings. Pure — no I/O at all. |
| `takeout_scout/__init__.py` | modify | Export the new public names. |
| `takeout_scout/app.py` | modify | Three touchpoints: the offer, the streaming output pane, the work-list view. |
| `tests/test_inventory_runner.py` | create | Tasks 1–2. |
| `tests/test_index_reader.py` | create | Task 3. |
| `tests/test_worklist.py` | create | Task 4. |
| `tests/test_licence_boundary.py` | create | Task 5. |
| `tests/test_app_contracts.py` | modify | Task 6 — extend `KNOWN_OBJECTS` so the guard covers the new integration code. |
| `README.md` | modify | Task 6 — document the deep pass. |

Tasks 1–5 add no Streamlit code at all. Task 6 is the only one that touches `app.py`, and by then everything it calls is already tested.

---

## Task 1: Locate Inventory

**Files:**
- Create: `takeout_scout/inventory_runner.py`
- Create: `tests/test_inventory_runner.py`

**Interfaces:**
- Consumes: nothing.
- Produces: `InventoryTool` (frozen dataclass with `script: Path`, `source: str`), and `find_inventory(remembered: str | None = None) -> InventoryTool | None`.

**Why this first.** Everything else is pointless if Scout cannot find the tool, and "not found" is the common case for anyone who is not you. Getting the None-not-exception decision right up front keeps it out of every later signature.

- [ ] **Step 1: Write the failing tests**

Create `tests/test_inventory_runner.py`:

```python
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
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `uv run pytest tests/test_inventory_runner.py -q`

Expected: FAIL — `ModuleNotFoundError: No module named 'takeout_scout.inventory_runner'`

- [ ] **Step 3: Write the implementation**

Create `takeout_scout/inventory_runner.py`:

```python
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
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `uv run pytest tests/test_inventory_runner.py -q`

Expected: PASS, 5 tests.

- [ ] **Step 5: Prove the tests can fail**

Temporarily change `find_inventory` to `return None` unconditionally. Run the tests again. Expected: 4 of 5 fail (`test_returns_none_when_absent` still passes, correctly — a trivial mutant satisfies a trivially-empty case). Restore.

Report the failure output.

- [ ] **Step 6: Run the full suite**

Run: `uv run pytest -q`

Expected: 218 prior tests plus 5 new, all passing.

- [ ] **Step 7: Commit**

Run: `git add takeout_scout/inventory_runner.py tests/test_inventory_runner.py`

Run: `git commit -m "feat: locate takeout_inventory.py for the deep pass" -m "Returns None when absent rather than raising - Scout must stay fully usable without Inventory." -m "Co-Authored-By: Claude Opus 5 (1M context) <noreply@anthropic.com>"`

---

## Task 2: Build and stream the deep-pass commands

**Files:**
- Modify: `takeout_scout/inventory_runner.py`
- Modify: `tests/test_inventory_runner.py`

**Interfaces:**
- Consumes: `InventoryTool` from Task 1.
- Produces: `InventoryFailed(Exception)` with attributes `phase: str`, `returncode: int`, `tail: list[str]`; `deep_pass_commands(tool, takeout_dir) -> list[tuple[str, list[str]]]`; `run_streaming(cmd, cwd) -> Iterator[str]`.

**Note the return type of `deep_pass_commands`:** a list of `(phase_name, argv)` pairs, so a failure can name *which* phase failed rather than just showing a command line.

- [ ] **Step 1: Write the failing tests**

Append to `tests/test_inventory_runner.py`:

```python
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
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `uv run pytest tests/test_inventory_runner.py -q`

Expected: FAIL — `ImportError: cannot import name 'InventoryFailed'`

- [ ] **Step 3: Write the implementation**

Append to `takeout_scout/inventory_runner.py` (and add `subprocess` plus `Iterator` to the imports at the top):

```python
INDEX_SQLITE_NAME = "takeout-index.sqlite"
INDEX_JSON_NAME = "takeout-index.json"

# How many trailing output lines to keep for a failure report. Enough to show
# a traceback, few enough not to flood the page.
FAILURE_TAIL_LINES = 20


class InventoryFailed(Exception):
    """A deep-pass phase exited non-zero.

    Carries the phase name so the report can say which half failed - a scan
    that dies is a different problem from an index that dies.
    """

    def __init__(self, phase: str, returncode: int, tail: list[str]) -> None:
        super().__init__(f"{phase} exited with code {returncode}")
        self.phase = phase
        self.returncode = returncode
        self.tail = tail


def deep_pass_commands(
    tool: InventoryTool, takeout_dir: Path
) -> list[tuple[str, list[str]]]:
    """The commands to run, in order, as (phase name, argv) pairs.

    Two separate commands rather than one, so a failure names the phase.

    Run through `uv run` because Inventory declares its dependencies in a
    PEP 723 header; uv resolves them without Scout knowing what they are,
    which is exactly the arm's-length relationship the licence needs.
    """
    script = str(tool.script)
    export = Path(takeout_dir)
    return [
        ("scan", ["uv", "run", script, "scan", "--takeout", str(export)]),
        (
            "index",
            [
                "uv", "run", script, "index",
                "--out-sqlite", str(export / INDEX_SQLITE_NAME),
                "--out-json", str(export / INDEX_JSON_NAME),
            ],
        ),
    ]


def run_streaming(
    cmd: list[str], cwd: Path, phase: str = "inventory"
) -> Iterator[str]:
    """Yield the subprocess's output lines as they arrive.

    rich detects that stdout is not a terminal and emits plain text without
    escape codes or live-updating bars, so the lines arrive ready to display.

    errors="replace" because Inventory prints file names, and a Takeout export
    contains filenames in every script on earth. A mangled character in a
    progress line must never abort a twenty-minute scan.
    """
    tail: list[str] = []
    process = subprocess.Popen(
        cmd,
        cwd=str(cwd),
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        encoding="utf-8",
        errors="replace",
        bufsize=1,
    )

    assert process.stdout is not None
    for raw in process.stdout:
        line = raw.rstrip("\n")
        tail.append(line)
        del tail[:-FAILURE_TAIL_LINES]
        yield line

    returncode = process.wait()
    if returncode != 0:
        raise InventoryFailed(phase, returncode, tail)
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `uv run pytest tests/test_inventory_runner.py -q`

Expected: PASS, 12 tests total in the file.

- [ ] **Step 5: Prove the command tests can fail**

Change the `scan` argv to use `--dir` instead of `--takeout`. Run the tests. Expected: `test_scan_command` fails. Restore.

Report the failure output.

- [ ] **Step 6: Run the full suite**

Run: `uv run pytest -q`

Expected: all passing.

- [ ] **Step 7: Commit**

Run: `git add takeout_scout/inventory_runner.py tests/test_inventory_runner.py`

Run: `git commit -m "feat: build and stream the Inventory deep-pass commands" -m "Two phases as (name, argv) pairs so a failure names which half died. Output is streamed so a twenty-minute scan does not look like a hang." -m "Co-Authored-By: Claude Opus 5 (1M context) <noreply@anthropic.com>"`

---

## Task 3: Read the index, read-only

**Files:**
- Create: `takeout_scout/index_reader.py`
- Create: `tests/test_index_reader.py`

**Interfaces:**
- Consumes: nothing from earlier tasks.
- Produces: `IndexUnusable(Exception)`; `IndexedPairing` (frozen dataclass: `media_path: str`, `archive: str | None`, `sidecar_path: str | None`, `rule: str`, `confidence: str`); `TakeoutIndex` with classmethod `open(path: Path) -> TakeoutIndex` and methods `pairings() -> list[IndexedPairing]`, `counts_by_rule() -> dict[str, int]`, `counts_by_confidence() -> dict[str, int]`, `unparseable_sidecars() -> list[tuple[str, str]]`, `claimed_sidecar_paths() -> set[str]`, `all_sidecar_paths() -> set[str]`.

**Context.** `INDEX_SCHEMA_VERSION` in Inventory is `1`. A higher value, or a missing `index_meta` table, must be refused rather than guessed at. This is a seam between two independently-versioned repositories.

- [ ] **Step 1: Write the failing tests**

Create `tests/test_index_reader.py`:

```python
"""Tests for takeout_scout.index_reader.

The index is written by Takeout_Inventory, a separate repository on its own
release cycle. These tests build synthetic databases from the schema literal,
which means they cannot catch schema drift by themselves - the version gate
and the column assertion are what turn drift into a clear error instead of a
wrong answer. That seam is real and is accepted knowingly.
"""
import sqlite3
from pathlib import Path

import pytest

from takeout_scout.index_reader import IndexUnusable, TakeoutIndex

SCHEMA = """
CREATE TABLE sidecar (
  id INTEGER PRIMARY KEY, archive TEXT, path TEXT NOT NULL, name TEXT NOT NULL,
  role TEXT, title TEXT, taken_at TEXT, lat REAL, lon REAL, device TEXT,
  trashed INTEGER NOT NULL DEFAULT 0, archived INTEGER NOT NULL DEFAULT 0,
  from_partner INTEGER NOT NULL DEFAULT 0, parse_error TEXT
);
CREATE TABLE media (
  id INTEGER PRIMARY KEY, archive TEXT, path TEXT NOT NULL, area TEXT NOT NULL,
  folder TEXT NOT NULL, name TEXT NOT NULL, ext TEXT, size INTEGER,
  actual_type TEXT, sidecar_id INTEGER REFERENCES sidecar(id),
  rule TEXT NOT NULL, confidence TEXT NOT NULL
);
CREATE TABLE archive (
  name TEXT PRIMARY KEY, size INTEGER NOT NULL, mtime INTEGER NOT NULL,
  members INTEGER NOT NULL, error TEXT
);
CREATE TABLE index_meta (key TEXT PRIMARY KEY, value TEXT);
"""


def build_index(path: Path, *, schema_version: str = "1",
                with_meta: bool = True, media=(), sidecars=()) -> Path:
    con = sqlite3.connect(path)
    con.executescript(SCHEMA)
    for row in sidecars:
        con.execute(
            "INSERT INTO sidecar (id, archive, path, name, parse_error) "
            "VALUES (?,?,?,?,?)", row)
    for row in media:
        con.execute(
            "INSERT INTO media (id, archive, path, area, folder, name, "
            "sidecar_id, rule, confidence) VALUES (?,?,?,?,?,?,?,?,?)", row)
    if with_meta:
        con.execute("INSERT INTO index_meta VALUES ('schema_version', ?)",
                    (schema_version,))
        con.execute("INSERT INTO index_meta VALUES ('tool_version', '0.1.0')")
    con.commit()
    con.close()
    return path


@pytest.fixture
def simple_index(tmp_path):
    """One own pairing, one orphan, one unparseable sidecar."""
    return build_index(
        tmp_path / "takeout-index.sqlite",
        sidecars=[
            (1, "part1.zip", "Photos/a.jpg.json", "a.jpg.json", None),
            (2, "part2.zip", "Photos/bad.json", "bad.json", "expected value"),
        ],
        media=[
            (1, "part1.zip", "Photos/a.jpg", "Photos", "Photos", "a.jpg",
             1, "exact", "own"),
            (2, "part1.zip", "Photos/lonely.jpg", "Photos", "Photos",
             "lonely.jpg", None, "orphan", "none"),
        ],
    )


class TestOpen:
    def test_reads_a_valid_index(self, simple_index):
        index = TakeoutIndex.open(simple_index)
        assert len(index.pairings()) == 2

    def test_missing_file(self, tmp_path):
        with pytest.raises(IndexUnusable):
            TakeoutIndex.open(tmp_path / "nope.sqlite")

    def test_newer_schema_is_refused(self, tmp_path):
        """Guessing at an unknown layout is how a wrong answer ships."""
        path = build_index(tmp_path / "i.sqlite", schema_version="2")
        with pytest.raises(IndexUnusable, match="schema"):
            TakeoutIndex.open(path)

    def test_missing_index_meta_is_refused(self, tmp_path):
        path = build_index(tmp_path / "i.sqlite", with_meta=False)
        with pytest.raises(IndexUnusable):
            TakeoutIndex.open(path)

    def test_missing_column_is_refused(self, tmp_path):
        """Schema drift must surface as an error, never as a wrong answer."""
        path = tmp_path / "i.sqlite"
        con = sqlite3.connect(path)
        con.executescript(
            "CREATE TABLE media (id INTEGER PRIMARY KEY, path TEXT);"
            "CREATE TABLE sidecar (id INTEGER PRIMARY KEY, path TEXT);"
            "CREATE TABLE index_meta (key TEXT PRIMARY KEY, value TEXT);"
            "INSERT INTO index_meta VALUES ('schema_version', '1');")
        con.commit()
        con.close()
        with pytest.raises(IndexUnusable):
            TakeoutIndex.open(path)

    def test_connection_is_read_only(self, simple_index):
        """'Opened read-only' is a claim about a URI until something writes."""
        index = TakeoutIndex.open(simple_index)
        with pytest.raises(sqlite3.OperationalError):
            index._con.execute("DELETE FROM media")


class TestQueries:
    def test_pairing_fields(self, simple_index):
        index = TakeoutIndex.open(simple_index)
        by_path = {p.media_path: p for p in index.pairings()}

        paired = by_path["Photos/a.jpg"]
        assert paired.sidecar_path == "Photos/a.jpg.json"
        assert paired.rule == "exact"
        assert paired.confidence == "own"
        assert paired.archive == "part1.zip"

        orphan = by_path["Photos/lonely.jpg"]
        assert orphan.sidecar_path is None
        assert orphan.rule == "orphan"

    def test_counts_by_rule(self, simple_index):
        assert TakeoutIndex.open(simple_index).counts_by_rule() == {
            "exact": 1, "orphan": 1}

    def test_counts_by_confidence(self, simple_index):
        assert TakeoutIndex.open(simple_index).counts_by_confidence() == {
            "own": 1, "none": 1}

    def test_unparseable_sidecars(self, simple_index):
        assert TakeoutIndex.open(simple_index).unparseable_sidecars() == [
            ("Photos/bad.json", "expected value")]

    def test_sidecar_path_sets(self, simple_index):
        index = TakeoutIndex.open(simple_index)
        assert index.claimed_sidecar_paths() == {"Photos/a.jpg.json"}
        assert index.all_sidecar_paths() == {
            "Photos/a.jpg.json", "Photos/bad.json"}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `uv run pytest tests/test_index_reader.py -q`

Expected: FAIL — `ModuleNotFoundError: No module named 'takeout_scout.index_reader'`

- [ ] **Step 3: Write the implementation**

Create `takeout_scout/index_reader.py`:

```python
"""Read the pairing index published by Takeout_Inventory.

Read-only, always. This file belongs to another program; Scout must be
incapable of writing to it.

The index answers the one question Scout's own scan gets wrong. Scout pairs a
photo to its sidecar within a single archive; on the real export measured by
Inventory, 71.7% of photos have their sidecar in a *different* archive.
"""
from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import quote

# The schema this reader understands. Inventory's INDEX_SCHEMA_VERSION.
# A higher value means Inventory moved on and Scout has not; refuse rather
# than guess at a layout that may have changed underneath the same names.
SUPPORTED_SCHEMA_VERSION = 1

_MEDIA_COLUMNS = {
    "id", "archive", "path", "area", "folder", "name", "ext", "size",
    "actual_type", "sidecar_id", "rule", "confidence",
}
_SIDECAR_COLUMNS = {
    "id", "archive", "path", "name", "role", "title", "taken_at", "lat",
    "lon", "device", "trashed", "archived", "from_partner", "parse_error",
}


class IndexUnusable(Exception):
    """The index is missing, unreadable, or a schema this version cannot read."""


@dataclass(frozen=True)
class IndexedPairing:
    """One media file and the sidecar Inventory paired it with.

    `confidence` licenses what a consumer may believe:
      own     - the sidecar names this file: date and GPS
      related - it names a different file: date only, never GPS
      none    - residue
    """

    media_path: str
    archive: str | None
    sidecar_path: str | None
    rule: str
    confidence: str


class TakeoutIndex:
    """A read-only view of one takeout-index.sqlite."""

    def __init__(self, con: sqlite3.Connection) -> None:
        self._con = con

    @classmethod
    def open(cls, path: Path) -> "TakeoutIndex":
        path = Path(path)
        if not path.is_file():
            raise IndexUnusable(f"no index at {path}")

        # quote() so a path with spaces or '#' survives the URI round-trip.
        uri = f"file:{quote(str(path.resolve()))}?mode=ro"
        try:
            con = sqlite3.connect(uri, uri=True)
            con.row_factory = sqlite3.Row
        except sqlite3.Error as exc:
            raise IndexUnusable(f"cannot open {path}: {exc}") from exc

        try:
            cls._verify(con)
        except Exception:
            con.close()
            raise
        return cls(con)

    @staticmethod
    def _verify(con: sqlite3.Connection) -> None:
        try:
            rows = con.execute(
                "SELECT value FROM index_meta WHERE key = 'schema_version'"
            ).fetchall()
        except sqlite3.Error as exc:
            raise IndexUnusable(
                "no index_meta table - this index predates schema versioning"
            ) from exc

        if not rows:
            raise IndexUnusable("index_meta has no schema_version")

        try:
            version = int(rows[0][0])
        except (TypeError, ValueError) as exc:
            raise IndexUnusable(f"unreadable schema_version: {rows[0][0]!r}") from exc

        if version > SUPPORTED_SCHEMA_VERSION:
            raise IndexUnusable(
                f"index schema {version} is newer than the supported "
                f"{SUPPORTED_SCHEMA_VERSION}; update Takeout Scout"
            )

        for table, expected in (("media", _MEDIA_COLUMNS),
                                ("sidecar", _SIDECAR_COLUMNS)):
            try:
                present = {r["name"] for r in
                           con.execute(f"PRAGMA table_info({table})")}
            except sqlite3.Error as exc:
                raise IndexUnusable(f"cannot inspect {table}: {exc}") from exc
            missing = expected - present
            if missing:
                raise IndexUnusable(
                    f"{table} is missing columns: {sorted(missing)}")

    def pairings(self) -> list[IndexedPairing]:
        rows = self._con.execute(
            "SELECT m.path AS media_path, m.archive AS archive, "
            "       s.path AS sidecar_path, m.rule AS rule, "
            "       m.confidence AS confidence "
            "FROM media m LEFT JOIN sidecar s ON s.id = m.sidecar_id"
        ).fetchall()
        return [
            IndexedPairing(
                media_path=r["media_path"],
                archive=r["archive"],
                sidecar_path=r["sidecar_path"],
                rule=r["rule"],
                confidence=r["confidence"],
            )
            for r in rows
        ]

    def counts_by_rule(self) -> dict[str, int]:
        return {r["rule"]: r["n"] for r in self._con.execute(
            "SELECT rule, COUNT(*) AS n FROM media GROUP BY rule")}

    def counts_by_confidence(self) -> dict[str, int]:
        return {r["confidence"]: r["n"] for r in self._con.execute(
            "SELECT confidence, COUNT(*) AS n FROM media GROUP BY confidence")}

    def unparseable_sidecars(self) -> list[tuple[str, str]]:
        """Sidecars whose JSON could not be read, with the reason.

        Distinct from a photo having no sidecar: the metadata exists and is
        corrupt. Collapsing the two would misreport the residue.
        """
        return [(r["path"], r["parse_error"]) for r in self._con.execute(
            "SELECT path, parse_error FROM sidecar "
            "WHERE parse_error IS NOT NULL ORDER BY path")]

    def claimed_sidecar_paths(self) -> set[str]:
        return {r["path"] for r in self._con.execute(
            "SELECT DISTINCT s.path FROM sidecar s "
            "JOIN media m ON m.sidecar_id = s.id")}

    def all_sidecar_paths(self) -> set[str]:
        return {r["path"] for r in self._con.execute(
            "SELECT path FROM sidecar")}
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `uv run pytest tests/test_index_reader.py -q`

Expected: PASS, 11 tests.

- [ ] **Step 5: Prove the refusals can fail**

Remove the `if version > SUPPORTED_SCHEMA_VERSION:` block. Run the tests. Expected: `test_newer_schema_is_refused` fails.

Then restore it, and instead change the URI to drop `?mode=ro`. Run again. Expected: `test_connection_is_read_only` fails.

Restore both. Report both failure outputs.

- [ ] **Step 6: Run the full suite**

Run: `uv run pytest -q`

Expected: all passing.

- [ ] **Step 7: Commit**

Run: `git add takeout_scout/index_reader.py tests/test_index_reader.py`

Run: `git commit -m "feat: read Inventory's pairing index, read-only" -m "Refuses a newer schema or a missing column rather than guessing - this is a seam between two independently-versioned repositories, and a wrong answer would be worse than an error." -m "Co-Authored-By: Claude Opus 5 (1M context) <noreply@anthropic.com>"`

---

## Task 4: Derive the work list

**Files:**
- Create: `takeout_scout/worklist.py`
- Create: `tests/test_worklist.py`

**Interfaces:**
- Consumes: `TakeoutIndex` and `IndexedPairing` from Task 3.
- Produces: `Finding` (frozen dataclass: `kind: str`, `path: str`, `detail: str`); `build_worklist(index) -> list[Finding]`; `compare_with_scout(index, scout_pairings: dict[str, str | None]) -> tuple[int, int, list[Finding]]`.

**Finding kinds — use these exact strings:** `orphan_media`, `ambiguous_pairing`, `orphan_sidecar`, `related_pairing`, `unparseable_sidecar`, `disagreement`.

- [ ] **Step 1: Write the failing tests**

Create `tests/test_worklist.py`:

```python
"""Tests for takeout_scout.worklist.

Pure functions over index rows. The point of the work list is that it says
what is *wrong*, so every test here is about a defect being surfaced, not
about an inventory being complete.
"""
import pytest

from takeout_scout.index_reader import IndexedPairing
from takeout_scout.worklist import Finding, build_worklist, compare_with_scout


class FakeIndex:
    """Stands in for TakeoutIndex - worklist takes rows, not a database."""

    def __init__(self, pairings=(), unparseable=(), all_sidecars=None,
                 claimed=None):
        self._pairings = list(pairings)
        self._unparseable = list(unparseable)
        self._all = set(all_sidecars or ())
        self._claimed = set(claimed or ())

    def pairings(self):
        return self._pairings

    def unparseable_sidecars(self):
        return self._unparseable

    def all_sidecar_paths(self):
        return self._all

    def claimed_sidecar_paths(self):
        return self._claimed


def _kinds(findings):
    return sorted({f.kind for f in findings})


class TestBuildWorklist:
    def test_a_clean_index_yields_nothing(self):
        index = FakeIndex(
            pairings=[IndexedPairing("a.jpg", "p1.zip", "a.jpg.json",
                                     "exact", "own")],
            all_sidecars={"a.jpg.json"}, claimed={"a.jpg.json"})
        assert build_worklist(index) == []

    def test_orphan_media(self):
        index = FakeIndex(pairings=[
            IndexedPairing("lonely.jpg", "p1.zip", None, "orphan", "none")])
        findings = build_worklist(index)
        assert _kinds(findings) == ["orphan_media"]
        assert findings[0].path == "lonely.jpg"

    def test_ambiguous_is_not_reported_as_an_orphan(self):
        """Two candidates is a different problem from none."""
        index = FakeIndex(pairings=[
            IndexedPairing("x.jpg", "p1.zip", None, "ambiguous", "none")])
        assert _kinds(build_worklist(index)) == ["ambiguous_pairing"]

    def test_related_pairing_is_flagged_for_gps(self):
        """GPTH issue #139: photos silently acquiring another photo's GPS."""
        index = FakeIndex(
            pairings=[IndexedPairing("a-edited.jpg", "p1.zip", "a.jpg.json",
                                     "edited", "related")],
            all_sidecars={"a.jpg.json"}, claimed={"a.jpg.json"})
        findings = build_worklist(index)
        assert _kinds(findings) == ["related_pairing"]
        assert "GPS" in findings[0].detail

    def test_orphan_sidecar(self):
        """A sidecar naming a file that is not in the export."""
        index = FakeIndex(pairings=[], all_sidecars={"ghost.jpg.json"},
                          claimed=set())
        findings = build_worklist(index)
        assert _kinds(findings) == ["orphan_sidecar"]
        assert findings[0].path == "ghost.jpg.json"

    def test_unparseable_sidecar_is_distinct_from_missing(self):
        index = FakeIndex(unparseable=[("bad.json", "expected value")],
                          all_sidecars={"bad.json"}, claimed={"bad.json"})
        findings = build_worklist(index)
        assert _kinds(findings) == ["unparseable_sidecar"]
        assert "expected value" in findings[0].detail

    def test_findings_are_sorted_for_stable_display(self):
        index = FakeIndex(pairings=[
            IndexedPairing("z.jpg", "p.zip", None, "orphan", "none"),
            IndexedPairing("a.jpg", "p.zip", None, "orphan", "none")])
        assert [f.path for f in build_worklist(index)] == ["a.jpg", "z.jpg"]


class TestCompareWithScout:
    def test_agreement(self):
        index = FakeIndex(pairings=[
            IndexedPairing("a.jpg", "p.zip", "a.jpg.json", "exact", "own")])
        agree, disagree, findings = compare_with_scout(
            index, {"a.jpg": "a.jpg.json"})
        assert (agree, disagree) == (1, 0)
        assert findings == []

    def test_disagreement_is_reported(self):
        """Scout paired within one archive; Inventory paired across all."""
        index = FakeIndex(pairings=[
            IndexedPairing("a.jpg", "p2.zip", "other/a.jpg.json",
                           "cross-directory", "own")])
        agree, disagree, findings = compare_with_scout(
            index, {"a.jpg": "wrong/a.jpg.json"})
        assert (agree, disagree) == (0, 1)
        assert findings[0].kind == "disagreement"
        assert "other/a.jpg.json" in findings[0].detail

    def test_scout_found_none_where_inventory_paired(self):
        """The 71.7% case: the sidecar was in another archive."""
        index = FakeIndex(pairings=[
            IndexedPairing("a.jpg", "p2.zip", "far/a.jpg.json",
                           "cross-directory", "own")])
        agree, disagree, _ = compare_with_scout(index, {"a.jpg": None})
        assert (agree, disagree) == (0, 1)

    def test_media_scout_never_saw_is_not_a_disagreement(self):
        """Only compare what both sides looked at."""
        index = FakeIndex(pairings=[
            IndexedPairing("a.jpg", "p.zip", "a.jpg.json", "exact", "own")])
        agree, disagree, findings = compare_with_scout(index, {})
        assert (agree, disagree) == (0, 0)
        assert findings == []

    def test_both_none_is_agreement(self):
        index = FakeIndex(pairings=[
            IndexedPairing("a.jpg", "p.zip", None, "orphan", "none")])
        agree, disagree, _ = compare_with_scout(index, {"a.jpg": None})
        assert (agree, disagree) == (1, 0)
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `uv run pytest tests/test_worklist.py -q`

Expected: FAIL — `ModuleNotFoundError: No module named 'takeout_scout.worklist'`

- [ ] **Step 3: Write the implementation**

Create `takeout_scout/worklist.py`:

```python
"""Turn Inventory's index into a list of what needs repairing.

Not an inventory - Inventory already produces one. This is the subset that is
*wrong*: media with no metadata, metadata with no media, pairings whose
location cannot be trusted, and the places where Scout's own per-archive scan
disagrees with Inventory's global one.

Nothing here writes to the archives. The output is a work list.
"""
from __future__ import annotations

from dataclasses import dataclass

ORPHAN_MEDIA = "orphan_media"
AMBIGUOUS_PAIRING = "ambiguous_pairing"
ORPHAN_SIDECAR = "orphan_sidecar"
RELATED_PAIRING = "related_pairing"
UNPARSEABLE_SIDECAR = "unparseable_sidecar"
DISAGREEMENT = "disagreement"


@dataclass(frozen=True)
class Finding:
    """One thing that is wrong, and enough context to act on it."""

    kind: str
    path: str
    detail: str


def build_worklist(index) -> list[Finding]:
    """Every defect the index describes, sorted by path for a stable display."""
    findings: list[Finding] = []

    for pairing in index.pairings():
        if pairing.rule == "ambiguous":
            findings.append(Finding(
                AMBIGUOUS_PAIRING, pairing.media_path,
                "more than one candidate sidecar; Inventory refused to guess"))
        elif pairing.sidecar_path is None:
            findings.append(Finding(
                ORPHAN_MEDIA, pairing.media_path,
                "no sidecar: neither date nor location is recoverable"))
        elif pairing.confidence == "related":
            findings.append(Finding(
                RELATED_PAIRING, pairing.media_path,
                f"paired to {pairing.sidecar_path} by rule "
                f"'{pairing.rule}': the date describes this photograph, "
                f"the GPS describes a different one"))

    for path in index.all_sidecar_paths() - index.claimed_sidecar_paths():
        findings.append(Finding(
            ORPHAN_SIDECAR, path,
            "metadata for a file that is not in the export"))

    for path, error in index.unparseable_sidecars():
        findings.append(Finding(
            UNPARSEABLE_SIDECAR, path,
            f"metadata exists but could not be read: {error}"))

    return sorted(findings, key=lambda f: (f.path, f.kind))


def compare_with_scout(
    index, scout_pairings: dict[str, str | None]
) -> tuple[int, int, list[Finding]]:
    """(agreements, disagreements, findings) between the two pairings.

    Scout pairs within a single archive. Inventory pairs across all of them,
    and on the real export 71.7% of photos have their sidecar in a different
    archive - so on a multi-part export this disagreement count is the honest
    measure of what the quick scan got wrong.

    Only media present on both sides is compared. Anything Scout never looked
    at is not a disagreement.
    """
    agreements = 0
    findings: list[Finding] = []

    for pairing in index.pairings():
        if pairing.media_path not in scout_pairings:
            continue
        scout_said = scout_pairings[pairing.media_path]
        if scout_said == pairing.sidecar_path:
            agreements += 1
            continue
        findings.append(Finding(
            DISAGREEMENT, pairing.media_path,
            f"Scout paired {scout_said or 'nothing'}; "
            f"Inventory paired {pairing.sidecar_path or 'nothing'} "
            f"by rule '{pairing.rule}'"))

    findings.sort(key=lambda f: f.path)
    return agreements, len(findings), findings
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `uv run pytest tests/test_worklist.py -q`

Expected: PASS, 12 tests.

- [ ] **Step 5: Prove the tests can fail**

Change `build_worklist` to `return []` unconditionally. Run the tests. Expected: 6 of 7 `TestBuildWorklist` tests fail (`test_a_clean_index_yields_nothing` still passes — a trivial mutant satisfies a trivially-empty case). Restore.

Then change `compare_with_scout` to `return (0, 0, [])`. Run again. Expected: at least 3 `TestCompareWithScout` tests fail. Restore.

Report both failure outputs.

- [ ] **Step 6: Run the full suite**

Run: `uv run pytest -q`

Expected: all passing.

- [ ] **Step 7: Commit**

Run: `git add takeout_scout/worklist.py tests/test_worklist.py`

Run: `git commit -m "feat: derive a work list from the pairing index" -m "Reports what is wrong rather than what exists, including where Scout's per-archive pairing disagrees with Inventory's global one." -m "Co-Authored-By: Claude Opus 5 (1M context) <noreply@anthropic.com>"`

---

## Task 5: Export the new names, and pin the licence boundary

**Files:**
- Modify: `takeout_scout/__init__.py`
- Create: `tests/test_licence_boundary.py`

**Interfaces:**
- Consumes: every public name from Tasks 1–4.
- Produces: importable names from `takeout_scout` for Task 6.

**Why the boundary needs a test.** "Never import Inventory" is a design constraint, and a design constraint with no test is a comment. It is also the constraint whose violation would be least visible: the code would work perfectly, and only the licence would be wrong.

- [ ] **Step 1: Write the failing test**

Create `tests/test_licence_boundary.py`:

```python
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
```

- [ ] **Step 2: Run the test to verify it passes, then prove it can fail**

Run: `uv run pytest tests/test_licence_boundary.py -q`

Expected: PASS.

Now prove it works. Add `import takeout_inventory  # noqa` as the first line of the body of `find_inventory` in `takeout_scout/inventory_runner.py`. Run again.

Expected: FAIL, naming `inventory_runner.py` and the line number.

**If it passes, the guard is broken — stop and report BLOCKED.** Remove the line afterwards.

- [ ] **Step 3: Export the new names**

In `takeout_scout/__init__.py`, add these imports beside the existing ones and the names to `__all__`, following the file's existing grouping-comment style:

```python
from takeout_scout.inventory_runner import (
    InventoryTool,
    InventoryFailed,
    find_inventory,
    deep_pass_commands,
    run_streaming,
)
from takeout_scout.index_reader import (
    IndexUnusable,
    IndexedPairing,
    TakeoutIndex,
)
from takeout_scout.worklist import (
    Finding,
    build_worklist,
    compare_with_scout,
)
```

Add to `__all__`, in a group commented `# Inventory integration`:

```python
    "InventoryTool",
    "InventoryFailed",
    "find_inventory",
    "deep_pass_commands",
    "run_streaming",
    "IndexUnusable",
    "IndexedPairing",
    "TakeoutIndex",
    "Finding",
    "build_worklist",
    "compare_with_scout",
```

- [ ] **Step 4: Verify the package still imports without Streamlit**

Run: `uv run python -c "import takeout_scout, sys; print('streamlit' in sys.modules)"`

Expected: `False`

**This matters.** If `__init__` ever pulls in Streamlit, every consumer of the package — the whole test suite included — starts requiring it. If this prints `True`, something imported `app`; stop and report.

- [ ] **Step 5: Verify the exports resolve**

Run: `uv run python -c "from takeout_scout import find_inventory, TakeoutIndex, build_worklist, compare_with_scout; print('exports ok')"`

Expected: `exports ok`

- [ ] **Step 6: Run the full suite**

Run: `uv run pytest -q`

Expected: all passing.

- [ ] **Step 7: Commit**

Run: `git add takeout_scout/__init__.py tests/test_licence_boundary.py`

Run: `git commit -m "feat: export the Inventory integration, and test the licence boundary" -m "Scout is GPL-3 and Inventory AGPL-3; importing rather than executing would make a combined work and pull the AGPL network clause onto a web UI. A design constraint with no test is a comment, so it is tested." -m "Co-Authored-By: Claude Opus 5 (1M context) <noreply@anthropic.com>"`

---

## Task 6: The three UI touchpoints

**Files:**
- Modify: `takeout_scout/app.py`

**Interfaces:**
- Consumes: everything exported in Task 5.
- Produces: nothing later tasks depend on. This is the last task.

**Context.** `app.py` has no test harness, which is exactly why Tasks 1–5 put every decision outside it. This task is wiring only: no new logic that could be tested elsewhere belongs here.

Existing patterns to follow: `STATE_DIR` / `load_recent_folders` / `save_recent_folders` around `app.py:135-165` show how Scout remembers things between sessions. Session state is initialised in the `if 'x' not in st.session_state` block around `app.py:300-320`.

- [ ] **Step 1: Add session state and the remembered path**

In the session-state initialisation block, following the existing style:

```python
    if 'inventory_path' not in st.session_state:
        st.session_state.inventory_path = load_inventory_path()
    if 'worklist' not in st.session_state:
        st.session_state.worklist = None  # list[Finding] once a deep pass has run
    if 'deep_pass_summary' not in st.session_state:
        st.session_state.deep_pass_summary = None
```

Beside `RECENT_FOLDERS_PATH`, add the storage path and its two helpers, mirroring `load_recent_folders` / `save_recent_folders` exactly in structure and error handling:

```python
INVENTORY_PATH_FILE = STATE_DIR / 'inventory_path.json'


def load_inventory_path() -> Optional[str]:
    """The takeout_inventory.py path chosen in an earlier session, if any."""
    if INVENTORY_PATH_FILE.exists():
        try:
            with open(INVENTORY_PATH_FILE, 'r', encoding='utf-8') as f:
                return json.load(f).get('path')
        except Exception as e:
            logger.warning(f"Could not read remembered Inventory path: {e}")
    return None


def save_inventory_path(path: str) -> None:
    """Remember a takeout_inventory.py path for next time."""
    try:
        with open(INVENTORY_PATH_FILE, 'w', encoding='utf-8') as f:
            json.dump({'path': path}, f, indent=2)
    except Exception as e:
        logger.warning(f"Could not save Inventory path: {e}")
```

- [ ] **Step 2: Add the offer**

A new function, called from `main()` after results are displayed:

```python
def show_deep_pass_offer():
    """Offer the Inventory deep pass, if Inventory can be found.

    Scout pairs photos to sidecars within one archive. Inventory pairs across
    all of them, which on a real multi-part export is a different answer for
    most photos. This is where that gap gets closed.
    """
    if not st.session_state.results:
        return

    tool = find_inventory(st.session_state.inventory_path)

    st.divider()
    st.header("🔬 Deep pass")

    if tool is None:
        st.info(
            "Takeout Inventory was not found, so the deep cross-archive pass "
            "is unavailable. Scout works fine without it — this would add "
            "correct media↔sidecar pairing across archive boundaries."
        )
        with st.expander("Point Scout at takeout_inventory.py"):
            entered = st.text_input(
                "Path to takeout_inventory.py",
                key="inventory_path_input",
                help="Part of the separate Takeout_Inventory project.",
            )
            if entered and st.button("Save path"):
                cleaned = clean_file_path(entered)
                st.session_state.inventory_path = cleaned
                save_inventory_path(cleaned)
                st.rerun()
        return

    directories = sorted({_export_dir_for(r.path) for r in st.session_state.results})

    st.caption(f"Using `{tool.script}` (found: {tool.source})")
    if len(directories) == 1:
        chosen = directories[0]
        st.write(f"Will analyse: `{chosen}`")
    else:
        chosen = st.selectbox(
            "Which export directory?",
            directories,
            help="Inventory analyses one directory at a time.",
        )

    st.caption(
        "Reads every archive and resolves pairings across all of them. "
        "The first run takes a while; results are cached per archive, so "
        "later runs are fast. Nothing in your archives is modified."
    )

    if st.button("🔬 Run deep pass", type="primary", width="stretch"):
        run_deep_pass(tool, Path(chosen))
```

Add this helper beside `show_deep_pass_offer`:

```python
def _export_dir_for(scanned_path: str) -> str:
    """The directory Inventory should be pointed at for a scanned result.

    ArchiveSummary.path is whatever was scanned. For an archive
    (D:/exports/takeout-001.zip) the export directory is its parent. For a
    directory scan (D:/exports/Takeout) the path *is* the export directory —
    taking .parent there would send Inventory one level too high, where it
    would find the wrong thing or nothing.
    """
    path = Path(scanned_path)
    return str(path if path.is_dir() else path.parent)
```

- [ ] **Step 3: Add the streaming runner**

```python
def run_deep_pass(tool: InventoryTool, takeout_dir: Path):
    """Run Inventory's two phases, streaming output into the page."""
    output = st.empty()
    lines: List[str] = []

    for phase, cmd in deep_pass_commands(tool, takeout_dir):
        st.write(f"**{phase}**")
        try:
            for line in run_streaming(cmd, takeout_dir, phase=phase):
                lines.append(line)
                output.code("\n".join(lines[-40:]))
        except FileNotFoundError:
            st.error(
                "Could not run `uv`. Takeout Inventory is a PEP 723 script "
                "and needs uv on PATH to resolve its dependencies."
            )
            return
        except InventoryFailed as failure:
            st.error(f"The {failure.phase} phase exited with code {failure.returncode}.")
            st.code("\n".join(failure.tail))
            return

    index_path = takeout_dir / 'takeout-index.sqlite'
    try:
        index = TakeoutIndex.open(index_path)
    except IndexUnusable as e:
        st.error(f"The deep pass finished but its index could not be read: {e}")
        return

    scout_pairings = _scout_pairings()
    agree, disagree, disagreements = compare_with_scout(index, scout_pairings)

    st.session_state.worklist = build_worklist(index) + disagreements
    st.session_state.deep_pass_summary = {
        'rules': index.counts_by_rule(),
        'confidence': index.counts_by_confidence(),
        'agreements': agree,
        'disagreements': disagree,
        'compared': len(scout_pairings),
    }
    st.success("Deep pass complete.")
    st.rerun()


def _scout_pairings() -> dict:
    """Scout's own media→sidecar answers, for comparison with Inventory's.

    Read from the discovery records the quick scan already wrote, so this
    compares what Scout actually concluded rather than recomputing it.
    """
    pairings = {}
    for result in st.session_state.results:
        try:
            discovery = load_takeout_discovery(Path(result.path))
            if not discovery:
                continue
            for fd in discovery.iter_file_details():
                if fd.file_type in ('photo', 'video'):
                    pairings[fd.path] = fd.sidecar_path
        except Exception:
            logger.exception(f"Could not read Scout pairings for {result.path}")
            continue
    return pairings
```

- [ ] **Step 4: Add the work-list view**

```python
def show_worklist():
    """What the deep pass found that needs attention."""
    if not st.session_state.worklist and not st.session_state.deep_pass_summary:
        return

    summary = st.session_state.deep_pass_summary
    st.divider()
    st.header("🧾 Work list")

    if summary and summary['compared']:
        col1, col2, col3 = st.columns(3)
        col1.metric("Compared", f"{summary['compared']:,}")
        col2.metric("Agreed", f"{summary['agreements']:,}")
        col3.metric("Scout was wrong", f"{summary['disagreements']:,}")
        st.caption(
            "Scout pairs within one archive; Inventory pairs across all of "
            "them. Where they differ, Inventory is right."
        )

    findings = st.session_state.worklist or []
    if not findings:
        st.success("Nothing needs attention.")
        return

    counts = Counter(f.kind for f in findings)
    st.write(" · ".join(f"{n:,} {kind.replace('_', ' ')}"
                        for kind, n in sorted(counts.items())))

    df = pd.DataFrame([
        {'kind': f.kind, 'path': f.path, 'detail': f.detail}
        for f in findings
    ])
    st.dataframe(df, hide_index=True, width="stretch")

    st.download_button(
        label="📥 Export work list (CSV)",
        data=df.to_csv(index=False).encode('utf-8'),
        file_name=f"worklist_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
        mime='text/csv',
    )
    st.caption("Nothing in your archives has been modified.")
```

- [ ] **Step 5: Wire the imports and the calls**

Add to the `takeout_scout` import block at the top of `app.py`:

```python
from takeout_scout import (
    InventoryTool,
    InventoryFailed,
    find_inventory,
    deep_pass_commands,
    run_streaming,
    IndexUnusable,
    TakeoutIndex,
    build_worklist,
    compare_with_scout,
)
```

Add `from collections import Counter` beside the existing standard-library imports if it is not already present.

In `main()`, after the results section and before the analysis sections, call:

```python
    show_deep_pass_offer()
    show_worklist()
```

Also add `st.session_state.worklist = None` and
`st.session_state.deep_pass_summary = None` to the "Clear Results" handler,
alongside the resets already there — a stale work list describing cleared
results would be worse than none.

- [ ] **Step 6: Extend the contract guard to cover the new code**

`tests/test_app_contracts.py` walks `app.py`'s AST and checks that every
attribute access on a known local name is real. Its `KNOWN_OBJECTS` currently
covers `discovery`, `fd` and `hash_index` — so none of the code this task adds
is checked. Add the two new local names:

```python
KNOWN_OBJECTS = {
    "discovery": TakeoutDiscovery,
    "fd": FileDetails,
    "hash_index": HashIndex,
    "index": TakeoutIndex,
    "tool": InventoryTool,
}
```

Import `TakeoutIndex` and `InventoryTool` in that test file alongside the
existing imports.

**`_valid_attrs_for` needs no change**, but check why: it instantiates
non-dataclass classes with `cls()` to see attributes created in `__init__`.
`TakeoutIndex()` requires a connection argument and `InventoryTool` is a frozen
dataclass. `InventoryTool` takes the dataclass branch; `TakeoutIndex` does not
and would raise on `cls()`. Add a branch for classes that cannot be
instantiated bare:

```python
def _valid_attrs_for(cls) -> set[str]:
    """The set of legitimate attribute names for one of KNOWN_OBJECTS's classes."""
    if dataclasses.is_dataclass(cls):
        return {f.name for f in dataclasses.fields(cls)} | set(dir(cls))
    try:
        # HashIndex: _by_hash and _by_path are created in __init__, so they do
        # not appear on the class itself — it must be instantiated to see them.
        return set(dir(cls()))
    except TypeError:
        # TakeoutIndex needs a connection; its public surface is all methods,
        # which are visible on the class.
        return set(dir(cls))
```

Run: `uv run python -c "import ast,pathlib; ast.parse(pathlib.Path('takeout_scout/app.py').read_text(encoding='utf-8')); print('app parses')"`

Expected: `app parses`

Run: `uv run pytest tests/test_app_contracts.py -q`

Expected: PASS. `_scout_pairings` uses `discovery.iter_file_details()` and
`fd.file_type` / `fd.sidecar_path`, all already known; the new `index.*` and
`tool.*` accesses are now checked too.

**Prove the extension works.** Temporarily change `index.counts_by_rule()` to
`index.counts_by_rule_typo()` in `run_deep_pass` and run the guard. Expected:
FAIL naming that line. Restore. Report the failure output — without this the
two new entries could be inert.

- [ ] **Step 7: Run the full suite**

Run: `uv run pytest -q`

Expected: all passing, at the total reached in Task 5.

- [ ] **Step 8: Update the README**

Add a section after the existing feature list:

```markdown
## Deep pass with Takeout Inventory

Scout pairs each photo with its `.json` sidecar within a single archive. On a
multi-part export that is usually the wrong answer — in one measured export,
71.7% of photos had their sidecar in a *different* archive.

If [`Takeout_Inventory`](https://github.com/conradstorz/Takeout_Inventory) is
available, Scout offers to run it after a scan. It resolves pairings across
every archive at once and publishes an index, which Scout reads back as a work
list: orphaned media, orphaned sidecars, pairings whose location data cannot be
trusted, and every place Scout's own answer was wrong.

Inventory is run as a separate program, never imported — Scout is GPL-3.0 and
Inventory is AGPL-3.0. Scout works fully without it; the offer simply does not
appear.

**Nothing in your archives is modified.** The output is a list.
```

**The doc guard (`tests/test_docs.py`) checks that file paths named inside bash fences exist**, so do not add a fenced command naming a path outside this repository.

- [ ] **Step 9: Run the full suite once more**

Run: `uv run pytest -q`

Expected: all passing. The README gained no bash fence, so the guard's parametrized case count is unchanged.

- [ ] **Step 10: Commit**

Run: `git add takeout_scout/app.py tests/test_app_contracts.py README.md`

Run: `git commit -m "feat: offer the Inventory deep pass and show its work list" -m "Three touchpoints: the offer, a live output pane, and the work list. All decisions live in tested modules; app.py is wiring only, because it has no test harness." -m "Co-Authored-By: Claude Opus 5 (1M context) <noreply@anthropic.com>"`

---

## Definition of done

1. `uv run pytest -q` passes, with all 218 prior tests among them.
2. `uv run python -c "import takeout_scout, sys; print('streamlit' in sys.modules)"` prints `False`.
3. `tests/test_licence_boundary.py` passes, and fails when an `import takeout_inventory` is added anywhere under `takeout_scout/`.
4. With Inventory present, a scan offers the deep pass; with it absent, Scout behaves exactly as it does today.
5. Every mutation named in Tasks 1–5 produced the failure it was supposed to.

## Out of scope — deliberately not built

- **Acting on the work list.** Renaming files, writing sidecar dates into EXIF, moving photos into albums. Destructive operations on irreplaceable data; needs its own design and its own dry-run argument.
- **Teaching Inventory EXIF and content hashing**, which would let it subsume Scout's engine. A large job in the other repository.
- **Using the index for duplicate or EXIF analysis.** The index carries neither; Scout's own engine remains the only source for both.
