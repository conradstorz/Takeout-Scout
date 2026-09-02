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

import os
import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import Iterator

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
    # PYTHONUNBUFFERED because bufsize=1 only line-buffers *our* reads; a
    # Python child writing to a pipe block-buffers unless told otherwise, and
    # bursty output defeats the point of streaming at all.
    env = {**os.environ, "PYTHONUNBUFFERED": "1"}
    process = subprocess.Popen(
        cmd,
        cwd=str(cwd),
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        encoding="utf-8",
        errors="replace",
        bufsize=1,
        env=env,
    )

    try:
        assert process.stdout is not None
        for raw in process.stdout:
            line = raw.rstrip("\n")
            tail.append(line)
            del tail[:-FAILURE_TAIL_LINES]
            yield line

        returncode = process.wait()
        if returncode != 0:
            raise InventoryFailed(phase, returncode, tail)
    finally:
        # Runs on normal completion, on InventoryFailed, and on GeneratorExit
        # when a consumer abandons the iteration. Without it an interrupted
        # Streamlit rerun leaves a multi-minute archive scan running detached.
        if process.poll() is None:
            process.terminate()
            try:
                process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait()
        if process.stdout is not None:
            process.stdout.close()
