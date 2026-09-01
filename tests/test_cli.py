"""Tests for the console-script launcher.

`takeout-scout` is the entry point an installed wheel exposes. It broke once
already: the script pointed at a module the wheel did not contain, so any
non-editable install died with ModuleNotFoundError. Nothing caught it because
nothing tested the launcher.

These tests cover the two things that failure mode depended on — that the
command targets the packaged app, and that the exit code reaching the shell is
Streamlit's own.
"""
from __future__ import annotations

import subprocess
from pathlib import Path

import pytest

from takeout_scout import cli

# main() builds [python, "-m", "streamlit", "run", <script>, *sys.argv[1:]],
# so the script sits at a fixed index. It is not cmd[-1] — forwarded arguments
# come after it.
SCRIPT_ARG_INDEX = 4


def _fake_run(returncode: int, recorder: dict):
    """A subprocess.run stand-in that records its command and returns a code."""

    def run(cmd, **kwargs):
        recorder["cmd"] = cmd
        recorder["kwargs"] = kwargs
        return subprocess.CompletedProcess(cmd, returncode)

    return run


def test_main_targets_the_packaged_app(monkeypatch: pytest.MonkeyPatch) -> None:
    """The launcher must run the app.py that ships inside the package.

    This is the original bug: the entry point named a module that was not in
    the wheel. Asserting the resolved target is a real file inside
    takeout_scout/ is what makes that unrepeatable.
    """
    seen: dict = {}
    monkeypatch.setattr(cli.subprocess, "run", _fake_run(0, seen))
    # Pin argv: main() appends sys.argv[1:], which under pytest is pytest's own
    # flags. The script path is a fixed position, not the last element.
    monkeypatch.setattr(cli.sys, "argv", ["takeout-scout"])

    with pytest.raises(SystemExit):
        cli.main()

    target = Path(seen["cmd"][SCRIPT_ARG_INDEX])
    assert target.name == "app.py"
    assert target.parent.name == "takeout_scout"
    assert target.is_absolute(), f"target must be absolute, got {target}"
    assert target.exists(), f"launcher points at a file that does not exist: {target}"


def test_main_invokes_streamlit_run(monkeypatch: pytest.MonkeyPatch) -> None:
    """It launches via `python -m streamlit run`, not by importing the app."""
    seen: dict = {}
    monkeypatch.setattr(cli.subprocess, "run", _fake_run(0, seen))

    with pytest.raises(SystemExit):
        cli.main()

    assert seen["cmd"][1:4] == ["-m", "streamlit", "run"]


def test_main_forwards_extra_arguments(monkeypatch: pytest.MonkeyPatch) -> None:
    """Arguments after the command reach Streamlit, e.g. --server.port."""
    seen: dict = {}
    monkeypatch.setattr(cli.subprocess, "run", _fake_run(0, seen))
    monkeypatch.setattr(cli.sys, "argv", ["takeout-scout", "--server.port=8899"])

    with pytest.raises(SystemExit):
        cli.main()

    assert seen["cmd"][-1] == "--server.port=8899"


@pytest.mark.parametrize("code", [0, 1, 2, 7, 130])
def test_main_propagates_the_child_exit_code(
    code: int, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The shell must see Streamlit's own code, not a flattened 0 or 1.

    A supervisor or CI step needs to tell "port already in use" from "killed"
    from "bad argument". An earlier version used check=True and exited 1 for
    every failure, throwing that distinction away.
    """
    monkeypatch.setattr(cli.subprocess, "run", _fake_run(code, {}))

    with pytest.raises(SystemExit) as exc:
        cli.main()

    assert exc.value.code == code


def test_main_exits_zero_on_keyboard_interrupt(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Ctrl-C is how a user stops the server; that is not a failure."""

    def interrupt(cmd, **kwargs):
        raise KeyboardInterrupt

    monkeypatch.setattr(cli.subprocess, "run", interrupt)

    with pytest.raises(SystemExit) as exc:
        cli.main()

    assert exc.value.code == 0
