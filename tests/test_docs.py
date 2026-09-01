"""Guard: documented commands must name files that exist.

The merge that reconciled this repository's two histories shipped a README
telling readers to run Streamlit against a Tkinter script. It could never
work, and nothing here noticed, because nothing tested documentation.

This checks one narrow thing: a token that looks like a file in this
repository, appearing inside a shell block in reader-facing documentation,
must exist on disk. It is a spell-checker for commands, not a shell parser,
and it must not become one.
"""
from __future__ import annotations

import re
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parent.parent

# Only fences opened as bash or sh. A bare fence is excluded, which keeps
# ASCII-art directory trees out of reach - they name files created at
# runtime, and are not commands anyone runs.
SHELL_FENCE = re.compile(r"^```(?:bash|sh)\s*$")
FENCE_CLOSE = re.compile(r"^```\s*$")

CHECKED_SUFFIXES = (".py", ".toml", ".md", ".json")

# A token carrying any of these is not a plain path, and resolving it would
# mean interpreting the shell. Skip rather than guess.
SKIP_MARKERS = ("://", "$", "*", "?", '"', "'", "<", ">", "=")

# A token after a redirect is an output the command creates, not an input
# that must already exist. Checking it would fail on correct commands.
REDIRECT_OPERATORS = {">", ">>", "1>", "2>", "&>", "2>>"}

# Directories holding no reader-facing documentation. Kept alongside the
# dot-component rule below because neither of these begins with a dot.
EXCLUDED_DIRS = {"node_modules", "site-packages"}

# docs/superpowers/ is process history - specs and plans quote commands from
# before and after a change on purpose. Holding them to this rule would mean
# deleting the record of what was fixed in order to keep the suite green.
EXCLUDED_PREFIX = ("docs", "superpowers")


def _markdown_files() -> list[Path]:
    """Reader-facing Markdown, excluding process history and vendored trees.

    Anything under a dot-directory (.venv, .git, .pytest_cache,
    .superpowers, ...) is process or tool scratch, never reader-facing
    documentation, so a single rule - skip any path with a dot-prefixed
    component - replaces what used to be a growing denylist.
    """
    found = []
    for path in REPO_ROOT.rglob("*.md"):
        parts = path.relative_to(REPO_ROOT).parts
        if any(part.startswith(".") for part in parts):
            continue
        if EXCLUDED_DIRS & set(parts):
            continue
        if parts[: len(EXCLUDED_PREFIX)] == EXCLUDED_PREFIX:
            continue
        found.append(path)
    return sorted(found)


def _shell_block_lines(text: str):
    """Yield (line_number, line) for lines inside bash/sh fences."""
    inside = False
    for lineno, line in enumerate(text.splitlines(), start=1):
        if not inside:
            if SHELL_FENCE.match(line):
                inside = True
            continue
        if FENCE_CLOSE.match(line):
            inside = False
            continue
        yield lineno, line


def _candidate_paths(line: str):
    """Yield tokens from one shell line that look like repository files."""
    tokens = line.split()
    for index, raw in enumerate(tokens):
        if index > 0 and tokens[index - 1] in REDIRECT_OPERATORS:
            continue
        token = raw.strip("`,;:()[]")
        if any(marker in token for marker in SKIP_MARKERS):
            continue
        if not token.endswith(CHECKED_SUFFIXES):
            continue
        yield token


def test_markdown_files_are_found() -> None:
    """Without this, an empty file list would make the guard pass vacuously."""
    names = {path.name for path in _markdown_files()}
    assert "README.md" in names, f"README.md not collected; found {sorted(names)}"


def test_readme_shell_blocks_yield_paths() -> None:
    """Without this, an extractor that matched nothing would pass vacuously."""
    text = (REPO_ROOT / "README.md").read_text(encoding="utf-8")
    found = {
        token
        for _, line in _shell_block_lines(text)
        for token in _candidate_paths(line)
    }
    assert any(token.endswith(".py") for token in found), (
        f"extractor found no Python paths in README shell blocks: {sorted(found)}"
    )


def test_redirect_targets_are_not_checked() -> None:
    """An output a command creates must not be treated as a required input.

    Whitespace-splitting happens before the SKIP_MARKERS check, so a spaced
    redirect (`> report.json`) puts the `>` on its own token and leaves the
    target token with no marker to skip on. The no-space form (`2>report.json`)
    already contains a marker character and was never broken.
    """
    found = set(_candidate_paths("uv run python scan.py > report.json"))
    assert "scan.py" in found
    assert "report.json" not in found

    found_no_space = set(_candidate_paths("uv run python scan.py 2>report.json"))
    assert "scan.py" in found_no_space
    assert "report.json" not in found_no_space


def test_superpowers_docs_and_dot_dirs_are_excluded() -> None:
    """The exclusion is deliberate, not an accident of the glob."""
    collected = {path.relative_to(REPO_ROOT).as_posix() for path in _markdown_files()}
    assert not any(name.startswith("docs/superpowers/") for name in collected)
    assert not any(
        part.startswith(".")
        for name in collected
        for part in name.split("/")
    )


@pytest.mark.parametrize(
    "md_path",
    _markdown_files(),
    ids=lambda p: p.relative_to(REPO_ROOT).as_posix(),
)
def test_shell_blocks_name_real_files(md_path: Path) -> None:
    missing = []
    for lineno, line in _shell_block_lines(md_path.read_text(encoding="utf-8")):
        for token in _candidate_paths(line):
            if not (REPO_ROOT / token).exists():
                missing.append(
                    f"{md_path.relative_to(REPO_ROOT).as_posix()}:{lineno} -> {token}"
                )
    assert not missing, (
        "Documented commands name files that do not exist:\n  " + "\n  ".join(missing)
    )
