# Retire the Tkinter UI — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Delete the redundant Tkinter interface and repair the documentation that the `merge/ui-onto-package` merge left describing a repository that no longer exists.

**Architecture:** Three tasks. The first adds a test that catches documentation naming files that do not exist — the exact class of defect the merge shipped. The second deletes `ts.py` and repairs `README.md`, with the new test going red in between as proof it works. The third repairs the two remaining documents, which the test cannot reach.

**Tech Stack:** Python 3.10+, pytest, `uv`. No new dependencies.

**Spec:** [`../specs/2026-09-01-retire-tkinter-ui-design.md`](../specs/2026-09-01-retire-tkinter-ui-design.md)

## Global Constraints

- Repository: `D:\Users\Conrad\Documents\programming\Takeout-Scout`. Branch: `merge/ui-onto-package`. Do not create a new branch.
- Run Python only through `uv`: `uv run pytest`, `uv run python`. Never `pip install`, never `python -m venv`, never activate a venv.
- Do not chain shell commands with `&&`. Issue them separately.
- Do not modify `app.py` or anything under `takeout_scout/`. This plan touches documentation, one deleted file, and one new test file.
- Do not tidy untracked files. `logs/`, `state/`, `takeouts_discovered/`, `discoveries_index.json`, `__pycache__/` and `.venv/` stay exactly as they are.
- The pre-existing suite is **170 passing tests**. That number must not change. A change in it means the deletion was not inert, and is a stop-and-report condition. Do not pin the *total* — the new guard adds one parametrized case per Markdown file found on disk, including untracked ones such as `MERGE_REPORT.md`, so the total is derived and may legitimately differ from any number written here.
- `MERGE_REPORT.md` is untracked and stays untracked. It contains no code fences, so the new guard collects it and finds nothing to check. Do not delete it, do not commit it.
- The README's positioning paragraph must not claim Scout uses `Takeout_Inventory`. It does not — `git grep -n "takeout_inventory"` returns nothing. Stating an intended architecture as a current one is the specific error this plan exists to avoid repeating.

---

## Amendment to the spec, decided during planning

The spec says the new test checks "every tracked Markdown file". That is wrong,
and would make this plan fail on its own documents.

This plan file and the spec both quote `streamlit run ts.py` — the broken
command — as the record of what was fixed. Under a repository-wide rule the
guard would go red the moment `ts.py` is deleted, and the only way back to
green would be deleting the historical record it exists to preserve.

**The guard therefore covers reader-facing documentation only, and skips
`docs/superpowers/`.** Specs and plans are process history: they deliberately
quote commands from before and after a change. Reader-facing documentation is
what has to be executable, and it is where both merge defects landed.

---

## File Structure

| File | Change | Responsibility |
| --- | --- | --- |
| `ts.py` | **delete** | 501-line Tkinter GUI. Every capability is a subset of `app.py`. Nothing imports it. |
| `tests/test_docs.py` | **create** | The guard: a file path named in a shell block in reader-facing docs must exist. |
| `README.md` | modify | Positioning paragraph, two broken usage commands, the Legacy section, the Project Structure block, one false sentence about a GUI button. |
| `DISCOVERY_TRACKING.md` | modify | Three references to the deleted file. |
| `METADATA_FEATURES.md` | modify | A `GUI (ts.py)` subsection that merges into the Web UI one. |

---

## Task 1: The documentation guard

**Files:**
- Create: `tests/test_docs.py`

**Interfaces:**
- Consumes: nothing from earlier tasks.
- Produces: `tests/test_docs.py`, which Task 2 relies on going red after `ts.py` is deleted and green again after `README.md` is repaired.

**Why this is first.** The merge shipped a README instructing readers to run
`streamlit run ts.py` — Streamlit pointed at a Tkinter script. It fails for
every reader who follows the README's own Usage section, and nothing in the
repository noticed, because nothing tests documentation. Writing the guard
before the deletion makes the deletion itself the proof the guard works.

- [ ] **Step 1: Write the test file**

Create `tests/test_docs.py` with exactly this content:

```python
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
```

- [ ] **Step 2: Run the new tests**

Run: `uv run pytest tests/test_docs.py -q`

Expected: PASS. `ts.py` still exists, so `streamlit run ts.py` resolves.

**A pass here proves nothing yet.** Step 3 is what proves the guard can fail.

- [ ] **Step 3: Prove the guard can fail (mutation)**

Break the README on purpose. Write this to a scratch file and run it, so no
shell quoting is involved:

```python
# scratch_mutate.py
import pathlib

p = pathlib.Path("README.md")
text = p.read_text(encoding="utf-8")
assert "streamlit run ts.py" in text
p.write_text(text.replace("streamlit run ts.py", "streamlit run nope.py", 1), encoding="utf-8")
```

Run: `uv run python scratch_mutate.py`

Run: `uv run pytest tests/test_docs.py -q`

Expected: FAIL, with a message naming `README.md:63 -> nope.py`.

If it passes, the guard is broken — stop and report. Do not proceed.

Revert and clean up:

Run: `git checkout -- README.md`

Run: `rm scratch_mutate.py`

- [ ] **Step 4: Run the full suite**

Run: `uv run pytest -q`

Expected: PASS.

**Do not pin the total.** The new file adds 3 fixed tests plus one
parametrized case per reader-facing Markdown file *present on disk*, tracked
or not. At the time of writing that is four — `README.md`,
`DISCOVERY_TRACKING.md`, `METADATA_FEATURES.md`, and the untracked
`MERGE_REPORT.md` — giving **177 passed**. If your working tree holds a
different set of Markdown files the total moves, and that is correct
behaviour, not a failure.

**The binding assertion is that all 170 pre-existing tests still pass**, and
that every new test passes. Record whatever total you observe in your report
so later tasks can compare against it.

- [ ] **Step 5: Commit**

Run: `git add tests/test_docs.py`

Run: `git commit -m "test: guard against documented commands naming missing files"`

---

## Task 2: Delete `ts.py` and repair the README

**Files:**
- Delete: `ts.py`
- Modify: `README.md` (5 regions, all specified below)

**Interfaces:**
- Consumes: `tests/test_docs.py` from Task 1.
- Produces: a repository with one user interface. Task 3 relies on `ts.py` being gone so its `git grep` verification is meaningful.

**Context.** `ts.py` is a 501-line Tkinter GUI. `app.py` is a 1,786-line
Streamlit app with every one of its capabilities plus date, duplicate,
timeline, orphan, cross-archive and deep-scan analysis. Nothing imports
`ts.py`; no test covers it; `pyproject.toml` never mentions it and points its
console script at `run_app.py`, which launches `app.py`.

- [ ] **Step 1: Delete the file**

Run: `git rm ts.py`

- [ ] **Step 2: Run the suite and watch the guard go red**

Run: `uv run pytest -q`

Expected: FAIL. `test_shell_blocks_name_real_files[README.md]` reports four
missing paths:

```
README.md:63 -> ts.py
README.md:68 -> ts.py
README.md:103 -> ts.py
README.md:105 -> ts.py
```

**Also expected: the 170 pre-existing tests still pass.** That is the evidence
the deletion was inert. If any of them fail, something did import `ts.py` —
stop and report rather than working around it.

**Edit order matters.** Steps 3–6 below cite line numbers, and none of them
changes the line count above the region it edits. The positioning paragraph
adds lines at the very top of the file, so it goes **last**, in Step 7. Doing
it earlier would shift every line number in this task by seven.

- [ ] **Step 3: Fix the two broken usage commands**

Lines 62–69 currently read (fences shown as `~~~` here only so this plan can
quote them; use real backtick fences in the file):

```
Run the web application:
~~~bash
streamlit run ts.py
~~~

Or use uv:
~~~bash
uv run streamlit run ts.py
~~~
```

Change both occurrences of `ts.py` to `app.py`. Nothing else on those lines
changes.

- [ ] **Step 4: Delete the Legacy Tkinter Version section**

Remove this entire section (currently lines 99–106), heading included, leaving
one blank line between the section before it and `## Project Structure`:

```
## Legacy Tkinter Version

The original tkinter desktop version is still available as `ts.py`:
~~~bash
python ts.py
# or
uv run python ts.py
~~~
```

- [ ] **Step 5: Rewrite the Project Structure block**

The current block predates the package split: it lists `app.py` and `ts.py` at
top level and omits `takeout_scout/`, `tests/` and `run_app.py`. Replace the
contents of the fenced block under `## Project Structure` — keeping it a bare
fence, not a `bash` one — with:

```
Takeout-Scout/
├── app.py                     # Streamlit web application
├── run_app.py                 # Launcher: starts Streamlit on app.py
├── takeout_scout/             # Scanning engine (importable package)
│   ├── scanner.py             # Archive and directory scanning
│   ├── sidecar.py             # Google Takeout JSON sidecar parsing
│   ├── hashing.py             # File hashing utilities
│   ├── metadata.py            # EXIF metadata extraction
│   ├── discovery.py           # Discovery tracking system
│   ├── models.py              # Data models
│   ├── constants.py           # Constants and configuration
│   ├── logging.py             # Logging configuration
│   └── utils.py               # Utility functions
├── tests/                     # pytest suite for takeout_scout/
├── docs/superpowers/          # Design specs and implementation plans
├── logs/                      # Log files (auto-created)
│   └── takeout_scout.log
├── state/                     # Persistent state (auto-created)
│   └── takeout_index.json
├── takeouts_discovered/       # Per-source discovery records (auto-created)
├── README.md                  # This file
├── DISCOVERY_TRACKING.md      # How discovery records work
├── METADATA_FEATURES.md       # EXIF extraction details
├── LICENSE                    # GNU GPL v3 License
├── pyproject.toml             # Project configuration
├── requirements.txt           # Dependency list for pip users
└── .gitignore                 # Git ignore rules
```

- [ ] **Step 6: Fix the Logging section**

It currently reads:

```
All operations are logged to `logs/takeout_scout.log` with automatic rotation at 5MB. Access logs via the "Open Logs..." button in the GUI.
```

That button was `ts.py`'s. `app.py` has no equivalent — `grep -n "Open Logs" app.py` returns nothing. Replace the whole line with:

```
All operations are logged to `logs/takeout_scout.log` with automatic rotation at 5MB.
```

**Leave the Design Philosophy bullet "User-friendly - Clear GUI with progress
indicators and helpful messages" untouched.** "GUI" describes the Streamlit
interface just as well, so this change does not make it false, and rewriting it
is scope this task does not have.

- [ ] **Step 7: Add the positioning paragraph (last, because it shifts line numbers)**

`README.md` currently opens:

```
# Takeout Scout

A modern, web-based tool for scanning and analyzing Google Takeout archives without extraction.
```

Insert a paragraph so it reads:

```
# Takeout Scout

A modern, web-based tool for scanning and analyzing Google Takeout archives without extraction.

> **Where this fits.** [`Google_Takeout_Downloader`](https://github.com/conradstorz/Google_Takeout_Downloader)
> fetches the archives. **Takeout Scout** explores them interactively in a
> browser. [`Takeout_Inventory`](https://github.com/conradstorz/Takeout_Inventory)
> produces a machine-readable pairing index and a static HTML report. Scout and
> Inventory scan independently today — they share no code.
```

The last sentence is load-bearing and must not be softened into a claim that
Scout uses Inventory. It does not. It stops being true on the day the two are
wired together, and on that day it is one sentence to edit.

- [ ] **Step 8: Run the suite**

Run: `uv run pytest -q`

Expected: all pass, at **the same total Task 1's report recorded** — no
Markdown file was added or removed, so the parametrized case count is
unchanged. The 170 pre-existing tests must still be among the passes.

- [ ] **Step 9: Verify the app is still intact and the launcher resolves**

Run: `uv run python -c "import ast,pathlib; ast.parse(pathlib.Path('app.py').read_text(encoding='utf-8')); print('app.py parses')"`

Expected: `app.py parses`

Run: `uv run python -c "import run_app; print(run_app.main)"`

Expected: a line like `<function main at 0x...>`

- [ ] **Step 10: Commit**

Run: `git add README.md`

Step 1's `git rm` already staged the deletion. Naming `ts.py` here would fail
with `fatal: pathspec 'ts.py' did not match any files`, because the file no
longer exists on disk to be matched.

Then commit. Use repeated `-m` flags for the paragraphs — **do not use a
heredoc.** Heredocs fail to parse in this project's shell when the body
contains backticks or nested quotes, which cost one attempt during planning:

```
git commit -m "refactor: remove the redundant Tkinter UI and repair the README" -m "Every capability of ts.py was a subset of app.py's; nothing imported it and no test covered it. Removing it also removes the reason the README documented a Streamlit command pointed at a Tkinter script, which could never work." -m "The opening paragraph now says how Scout relates to the Downloader and to Takeout_Inventory, and says plainly that Scout and Inventory share no code - which is true today, and is what both the old superseded banner and the tempting 'Inventory is the engine' framing got wrong."
```

---

## Task 3: Repair the two remaining documents

**Files:**
- Modify: `DISCOVERY_TRACKING.md` (3 regions)
- Modify: `METADATA_FEATURES.md` (1 region)

**Interfaces:**
- Consumes: `ts.py` being deleted, from Task 2.
- Produces: nothing later tasks depend on. This is the last task.

**Why this is separate.** Neither reference sits inside a shell block, so the
Task 1 guard cannot see them. They are found by grep, and verified by grep.

- [ ] **Step 1: Remove `ts.py` from the DISCOVERY_TRACKING directory tree**

The block at lines 8–21 currently begins:

```
takeout_scout/
├── app.py
├── ts.py
├── discoveries_index.json          # Main index of all discoveries
```

The root name is also wrong — `takeout_scout/` is the Python package, not the
repository. Change those four lines to:

```
Takeout-Scout/
├── app.py
├── discoveries_index.json          # Main index of all discoveries
```

Leave the rest of the block unchanged.

- [ ] **Step 2: Fix the "Files Created" sentence**

Line 26 currently reads:

```
Located in the same directory where app.py/ts.py is run from.
```

Replace with:

```
Located in the same directory the app is run from.
```

- [ ] **Step 3: Remove the cross-interface compatibility bullet**

The Compatibility section at lines 259–262 currently reads:

```
### Compatibility
- Works with both GUI (ts.py) and Web (app.py) versions
- All discovery files are portable (can be copied to other systems)
- JSON format ensures long-term readability
```

Delete the first bullet only, leaving:

```
### Compatibility
- All discovery files are portable (can be copied to other systems)
- JSON format ensures long-term readability
```

With one interface there is no cross-interface claim left to make, and
inventing a replacement bullet would be filler.

- [ ] **Step 4: Merge the two Display Columns subsections**

`METADATA_FEATURES.md` lines 48–57 currently read:

```
#### GUI (ts.py)
New columns in the table view:
- **w/EXIF**: Count of photos with EXIF data
- **w/GPS**: Count of photos with GPS coordinates
- **w/Date**: Count of photos with original date/time
- **Checked**: Total photos analyzed for metadata

#### Web UI (app.py)
Same columns added to the Streamlit interface
```

Replace both subsections with one:

```
#### Web UI (app.py)
New columns in the table view:
- **w/EXIF**: Count of photos with EXIF data
- **w/GPS**: Count of photos with GPS coordinates
- **w/Date**: Count of photos with original date/time
- **Checked**: Total photos analyzed for metadata
```

- [ ] **Step 5: Verify no reference survives**

Run: `git grep -n "\bts\.py" -- ":!docs/"`

Expected: no output. `git grep` exits 1 when it finds nothing — that exit code
is the pass condition here, not a failure.

**The `\b` is required.** Without it the pattern matches inside
`constants.py` — `consta` + `nts.py` — and the README's own Project Structure
block lists that module, so an unanchored search reports a hit that has
nothing to do with the deleted file.

Run: `git grep -n -i "tkinter" -- ":!docs/" ":!tests/test_docs.py"`

Expected: no output. `tests/test_docs.py` is excluded because the guard's
docstring names Tkinter to explain what the guard exists to catch — describing
the removed file is the opposite of depending on it.

`docs/` is excluded because the spec and this plan quote the removed file on
purpose, as the record of what was changed.

- [ ] **Step 6: Run the full suite**

Run: `uv run pytest -q`

Expected: all pass, at the same total the earlier tasks recorded. The 170
pre-existing tests must still be among them.

- [ ] **Step 7: Commit**

Run: `git add DISCOVERY_TRACKING.md METADATA_FEATURES.md`

Run: `git commit -m "docs: drop the last references to the removed Tkinter UI"`

---

## Definition of done

1. `uv run pytest -q` passes, with the original **170 unchanged** plus the new guard's tests (3 fixed, plus one parametrized case per reader-facing Markdown file on disk — 4 at time of writing, so 177 total). The 170 is the binding number; the total moves with the Markdown file list.
2. `git grep -n "\bts\.py" -- ":!docs/"` returns nothing. The word boundary matters: without it the pattern matches inside `constants.py`.
3. `ts.py` does not exist.
4. `README.md` names `app.py` in both Usage commands, carries the "Where this fits" paragraph, and its Project Structure block lists `takeout_scout/`, `tests/` and `run_app.py`.

## Out of scope — found while planning, deliberately not fixed

These are real, they are pre-existing, and none is caused by removing the
Tkinter UI. Fixing them here would smuggle unrelated changes into a deletion.

- **`README.md` "Future Enhancements" is stale.** It lists "Duplicate detection and reporting" as planned; `app.py` already ships `show_duplicate_analysis()`.
- **`takeouts_discovered/photos_cStorz3_takeout-20190715T134005Z-002_a2ac6b7b7c32.takeout_scout` is tracked** in a public repository. It is a scan artifact whose filename carries an account name. Worth a separate decision about whether it belongs in git at all.
- **`uv.lock` is untracked** while `requirements.txt` is tracked. Not wrong, but the two will drift.
