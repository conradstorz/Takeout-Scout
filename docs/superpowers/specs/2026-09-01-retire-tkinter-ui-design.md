# Retire the Tkinter UI — design

**Date:** 2026-09-01
**Status:** approved; not yet implemented.
**Branch:** `merge/ui-onto-package` (PR #1), because two of the four defects
below were introduced by that merge and should not land in `main`.

## The problem

`merge/ui-onto-package` reconciled two divergent histories of this repository.
The published `origin/main` carried the `takeout_scout/` package and the
Streamlit app; the local branch carried a second, competing UI. The merge
resolved the code conflicts and left the documentation describing a repository
that no longer exists.

The visible symptom is two user interfaces:

| | `ts.py` (Tkinter) | `app.py` (Streamlit) |
| --- | --- | --- |
| lines | 501 | 1,786 |
| scan, results table, CSV export | yes | yes |
| date / duplicate / timeline / orphan / cross-archive analysis | no | yes |
| deep scan, folder tree, full inventory | no | yes |
| tests | none | none |
| named in `pyproject.toml` | no | yes — dependencies and console script |

Every capability of `ts.py` is a strict subset of `app.py`. Nothing imports it.
`pyproject.toml` declares `streamlit` and `pandas` as unconditional
dependencies and points its console script at `app.py`, so the packaging
already behaves as though `ts.py` were absent.

Its one genuine distinction is that it needs no browser and no localhost
server. That is a real gap, and a desktop GUI is the wrong way to fill it —
see *Deferred*.

## Decision

Delete `ts.py`, and repair the documentation that referenced it.

## Scope

**In.** Removing `ts.py`; the four documentation defects listed below; a test
that prevents the third one from recurring.

**Out.** Any change to `app.py` or to `takeout_scout/`. Making Scout consume
`Takeout_Inventory`'s index. A command-line interface.

## The four defects

### 1. `ts.py` itself

Deleted. Recovery is `git show 44becf8:ts.py` — this repository is public and
its history is the archive. Keeping the file in a `legacy/` directory was
considered and rejected: it imports from `takeout_scout/`, so every future
change to the package would carry an obligation to not break it, and no test
would report a break. That is a maintenance cost with no maintenance attached.

### 2. `README.md` names a command that cannot work

Lines 63 and 68 instruct the reader to run:

```
streamlit run ts.py
```

Streamlit pointed at the Tkinter file. It fails for every reader who follows
the README's own Usage section. Both become `streamlit run app.py`.

The *Legacy Tkinter Version* section (lines 99–106) goes with the file.

### 3. `README.md` describes a structure that predates the package split

The *Project Structure* block lists `app.py` and `ts.py` at top level and omits
`takeout_scout/`, `tests/`, `run_app.py` and `requirements.txt`. It is rewritten
against the actual tree. (`uv.lock` is untracked and stays out of the diagram.)

### 4. `README.md` describes a button that will not exist

The Logging section ends:

> Access logs via the "Open Logs..." button in the GUI.

That button is `ts.py`'s `on_open_logs`. `app.py` has no equivalent —
`grep -n "Open Logs" app.py` returns nothing — so the sentence is arguably
already false for the web interface, and is unambiguously false once `ts.py`
is gone. It is deleted; the rotation sentence before it stays.

**Found after this spec was first written**, by reading the README end to end
instead of grepping for `ts.py`. It is the one reference that never names the
file, which is why the grep missed it and why the new test cannot see it
either — it is prose, not a command.

### 5. Two other documents reference the deleted file

- `DISCOVERY_TRACKING.md` lines 11, 26 and 260 — a directory diagram, a
  sentence about where files are written, and a compatibility note claiming
  the format "works with both GUI (ts.py) and Web (app.py) versions".
- `METADATA_FEATURES.md` line 50 — a `#### GUI (ts.py)` subsection listing
  table columns, immediately followed by a `#### Web UI (app.py)` subsection
  that says only "Same columns added". The two collapse into one.

## The positioning paragraph

Commit `efb7e08` added a banner to the README reading *"Superseded by
Takeout_Inventory … This repository is no longer developed."* The merge dropped
it. It is not restored, because it is no longer true: Scout is developed, as
the interactive interface to this problem.

The tempting replacement — *"Scout is the UI, Inventory is the engine"* — is
also not written, because it is not true either. **Scout imports nothing from
`Takeout_Inventory`.** It has its own scanning engine in `takeout_scout/`:
`scanner.py`, `sidecar.py`, `hashing.py`, `metadata.py`. Describing an intended
architecture as though it were the current one is how a README starts lying.

What is written is what is true today:

> **Where this fits.** `Google_Takeout_Downloader` fetches the archives.
> **Takeout Scout** explores them interactively in a browser.
> `Takeout_Inventory` produces a machine-readable pairing index and a static
> HTML report. Scout and Inventory scan independently today — they share no
> code.

The final sentence is the load-bearing one. It stops being true on the day the
two are wired together, and on that day it is one sentence to edit.

## Testing

The deletion needs no regression test — it removes unreachable code, and the
existing suite passing unchanged is the proof that it was unreachable.

Defect 2 does need one. A merge shipped a documented command that could never
execute, and nothing in the repository noticed. Add `tests/test_docs.py`:

> Every path that looks like a file in this repository, appearing inside a
> fenced shell block in reader-facing documentation, must exist on disk.

Scope it deliberately. "Fenced shell block" means a fence opened as ```bash
or ```sh only — a bare ``` fence is excluded, which is what keeps the
ASCII-art *Project Structure* tree out of the test's reach. Within those
blocks it matches tokens ending in `.py`, `.toml`, `.md` or `.json`, and skips
anything containing a URL scheme, a shell variable, or a wildcard. It is a
spell-checker for commands, not a shell parser, and it must not become one.

**Amended during planning: "reader-facing", not "tracked".** `docs/superpowers/`
is excluded. This document and its plan both quote `streamlit run ts.py` as the
record of what was fixed; under a repository-wide rule the guard would go red
the moment `ts.py` is deleted, and the only route back to green would be
deleting the history the test exists to protect. Reader-facing documentation is
what has to be executable, and it is where every defect above landed.

This is the repository's only test that covers documentation.

## Verification

1. The full test suite passes, with the same count as before the change plus
   the new documentation test. An unchanged count for the pre-existing tests is
   the evidence that deleting `ts.py` was inert.
2. `app.py` parses and `run_app.main` imports, so the console script resolves.
3. `git grep -n "ts\.py" -- ":!docs/"` returns nothing. `docs/` is excluded
   because this spec and its plan quote the removed file deliberately.

## Deferred

**A command-line interface.** The capability `ts.py` uniquely provided was not
"a desktop window" but "runs without a browser and a server". A
`takeout-scout scan <path>` command would serve that better, would suit a
headless machine, and — unlike either graphical interface — could be tested.
It is a new feature, it needs its own design, and bundling it here would block
a deletion behind a design.

**Consuming the `Takeout_Inventory` index.** Replacing Scout's scanning engine
with the published pairing index is the change that would make "Scout is the
interface, Inventory is the engine" a true statement. It is a substantial
piece of work and gets its own spec.
