# Merge Report: backup-local-main → merge/ui-onto-package

Merge commit: `20e749e`

## Baseline / result

- Tests before merge (HEAD, origin/main): **170 passed**
- Tests after merge: **170 passed**, unchanged, no assertions weakened
- `uv run python -c "import app"` — OK
- `uv run python -c "import ts"` — OK

## Conflict resolution, one line each

### app.py (6 hunks)

1. Inline loguru-with-fallback shim + `LOG_DIR`/`STATE_DIR`/`INDEX_PATH` index helpers + `MEDIA_PHOTO_EXT`/`SERVICE_HINTS`/`PARTS_PAT` + `ArchiveSummary` dataclass + `guess_service_from_members`/`iter_zip_members`/`iter_tar_members`/`tally_exts`/`derive_parts_group` — all dropped; every one already exists in `takeout_scout` (logging.py, constants.py, models.py, scanner.py) and HEAD already imports them. Kept local's genuinely new `RECENT_FOLDERS_PATH`/`load_recent_folders`/`save_recent_folders`/`add_recent_folder` (no package equivalent).
2. Second copy of the same duplication (`validate_zip`/`validate_tar`/`scan_archive`/`scan_directory`/`find_archives_and_dirs` reimplemented with a `progress_callback` parameter) — dropped entirely; package versions (imported at top of file) don't support `progress_callback`, so callers were rewritten to use `st.spinner`/`st.progress` around the plain calls instead. Local's `find_archives_and_dirs` also silently added a "must contain 'takeout' in the filename" filter not present in `takeout_scout.scanner.find_archives_and_dirs` — not backported, since `tests/test_scanner.py::TestFindArchivesAndDirs::test_find_zip_files` scans for files named `archive1.zip`/`archive2.zip` with no "takeout" in the name and expects them to be found.
3. Session-state init: merged both sides — HEAD's `compute_hashes`/`hash_index`/`parse_sidecars` plus local's `recent_folders`/`current_browse_path`, plus a new `deep_scan_results` dict needed by the ported deep-scan feature.
4. Sidebar "Folder input" text box (HEAD) vs. Browse/Paste/Upload tabs with recent-folder history (local) — kept local's tabs (genuinely new UI capability), since HEAD's plain single-box input has no feature the tabs don't already cover.
5. The big one: HEAD's `scan_single_file(index, file_info)` + `_update_hash_index` vs. local's welcome-screen text, `get_file_icon`/`get_status_color`/`get_status_text`, `clean_file_path`, `load_folder`/`load_files`, and a second `scan_single_file(index)`. Resolved by keeping local's UI helper functions (rewritten to call the package `find_archives_and_dirs`/`scan_archive`/`scan_directory` without `progress_callback`), and rewriting `scan_single_file` to combine HEAD's `compute_hashes`/`parse_sidecars` kwargs and hash-index update with the "pop from pending list on success" behavior added later on `backup-local-main`'s `ts.py` (the "file cards clear after scanning" feature).
6. `scan_all_pending`'s per-file scan call: `compute_hashes`/`parse_sidecars` kwargs (HEAD) vs. `progress_callback` (local) — kept the kwargs (package doesn't support callbacks) and ported the "remove from pending list on success, keep on error" behavior from `backup-local-main`'s `ts.py`.

### ts.py (4 hunks)

All four hunks pit HEAD's Tkinter GUI (already using `takeout_scout` throughout) against a full rewrite of `ts.py` into a second, largely redundant Streamlit app (`backup-local-main` turned `ts.py` into essentially a fork of `app.py`, later extended with deep-scan/ignore/clear-card/folder-album-display features — see commits `06461f6`, `218df2d`, `81f671d`, `1c5f9cd`, `b8f795a`). Since `app.py` already serves the Streamlit role on HEAD, all four hunks resolve to the HEAD side (`git checkout --ours -- ts.py`); `ts.py` is byte-identical to `origin/main`'s version. The genuinely new capabilities that lived only in that Streamlit `ts.py` (deep scan, ignore button, card-clearing, self-launch) were ported into `app.py` instead — see below.

## UI features carried forward, and how they reach package code

- **Browse/Paste/Upload tabs + recent folders** (`app.py`) — pure UI, no package dependency; `load_folder`/`load_files` call `takeout_scout.find_archives_and_dirs` and `validate_and_get_info` (unchanged from HEAD).
- **Ignore button for pending files** (`app.py::ignore_file`) — new function, ported from `backup-local-main`'s `ts.py`.
- **File cards that clear after scanning** (`app.py::display_file_cards`, `scan_single_file`, `scan_all_pending`) — scanned/ignored entries are popped from `st.session_state.pending_files` instead of being left in the list with a `SCANNED` status.
- **Deep scan / folder & album display** (`app.py::deep_scan_archive`, `deep_scan_directory`, `analyze_file_structure`, `detect_organization_type`, `perform_deep_scan`, `build_folder_tree`, `display_folder_tree`, `display_deep_scan_card`) — ported from `backup-local-main`'s `ts.py`, rewritten to import `iter_zip_members`/`iter_tar_members` from `takeout_scout.scanner` and `MEDIA_PHOTO_EXT` from `takeout_scout.constants` instead of the local duplicate definitions that were dropped in hunks 1–2.
- **Self-launching entry point** — `app.py`'s `if __name__ == '__main__':` now checks `streamlit.runtime.scriptrunner.get_script_run_ctx()` and re-execs itself via `streamlit run` if not already inside Streamlit, ported from `backup-local-main`'s `ts.py`.

`process_folder`, `add_files_to_pending`, `show_pending_files`, and `show_results` in `app.py` became unreachable once the tabbed UI, file cards, and `display_results_table` replaced their call sites in `main()`; removed as dead code rather than left in place, since they duplicated functionality the surviving functions already provide.

## Licence finding

`pyproject.toml` declares `license = { text = "GPL-3.0-or-later" }`, the `License :: OSI Approved :: GNU General Public License v3 (GPLv3)` classifier, and `README.md`'s own License section says "GNU General Public License v3 (GPLv3) — See LICENSE file for details" — all three agree. No `LICENSE` file existed in the tree despite commit `038ee30 "changed license to gnu3"`. Added the full, unmodified GPL-3.0 text (674 lines, fetched verbatim from `https://www.gnu.org/licenses/gpl-3.0.txt`) as `LICENSE`.

## README

Removed the "Superseded by Takeout_Inventory" banner (commit `efb7e08` on `backup-local-main`) per instructions — it was written about the pre-package version of this project and doesn't reflect the current package's active sidecar parsing/pairing work. Every other README change from both branches (auto-merged cleanly) is kept.

## Bug fixed in passing

`_update_hash_index()` (called from `scan_single_file`/`scan_all_pending`, both of which I was rewiring in hunks 5–6) called `load_takeout_discovery()` with **no arguments**, but `takeout_scout.discovery.load_takeout_discovery(path)` requires a `path`. It also iterated `discovery.archives`/`discovery.directories`, then `source.files` — none of which exist on `TakeoutDiscovery` (which has a flat `file_details: List[Dict]`, not an archives/directories split). This meant enabling "Compute file hashes" and scanning anything would have raised `TypeError` immediately. Fixed to call `load_takeout_discovery(path)` and iterate `discovery.file_details` (dicts with `file_hash`/`path`/`size` keys), and to pass `size` to `HashIndex.add()`, which is a required positional argument.

## Known pre-existing issues found but NOT fixed (out of scope — not part of any conflict hunk)

These all predate the merge (present on `origin/main` before `backup-local-main` was merged in) and are untouched, unconflicted HEAD-only code:

- `show_date_analysis`, `show_timeline_analysis`, `show_orphan_analysis`, `show_full_inventory` (`app.py`) all call `load_takeout_discovery(Path(result.path))` and then read `discovery.archives`/`discovery.directories`/`source.files` — the same shape mismatch fixed in `_update_hash_index` above, but in code paths I never touched. These will raise or silently produce empty results at runtime today.
- `show_duplicate_analysis`, `_export_duplicate_report`, `show_cross_archive_analysis` access `hash_index._index` and unpack duplicate-group entries as 2-tuples `(source, path)` — the real attribute is `HashIndex._by_hash`, and entries are 3-tuples `(source_path, file_path, size)`.
- `[project.scripts] takeout-scout = "takeout_scout:main"` in `pyproject.toml` (this line auto-merged cleanly, picking HEAD's form over `backup-local-main`'s `app:main`/`ts:main` pair) points at a `main` function that doesn't exist in `takeout_scout/__init__.py` — the installed console script is currently broken.
- A root-level `takeout_scout.py` launcher script was added by the merge (from `backup-local-main`, non-conflicting auto-add). It shells out to `streamlit run ts.py`, which is now wrong since `ts.py` resolved to the Tkinter GUI, not the Streamlit app. It does **not** collide with the `takeout_scout/` package at import time (verified: `import takeout_scout` resolves to the package, not this file), but as a launcher it is broken. Left untouched per "don't change files you weren't asked to change" — flagging here instead.

None of the above affect `pytest` (no test file exercises `app.py`) or the "both entry points import" check, since these are runtime/logic bugs inside functions that only execute when a user interacts with the Streamlit app, not at import time.
