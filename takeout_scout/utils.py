"""
Utility functions for Takeout Scout.

Common helper functions used across the application.
"""
from __future__ import annotations

import shutil
from pathlib import Path
from typing import Iterable


def human_size(n: int) -> str:
    """Convert bytes to human-readable size string.
    
    Args:
        n: Size in bytes
        
    Returns:
        Human-readable string (e.g., "1.23 GB")
        
    Examples:
        >>> human_size(1024)
        '1.00 KB'
        >>> human_size(1536)
        '1.50 KB'
        >>> human_size(1073741824)
        '1.00 GB'
    """
    if n < 0:
        return f"-{human_size(-n)}"
    
    units = ['B', 'KB', 'MB', 'GB', 'TB']
    size = float(n)
    
    for unit in units:
        if size < 1024 or unit == 'TB':
            return f"{size:.2f} {unit}"
        size /= 1024

    return f"{size:.2f} TB"


def partition_known_paths(
    candidates: Iterable[Path],
    known: set[str],
) -> tuple[list[Path], list[Path]]:
    """Split candidates into (new, already_known), preserving order.

    A path is already known if `str(path)` is in `known`. Callers build
    `known` from the paths they have already queued or already scanned, so
    loading the same folder twice does not queue everything a second time.

    Duplicates *within* `candidates` also collapse: the first occurrence is
    new, later ones are already known.
    """
    new: list[Path] = []
    already: list[Path] = []
    seen: set[str] = set(known)

    for candidate in candidates:
        key = str(candidate)
        if key in seen:
            already.append(candidate)
        else:
            new.append(candidate)
            seen.add(key)

    return new, already


def unreferenced_dirs(
    dirs: Iterable[Path],
    in_use: Iterable[str],
) -> list[Path]:
    """Directories from `dirs` that no path in `in_use` lives under.

    Used to clean up temp directories from earlier uploads without deleting
    one whose files are still queued for scanning. A directory is referenced
    if any in-use path is the directory itself or sits beneath it.

    Order is preserved, and a directory appearing twice is returned once.
    """
    in_use_paths = [Path(p).resolve() for p in in_use]

    result: list[Path] = []
    seen: set[Path] = set()

    for d in dirs:
        resolved = d.resolve()
        if resolved in seen:
            continue

        # `Path.is_relative_to` performs a segment-wise comparison, not a
        # string prefix match, so "/tmp/scout_a" does not falsely count as
        # containing "/tmp/scout_ab/x".
        referenced = any(
            used == resolved or used.is_relative_to(resolved)
            for used in in_use_paths
        )
        if not referenced:
            result.append(d)
            seen.add(resolved)

    return result


def remove_dirs(paths: Iterable[Path]) -> tuple[list[Path], list[tuple[Path, str]]]:
    """Delete directories, returning (removed, failures). Never raises.

    Each failure is (path, message). A path that does not exist counts as
    removed - the caller wanted it gone and it is gone.
    """
    removed: list[Path] = []
    failures: list[tuple[Path, str]] = []

    for path in paths:
        if not path.exists():
            removed.append(path)
            continue
        try:
            shutil.rmtree(path)
            removed.append(path)
        except OSError as e:
            failures.append((path, str(e)))

    return removed, failures
