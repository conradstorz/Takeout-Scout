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
    def open(cls, path: Path) -> TakeoutIndex:
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
        """Per-asset sidecar paths - the only ones that could be orphans.

        The sidecar table holds every .json member Inventory found, including
        album metadata and account-level lists. Those are not per-photo
        sidecars and were never candidates for pairing, so counting them as
        orphans would invent defects. Inventory sets `role` precisely so a
        consumer can tell them apart.
        """
        return {r["path"] for r in self._con.execute(
            "SELECT path FROM sidecar WHERE role = 'sidecar'")}
