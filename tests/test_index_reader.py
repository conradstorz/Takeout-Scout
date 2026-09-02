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
        # (id, archive, path, name, parse_error) with role defaulted, or a
        # 6-tuple ending in an explicit role.
        if len(row) == 6:
            ident, archive, path_, name, parse_error, role = row
        else:
            (ident, archive, path_, name, parse_error), role = row, "sidecar"
        con.execute(
            "INSERT INTO sidecar (id, archive, path, name, parse_error, role) "
            "VALUES (?,?,?,?,?,?)",
            (ident, archive, path_, name, parse_error, role))
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

    def test_album_metadata_is_not_a_sidecar_candidate(self, tmp_path):
        """Inventory's sidecar table holds every .json member.

        Album metadata and account-level lists are not per-photo sidecars and
        were never candidates for pairing. Counting them as orphans would
        invent defects in a list whose whole value is that its entries are
        real.
        """
        path = build_index(
            tmp_path / "i.sqlite",
            sidecars=[
                (1, "p1.zip", "Photos/a.jpg.json", "a.jpg.json", None, "sidecar"),
                (2, "p1.zip", "Album/metadata.json", "metadata.json", None,
                 "album-metadata"),
            ],
            media=[(1, "p1.zip", "Photos/a.jpg", "Photos", "Photos", "a.jpg",
                    1, "exact", "own")],
        )
        index = TakeoutIndex.open(path)

        assert index.all_sidecar_paths() == {"Photos/a.jpg.json"}
        assert index.all_sidecar_paths() - index.claimed_sidecar_paths() == set()
