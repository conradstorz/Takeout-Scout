"""Tests for takeout_scout.worklist.

Pure functions over index rows. The point of the work list is that it says
what is *wrong*, so every test here is about a defect being surfaced, not
about an inventory being complete.
"""
import pytest

from takeout_scout.index_reader import IndexedPairing
from takeout_scout.worklist import (
    Finding,
    build_worklist,
    compare_with_scout,
    is_comparable,
)


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
            index, {("p.zip", "a.jpg"): "a.jpg.json"})
        assert (agree, disagree) == (1, 0)
        assert findings == []

    def test_disagreement_is_reported(self):
        """Scout paired within one archive; Inventory paired across all."""
        index = FakeIndex(pairings=[
            IndexedPairing("a.jpg", "p2.zip", "other/a.jpg.json",
                           "cross-directory", "own")])
        agree, disagree, findings = compare_with_scout(
            index, {("p2.zip", "a.jpg"): "wrong/a.jpg.json"})
        assert (agree, disagree) == (0, 1)
        assert findings[0].kind == "disagreement"
        assert "other/a.jpg.json" in findings[0].detail

    def test_scout_found_none_where_inventory_paired(self):
        """The 71.7% case: the sidecar was in another archive."""
        index = FakeIndex(pairings=[
            IndexedPairing("a.jpg", "p2.zip", "far/a.jpg.json",
                           "cross-directory", "own")])
        agree, disagree, _ = compare_with_scout(
            index, {("p2.zip", "a.jpg"): None})
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
        agree, disagree, _ = compare_with_scout(
            index, {("p.zip", "a.jpg"): None})
        assert (agree, disagree) == (1, 0)

    def test_same_path_in_two_archives_does_not_collide(self):
        """A member path is not unique across an export.

        Inventory's index is keyed on (archive, path) for this reason. Keying
        on the path alone let one archive's answer overwrite another's, and
        silently corrupted the counts.
        """
        index = FakeIndex(pairings=[
            IndexedPairing("IMG_1.jpg", "part1.zip", "IMG_1.jpg.json",
                           "exact", "own"),
            IndexedPairing("IMG_1.jpg", "part2.zip", "other/IMG_1.jpg.json",
                           "cross-directory", "own"),
        ])
        scout = {
            ("part1.zip", "IMG_1.jpg"): "IMG_1.jpg.json",   # Scout got this one right
            ("part2.zip", "IMG_1.jpg"): None,               # and missed this one
        }

        agree, disagree, findings = compare_with_scout(index, scout)

        assert (agree, disagree) == (1, 1)
        assert findings[0].kind == "disagreement"
        assert "part2.zip" in findings[0].detail


class TestIsComparable:
    def test_media_is_comparable(self):
        assert is_comparable("photo")
        assert is_comparable("video")

    def test_json_and_other_are_not(self):
        assert not is_comparable("json")
        assert not is_comparable("other")

    def test_none_is_not(self):
        """file_type is Optional on FileDetails."""
        assert not is_comparable(None)
