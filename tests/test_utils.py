"""
Tests for takeout_scout.utils module.
"""
from pathlib import Path

import pytest
from takeout_scout.utils import (
    human_size,
    partition_known_paths,
    remove_dirs,
    unreferenced_dirs,
)


class TestHumanSize:
    """Tests for the human_size function."""
    
    def test_bytes(self):
        """Test byte values."""
        assert human_size(0) == "0.00 B"
        assert human_size(100) == "100.00 B"
        assert human_size(1023) == "1023.00 B"
    
    def test_kilobytes(self):
        """Test kilobyte values."""
        assert human_size(1024) == "1.00 KB"
        assert human_size(1536) == "1.50 KB"
        assert human_size(1024 * 100) == "100.00 KB"
    
    def test_megabytes(self):
        """Test megabyte values."""
        assert human_size(1024 * 1024) == "1.00 MB"
        assert human_size(1024 * 1024 * 5) == "5.00 MB"
        assert human_size(1024 * 1024 * 500) == "500.00 MB"
    
    def test_gigabytes(self):
        """Test gigabyte values."""
        assert human_size(1024 ** 3) == "1.00 GB"
        assert human_size(1024 ** 3 * 2.5) == "2.50 GB"
    
    def test_terabytes(self):
        """Test terabyte values."""
        assert human_size(1024 ** 4) == "1.00 TB"
        assert human_size(1024 ** 4 * 10) == "10.00 TB"


class TestPartitionKnownPaths:
    """Tests for the partition_known_paths function."""

    def test_all_new(self):
        """All-new candidates land entirely in `new`."""
        candidates = [Path("/a"), Path("/b"), Path("/c")]
        new, already = partition_known_paths(candidates, known=set())
        assert new == candidates
        assert already == []

    def test_known_candidate_goes_to_already(self):
        """A candidate already in `known` lands in `already`, not `new`."""
        candidates = [Path("/a"), Path("/b")]
        new, already = partition_known_paths(candidates, known={str(Path("/a"))})
        assert new == [Path("/b")]
        assert already == [Path("/a")]

    def test_duplicate_within_candidates_collapses(self):
        """The first occurrence of a repeated path is new; later ones are known."""
        candidates = [Path("/a"), Path("/b"), Path("/a")]
        new, already = partition_known_paths(candidates, known=set())
        assert new == [Path("/a"), Path("/b")]
        assert already == [Path("/a")]

    def test_order_is_preserved(self):
        """Both returned lists preserve the original candidate order."""
        candidates = [Path("/c"), Path("/a"), Path("/b")]
        new, already = partition_known_paths(candidates, known={str(Path("/a"))})
        assert new == [Path("/c"), Path("/b")]
        assert already == [Path("/a")]

    def test_empty_candidates(self):
        """Empty candidates produce two empty lists."""
        new, already = partition_known_paths([], known={str(Path("/a"))})
        assert new == []
        assert already == []


class TestRemoveDirs:
    """Tests for the remove_dirs function."""

    def test_removes_real_directories_with_files(self, tmp_path):
        """Two real directories with files in them are both removed."""
        dir1 = tmp_path / "dir1"
        dir2 = tmp_path / "dir2"
        dir1.mkdir()
        dir2.mkdir()
        (dir1 / "file.txt").write_text("hello")
        (dir2 / "file.txt").write_text("world")

        removed, failures = remove_dirs([dir1, dir2])

        assert set(removed) == {dir1, dir2}
        assert failures == []
        assert not dir1.exists()
        assert not dir2.exists()

    def test_nonexistent_path_counts_as_removed(self, tmp_path):
        """A path that does not exist is counted as removed, no failure."""
        missing = tmp_path / "does-not-exist"

        removed, failures = remove_dirs([missing])

        assert removed == [missing]
        assert failures == []

    def test_mix_of_existing_and_nonexistent(self, tmp_path):
        """A mix of existing and non-existent dirs all land in `removed`."""
        existing = tmp_path / "exists"
        existing.mkdir()
        missing = tmp_path / "missing"

        removed, failures = remove_dirs([existing, missing])

        assert set(removed) == {existing, missing}
        assert failures == []
        assert not existing.exists()

    def test_empty_input(self):
        """Empty input produces two empty lists."""
        removed, failures = remove_dirs([])
        assert removed == []
        assert failures == []


class TestUnreferencedDirs:
    """Tests for the unreferenced_dirs function."""

    def test_dir_with_no_in_use_path_under_it_is_unreferenced(self, tmp_path):
        """A directory with no in-use path under it is returned."""
        stale = tmp_path / "scout_stale"
        stale.mkdir()

        result = unreferenced_dirs([stale], in_use={str(tmp_path / "elsewhere" / "f.zip")})

        assert result == [stale]

    def test_dir_containing_an_in_use_path_is_not_returned(self, tmp_path):
        """A directory containing an in-use path is not returned."""
        active = tmp_path / "scout_active"
        active.mkdir()
        in_use_path = active / "a.zip"

        result = unreferenced_dirs([active], in_use={str(in_use_path)})

        assert result == []

    def test_shared_name_prefix_does_not_count_as_containment(self, tmp_path):
        """A directory whose name is a prefix of another directory's name,
        where only the *other* one holds an in-use path, is still returned.

        This is the containment trap: string prefix matching would wrongly
        treat "scout_a" as containing "scout_ab/x" because "scout_a" is a
        string prefix of "scout_ab". Path.is_relative_to must not be fooled
        by this — the two are siblings, not parent and child.
        """
        prefix_dir = tmp_path / "scout_a"
        prefix_dir.mkdir()
        other_dir = tmp_path / "scout_ab"
        other_dir.mkdir()
        in_use_path = other_dir / "x"

        result = unreferenced_dirs([prefix_dir, other_dir], in_use={str(in_use_path)})

        assert result == [prefix_dir]

    def test_empty_in_use_returns_every_dir(self, tmp_path):
        """Empty in_use means every directory is unreferenced."""
        dir1 = tmp_path / "dir1"
        dir2 = tmp_path / "dir2"
        dir1.mkdir()
        dir2.mkdir()

        result = unreferenced_dirs([dir1, dir2], in_use=set())

        assert result == [dir1, dir2]

    def test_empty_dirs_returns_empty_list(self, tmp_path):
        """Empty dirs produces an empty list."""
        result = unreferenced_dirs([], in_use={str(tmp_path / "f.zip")})

        assert result == []
