"""
Tests for takeout_scout.utils module.
"""
import pytest
from takeout_scout.utils import human_size


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
