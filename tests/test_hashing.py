"""
Tests for takeout_scout.hashing module.
"""
import io
import pytest
import tarfile
import zipfile
from pathlib import Path

from takeout_scout.hashing import (
    calculate_hash,
    hash_file,
    hash_zip_member,
    hash_tar_member,
    HashIndex,
)


class TestCalculateHash:
    """Tests for calculate_hash function."""
    
    def test_hash_bytes_md5(self):
        """Test MD5 hash of bytes."""
        data = b'Hello, World!'
        result = calculate_hash(data, algorithm='md5')
        # Known MD5 hash of "Hello, World!"
        assert result == '65a8e27d8879283831b664bd8b7f0ad4'
    
    def test_hash_bytes_sha256(self):
        """Test SHA256 hash of bytes."""
        data = b'Hello, World!'
        result = calculate_hash(data, algorithm='sha256')
        # Known SHA256 hash
        assert result == 'dffd6021bb2bd5b0af676290809ec3a53191dd81c7f70a4b28688a362182986f'
    
    def test_hash_file_object(self):
        """Test hashing from file-like object."""
        data = b'Test data for hashing'
        file_obj = io.BytesIO(data)
        result = calculate_hash(file_obj, algorithm='md5')
        
        # Compare with direct bytes hash
        expected = calculate_hash(data, algorithm='md5')
        assert result == expected
    
    def test_empty_data(self):
        """Test hash of empty data."""
        result = calculate_hash(b'', algorithm='md5')
        # Known MD5 of empty string
        assert result == 'd41d8cd98f00b204e9800998ecf8427e'
    
    def test_same_content_same_hash(self):
        """Test that identical content produces identical hash."""
        data1 = b'identical content'
        data2 = b'identical content'
        assert calculate_hash(data1) == calculate_hash(data2)
    
    def test_different_content_different_hash(self):
        """Test that different content produces different hash."""
        data1 = b'content one'
        data2 = b'content two'
        assert calculate_hash(data1) != calculate_hash(data2)


class TestHashFile:
    """Tests for hash_file function."""
    
    def test_hash_existing_file(self, tmp_path):
        """Test hashing a file on disk."""
        test_file = tmp_path / "test.txt"
        test_file.write_bytes(b'File content for testing')
        
        result = hash_file(test_file)
        assert result is not None
        assert len(result) == 32  # MD5 hex length
    
    def test_hash_nonexistent_file(self, tmp_path):
        """Test hashing a file that doesn't exist."""
        fake_path = tmp_path / "does_not_exist.txt"
        result = hash_file(fake_path)
        assert result is None
    
    def test_consistent_hash(self, tmp_path):
        """Test that same file produces same hash."""
        test_file = tmp_path / "test.txt"
        test_file.write_bytes(b'Consistent content')
        
        hash1 = hash_file(test_file)
        hash2 = hash_file(test_file)
        assert hash1 == hash2


class TestHashZipMember:
    """Tests for hash_zip_member function."""
    
    def test_hash_zip_member(self, tmp_path):
        """Test hashing a file inside a ZIP."""
        zip_path = tmp_path / "test.zip"
        content = b'Content inside zip'
        
        with zipfile.ZipFile(zip_path, 'w') as zf:
            zf.writestr('inner.txt', content)
        
        with zipfile.ZipFile(zip_path, 'r') as zf:
            result = hash_zip_member(zf, 'inner.txt')
        
        # Should match direct hash of content
        expected = calculate_hash(content)
        assert result == expected
    
    def test_hash_nonexistent_member(self, tmp_path):
        """Test hashing a member that doesn't exist."""
        zip_path = tmp_path / "test.zip"
        
        with zipfile.ZipFile(zip_path, 'w') as zf:
            zf.writestr('exists.txt', b'data')
        
        with zipfile.ZipFile(zip_path, 'r') as zf:
            result = hash_zip_member(zf, 'does_not_exist.txt')
        
        assert result is None


class TestHashTarMember:
    """Tests for hash_tar_member function."""
    
    def test_hash_tar_member(self, tmp_path):
        """Test hashing a file inside a TAR."""
        tar_path = tmp_path / "test.tar"
        content = b'Content inside tar'
        
        with tarfile.open(tar_path, 'w') as tf:
            info = tarfile.TarInfo(name='inner.txt')
            info.size = len(content)
            tf.addfile(info, io.BytesIO(content))
        
        with tarfile.open(tar_path, 'r') as tf:
            result = hash_tar_member(tf, 'inner.txt')
        
        # Should match direct hash of content
        expected = calculate_hash(content)
        assert result == expected


class TestHashIndex:
    """Tests for HashIndex class."""
    
    def test_add_and_get_hash(self):
        """Test adding files and retrieving hash."""
        index = HashIndex()
        index.add('abc123', '/archive.zip', 'photo.jpg', 1000)
        
        result = index.get_hash('/archive.zip', 'photo.jpg')
        assert result == 'abc123'
    
    def test_get_duplicates(self):
        """Test finding files with same hash."""
        index = HashIndex()
        index.add('same_hash', '/archive1.zip', 'photo.jpg', 1000)
        index.add('same_hash', '/archive2.zip', 'photo.jpg', 1000)
        index.add('different', '/archive3.zip', 'other.jpg', 500)
        
        duplicates = index.get_duplicates('same_hash')
        assert len(duplicates) == 2
        
        other = index.get_duplicates('different')
        assert len(other) == 1
    
    def test_find_all_duplicates(self):
        """Test finding all duplicate sets."""
        index = HashIndex()
        # Add duplicates
        index.add('dup1', '/a.zip', 'photo1.jpg', 1000)
        index.add('dup1', '/b.zip', 'photo1.jpg', 1000)
        # Add unique
        index.add('unique', '/c.zip', 'unique.jpg', 500)
        # Add another duplicate set
        index.add('dup2', '/a.zip', 'photo2.jpg', 2000)
        index.add('dup2', '/b.zip', 'photo2.jpg', 2000)
        index.add('dup2', '/c.zip', 'photo2.jpg', 2000)
        
        all_dups = index.find_all_duplicates()
        assert len(all_dups) == 2
        assert 'dup1' in all_dups
        assert 'dup2' in all_dups
        assert 'unique' not in all_dups
    
    def test_get_duplicate_stats(self):
        """Test duplicate statistics calculation."""
        index = HashIndex()
        # 3 files with same hash (2 are duplicates)
        index.add('dup', '/a.zip', 'p1.jpg', 1000)
        index.add('dup', '/b.zip', 'p1.jpg', 1000)
        index.add('dup', '/c.zip', 'p1.jpg', 1000)
        # 1 unique file
        index.add('uniq', '/a.zip', 'p2.jpg', 500)
        
        stats = index.get_duplicate_stats()
        assert stats['total_files'] == 4
        assert stats['unique_hashes'] == 2
        assert stats['duplicate_sets'] == 1
        assert stats['duplicate_files'] == 2  # 3 files - 1 original
        assert stats['wasted_bytes'] == 2000  # 2 x 1000
    
    def test_to_dict_from_dict_roundtrip(self):
        """Test serialization roundtrip."""
        index = HashIndex()
        index.add('hash1', '/archive.zip', 'file1.jpg', 1000)
        index.add('hash2', '/archive.zip', 'file2.jpg', 2000)
        
        data = index.to_dict()
        restored = HashIndex.from_dict(data)
        
        assert restored.get_hash('/archive.zip', 'file1.jpg') == 'hash1'
        assert restored.get_hash('/archive.zip', 'file2.jpg') == 'hash2'
    
    def test_empty_index_stats(self):
        """Test stats on empty index."""
        index = HashIndex()
        stats = index.get_duplicate_stats()
        
        assert stats['total_files'] == 0
        assert stats['unique_hashes'] == 0
        assert stats['duplicate_sets'] == 0
        assert stats['wasted_bytes'] == 0


class TestScannerWithHashing:
    """Integration tests for scanner with hashing enabled."""
    
    def test_scan_zip_with_hashes(self, tmp_path):
        """Test scanning ZIP with hash computation."""
        from takeout_scout.scanner import scan_archive
        from takeout_scout.discovery import load_takeout_discovery
        
        zip_path = tmp_path / "test.zip"
        with zipfile.ZipFile(zip_path, 'w') as zf:
            zf.writestr('photo.jpg', b'fake image data')
            zf.writestr('video.mp4', b'fake video data')
        
        summary = scan_archive(zip_path, save_discovery=True, compute_hashes=True)
        
        assert summary.file_count == 2
        
        # Load discovery and check hashes are saved
        discovery = load_takeout_discovery(zip_path)
        assert discovery is not None
        
        for fd in discovery.file_details:
            assert fd.get('file_hash') is not None
    
    def test_scan_directory_with_hashes(self, tmp_path):
        """Test scanning directory with hash computation."""
        from takeout_scout.scanner import scan_directory
        from takeout_scout.discovery import load_takeout_discovery
        
        # Create test files
        (tmp_path / "photo.jpg").write_bytes(b'image data')
        (tmp_path / "doc.txt").write_bytes(b'text data')
        
        summary = scan_directory(tmp_path, save_discovery=True, compute_hashes=True)
        
        assert summary.file_count == 2
        
        # Load discovery and check hashes
        discovery = load_takeout_discovery(tmp_path)
        assert discovery is not None
        
        for fd in discovery.file_details:
            assert fd.get('file_hash') is not None
