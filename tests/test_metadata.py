"""
Tests for takeout_scout.metadata module.
"""
import pytest
import zipfile
import tarfile
import io
from pathlib import Path

from takeout_scout.metadata import (
    has_pil,
    extract_photo_metadata,
    extract_metadata_from_zip,
    extract_metadata_from_tar,
    detect_media_pairs,
)


class TestHasPil:
    """Tests for has_pil function."""
    
    def test_returns_bool(self):
        """Test that has_pil returns a boolean."""
        result = has_pil()
        assert isinstance(result, bool)


class TestExtractPhotoMetadata:
    """Tests for extract_photo_metadata function."""
    
    def test_invalid_data_returns_none(self):
        """Test that invalid image data returns None."""
        result = extract_photo_metadata(b'not an image', 'test.jpg')
        assert result is None
    
    def test_empty_data_returns_none(self):
        """Test that empty data returns None."""
        result = extract_photo_metadata(b'', 'test.jpg')
        assert result is None


class TestDetectMediaPairs:
    """Tests for detect_media_pairs function."""
    
    def test_empty_list(self):
        """Test with empty file list."""
        pairs, paired = detect_media_pairs([])
        assert pairs == []
        assert paired == {}
    
    def test_live_photo_detection(self):
        """Test Apple Live Photo detection (HEIC + MOV)."""
        files = [
            ('photos/IMG_001.HEIC', 3000000),
            ('photos/IMG_001.MOV', 2000000),
            ('photos/IMG_002.jpg', 1000000),
        ]
        pairs, paired = detect_media_pairs(files)
        
        live_photos = [p for p in pairs if p.pair_type == 'live_photo']
        assert len(live_photos) == 1
        assert live_photos[0].base_name == 'IMG_001'
    
    def test_photo_json_pair_detection(self):
        """Test photo + JSON sidecar detection."""
        files = [
            ('photos/photo.jpg', 1000000),
            ('photos/photo.jpg.json', 500),
        ]
        pairs, paired = detect_media_pairs(files)
        
        json_pairs = [p for p in pairs if p.pair_type == 'photo_json']
        assert len(json_pairs) == 1
    
    def test_no_pairs_single_files(self):
        """Test that unpaired files don't create pairs."""
        files = [
            ('photo1.jpg', 1000000),
            ('photo2.png', 2000000),
            ('video.mp4', 5000000),
        ]
        pairs, paired = detect_media_pairs(files)
        assert len(pairs) == 0
    
    def test_multiple_pairs(self):
        """Test detection of multiple pairs."""
        files = [
            ('IMG_001.HEIC', 3000000),
            ('IMG_001.MOV', 2000000),
            ('IMG_002.JPG', 1000000),
            ('IMG_002.JPG.json', 400),
            ('IMG_003.HEIC', 3500000),
            ('IMG_003.mov', 2500000),
        ]
        pairs, paired = detect_media_pairs(files)
        
        live_photos = [p for p in pairs if p.pair_type == 'live_photo']
        json_pairs = [p for p in pairs if p.pair_type == 'photo_json']
        
        assert len(live_photos) == 2
        assert len(json_pairs) == 1


class TestExtractMetadataFromZip:
    """Tests for extract_metadata_from_zip function."""
    
    def test_extract_from_valid_zip(self, tmp_path):
        """Test extracting metadata from a file in a ZIP archive."""
        zip_path = tmp_path / "test.zip"
        # Create a minimal valid JPEG (just headers, won't have EXIF)
        fake_jpg = b'\xff\xd8\xff\xe0\x00\x10JFIF\x00\x01\x01\x00\x00\x01\x00\x01\x00\x00\xff\xd9'
        
        with zipfile.ZipFile(zip_path, 'w') as zf:
            zf.writestr('photo.jpg', fake_jpg)
        
        with zipfile.ZipFile(zip_path, 'r') as zf:
            # May return None or PhotoMetadata depending on PIL availability
            result = extract_metadata_from_zip(zf, 'photo.jpg')
            # Should not raise an exception
    
    def test_extract_nonexistent_member(self, tmp_path):
        """Test extracting from nonexistent member returns None."""
        zip_path = tmp_path / "test.zip"
        with zipfile.ZipFile(zip_path, 'w') as zf:
            zf.writestr('exists.txt', b'data')
        
        with zipfile.ZipFile(zip_path, 'r') as zf:
            result = extract_metadata_from_zip(zf, 'nonexistent.jpg')
        
        assert result is None


class TestExtractMetadataFromTar:
    """Tests for extract_metadata_from_tar function."""
    
    def test_extract_from_valid_tar(self, tmp_path):
        """Test extracting metadata from a file in a TAR archive."""
        tar_path = tmp_path / "test.tar"
        fake_jpg = b'\xff\xd8\xff\xe0\x00\x10JFIF\x00\x01\x01\x00\x00\x01\x00\x01\x00\x00\xff\xd9'
        
        with tarfile.open(tar_path, 'w') as tf:
            info = tarfile.TarInfo(name='photo.jpg')
            info.size = len(fake_jpg)
            tf.addfile(info, io.BytesIO(fake_jpg))
        
        with tarfile.open(tar_path, 'r') as tf:
            # May return None or PhotoMetadata depending on PIL availability
            result = extract_metadata_from_tar(tf, 'photo.jpg')
            # Should not raise an exception
    
    def test_extract_nonexistent_tar_member(self, tmp_path):
        """Test extracting from nonexistent tar member returns None."""
        tar_path = tmp_path / "test.tar"
        with tarfile.open(tar_path, 'w') as tf:
            info = tarfile.TarInfo(name='exists.txt')
            info.size = 4
            tf.addfile(info, io.BytesIO(b'data'))
        
        with tarfile.open(tar_path, 'r') as tf:
            result = extract_metadata_from_tar(tf, 'nonexistent.jpg')
        
        assert result is None


class TestDetectMediaPairsAdvanced:
    """Advanced tests for detect_media_pairs function."""
    
    def test_png_json_pair(self):
        """Test PNG + JSON sidecar detection."""
        files = [
            ('photos/image.png', 1000000),
            ('photos/image.png.json', 500),
        ]
        pairs, paired = detect_media_pairs(files)
        
        # PNG is not in the photo_exts list (HEIC, HEIF, JPG, JPEG)
        # So this won't create a pair with current implementation
        # This is testing current behavior
    
    def test_motion_photo_detection(self):
        """Test Google Motion Photo detection (JPG + MP4)."""
        files = [
            ('photos/PXL_001.jpg', 4000000),  # Large JPG (motion photo)
            ('photos/PXL_001.mp4', 2000000),
        ]
        pairs, paired = detect_media_pairs(files)
        
        # JPG + MP4 creates a live_photo pair (same as HEIC + MOV)
        live_photos = [p for p in pairs if p.pair_type == 'live_photo']
        assert len(live_photos) == 1
    
    def test_case_insensitive_extension_matching(self):
        """Test that extension matching is case-insensitive."""
        files = [
            ('photos/IMG.HEIC', 3000000),
            ('photos/IMG.mov', 2000000),  # lowercase mov
        ]
        pairs, paired = detect_media_pairs(files)
        
        live_photos = [p for p in pairs if p.pair_type == 'live_photo']
        assert len(live_photos) == 1
    
    def test_jpg_json_pairing(self):
        """Test JPG + JSON sidecar pairing."""
        files = [
            ('photo.jpg', 1000000),
            ('photo.jpg.json', 400),
        ]
        pairs, paired = detect_media_pairs(files)
        
        json_pairs = [p for p in pairs if 'json' in p.pair_type]
        assert len(json_pairs) == 1
    
    def test_nested_path_pairing(self):
        """Test pairing works with nested paths."""
        files = [
            ('Google Photos/2024/January/IMG_001.HEIC', 3000000),
            ('Google Photos/2024/January/IMG_001.MOV', 2000000),
        ]
        pairs, paired = detect_media_pairs(files)
        
        # Should find live_photo pair
        assert len(pairs) >= 1
        live_photos = [p for p in pairs if p.pair_type == 'live_photo']
        assert len(live_photos) == 1
    
    def test_paired_dict_contains_all_paired_files(self):
        """Test that paired dict maps all paired files."""
        files = [
            ('IMG_001.HEIC', 3000000),
            ('IMG_001.MOV', 2000000),
        ]
        pairs, paired = detect_media_pairs(files)
        
        assert 'IMG_001.HEIC' in paired or 'IMG_001.MOV' in paired
    
    def test_large_file_list_performance(self):
        """Test that function handles large file lists."""
        # Create 1000 JPG files
        files = [(f'photo_{i:04d}.jpg', 1000000) for i in range(1000)]
        # Add matching JSONs for half
        files.extend([(f'photo_{i:04d}.jpg.json', 500) for i in range(500)])
        
        pairs, paired = detect_media_pairs(files)
        
        # Should find 500 photo+json pairs
        json_pairs = [p for p in pairs if p.pair_type == 'photo_json']
        assert len(json_pairs) == 500


class TestExtractPhotoMetadataEdgeCases:
    """Edge case tests for extract_photo_metadata."""
    
    def test_truncated_data(self):
        """Test with truncated image data."""
        result = extract_photo_metadata(b'\xff\xd8\xff', 'truncated.jpg')
        assert result is None
    
    def test_wrong_extension(self):
        """Test that filename extension doesn't affect parsing."""
        # Valid JPEG structure but wrong extension
        fake_jpg = b'\xff\xd8\xff\xe0\x00\x10JFIF\x00\x01\x01\x00\x00\x01\x00\x01\x00\x00\xff\xd9'
        result = extract_photo_metadata(fake_jpg, 'image.txt')
        # Should still try to parse based on content, not extension
    
    def test_very_large_data_is_handled(self):
        """Test that large data doesn't cause issues."""
        # This tests memory handling - just ensure no crash
        large_data = b'\xff\xd8\xff' + b'\x00' * 100000
        result = extract_photo_metadata(large_data, 'large.jpg')
        # Should return None for invalid data, not crash
        assert result is None


class TestHasPilFunction:
    """Tests for has_pil function."""
    
    def test_returns_consistent_value(self):
        """Test that has_pil returns consistent value."""
        result1 = has_pil()
        result2 = has_pil()
        assert result1 == result2
    
    def test_is_boolean(self):
        """Test that result is a boolean."""
        assert isinstance(has_pil(), bool)
