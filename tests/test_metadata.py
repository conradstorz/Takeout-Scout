"""
Tests for takeout_scout.metadata module.
"""
import pytest
from takeout_scout.metadata import (
    has_pil,
    extract_photo_metadata,
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
