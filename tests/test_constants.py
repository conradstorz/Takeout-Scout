"""
Tests for takeout_scout.constants module.
"""
import pytest
from takeout_scout.constants import (
    MEDIA_PHOTO_EXT,
    MEDIA_VIDEO_EXT,
    JSON_EXT,
    SERVICE_HINTS,
    PARTS_PAT,
    classify_file,
    get_default_paths,
)


class TestFileExtensions:
    """Tests for file extension sets."""
    
    def test_photo_extensions_lowercase(self):
        """Test that all photo extensions are lowercase."""
        for ext in MEDIA_PHOTO_EXT:
            assert ext == ext.lower()
            assert ext.startswith('.')
    
    def test_video_extensions_lowercase(self):
        """Test that all video extensions are lowercase."""
        for ext in MEDIA_VIDEO_EXT:
            assert ext == ext.lower()
            assert ext.startswith('.')
    
    def test_common_photo_formats(self):
        """Test common photo formats are included."""
        assert '.jpg' in MEDIA_PHOTO_EXT
        assert '.jpeg' in MEDIA_PHOTO_EXT
        assert '.png' in MEDIA_PHOTO_EXT
        assert '.heic' in MEDIA_PHOTO_EXT
        assert '.gif' in MEDIA_PHOTO_EXT
    
    def test_raw_photo_formats(self):
        """Test RAW photo formats are included."""
        assert '.raw' in MEDIA_PHOTO_EXT
        assert '.dng' in MEDIA_PHOTO_EXT
        assert '.arw' in MEDIA_PHOTO_EXT  # Sony
        assert '.cr2' in MEDIA_PHOTO_EXT  # Canon
        assert '.nef' in MEDIA_PHOTO_EXT  # Nikon
    
    def test_common_video_formats(self):
        """Test common video formats are included."""
        assert '.mp4' in MEDIA_VIDEO_EXT
        assert '.mov' in MEDIA_VIDEO_EXT
        assert '.avi' in MEDIA_VIDEO_EXT
        assert '.mkv' in MEDIA_VIDEO_EXT
    
    def test_json_extension(self):
        """Test JSON extension."""
        assert '.json' in JSON_EXT


class TestServiceHints:
    """Tests for service detection patterns."""
    
    def test_google_photos_pattern(self):
        """Test Google Photos detection."""
        pattern = SERVICE_HINTS['Google Photos']
        assert pattern.search('Takeout/Google Photos/2024')
        assert pattern.search('google photos backup')
    
    def test_google_drive_pattern(self):
        """Test Google Drive detection."""
        pattern = SERVICE_HINTS['Google Drive']
        assert pattern.search('Google Drive/My Files')
    
    def test_youtube_pattern(self):
        """Test YouTube detection."""
        pattern = SERVICE_HINTS['YouTube']
        assert pattern.search('YouTube/uploads')


class TestPartsPat:
    """Tests for multi-part archive pattern."""
    
    def test_matches_multipart_zip(self):
        """Test matching multi-part ZIP files."""
        match = PARTS_PAT.match('takeout-20240101-001.zip')
        assert match is not None
        assert match.group('prefix') == 'takeout-20240101'
    
    def test_matches_multipart_tgz(self):
        """Test matching multi-part TGZ files."""
        match = PARTS_PAT.match('backup-001.tgz')
        assert match is not None
    
    def test_no_match_single_file(self):
        """Test non-matching single archive."""
        match = PARTS_PAT.match('takeout.zip')
        assert match is None


class TestClassifyFile:
    """Tests for classify_file function."""
    
    def test_classify_photo(self):
        """Test photo classification."""
        assert classify_file('image.jpg') == 'photo'
        assert classify_file('photo.JPEG') == 'photo'
        assert classify_file('path/to/IMG_001.heic') == 'photo'
        assert classify_file('raw.DNG') == 'photo'
    
    def test_classify_video(self):
        """Test video classification."""
        assert classify_file('video.mp4') == 'video'
        assert classify_file('movie.MOV') == 'video'
        assert classify_file('path/clip.avi') == 'video'
    
    def test_classify_json(self):
        """Test JSON classification."""
        assert classify_file('metadata.json') == 'json'
        assert classify_file('photo.jpg.json') == 'json'
    
    def test_classify_other(self):
        """Test other file classification."""
        assert classify_file('readme.txt') == 'other'
        assert classify_file('data.xml') == 'other'
        assert classify_file('noextension') == 'other'


class TestGetDefaultPaths:
    """Tests for get_default_paths function."""
    
    def test_returns_dict(self):
        """Test that it returns a dictionary."""
        paths = get_default_paths()
        assert isinstance(paths, dict)
    
    def test_required_keys(self):
        """Test that required keys are present."""
        paths = get_default_paths()
        assert 'log_dir' in paths
        assert 'state_dir' in paths
        assert 'discoveries_dir' in paths
        assert 'index_path' in paths
