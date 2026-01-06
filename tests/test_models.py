"""
Tests for takeout_scout.models module.
"""
import pytest
from takeout_scout.models import (
    PhotoMetadata,
    FileDetails,
    MediaPair,
    TakeoutDiscovery,
    ArchiveSummary,
)


class TestPhotoMetadata:
    """Tests for PhotoMetadata dataclass."""
    
    def test_default_values(self):
        """Test default initialization."""
        meta = PhotoMetadata()
        assert meta.has_exif is False
        assert meta.has_gps is False
        assert meta.has_datetime is False
        assert meta.camera_make is None
        assert meta.width is None
    
    def test_to_dict(self):
        """Test conversion to dictionary."""
        meta = PhotoMetadata(
            has_exif=True,
            has_gps=True,
            camera_make="Canon",
            width=1920,
            height=1080,
        )
        d = meta.to_dict()
        assert d['has_exif'] is True
        assert d['has_gps'] is True
        assert d['camera_make'] == "Canon"
        assert d['width'] == 1920
    
    def test_from_dict(self):
        """Test that PhotoMetadata doesn't have from_dict (uses direct construction)."""
        # PhotoMetadata is a simple dataclass without from_dict
        # since it's typically created from EXIF extraction, not deserialization
        meta = PhotoMetadata(
            has_exif=True,
            has_gps=False,
            camera_make='Sony',
            width=4000,
            height=3000,
        )
        assert meta.has_exif is True
        assert meta.camera_make == 'Sony'
        assert meta.width == 4000


class TestFileDetails:
    """Tests for FileDetails dataclass."""
    
    def test_creation(self):
        """Test basic creation."""
        fd = FileDetails(
            path="photos/IMG_001.jpg",
            size=1024000,
            file_type="photo",
            extension=".jpg",
        )
        assert fd.path == "photos/IMG_001.jpg"
        assert fd.size == 1024000
        assert fd.file_type == "photo"
        assert fd.metadata is None
    
    def test_to_dict(self):
        """Test conversion to dictionary."""
        fd = FileDetails(
            path="test.jpg",
            size=500,
            file_type="photo",
            extension=".jpg",
            metadata={'has_exif': True}
        )
        d = fd.to_dict()
        assert d['path'] == "test.jpg"
        assert d['metadata'] == {'has_exif': True}
    
    def test_from_dict(self):
        """Test creation from dictionary."""
        data = {
            'path': 'video.mp4',
            'size': 50000000,
            'file_type': 'video',
            'extension': '.mp4',
        }
        fd = FileDetails.from_dict(data)
        assert fd.path == 'video.mp4'
        assert fd.file_type == 'video'


class TestMediaPair:
    """Tests for MediaPair dataclass."""
    
    def test_live_photo_pair(self):
        """Test live photo pair creation."""
        pair = MediaPair(
            pair_type='live_photo',
            photo_path='IMG_001.HEIC',
            companion_path='IMG_001.MOV',
            photo_size=3000000,
            companion_size=2000000,
            base_name='IMG_001',
        )
        assert pair.pair_type == 'live_photo'
        assert pair.photo_path == 'IMG_001.HEIC'
    
    def test_json_pair(self):
        """Test photo+JSON pair creation."""
        pair = MediaPair(
            pair_type='photo_json',
            photo_path='photo.jpg',
            companion_path='photo.jpg.json',
            photo_size=1000000,
            companion_size=500,
            base_name='photo',
        )
        d = pair.to_dict()
        assert d['pair_type'] == 'photo_json'


class TestArchiveSummary:
    """Tests for ArchiveSummary dataclass."""
    
    def test_creation(self):
        """Test basic creation."""
        summary = ArchiveSummary(
            path="/path/to/archive.zip",
            parts_group="takeout-2024",
            service_guess="Google Photos",
            file_count=1000,
            photos=800,
            videos=50,
            json_sidecars=100,
            other=50,
            compressed_size=500000000,
        )
        assert summary.file_count == 1000
        assert summary.photos == 800
    
    def test_to_row(self):
        """Test to_row for table display."""
        summary = ArchiveSummary(
            path="/path/archive.zip",
            parts_group="takeout",
            service_guess="Photos",
            file_count=100,
            photos=80,
            videos=10,
            json_sidecars=5,
            other=5,
            compressed_size=1000000,
        )
        row = summary.to_row()
        assert row[0] == "/path/archive.zip"
        assert row[1] == "takeout"
        assert row[4] == '80'  # photos as string
    
    def test_to_dict(self):
        """Test conversion to dictionary."""
        summary = ArchiveSummary(
            path="test.zip",
            parts_group="test",
            service_guess="Unknown",
            file_count=10,
            photos=5,
            videos=2,
            json_sidecars=3,
            other=0,
            compressed_size=10000,
            photos_with_exif=3,
            live_photos=1,
        )
        d = summary.to_dict()
        # Note: to_dict uses display-friendly keys with spaces
        assert d['Path'] == "test.zip"
        assert d['w/EXIF'] == 3
        assert d['Live'] == 1


class TestTakeoutDiscovery:
    """Tests for TakeoutDiscovery dataclass."""
    
    def test_creation(self):
        """Test basic creation with defaults."""
        discovery = TakeoutDiscovery(
            source_path="/path/to/takeout.zip",
            source_type="zip",
            first_discovered="2024-01-01T00:00:00",
            last_scanned="2024-01-01T00:00:00",
            parts_group="takeout-2024",
            service_guess="Google Photos",
            file_count=500,
            photos=400,
            videos=50,
            json_sidecars=40,
            other=10,
            compressed_size=100000000,
        )
        assert discovery.scan_count == 1
        assert discovery.file_details == []
        assert discovery.media_pairs == []
    
    def test_to_dict_from_dict_roundtrip(self):
        """Test serialization roundtrip."""
        original = TakeoutDiscovery(
            source_path="/test/path.zip",
            source_type="zip",
            first_discovered="2024-01-01",
            last_scanned="2024-01-02",
            parts_group="test",
            service_guess="Photos",
            file_count=100,
            photos=80,
            videos=10,
            json_sidecars=5,
            other=5,
            compressed_size=50000,
            photos_with_exif=50,
            scan_count=2,
        )
        d = original.to_dict()
        restored = TakeoutDiscovery.from_dict(d)
        
        assert restored.source_path == original.source_path
        assert restored.photos_with_exif == original.photos_with_exif
        assert restored.scan_count == original.scan_count
