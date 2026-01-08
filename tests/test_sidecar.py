"""Tests for the sidecar module."""
import json
import tempfile
import tarfile
import zipfile
from datetime import datetime, timezone
from pathlib import Path

import pytest

from takeout_scout.sidecar import (
    GeoLocation,
    SidecarMetadata,
    DateAnalysis,
    parse_sidecar,
    parse_sidecar_from_file,
    parse_sidecar_from_zip,
    parse_sidecar_from_tar,
    find_sidecar_for_media,
    _parse_timestamp,
    _parse_geo,
)


class TestParseTimestamp:
    """Tests for _parse_timestamp helper."""
    
    def test_valid_timestamp(self):
        """Test parsing a valid timestamp."""
        time_obj = {"timestamp": "1563198245", "formatted": "Jul 15, 2019, 2:04:05 PM UTC"}
        result = _parse_timestamp(time_obj)
        assert result is not None
        assert result.year == 2019
        assert result.month == 7
        assert result.day == 15
    
    def test_none_input(self):
        """Test with None input."""
        assert _parse_timestamp(None) is None
    
    def test_empty_dict(self):
        """Test with empty dict."""
        assert _parse_timestamp({}) is None
    
    def test_invalid_timestamp(self):
        """Test with invalid timestamp value."""
        assert _parse_timestamp({"timestamp": "invalid"}) is None


class TestParseGeo:
    """Tests for _parse_geo helper."""
    
    def test_valid_coordinates(self):
        """Test parsing valid coordinates."""
        geo_obj = {"latitude": 40.7128, "longitude": -74.0060, "altitude": 10.0}
        result = _parse_geo(geo_obj)
        assert result is not None
        assert result.latitude == 40.7128
        assert result.longitude == -74.0060
        assert result.altitude == 10.0
    
    def test_zero_coordinates_returns_none(self):
        """Test that 0,0 coordinates return None (Google's 'no location' marker)."""
        geo_obj = {"latitude": 0.0, "longitude": 0.0}
        assert _parse_geo(geo_obj) is None
    
    def test_none_input(self):
        """Test with None input."""
        assert _parse_geo(None) is None


class TestGeoLocation:
    """Tests for GeoLocation dataclass."""
    
    def test_to_dict(self):
        """Test serialization."""
        geo = GeoLocation(latitude=40.7, longitude=-74.0, altitude=10.0)
        d = geo.to_dict()
        assert d['latitude'] == 40.7
        assert d['longitude'] == -74.0
        assert d['altitude'] == 10.0
    
    def test_from_dict(self):
        """Test deserialization."""
        data = {'latitude': 40.7, 'longitude': -74.0, 'altitude': 10.0}
        geo = GeoLocation.from_dict(data)
        assert geo.latitude == 40.7
        assert geo.longitude == -74.0
        assert geo.altitude == 10.0


class TestSidecarMetadata:
    """Tests for SidecarMetadata dataclass."""
    
    def test_has_geo_true(self):
        """Test has_geo property when geo data is present."""
        meta = SidecarMetadata(geo_location=GeoLocation(40.7, -74.0))
        assert meta.has_geo is True
    
    def test_has_geo_false(self):
        """Test has_geo property when no geo data."""
        meta = SidecarMetadata()
        assert meta.has_geo is False
    
    def test_best_timestamp_priority(self):
        """Test best_timestamp returns photo_taken_time first."""
        photo_time = datetime(2019, 7, 15, 14, 0, 0, tzinfo=timezone.utc)
        creation_time = datetime(2019, 7, 20, 10, 0, 0, tzinfo=timezone.utc)
        
        meta = SidecarMetadata(
            photo_taken_time=photo_time,
            creation_time=creation_time,
        )
        assert meta.best_timestamp == photo_time
    
    def test_best_timestamp_fallback(self):
        """Test best_timestamp falls back to creation_time."""
        creation_time = datetime(2019, 7, 20, 10, 0, 0, tzinfo=timezone.utc)
        
        meta = SidecarMetadata(creation_time=creation_time)
        assert meta.best_timestamp == creation_time
    
    def test_to_dict_from_dict_roundtrip(self):
        """Test serialization roundtrip."""
        meta = SidecarMetadata(
            title="test.jpg",
            description="A test photo",
            photo_taken_time=datetime(2019, 7, 15, 14, 0, 0, tzinfo=timezone.utc),
            geo_location=GeoLocation(40.7, -74.0),
            people=["Alice", "Bob"],
        )
        
        d = meta.to_dict()
        restored = SidecarMetadata.from_dict(d)
        
        assert restored.title == "test.jpg"
        assert restored.description == "A test photo"
        assert restored.photo_taken_time == meta.photo_taken_time
        assert restored.geo_location.latitude == 40.7
        assert restored.people == ["Alice", "Bob"]


class TestParseSidecar:
    """Tests for parse_sidecar function."""
    
    def test_parse_google_photos_json(self):
        """Test parsing a Google Photos sidecar JSON."""
        json_data = {
            "title": "IMG_1234.jpg",
            "description": "Vacation photo",
            "photoTakenTime": {
                "timestamp": "1563198005",
                "formatted": "Jul 15, 2019, 2:00:05 PM UTC"
            },
            "creationTime": {
                "timestamp": "1563198245",
                "formatted": "Jul 15, 2019, 2:04:05 PM UTC"
            },
            "geoData": {
                "latitude": 40.7128,
                "longitude": -74.0060,
                "altitude": 10.0
            },
            "people": [
                {"name": "Alice"},
                {"name": "Bob"}
            ],
            "url": "https://photos.google.com/photo/test"
        }
        
        content = json.dumps(json_data).encode('utf-8')
        result = parse_sidecar(content)
        
        assert result is not None
        assert result.title == "IMG_1234.jpg"
        assert result.description == "Vacation photo"
        assert result.photo_taken_time is not None
        assert result.photo_taken_time.year == 2019
        assert result.geo_location is not None
        assert result.geo_location.latitude == 40.7128
        assert result.people == ["Alice", "Bob"]
        assert result.url == "https://photos.google.com/photo/test"
    
    def test_parse_minimal_json(self):
        """Test parsing minimal JSON with just title."""
        json_data = {"title": "test.jpg"}
        content = json.dumps(json_data).encode('utf-8')
        result = parse_sidecar(content)
        
        assert result is not None
        assert result.title == "test.jpg"
        assert result.photo_taken_time is None
    
    def test_parse_invalid_json(self):
        """Test with invalid JSON."""
        result = parse_sidecar(b"not valid json")
        assert result is None
    
    def test_parse_invalid_encoding(self):
        """Test with invalid UTF-8."""
        result = parse_sidecar(b'\xff\xfe')
        assert result is None


class TestParseSidecarFromFile:
    """Tests for parse_sidecar_from_file."""
    
    def test_parse_from_file(self, tmp_path):
        """Test parsing from actual file."""
        json_data = {
            "title": "test.jpg",
            "photoTakenTime": {"timestamp": "1563198005"}
        }
        
        json_file = tmp_path / "test.jpg.json"
        json_file.write_text(json.dumps(json_data))
        
        result = parse_sidecar_from_file(json_file)
        
        assert result is not None
        assert result.title == "test.jpg"
    
    def test_file_not_found(self, tmp_path):
        """Test with non-existent file."""
        result = parse_sidecar_from_file(tmp_path / "nonexistent.json")
        assert result is None


class TestParseSidecarFromZip:
    """Tests for parse_sidecar_from_zip."""
    
    def test_parse_from_zip(self, tmp_path):
        """Test parsing JSON from inside a ZIP."""
        json_data = {
            "title": "test.jpg",
            "photoTakenTime": {"timestamp": "1563198005"}
        }
        
        zip_path = tmp_path / "test.zip"
        with zipfile.ZipFile(zip_path, 'w') as zf:
            zf.writestr("photos/test.jpg.json", json.dumps(json_data))
        
        with zipfile.ZipFile(zip_path, 'r') as zf:
            result = parse_sidecar_from_zip(zf, "photos/test.jpg.json")
        
        assert result is not None
        assert result.title == "test.jpg"
    
    def test_member_not_found(self, tmp_path):
        """Test with non-existent member."""
        zip_path = tmp_path / "test.zip"
        with zipfile.ZipFile(zip_path, 'w') as zf:
            zf.writestr("dummy.txt", "test")
        
        with zipfile.ZipFile(zip_path, 'r') as zf:
            result = parse_sidecar_from_zip(zf, "nonexistent.json")
        
        assert result is None


class TestParseSidecarFromTar:
    """Tests for parse_sidecar_from_tar."""
    
    def test_parse_from_tar(self, tmp_path):
        """Test parsing JSON from inside a TAR."""
        json_data = {
            "title": "test.jpg",
            "photoTakenTime": {"timestamp": "1563198005"}
        }
        
        tar_path = tmp_path / "test.tar"
        with tarfile.open(tar_path, 'w') as tf:
            json_content = json.dumps(json_data).encode('utf-8')
            
            import io
            json_file = io.BytesIO(json_content)
            info = tarfile.TarInfo(name="photos/test.jpg.json")
            info.size = len(json_content)
            tf.addfile(info, json_file)
        
        with tarfile.open(tar_path, 'r') as tf:
            member = tf.getmember("photos/test.jpg.json")
            result = parse_sidecar_from_tar(tf, member)
        
        assert result is not None
        assert result.title == "test.jpg"


class TestFindSidecarForMedia:
    """Tests for find_sidecar_for_media."""
    
    def test_find_direct_match(self):
        """Test finding sidecar with direct pattern (photo.jpg -> photo.jpg.json)."""
        available = {"photos/test.jpg", "photos/test.jpg.json", "photos/other.jpg"}
        result = find_sidecar_for_media("photos/test.jpg", available)
        assert result == "photos/test.jpg.json"
    
    def test_no_sidecar_found(self):
        """Test when no sidecar exists."""
        available = {"photos/test.jpg", "photos/other.jpg"}
        result = find_sidecar_for_media("photos/test.jpg", available)
        assert result is None
    
    def test_case_sensitive(self):
        """Test that matching is case-sensitive."""
        available = {"photos/test.jpg", "photos/test.JPG.json"}
        # Should not match if cases differ
        result = find_sidecar_for_media("photos/test.jpg", available)
        assert result is None


class TestDateAnalysis:
    """Tests for DateAnalysis dataclass."""
    
    def test_sidecar_coverage(self):
        """Test sidecar_coverage property."""
        analysis = DateAnalysis(total_media=100, with_sidecar=75)
        assert analysis.sidecar_coverage == 75.0
    
    def test_sidecar_coverage_zero_media(self):
        """Test sidecar_coverage with zero total."""
        analysis = DateAnalysis(total_media=0, with_sidecar=0)
        assert analysis.sidecar_coverage == 0.0
    
    def test_date_recovery_rate(self):
        """Test date_recovery_rate property."""
        analysis = DateAnalysis(total_media=100, with_photo_taken_time=80, with_creation_time=90)
        assert analysis.date_recovery_rate == 90.0  # max of the two
    
    def test_to_dict(self):
        """Test serialization."""
        analysis = DateAnalysis(
            total_media=100,
            with_sidecar=80,
            with_photo_taken_time=75,
            with_creation_time=80,
            with_geo=50,
        )
        
        d = analysis.to_dict()
        assert d['total_media'] == 100
        assert d['sidecar_coverage'] == 80.0
        assert d['date_recovery_rate'] == 80.0
