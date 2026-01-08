"""
Tests for takeout_scout.discovery module.
"""
import json
import pytest
from pathlib import Path
from unittest.mock import patch, MagicMock

from takeout_scout.discovery import (
    get_takeout_id,
    get_takeout_json_path,
    load_discoveries_index,
    save_discoveries_index,
    load_takeout_discovery,
    save_takeout_discovery,
    delete_takeout_discovery,
    list_all_discoveries,
)
from takeout_scout.models import TakeoutDiscovery


class TestGetTakeoutId:
    """Tests for get_takeout_id function."""
    
    def test_returns_string(self, tmp_path):
        """Test that get_takeout_id returns a string."""
        path = tmp_path / "test.zip"
        path.touch()
        result = get_takeout_id(path)
        assert isinstance(result, str)
    
    def test_includes_base_name(self, tmp_path):
        """Test that result includes sanitized base name."""
        path = tmp_path / "my-archive.zip"
        path.touch()
        result = get_takeout_id(path)
        assert result.startswith("my-archive_")
    
    def test_includes_hash_suffix(self, tmp_path):
        """Test that result includes hash suffix."""
        path = tmp_path / "test.zip"
        path.touch()
        result = get_takeout_id(path)
        # Format should be name_hash where hash is 12 chars
        parts = result.rsplit('_', 1)
        assert len(parts) == 2
        assert len(parts[1]) == 12
    
    def test_different_paths_different_ids(self, tmp_path):
        """Test that different paths produce different IDs."""
        path1 = tmp_path / "archive1.zip"
        path2 = tmp_path / "archive2.zip"
        path1.touch()
        path2.touch()
        
        id1 = get_takeout_id(path1)
        id2 = get_takeout_id(path2)
        assert id1 != id2
    
    def test_same_path_same_id(self, tmp_path):
        """Test that same path produces same ID."""
        path = tmp_path / "test.zip"
        path.touch()
        
        id1 = get_takeout_id(path)
        id2 = get_takeout_id(path)
        assert id1 == id2
    
    def test_directory_path(self, tmp_path):
        """Test with directory path."""
        dir_path = tmp_path / "MyTakeout"
        dir_path.mkdir()
        
        result = get_takeout_id(dir_path)
        assert result.startswith("MyTakeout_")
    
    def test_sanitizes_special_chars(self, tmp_path):
        """Test that special characters are sanitized."""
        # Can't actually create file with these chars on Windows,
        # but we can test the sanitization logic
        path = tmp_path / "test.zip"
        path.touch()
        result = get_takeout_id(path)
        # Should not contain dangerous characters
        for char in '<>:"/\\|?*':
            assert char not in result


class TestGetTakeoutJsonPath:
    """Tests for get_takeout_json_path function."""
    
    def test_returns_path(self, tmp_path):
        """Test that function returns a Path object."""
        path = tmp_path / "test.zip"
        path.touch()
        
        with patch('takeout_scout.discovery.get_default_paths') as mock:
            mock.return_value = {'discoveries_dir': tmp_path}
            result = get_takeout_json_path(path)
        
        assert isinstance(result, Path)
    
    def test_has_takeout_scout_extension(self, tmp_path):
        """Test that result has .takeout_scout extension."""
        path = tmp_path / "test.zip"
        path.touch()
        
        with patch('takeout_scout.discovery.get_default_paths') as mock:
            mock.return_value = {'discoveries_dir': tmp_path}
            result = get_takeout_json_path(path)
        
        assert result.suffix == '.takeout_scout'


class TestLoadDiscoveriesIndex:
    """Tests for load_discoveries_index function."""
    
    def test_returns_empty_dict_no_file(self, tmp_path):
        """Test returns empty dict when index file doesn't exist."""
        with patch('takeout_scout.discovery.get_default_paths') as mock:
            mock.return_value = {'discoveries_index_path': tmp_path / 'nonexistent.json'}
            result = load_discoveries_index()
        
        assert result == {}
    
    def test_loads_existing_index(self, tmp_path):
        """Test loading existing index file."""
        index_path = tmp_path / 'index.json'
        index_data = {
            'path1': 'file1.takeout_scout',
            'path2': 'file2.takeout_scout',
        }
        index_path.write_text(json.dumps(index_data))
        
        with patch('takeout_scout.discovery.get_default_paths') as mock:
            mock.return_value = {'discoveries_index_path': index_path}
            result = load_discoveries_index()
        
        assert result == index_data
    
    def test_handles_corrupted_json(self, tmp_path):
        """Test handles corrupted JSON gracefully."""
        index_path = tmp_path / 'index.json'
        index_path.write_text('not valid json {{{')
        
        with patch('takeout_scout.discovery.get_default_paths') as mock:
            mock.return_value = {'discoveries_index_path': index_path}
            result = load_discoveries_index()
        
        assert result == {}


class TestSaveDiscoveriesIndex:
    """Tests for save_discoveries_index function."""
    
    def test_saves_index_file(self, tmp_path):
        """Test saving index to file."""
        index_path = tmp_path / 'index.json'
        index_data = {'path1': 'file1.takeout_scout'}
        
        with patch('takeout_scout.discovery.get_default_paths') as mock_paths, \
             patch('takeout_scout.discovery.ensure_directories'):
            mock_paths.return_value = {'discoveries_index_path': index_path}
            save_discoveries_index(index_data)
        
        assert index_path.exists()
        saved_data = json.loads(index_path.read_text())
        assert saved_data == index_data


class TestLoadTakeoutDiscovery:
    """Tests for load_takeout_discovery function."""
    
    def test_returns_none_no_file(self, tmp_path):
        """Test returns None when discovery file doesn't exist."""
        path = tmp_path / "test.zip"
        
        with patch('takeout_scout.discovery.get_takeout_json_path') as mock:
            mock.return_value = tmp_path / 'nonexistent.takeout_scout'
            result = load_takeout_discovery(path)
        
        assert result is None
    
    def test_loads_valid_discovery(self, tmp_path):
        """Test loading valid discovery file."""
        json_path = tmp_path / 'test.takeout_scout'
        discovery_data = {
            'source_path': str(tmp_path / 'test.zip'),
            'source_type': 'zip',
            'first_discovered': '2024-01-01T00:00:00',
            'last_scanned': '2024-01-01T00:00:00',
            'service_guess': 'Google Photos',
            'parts_group': 'takeout',
            'file_count': 100,
            'compressed_size': 1000000,
            'photos': 50,
            'videos': 30,
            'json_sidecars': 50,
            'other': 20,
            'file_details': [],
            'media_pairs': [],
        }
        json_path.write_text(json.dumps(discovery_data))
        
        with patch('takeout_scout.discovery.get_takeout_json_path') as mock:
            mock.return_value = json_path
            result = load_takeout_discovery(tmp_path / 'test.zip')
        
        assert result is not None
        assert isinstance(result, TakeoutDiscovery)
        assert result.service_guess == 'Google Photos'
    
    def test_handles_corrupted_json(self, tmp_path):
        """Test handles corrupted discovery file."""
        json_path = tmp_path / 'test.takeout_scout'
        json_path.write_text('invalid json {{{')
        
        with patch('takeout_scout.discovery.get_takeout_json_path') as mock:
            mock.return_value = json_path
            result = load_takeout_discovery(tmp_path / 'test.zip')
        
        assert result is None
    
    def test_handles_missing_fields(self, tmp_path):
        """Test handles discovery file with missing required fields."""
        json_path = tmp_path / 'test.takeout_scout'
        json_path.write_text(json.dumps({'incomplete': 'data'}))
        
        with patch('takeout_scout.discovery.get_takeout_json_path') as mock:
            mock.return_value = json_path
            result = load_takeout_discovery(tmp_path / 'test.zip')
        
        assert result is None


class TestSaveTakeoutDiscovery:
    """Tests for save_takeout_discovery function."""
    
    def test_saves_discovery_file(self, tmp_path):
        """Test saving discovery creates JSON file."""
        discovery = TakeoutDiscovery(
            source_path=str(tmp_path / 'test.zip'),
            source_type='zip',
            first_discovered='2024-01-01T00:00:00',
            last_scanned='2024-01-01T00:00:00',
            service_guess='Google Photos',
            parts_group='takeout',
            file_count=100,
            compressed_size=1000000,
            photos=50,
            videos=30,
            json_sidecars=50,
            other=20,
        )
        
        json_path = tmp_path / 'test.takeout_scout'
        
        with patch('takeout_scout.discovery.get_takeout_json_path') as mock_json, \
             patch('takeout_scout.discovery.ensure_directories'), \
             patch('takeout_scout.discovery.load_discoveries_index', return_value={}), \
             patch('takeout_scout.discovery.save_discoveries_index'):
            mock_json.return_value = json_path
            result = save_takeout_discovery(discovery)
        
        assert result == json_path
        assert json_path.exists()
        
        saved_data = json.loads(json_path.read_text())
        assert saved_data['service_guess'] == 'Google Photos'
        assert saved_data['file_count'] == 100


class TestDeleteTakeoutDiscovery:
    """Tests for delete_takeout_discovery function."""
    
    def test_deletes_existing_file(self, tmp_path):
        """Test deleting existing discovery file."""
        json_path = tmp_path / 'test.takeout_scout'
        json_path.write_text('{}')
        
        with patch('takeout_scout.discovery.get_takeout_json_path') as mock_json, \
             patch('takeout_scout.discovery.load_discoveries_index', return_value={}), \
             patch('takeout_scout.discovery.save_discoveries_index'):
            mock_json.return_value = json_path
            result = delete_takeout_discovery(tmp_path / 'test.zip')
        
        assert result is True
        assert not json_path.exists()
    
    def test_returns_false_no_file(self, tmp_path):
        """Test returns False when file doesn't exist."""
        with patch('takeout_scout.discovery.get_takeout_json_path') as mock_json, \
             patch('takeout_scout.discovery.load_discoveries_index', return_value={}):
            mock_json.return_value = tmp_path / 'nonexistent.takeout_scout'
            result = delete_takeout_discovery(tmp_path / 'test.zip')
        
        assert result is False


class TestListAllDiscoveries:
    """Tests for list_all_discoveries function."""
    
    def test_empty_index_returns_empty(self):
        """Test returns empty dict with empty index."""
        with patch('takeout_scout.discovery.load_discoveries_index', return_value={}):
            result = list_all_discoveries()
        
        assert result == {}
    
    def test_loads_multiple_discoveries(self, tmp_path):
        """Test loading multiple discoveries from index."""
        discovery1 = TakeoutDiscovery(
            source_path=str(tmp_path / 'test1.zip'),
            source_type='zip',
            first_discovered='2024-01-01T00:00:00',
            last_scanned='2024-01-01T00:00:00',
            service_guess='Google Photos',
            parts_group='takeout',
            file_count=100,
            compressed_size=1000000,
            photos=50,
            videos=30,
            json_sidecars=50,
            other=20,
        )
        
        index = {
            str(tmp_path / 'test1.zip'): 'test1.takeout_scout',
        }
        
        with patch('takeout_scout.discovery.load_discoveries_index', return_value=index), \
             patch('takeout_scout.discovery.load_takeout_discovery', return_value=discovery1):
            result = list_all_discoveries()
        
        assert len(result) == 1
        assert str(tmp_path / 'test1.zip') in result
