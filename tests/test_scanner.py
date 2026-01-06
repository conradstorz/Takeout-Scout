"""
Tests for takeout_scout.scanner module.
"""
import pytest
import tempfile
import zipfile
import tarfile
import os
from pathlib import Path

from takeout_scout.scanner import (
    scan_archive,
    scan_directory,
    find_archives_and_dirs,
    guess_service_from_members,
    derive_parts_group,
    tally_exts,
)


class TestGuessServiceFromMembers:
    """Tests for guess_service_from_members function."""
    
    def test_google_photos(self):
        """Test Google Photos detection."""
        members = [
            'Takeout/Google Photos/2024/photo.jpg',
            'Takeout/Google Photos/2024/video.mp4',
        ]
        result = guess_service_from_members(members)
        assert result == 'Google Photos'
    
    def test_google_drive(self):
        """Test Google Drive detection."""
        members = [
            'Takeout/Google Drive/My Files/doc.pdf',
        ]
        result = guess_service_from_members(members)
        assert result == 'Google Drive'
    
    def test_unknown_service(self):
        """Test unknown service."""
        members = ['random/file.txt', 'another/data.bin']
        result = guess_service_from_members(members)
        assert result == 'Unknown'


class TestDerivePartsGroup:
    """Tests for derive_parts_group function."""
    
    def test_multipart_archive(self):
        """Test multi-part archive grouping."""
        path = Path('takeout-20240101T120000Z-001.zip')
        result = derive_parts_group(path)
        assert result == 'takeout-20240101T120000Z'
    
    def test_single_archive(self):
        """Test single archive (no parts)."""
        path = Path('takeout.zip')
        result = derive_parts_group(path)
        assert result == 'takeout'
    
    def test_tgz_extension(self):
        """Test .tgz extension handling."""
        path = Path('backup-001.tgz')
        result = derive_parts_group(path)
        assert result == 'backup'


class TestTallyExts:
    """Tests for tally_exts function."""
    
    def test_empty_list(self):
        """Test with empty list."""
        photos, videos, jsons, other = tally_exts([])
        assert photos == 0
        assert videos == 0
        assert jsons == 0
        assert other == 0
    
    def test_mixed_files(self):
        """Test with mixed file types."""
        members = [
            'photo1.jpg',
            'photo2.png',
            'video.mp4',
            'data.json',
            'readme.txt',
        ]
        photos, videos, jsons, other = tally_exts(members)
        assert photos == 2
        assert videos == 1
        assert jsons == 1
        assert other == 1
    
    def test_case_insensitive(self):
        """Test case insensitivity."""
        members = ['PHOTO.JPG', 'Video.MP4', 'data.JSON']
        photos, videos, jsons, other = tally_exts(members)
        assert photos == 1
        assert videos == 1
        assert jsons == 1


class TestScanArchive:
    """Tests for scan_archive function."""
    
    def test_scan_zip_archive(self, tmp_path):
        """Test scanning a ZIP archive."""
        # Create a test ZIP file
        zip_path = tmp_path / "test.zip"
        with zipfile.ZipFile(zip_path, 'w') as zf:
            zf.writestr('photo.jpg', b'fake jpg data')
            zf.writestr('video.mp4', b'fake mp4 data')
            zf.writestr('meta.json', b'{}')
        
        summary = scan_archive(zip_path, save_discovery=False)
        
        assert summary.file_count == 3
        assert summary.photos == 1
        assert summary.videos == 1
        assert summary.json_sidecars == 1
        assert summary.path == str(zip_path)
    
    def test_scan_tgz_archive(self, tmp_path):
        """Test scanning a TGZ archive."""
        # Create a test TGZ file
        tgz_path = tmp_path / "test.tgz"
        with tarfile.open(tgz_path, 'w:gz') as tf:
            # Add a file to the tar
            import io
            data = b'fake image data'
            info = tarfile.TarInfo(name='image.jpg')
            info.size = len(data)
            tf.addfile(info, io.BytesIO(data))
        
        summary = scan_archive(tgz_path, save_discovery=False)
        
        assert summary.file_count == 1
        assert summary.photos == 1
    
    def test_scan_invalid_archive(self, tmp_path):
        """Test scanning an invalid archive."""
        bad_path = tmp_path / "bad.zip"
        bad_path.write_bytes(b'not a zip file')
        
        summary = scan_archive(bad_path, save_discovery=False)
        
        assert summary.service_guess == '(error)'
        assert summary.file_count == 0


class TestScanDirectory:
    """Tests for scan_directory function."""
    
    def test_scan_empty_directory(self, tmp_path):
        """Test scanning an empty directory."""
        summary = scan_directory(tmp_path, save_discovery=False)
        
        assert summary.file_count == 0
        assert summary.path == str(tmp_path)
    
    def test_scan_directory_with_files(self, tmp_path):
        """Test scanning a directory with files."""
        # Create test files
        (tmp_path / "photo1.jpg").write_bytes(b'fake jpg')
        (tmp_path / "photo2.png").write_bytes(b'fake png')
        (tmp_path / "video.mp4").write_bytes(b'fake mp4')
        (tmp_path / "readme.txt").write_bytes(b'text')
        
        summary = scan_directory(tmp_path, save_discovery=False)
        
        assert summary.file_count == 4
        assert summary.photos == 2
        assert summary.videos == 1
        assert summary.other == 1
    
    def test_scan_nested_directory(self, tmp_path):
        """Test scanning a directory with subdirectories."""
        subdir = tmp_path / "subdir"
        subdir.mkdir()
        
        (tmp_path / "photo.jpg").write_bytes(b'data')
        (subdir / "nested.png").write_bytes(b'data')
        
        summary = scan_directory(tmp_path, save_discovery=False)
        
        assert summary.file_count == 2
        assert summary.photos == 2


class TestFindArchivesAndDirs:
    """Tests for find_archives_and_dirs function."""
    
    def test_find_zip_files(self, tmp_path):
        """Test finding ZIP files."""
        # Create test ZIP files
        zip1 = tmp_path / "archive1.zip"
        zip2 = tmp_path / "archive2.zip"
        with zipfile.ZipFile(zip1, 'w') as zf:
            zf.writestr('test.txt', 'data')
        with zipfile.ZipFile(zip2, 'w') as zf:
            zf.writestr('test.txt', 'data')
        
        archives, dirs = find_archives_and_dirs(tmp_path)
        
        assert len(archives) == 2
        assert zip1 in archives
        assert zip2 in archives
    
    def test_find_tgz_files(self, tmp_path):
        """Test finding TGZ files."""
        tgz = tmp_path / "archive.tgz"
        with tarfile.open(tgz, 'w:gz') as tf:
            import io
            info = tarfile.TarInfo('file.txt')
            info.size = 0
            tf.addfile(info)
        
        archives, dirs = find_archives_and_dirs(tmp_path)
        
        assert len(archives) == 1
        assert tgz in archives
    
    def test_find_takeout_directory(self, tmp_path):
        """Test finding Takeout directories."""
        takeout_dir = tmp_path / "Takeout"
        takeout_dir.mkdir()
        photos_dir = takeout_dir / "Google Photos"
        photos_dir.mkdir()
        
        archives, dirs = find_archives_and_dirs(tmp_path)
        
        # Should find the Takeout directory
        assert len(dirs) >= 1
    
    def test_empty_directory(self, tmp_path):
        """Test with empty directory."""
        archives, dirs = find_archives_and_dirs(tmp_path)
        
        assert len(archives) == 0
        assert len(dirs) == 0
