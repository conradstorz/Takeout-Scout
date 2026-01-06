"""
Data models for Takeout Scout.

This module contains all dataclasses used throughout the application
for representing photos, files, media pairs, and archive summaries.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional


@dataclass
class PhotoMetadata:
    """Container for photo EXIF metadata.
    
    Attributes:
        has_exif: Whether the photo has any EXIF data
        has_gps: Whether GPS coordinates are present
        has_datetime: Whether original capture datetime is present
        camera_make: Camera manufacturer (e.g., "Apple", "Canon")
        camera_model: Camera model (e.g., "iPhone 13 Pro")
        datetime_original: Original capture date/time string
        gps_latitude: GPS latitude in decimal degrees (if parsed)
        gps_longitude: GPS longitude in decimal degrees (if parsed)
        width: Image width in pixels
        height: Image height in pixels
    """
    has_exif: bool = False
    has_gps: bool = False
    has_datetime: bool = False
    camera_make: Optional[str] = None
    camera_model: Optional[str] = None
    datetime_original: Optional[str] = None
    gps_latitude: Optional[float] = None
    gps_longitude: Optional[float] = None
    width: Optional[int] = None
    height: Optional[int] = None
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        return {
            'has_exif': self.has_exif,
            'has_gps': self.has_gps,
            'has_datetime': self.has_datetime,
            'camera_make': self.camera_make,
            'camera_model': self.camera_model,
            'datetime_original': self.datetime_original,
            'gps_latitude': self.gps_latitude,
            'gps_longitude': self.gps_longitude,
            'width': self.width,
            'height': self.height,
        }


@dataclass
class FileDetails:
    """Detailed information about a file within a takeout.
    
    Attributes:
        path: Relative path within the archive
        size: File size in bytes
        file_type: Category ('photo', 'video', 'json', 'other')
        extension: File extension including dot (e.g., '.jpg')
        metadata: Optional EXIF or other metadata dictionary
    """
    path: str
    size: int
    file_type: str  # 'photo', 'video', 'json', 'other'
    extension: str
    metadata: Optional[Dict[str, Any]] = None
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        return {
            'path': self.path,
            'size': self.size,
            'file_type': self.file_type,
            'extension': self.extension,
            'metadata': self.metadata,
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'FileDetails':
        """Create FileDetails from dictionary."""
        return cls(
            path=data['path'],
            size=data['size'],
            file_type=data['file_type'],
            extension=data['extension'],
            metadata=data.get('metadata'),
        )


@dataclass
class MediaPair:
    """Represents paired files that form a single media entity.
    
    Examples: Apple Live Photos (HEIC+MOV), photo+JSON sidecar pairs.
    
    Attributes:
        pair_type: Type of pair ('live_photo', 'motion_photo', 'photo_json')
        photo_path: Path to the photo component
        companion_path: Path to the companion (video or JSON)
        photo_size: Size of photo in bytes
        companion_size: Size of companion in bytes
        base_name: Common base name without extension
    """
    pair_type: str  # 'live_photo', 'motion_photo', 'photo_json'
    photo_path: str
    companion_path: str
    photo_size: int
    companion_size: int
    base_name: str
    
    @property
    def total_size(self) -> int:
        """Total size of both files in the pair."""
        return self.photo_size + self.companion_size
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        return {
            'pair_type': self.pair_type,
            'photo_path': self.photo_path,
            'companion_path': self.companion_path,
            'photo_size': self.photo_size,
            'companion_size': self.companion_size,
            'base_name': self.base_name,
            'total_size': self.total_size,
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'MediaPair':
        """Create MediaPair from dictionary."""
        return cls(
            pair_type=data['pair_type'],
            photo_path=data['photo_path'],
            companion_path=data['companion_path'],
            photo_size=data['photo_size'],
            companion_size=data['companion_size'],
            base_name=data['base_name'],
        )


@dataclass
class TakeoutDiscovery:
    """Complete tracking information for a discovered takeout.
    
    This dataclass stores comprehensive information about a scanned
    Google Takeout archive or directory, including file statistics,
    metadata summaries, and detailed file listings.
    
    Attributes:
        source_path: Absolute path to the source archive/directory
        source_type: Type of source ('zip', 'tgz', 'directory')
        first_discovered: ISO format datetime of first scan
        last_scanned: ISO format datetime of most recent scan
        parts_group: Group name for multi-part archives
        service_guess: Detected Google service (e.g., "Google Photos")
        file_count: Total number of files
        photos: Number of photo files
        videos: Number of video files
        json_sidecars: Number of JSON sidecar files
        other: Number of other files
        compressed_size: Total size in bytes
        photos_with_exif: Photos with EXIF data
        photos_with_gps: Photos with GPS coordinates
        photos_with_datetime: Photos with datetime info
        photos_checked: Total photos analyzed for metadata
        live_photos: Count of Apple Live Photo pairs
        motion_photos: Count of Motion Photo files
        photo_json_pairs: Count of photo+JSON pairs
        scan_count: Number of times this source has been scanned
        file_details: List of FileDetails dictionaries
        media_pairs: List of MediaPair dictionaries
        notes: User notes
    """
    source_path: str
    source_type: str
    first_discovered: str
    last_scanned: str
    parts_group: str
    service_guess: str
    file_count: int
    photos: int
    videos: int
    json_sidecars: int
    other: int
    compressed_size: int
    photos_with_exif: int = 0
    photos_with_gps: int = 0
    photos_with_datetime: int = 0
    photos_checked: int = 0
    live_photos: int = 0
    motion_photos: int = 0
    photo_json_pairs: int = 0
    scan_count: int = 1
    file_details: List[Dict[str, Any]] = field(default_factory=list)
    media_pairs: List[Dict[str, Any]] = field(default_factory=list)
    notes: str = ''
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        return {
            'source_path': self.source_path,
            'source_type': self.source_type,
            'first_discovered': self.first_discovered,
            'last_scanned': self.last_scanned,
            'parts_group': self.parts_group,
            'service_guess': self.service_guess,
            'file_count': self.file_count,
            'photos': self.photos,
            'videos': self.videos,
            'json_sidecars': self.json_sidecars,
            'other': self.other,
            'compressed_size': self.compressed_size,
            'photos_with_exif': self.photos_with_exif,
            'photos_with_gps': self.photos_with_gps,
            'photos_with_datetime': self.photos_with_datetime,
            'photos_checked': self.photos_checked,
            'live_photos': self.live_photos,
            'motion_photos': self.motion_photos,
            'photo_json_pairs': self.photo_json_pairs,
            'scan_count': self.scan_count,
            'file_details': self.file_details,
            'media_pairs': self.media_pairs,
            'notes': self.notes,
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'TakeoutDiscovery':
        """Create TakeoutDiscovery from dictionary with defaults for missing fields."""
        return cls(
            source_path=data['source_path'],
            source_type=data['source_type'],
            first_discovered=data['first_discovered'],
            last_scanned=data['last_scanned'],
            parts_group=data['parts_group'],
            service_guess=data['service_guess'],
            file_count=data['file_count'],
            photos=data['photos'],
            videos=data['videos'],
            json_sidecars=data['json_sidecars'],
            other=data['other'],
            compressed_size=data['compressed_size'],
            photos_with_exif=data.get('photos_with_exif', 0),
            photos_with_gps=data.get('photos_with_gps', 0),
            photos_with_datetime=data.get('photos_with_datetime', 0),
            photos_checked=data.get('photos_checked', 0),
            live_photos=data.get('live_photos', 0),
            motion_photos=data.get('motion_photos', 0),
            photo_json_pairs=data.get('photo_json_pairs', 0),
            scan_count=data.get('scan_count', 1),
            file_details=data.get('file_details', []),
            media_pairs=data.get('media_pairs', []),
            notes=data.get('notes', ''),
        )


@dataclass
class ArchiveSummary:
    """Summary information about a scanned archive.
    
    This is the primary return type from scan_archive() and scan_directory(),
    containing aggregate statistics suitable for display in the UI.
    
    Attributes:
        path: Path to the archive or directory
        parts_group: Group name for multi-part archives
        service_guess: Detected Google service
        file_count: Total file count
        photos: Photo count
        videos: Video count
        json_sidecars: JSON sidecar count
        other: Other file count
        compressed_size: Total size in bytes
        photos_with_exif: Photos with EXIF data
        photos_with_gps: Photos with GPS coordinates
        photos_with_datetime: Photos with datetime info
        photos_checked: Photos analyzed for metadata
        live_photos: Live Photo pair count
        motion_photos: Motion Photo count
        photo_json_pairs: Photo+JSON pair count
    """
    path: str
    parts_group: str
    service_guess: str
    file_count: int
    photos: int
    videos: int
    json_sidecars: int
    other: int
    compressed_size: int
    photos_with_exif: int = 0
    photos_with_gps: int = 0
    photos_with_datetime: int = 0
    photos_checked: int = 0
    live_photos: int = 0
    motion_photos: int = 0
    photo_json_pairs: int = 0

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for display (with human-readable size)."""
        from takeout_scout.utils import human_size
        return {
            'Path': self.path,
            'Parts Group': self.parts_group,
            'Service': self.service_guess,
            'Files': self.file_count,
            'Photos': self.photos,
            'Videos': self.videos,
            'JSON': self.json_sidecars,
            'Other': self.other,
            'Size': human_size(self.compressed_size),
            'w/EXIF': self.photos_with_exif,
            'w/GPS': self.photos_with_gps,
            'w/Date': self.photos_with_datetime,
            'Checked': self.photos_checked,
            'Live': self.live_photos,
            'Motion': self.motion_photos,
            'P+J': self.photo_json_pairs,
        }
    
    def to_row(self) -> List[str]:
        """Convert to list of strings for table display."""
        from takeout_scout.utils import human_size
        return [
            self.path,
            self.parts_group,
            self.service_guess,
            str(self.file_count),
            str(self.photos),
            str(self.videos),
            str(self.json_sidecars),
            str(self.other),
            human_size(self.compressed_size),
            str(self.photos_with_exif),
            str(self.photos_with_gps),
            str(self.photos_with_datetime),
            str(self.photos_checked),
            str(self.live_photos),
            str(self.motion_photos),
            str(self.photo_json_pairs),
        ]
