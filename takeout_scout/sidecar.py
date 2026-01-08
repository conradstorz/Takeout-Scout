"""
Google Takeout JSON sidecar parsing.

Google Photos exports JSON metadata files alongside each media file.
These contain authoritative timestamps that survive ZIP/TAR compression
and extraction, unlike filesystem dates which are often lost.

JSON sidecar structure (Google Photos):
{
  "title": "IMG_1234.jpg",
  "description": "",
  "imageViews": "0",
  "creationTime": {
    "timestamp": "1563198245",
    "formatted": "Jul 15, 2019, 2:04:05 PM UTC"
  },
  "photoTakenTime": {
    "timestamp": "1563198005",
    "formatted": "Jul 15, 2019, 2:00:05 PM UTC"
  },
  "geoData": {
    "latitude": 40.7128,
    "longitude": -74.0060,
    "altitude": 10.0,
    "latitudeSpan": 0.0,
    "longitudeSpan": 0.0
  },
  "geoDataExif": { ... },
  "people": [],
  "url": "https://photos.google.com/photo/..."
}
"""
from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, BinaryIO
import zipfile
import tarfile

from .logging import logger


@dataclass
class GeoLocation:
    """Geographic coordinates from sidecar.
    
    Attributes:
        latitude: Decimal degrees (-90 to 90)
        longitude: Decimal degrees (-180 to 180)
        altitude: Meters above sea level (optional)
    """
    latitude: float
    longitude: float
    altitude: Optional[float] = None
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            'latitude': self.latitude,
            'longitude': self.longitude,
            'altitude': self.altitude,
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'GeoLocation':
        return cls(
            latitude=data['latitude'],
            longitude=data['longitude'],
            altitude=data.get('altitude'),
        )


@dataclass
class SidecarMetadata:
    """Metadata extracted from a Google Photos JSON sidecar.
    
    Attributes:
        title: Original filename/title
        description: User-added description
        photo_taken_time: When the photo was actually captured (authoritative)
        creation_time: When uploaded to Google Photos
        modification_time: Last edit time
        geo_location: GPS coordinates if available
        geo_location_exif: GPS from EXIF (may differ from geo_location)
        people: List of identified people
        url: Google Photos URL
        raw_data: Original JSON for future parsing needs
    """
    title: Optional[str] = None
    description: Optional[str] = None
    photo_taken_time: Optional[datetime] = None
    creation_time: Optional[datetime] = None
    modification_time: Optional[datetime] = None
    geo_location: Optional[GeoLocation] = None
    geo_location_exif: Optional[GeoLocation] = None
    people: List[str] = field(default_factory=list)
    url: Optional[str] = None
    raw_data: Optional[Dict[str, Any]] = None
    
    @property
    def has_geo(self) -> bool:
        """Check if any geo data is present."""
        return self.geo_location is not None or self.geo_location_exif is not None
    
    @property
    def best_timestamp(self) -> Optional[datetime]:
        """Get the most authoritative timestamp available.
        
        Priority: photo_taken_time > creation_time > modification_time
        """
        return self.photo_taken_time or self.creation_time or self.modification_time
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        return {
            'title': self.title,
            'description': self.description,
            'photo_taken_time': self.photo_taken_time.isoformat() if self.photo_taken_time else None,
            'creation_time': self.creation_time.isoformat() if self.creation_time else None,
            'modification_time': self.modification_time.isoformat() if self.modification_time else None,
            'geo_location': self.geo_location.to_dict() if self.geo_location else None,
            'geo_location_exif': self.geo_location_exif.to_dict() if self.geo_location_exif else None,
            'people': self.people,
            'url': self.url,
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'SidecarMetadata':
        """Create from dictionary."""
        return cls(
            title=data.get('title'),
            description=data.get('description'),
            photo_taken_time=datetime.fromisoformat(data['photo_taken_time']) if data.get('photo_taken_time') else None,
            creation_time=datetime.fromisoformat(data['creation_time']) if data.get('creation_time') else None,
            modification_time=datetime.fromisoformat(data['modification_time']) if data.get('modification_time') else None,
            geo_location=GeoLocation.from_dict(data['geo_location']) if data.get('geo_location') else None,
            geo_location_exif=GeoLocation.from_dict(data['geo_location_exif']) if data.get('geo_location_exif') else None,
            people=data.get('people', []),
            url=data.get('url'),
        )


def _parse_timestamp(time_obj: Optional[Dict[str, Any]]) -> Optional[datetime]:
    """Parse a Google Photos timestamp object.
    
    Args:
        time_obj: Dict with 'timestamp' (Unix seconds) and 'formatted' keys
        
    Returns:
        datetime in UTC, or None if parsing fails
    """
    if not time_obj:
        return None
    
    timestamp = time_obj.get('timestamp')
    if timestamp:
        try:
            # Google uses Unix timestamp as string
            ts = int(timestamp)
            return datetime.fromtimestamp(ts, tz=timezone.utc)
        except (ValueError, OSError) as e:
            logger.debug(f"Failed to parse timestamp {timestamp}: {e}")
    
    return None


def _parse_geo(geo_obj: Optional[Dict[str, Any]]) -> Optional[GeoLocation]:
    """Parse a Google Photos geo object.
    
    Args:
        geo_obj: Dict with latitude, longitude, altitude keys
        
    Returns:
        GeoLocation or None if coordinates are invalid/missing
    """
    if not geo_obj:
        return None
    
    lat = geo_obj.get('latitude', 0.0)
    lon = geo_obj.get('longitude', 0.0)
    
    # Google uses 0.0/0.0 for "no location"
    if lat == 0.0 and lon == 0.0:
        return None
    
    return GeoLocation(
        latitude=lat,
        longitude=lon,
        altitude=geo_obj.get('altitude'),
    )


def parse_sidecar(json_content: bytes) -> Optional[SidecarMetadata]:
    """Parse a Google Photos JSON sidecar file.
    
    Args:
        json_content: Raw JSON bytes
        
    Returns:
        SidecarMetadata or None if parsing fails
    """
    try:
        data = json.loads(json_content.decode('utf-8'))
    except (json.JSONDecodeError, UnicodeDecodeError) as e:
        logger.debug(f"Failed to parse JSON sidecar: {e}")
        return None
    
    # Extract people names
    people = []
    for person in data.get('people', []):
        name = person.get('name')
        if name:
            people.append(name)
    
    return SidecarMetadata(
        title=data.get('title'),
        description=data.get('description'),
        photo_taken_time=_parse_timestamp(data.get('photoTakenTime')),
        creation_time=_parse_timestamp(data.get('creationTime')),
        modification_time=_parse_timestamp(data.get('modificationTime')),
        geo_location=_parse_geo(data.get('geoData')),
        geo_location_exif=_parse_geo(data.get('geoDataExif')),
        people=people,
        url=data.get('url'),
        raw_data=data,
    )


def parse_sidecar_from_file(file_path: Path) -> Optional[SidecarMetadata]:
    """Parse a JSON sidecar from a file on disk.
    
    Args:
        file_path: Path to the .json file
        
    Returns:
        SidecarMetadata or None if parsing fails
    """
    try:
        content = file_path.read_bytes()
        return parse_sidecar(content)
    except OSError as e:
        logger.warning(f"Failed to read sidecar {file_path}: {e}")
        return None


def parse_sidecar_from_zip(
    zip_file: zipfile.ZipFile,
    member_name: str
) -> Optional[SidecarMetadata]:
    """Parse a JSON sidecar from within a ZIP archive.
    
    Args:
        zip_file: Open ZipFile object
        member_name: Path within the ZIP
        
    Returns:
        SidecarMetadata or None if parsing fails
    """
    try:
        content = zip_file.read(member_name)
        return parse_sidecar(content)
    except (KeyError, zipfile.BadZipFile) as e:
        logger.debug(f"Failed to read sidecar from ZIP: {e}")
        return None


def parse_sidecar_from_tar(
    tar_file: tarfile.TarFile,
    member: tarfile.TarInfo
) -> Optional[SidecarMetadata]:
    """Parse a JSON sidecar from within a TAR archive.
    
    Args:
        tar_file: Open TarFile object
        member: TarInfo for the JSON file
        
    Returns:
        SidecarMetadata or None if parsing fails
    """
    try:
        f = tar_file.extractfile(member)
        if f is None:
            return None
        content = f.read()
        return parse_sidecar(content)
    except (tarfile.TarError, OSError) as e:
        logger.debug(f"Failed to read sidecar from TAR: {e}")
        return None


def find_sidecar_for_media(media_path: str, available_paths: set) -> Optional[str]:
    """Find the JSON sidecar path for a media file.
    
    Google Photos creates sidecars with naming patterns like:
    - photo.jpg -> photo.jpg.json
    - photo.JPG -> photo.JPG.json  
    - video.mp4 -> video.mp4.json
    - photo(1).jpg -> photo(1).jpg.json
    
    Args:
        media_path: Path to the media file
        available_paths: Set of all available file paths
        
    Returns:
        Path to the sidecar if found, None otherwise
    """
    # Primary pattern: append .json
    sidecar_path = f"{media_path}.json"
    if sidecar_path in available_paths:
        return sidecar_path
    
    # Some edge cases with truncated names
    # e.g., "very_long_filename_that_got_trunca.jpg.json"
    base = Path(media_path).stem
    parent = str(Path(media_path).parent)
    
    # Look for any JSON in same folder starting with base name
    for path in available_paths:
        if path.endswith('.json') and str(Path(path).parent) == parent:
            json_stem = Path(path).stem  # e.g., "photo.jpg" from "photo.jpg.json"
            if json_stem == Path(media_path).name:
                return path
    
    return None


@dataclass
class DateAnalysis:
    """Analysis of date availability across a takeout.
    
    Attributes:
        total_media: Total number of media files
        with_sidecar: Media files with JSON sidecars
        with_photo_taken_time: Sidecars with photoTakenTime
        with_creation_time: Sidecars with creationTime
        with_geo: Sidecars with geo coordinates
        date_range: Tuple of (earliest, latest) dates if available
        missing_dates: List of media paths without recoverable dates
    """
    total_media: int = 0
    with_sidecar: int = 0
    with_photo_taken_time: int = 0
    with_creation_time: int = 0
    with_geo: int = 0
    date_range: Optional[Tuple[datetime, datetime]] = None
    missing_dates: List[str] = field(default_factory=list)
    
    @property
    def sidecar_coverage(self) -> float:
        """Percentage of media files with sidecars."""
        if self.total_media == 0:
            return 0.0
        return (self.with_sidecar / self.total_media) * 100
    
    @property
    def date_recovery_rate(self) -> float:
        """Percentage of media files with recoverable dates."""
        if self.total_media == 0:
            return 0.0
        recoverable = max(self.with_photo_taken_time, self.with_creation_time)
        return (recoverable / self.total_media) * 100
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            'total_media': self.total_media,
            'with_sidecar': self.with_sidecar,
            'with_photo_taken_time': self.with_photo_taken_time,
            'with_creation_time': self.with_creation_time,
            'with_geo': self.with_geo,
            'sidecar_coverage': self.sidecar_coverage,
            'date_recovery_rate': self.date_recovery_rate,
            'date_range': [
                self.date_range[0].isoformat(),
                self.date_range[1].isoformat()
            ] if self.date_range else None,
            'missing_dates_count': len(self.missing_dates),
        }
