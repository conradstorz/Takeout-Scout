#!/usr/bin/env python3
"""
Takeout Scout — Google Takeout Scanner (MVP)

A small, re-runnable GUI utility that:
  • Asks where to look for Google Takeout archives (ZIP/TGZ)
  • Scans archives non-destructively and summarizes their contents
  • Presents a prettified table (per-archive) with counts of photos/videos/JSON sidecars
  • Exports the summary to CSV
  • Logs all actions to ./logs/takeout_scout.log (rotated)

Design notes:
  • Pure standard library + loguru (optional but recommended). If loguru is not installed,
    it falls back to a minimal logger.
  • Idempotent and safe to re-run; does not modify archives.
  • Future steps (unpack, merge JSON→EXIF, dedupe, organize) can be added as additional
    buttons without changing the scan step. Each step should write to its own output
    directory so that runs are “restful.”

Author: ChatGPT for Conrad
License: MIT
"""
from __future__ import annotations

import csv
import io
import os
import re
import sys
import tarfile
import threading
import time
import zipfile
from collections import Counter, defaultdict
from dataclasses import dataclass, asdict
from datetime import datetime
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple

# --- PIL/Pillow for EXIF metadata (optional) ---------------------------------
try:
    from PIL import Image
    from PIL.ExifTags import TAGS
    _HAS_PIL = True
except ImportError:
    _HAS_PIL = False
    logger.warning('PIL/Pillow not installed; photo metadata extraction disabled.')

# --- Logging setup -----------------------------------------------------------
try:
    from loguru import logger  # type: ignore
    _HAS_LOGURU = True
except Exception:  # pragma: no cover
    import logging

    class _Shim:
        def __init__(self) -> None:
            logging.basicConfig(level=logging.INFO, format='[%(levelname)s] %(message)s')
            self._log = logging.getLogger('takeout_scout')
        def info(self, msg: str, *a, **kw):
            self._log.info(msg, *a, **kw)
        def warning(self, msg: str, *a, **kw):
            self._log.warning(msg, *a, **kw)
        def error(self, msg: str, *a, **kw):
            self._log.error(msg, *a, **kw)
        def exception(self, msg: str, *a, **kw):
            self._log.exception(msg, *a, **kw)
        def debug(self, msg: str, *a, **kw):
            self._log.debug(msg, *a, **kw)
    logger = _Shim()  # type: ignore
    _HAS_LOGURU = False

LOG_DIR = Path('logs')
LOG_DIR.mkdir(parents=True, exist_ok=True)
if _HAS_LOGURU:
    logger.add(
        LOG_DIR / 'takeout_scout.log',
        rotation='5 MB',
        retention=5,
        enqueue=True,
        backtrace=True,
        diagnose=False,
        level='INFO',
    )

import json
import hashlib

# --- State (persistent index) -----------------------------------------------
STATE_DIR = Path('state')
STATE_DIR.mkdir(parents=True, exist_ok=True)
INDEX_PATH = STATE_DIR / 'takeout_index.json'

# --- Discovery tracking -----------------------------------------------------
DISCOVERIES_DIR = Path('takeouts_discovered')
DISCOVERIES_DIR.mkdir(parents=True, exist_ok=True)
DISCOVERIES_INDEX_PATH = Path('discoveries_index.json')

# --- Simple size helpers -----------------------------------------------------

MEDIA_PHOTO_EXT = {
    # Common formats
    '.jpg', '.jpeg', '.jfif', '.png', '.heic', '.heif', '.webp', '.gif', '.bmp', 
    '.tif', '.tiff', '.avif', '.jxl',
    # RAW formats
    '.raw', '.dng', '.arw', '.cr2', '.cr3', '.nef', '.nrw', '.orf', '.rw2', 
    '.raf', '.srf', '.sr2', '.pef', '.srw',
    # Other
    '.psd', '.svg'
}
MEDIA_VIDEO_EXT = {
    '.mp4', '.mov', '.m4v', '.avi', '.mts', '.m2ts', '.wmv', '.3gp', '.mkv',
    '.webm', '.mpg', '.mpeg', '.flv', '.ogv', '.vob', '.ts', '.mxf'
}
JSON_EXT = {'.json'}

SERVICE_HINTS = {
    'Google Photos': re.compile(r'^Takeout/Google Photos/|Google Photos/', re.I),
    'Google Drive': re.compile(r'^Takeout/Google Drive/|Google Drive/', re.I),
    'Google Maps': re.compile(r'Maps|Location|Contributions', re.I),
    'Hangouts/Chat': re.compile(r'Hangouts|Chat', re.I),
    'Blogger/Album Archive': re.compile(r'Blogger|Album Archive|Picasa', re.I),
}

# --- Photo metadata helpers --------------------------------------------------

@dataclass
class PhotoMetadata:
    """Container for photo EXIF metadata."""
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


@dataclass
class FileDetails:
    """Detailed information about a file within a takeout."""
    path: str
    size: int
    file_type: str  # 'photo', 'video', 'json', 'other'
    extension: str
    metadata: Optional[Dict] = None  # EXIF or other metadata
    
    def to_dict(self) -> dict:
        return {
            'path': self.path,
            'size': self.size,
            'file_type': self.file_type,
            'extension': self.extension,
            'metadata': self.metadata,
        }


@dataclass
class MediaPair:
    """Represents paired files that form a single media entity (Live Photos, Motion Photos)."""
    pair_type: str  # 'live_photo', 'motion_photo', 'photo_json'
    photo_path: str
    companion_path: str  # video component or JSON sidecar
    photo_size: int
    companion_size: int
    base_name: str  # Common base name
    
    def to_dict(self) -> dict:
        return {
            'pair_type': self.pair_type,
            'photo_path': self.photo_path,
            'companion_path': self.companion_path,
            'photo_size': self.photo_size,
            'companion_size': self.companion_size,
            'base_name': self.base_name,
            'total_size': self.photo_size + self.companion_size,
        }


@dataclass
class TakeoutDiscovery:
    """Complete tracking information for a discovered takeout."""
    source_path: str
    source_type: str  # 'zip', 'tgz', 'directory'
    first_discovered: str  # ISO format datetime
    last_scanned: str  # ISO format datetime
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
    live_photos: int = 0  # Apple Live Photos (HEIC+MOV pairs)
    motion_photos: int = 0  # Google/Samsung Motion Photos
    photo_json_pairs: int = 0  # Photos with JSON sidecars
    scan_count: int = 1
    file_details: List[Dict] = None  # List of FileDetails.to_dict()
    media_pairs: List[Dict] = None  # List of MediaPair.to_dict()
    notes: str = ''
    
    def __post_init__(self):
        if self.file_details is None:
            self.file_details = []
        if self.media_pairs is None:
            self.media_pairs = []
    
    def to_dict(self) -> dict:
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


def extract_photo_metadata(file_data: bytes, filename: str) -> Optional[PhotoMetadata]:
    """Extract EXIF metadata from photo file bytes.
    
    Args:
        file_data: Raw bytes of the image file
        filename: Name of the file (for logging)
    
    Returns:
        PhotoMetadata object if extraction succeeds, None otherwise
    """
    if not _HAS_PIL:
        return None
    
    try:
        img = Image.open(io.BytesIO(file_data))
        metadata = PhotoMetadata()
        
        # Get basic dimensions
        metadata.width = img.width
        metadata.height = img.height
        
        # Try to get EXIF data
        exif_data = img.getexif()
        if not exif_data:
            return metadata
        
        metadata.has_exif = True
        
        # Extract common EXIF tags
        for tag_id, value in exif_data.items():
            tag_name = TAGS.get(tag_id, tag_id)
            
            if tag_name == 'Make':
                metadata.camera_make = str(value).strip()
            elif tag_name == 'Model':
                metadata.camera_model = str(value).strip()
            elif tag_name == 'DateTimeOriginal':
                metadata.datetime_original = str(value)
                metadata.has_datetime = True
            elif tag_name == 'DateTime' and not metadata.datetime_original:
                metadata.datetime_original = str(value)
                metadata.has_datetime = True
            elif tag_name == 'GPSInfo':
                metadata.has_gps = True
                # GPS data is complex; just mark presence for now
                # Future: parse GPSInfo dict for exact coordinates
        
        return metadata
    
    except Exception as e:
        logger.debug(f'Failed to extract metadata from {filename}: {e}')
        return None


def extract_metadata_from_zip(zf: zipfile.ZipFile, member_path: str) -> Optional[PhotoMetadata]:
    """Extract metadata from a photo inside a ZIP archive."""
    try:
        with zf.open(member_path) as f:
            file_data = f.read()
        return extract_photo_metadata(file_data, member_path)
    except Exception as e:
        logger.debug(f'Failed to read {member_path} from ZIP: {e}')
        return None


def extract_metadata_from_tar(tf: tarfile.TarFile, member_path: str) -> Optional[PhotoMetadata]:
    """Extract metadata from a photo inside a TAR archive."""
    try:
        member = tf.getmember(member_path)
        f = tf.extractfile(member)
        if f:
            file_data = f.read()
            f.close()
            return extract_photo_metadata(file_data, member_path)
    except Exception as e:
        logger.debug(f'Failed to read {member_path} from TAR: {e}')
    return None


# --- Live Photo / Motion Photo detection ------------------------------------

def detect_media_pairs(file_list: List[Tuple[str, int]]) -> Tuple[List[MediaPair], Dict[str, str]]:
    """Detect paired files that represent single media entities.
    
    Args:
        file_list: List of (path, size) tuples
    
    Returns:
        Tuple of (list of MediaPair objects, dict of path -> pair_type for paired files)
    """
    pairs: List[MediaPair] = []
    paired_files: Dict[str, str] = {}  # Maps file path to pair type
    
    # Build lookup dicts by base name
    files_by_base: Dict[str, List[Tuple[str, int, str]]] = defaultdict(list)
    
    for path, size in file_list:
        path_obj = Path(path)
        parent = str(path_obj.parent)
        name = path_obj.name
        ext = path_obj.suffix.lower()
        
        # Get base name without extension
        if name.endswith('.json') and not name == '.json':
            # Handle JSON sidecars like IMG_1234.jpg.json
            base = name[:-5]  # Remove .json
        else:
            base = path_obj.stem
        
        full_base = f"{parent}/{base}"
        files_by_base[full_base].append((path, size, ext))
    
    # Look for pairs
    for base_path, files in files_by_base.items():
        if len(files) < 2:
            continue
        
        # Group by extension
        by_ext: Dict[str, Tuple[str, int]] = {}
        for path, size, ext in files:
            by_ext[ext] = (path, size)
        
        # Check for Apple Live Photos (HEIC/JPG + MOV)
        photo_exts = {'.heic', '.heif', '.jpg', '.jpeg'}
        video_exts = {'.mov', '.mp4'}
        
        photo_ext = next((e for e in photo_exts if e in by_ext), None)
        video_ext = next((e for e in video_exts if e in by_ext), None)
        
        if photo_ext and video_ext:
            photo_path, photo_size = by_ext[photo_ext]
            video_path, video_size = by_ext[video_ext]
            
            pair = MediaPair(
                pair_type='live_photo',
                photo_path=photo_path,
                companion_path=video_path,
                photo_size=photo_size,
                companion_size=video_size,
                base_name=Path(photo_path).stem,
            )
            pairs.append(pair)
            paired_files[photo_path] = 'live_photo'
            paired_files[video_path] = 'live_photo_video'
        
        # Check for photo + JSON sidecar
        photo_with_json = None
        json_sidecar = None
        
        for ext in photo_exts:
            if ext in by_ext:
                photo_with_json = by_ext[ext]
                # Look for corresponding .json
                json_ext = ext + '.json'
                if json_ext in by_ext or '.json' in by_ext:
                    json_sidecar = by_ext.get(json_ext) or by_ext.get('.json')
                    break
        
        if photo_with_json and json_sidecar:
            photo_path, photo_size = photo_with_json
            json_path, json_size = json_sidecar
            
            # Only create pair if not already part of live photo
            if photo_path not in paired_files:
                pair = MediaPair(
                    pair_type='photo_json',
                    photo_path=photo_path,
                    companion_path=json_path,
                    photo_size=photo_size,
                    companion_size=json_size,
                    base_name=Path(photo_path).stem,
                )
                pairs.append(pair)
                paired_files[photo_path] = 'photo_json'
                paired_files[json_path] = 'json_sidecar'
    
    return pairs, paired_files


def detect_motion_photo_from_exif(metadata: Optional[PhotoMetadata]) -> bool:
    """Check if a photo is a Motion Photo based on EXIF data.
    
    Motion Photos have video embedded in the same file.
    """
    if not metadata or not metadata.has_exif:
        return False
    
    # This would require checking specific EXIF tags like:
    # - MotionPhoto: 1
    # - MicroVideo: 1
    # - etc.
    # For now, return False - can be enhanced later with actual EXIF parsing
    return False


PARTS_PAT = re.compile(r"^(?P<prefix>.+?)-(?:\d{3,})(?:\.zip|\.tgz|\.tar\.gz)$", re.I)


def human_size(n: int) -> str:
    units = ['B', 'KB', 'MB', 'GB', 'TB']
    size = float(n)
    for u in units:
        if size < 1024 or u == 'TB':
            return f"{size:.2f} {u}"
        size /= 1024
    return f"{size:.2f} TB"


# --- Index helpers -----------------------------------------------------------

def load_index() -> Dict[str, Dict[str, float]]:
    """Load mapping of absolute archive path -> {size, mtime}."""
    if INDEX_PATH.exists():
        try:
            with open(INDEX_PATH, 'r', encoding='utf-8') as f:
                return json.load(f)
        except Exception:
            logger.warning('Index file unreadable; starting fresh.')
    return {}


def save_index(index: Dict[str, Dict[str, float]]) -> None:
    try:
        with open(INDEX_PATH, 'w', encoding='utf-8') as f:
            json.dump(index, f, indent=2)
    except Exception as e:
        logger.exception(f'Failed to save index: {e}')


# --- Discovery tracking helpers ----------------------------------------------

def get_takeout_id(path: Path) -> str:
    """Generate a unique ID for a takeout source.
    
    Uses the absolute path to create a consistent identifier.
    For multi-part archives, uses the parts_group name.
    """
    abs_path = str(path.resolve())
    # Use first 12 chars of hash for uniqueness while keeping readability
    hash_suffix = hashlib.md5(abs_path.encode()).hexdigest()[:12]
    
    # Get a clean base name
    if path.is_dir():
        base = path.name
    else:
        base = path.stem
    
    # Sanitize for filename
    safe_base = re.sub(r'[<>:"/\\|?*]', '_', base)
    return f"{safe_base}_{hash_suffix}"


def get_takeout_json_path(path: Path) -> Path:
    """Get the JSON file path for a takeout discovery."""
    takeout_id = get_takeout_id(path)
    return DISCOVERIES_DIR / f"{takeout_id}.takeout_scout"


def load_discoveries_index() -> Dict[str, str]:
    """Load the main discoveries index.
    
    Returns a dict mapping source paths to their discovery JSON filenames.
    """
    if DISCOVERIES_INDEX_PATH.exists():
        try:
            with open(DISCOVERIES_INDEX_PATH, 'r', encoding='utf-8') as f:
                return json.load(f)
        except Exception:
            logger.warning('Discoveries index unreadable; starting fresh.')
    return {}


def save_discoveries_index(index: Dict[str, str]) -> None:
    """Save the main discoveries index."""
    try:
        with open(DISCOVERIES_INDEX_PATH, 'w', encoding='utf-8') as f:
            json.dump(index, f, indent=2)
    except Exception as e:
        logger.exception(f'Failed to save discoveries index: {e}')


def load_takeout_discovery(path: Path) -> Optional[TakeoutDiscovery]:
    """Load an existing takeout discovery record."""
    json_path = get_takeout_json_path(path)
    if not json_path.exists():
        return None
    
    try:
        with open(json_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        return TakeoutDiscovery(
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
    except Exception as e:
        logger.exception(f'Failed to load takeout discovery from {json_path}: {e}')
        return None


def save_takeout_discovery(discovery: TakeoutDiscovery) -> None:
    """Save a takeout discovery record."""
    json_path = get_takeout_json_path(Path(discovery.source_path))
    
    try:
        with open(json_path, 'w', encoding='utf-8') as f:
            json.dump(discovery.to_dict(), f, indent=2)
        
        # Update the main index
        index = load_discoveries_index()
        index[discovery.source_path] = json_path.name
        save_discoveries_index(index)
        
        logger.info(f'Saved takeout discovery: {json_path.name}')
    except Exception as e:
        logger.exception(f'Failed to save takeout discovery to {json_path}: {e}')

# --- Data model --------------------------------------------------------------

@dataclass
class ArchiveSummary:
    path: str
    parts_group: str
    service_guess: str
    file_count: int
    photos: int
    videos: int
    json_sidecars: int
    other: int
    compressed_size: int
    # Metadata statistics
    photos_with_exif: int = 0
    photos_with_gps: int = 0
    photos_with_datetime: int = 0
    photos_checked: int = 0  # Number of photos we attempted to read metadata from
    # Paired media
    live_photos: int = 0  # Apple Live Photos (HEIC+MOV pairs)
    motion_photos: int = 0  # Google/Samsung Motion Photos
    photo_json_pairs: int = 0  # Photos with JSON sidecars

    def to_row(self) -> List[str]:
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


# --- Scanner -----------------------------------------------------------------

def guess_service_from_members(members: Iterable[str]) -> str:
    joined = '\n'.join(members)
    for name, pat in SERVICE_HINTS.items():
        if pat.search(joined):
            return name
    return 'Unknown'


def iter_zip_members(zf: zipfile.ZipFile) -> Iterable[str]:
    for i in zf.infolist():
        if not i.is_dir():
            yield i.filename


def iter_tar_members(tf: tarfile.TarFile) -> Iterable[str]:
    for m in tf.getmembers():
        if m.isfile():
            yield m.name.lstrip('./')

def tally_exts(paths: Iterable[str]) -> Tuple[int, int, int, int]:
    photos = videos = jsons = other = 0
    for p in paths:
        ext = Path(p).suffix.lower()
        if ext in MEDIA_PHOTO_EXT:
            photos += 1
        elif ext in MEDIA_VIDEO_EXT:
            videos += 1
        elif ext in JSON_EXT:
            jsons += 1
        else:
            other += 1
    return photos, videos, jsons, other

# --- Archive iteration with per-archive progress ----------------------------

def iter_members_with_progress(path: Path, start_cb, tick_cb) -> List[str]:
    """Return a list of file members while calling progress callbacks.
    start_cb(total) is called once with the number of file entries.
    tick_cb() is called for each file entry.
    """
    members: List[str] = []
    if path.suffix.lower() == '.zip':
        with zipfile.ZipFile(path) as zf:
            infos = [i for i in zf.infolist() if not i.is_dir()]
            start_cb(len(infos))
            for i in infos:
                members.append(i.filename)
                tick_cb()
    elif path.suffix.lower() in {'.tgz', '.gz'} or path.name.lower().endswith('.tar.gz'):
        with tarfile.open(path, 'r:*') as tf:
            files = [m for m in tf.getmembers() if m.isfile()]
            start_cb(len(files))
            for m in files:
                members.append(m.name.lstrip('./'))
                tick_cb()
    else:
        start_cb(0)
    return members


def derive_parts_group(archive_path: Path) -> str:
    m = PARTS_PAT.match(archive_path.stem)
    if m:
        return m.group('prefix')
    # Also handle Google’s common Takeout-YYYYMMDD…-001.zip style
    m2 = re.match(r'^(Takeout-\d{8}T\d{6}Z-\w+?)-(?:\d{3,})$', archive_path.stem)
    if m2:
        return m2.group(1)
    return archive_path.stem


def scan_archive(path: Path, save_discovery: bool = True) -> ArchiveSummary:
    """Scan an archive and optionally save detailed discovery information.
    
    Args:
        path: Path to the archive file
        save_discovery: If True, saves detailed tracking info to JSON
    """
    try:
        size = path.stat().st_size
    except Exception:
        size = 0

    members: List[str] = []
    metadata_stats = {'exif': 0, 'gps': 0, 'datetime': 0, 'checked': 0}
    file_details_list: List[FileDetails] = []
    
    # Determine source type
    if path.suffix.lower() == '.zip':
        source_type = 'zip'
    elif path.suffix.lower() in {'.tgz', '.gz'} or path.name.lower().endswith('.tar.gz'):
        source_type = 'tgz'
    else:
        source_type = 'unknown'
    
    try:
        if path.suffix.lower() == '.zip':
            with zipfile.ZipFile(path) as zf:
                for info in zf.infolist():
                    if not info.is_dir():
                        member_path = info.filename
                        members.append(member_path)
                        
                        # Determine file type
                        ext = Path(member_path).suffix.lower()
                        if ext in MEDIA_PHOTO_EXT:
                            file_type = 'photo'
                        elif ext in MEDIA_VIDEO_EXT:
                            file_type = 'video'
                        elif ext in JSON_EXT:
                            file_type = 'json'
                        else:
                            file_type = 'other'
                        
                        # Create file detail record
                        file_detail = FileDetails(
                            path=member_path,
                            size=info.file_size,
                            file_type=file_type,
                            extension=ext,
                        )
                        
                        # Extract metadata from photo files
                        if _HAS_PIL and file_type == 'photo':
                            metadata = extract_metadata_from_zip(zf, member_path)
                            if metadata:
                                metadata_stats['checked'] += 1
                                if metadata.has_exif:
                                    metadata_stats['exif'] += 1
                                if metadata.has_gps:
                                    metadata_stats['gps'] += 1
                                if metadata.has_datetime:
                                    metadata_stats['datetime'] += 1
                                
                                # Store metadata in file detail
                                file_detail.metadata = {
                                    'has_exif': metadata.has_exif,
                                    'has_gps': metadata.has_gps,
                                    'has_datetime': metadata.has_datetime,
                                    'datetime_original': metadata.datetime_original,
                                    'camera_make': metadata.camera_make,
                                    'camera_model': metadata.camera_model,
                                    'width': metadata.width,
                                    'height': metadata.height,
                                }
                        
                        file_details_list.append(file_detail)
                        
        elif path.suffix.lower() in {'.tgz', '.gz'} or path.name.lower().endswith('.tar.gz'):
            with tarfile.open(path, 'r:*') as tf:
                for tar_member in tf.getmembers():
                    if tar_member.isfile():
                        member_path = tar_member.name.lstrip('./')
                        members.append(member_path)
                        
                        # Determine file type
                        ext = Path(member_path).suffix.lower()
                        if ext in MEDIA_PHOTO_EXT:
                            file_type = 'photo'
                        elif ext in MEDIA_VIDEO_EXT:
                            file_type = 'video'
                        elif ext in JSON_EXT:
                            file_type = 'json'
                        else:
                            file_type = 'other'
                        
                        # Create file detail record
                        file_detail = FileDetails(
                            path=member_path,
                            size=tar_member.size,
                            file_type=file_type,
                            extension=ext,
                        )
                        
                        # Extract metadata from photo files
                        if _HAS_PIL and file_type == 'photo':
                            metadata = extract_metadata_from_tar(tf, member_path)
                            if metadata:
                                metadata_stats['checked'] += 1
                                if metadata.has_exif:
                                    metadata_stats['exif'] += 1
                                if metadata.has_gps:
                                    metadata_stats['gps'] += 1
                                if metadata.has_datetime:
                                    metadata_stats['datetime'] += 1
                                
                                # Store metadata in file detail
                                file_detail.metadata = {
                                    'has_exif': metadata.has_exif,
                                    'has_gps': metadata.has_gps,
                                    'has_datetime': metadata.has_datetime,
                                    'datetime_original': metadata.datetime_original,
                                    'camera_make': metadata.camera_make,
                                    'camera_model': metadata.camera_model,
                                    'width': metadata.width,
                                    'height': metadata.height,
                                }
                        
                        file_details_list.append(file_detail)
        else:
            logger.warning(f"Skipping unsupported archive: {path}")
            return ArchiveSummary(
                path=str(path),
                parts_group=derive_parts_group(path),
                service_guess='(unsupported)',
                file_count=0,
                photos=0,
                videos=0,
                json_sidecars=0,
                other=0,
                compressed_size=size,
            )
    except Exception as e:
        logger.exception(f"Failed to read archive {path}: {e}")
        return ArchiveSummary(
            path=str(path),
            parts_group=derive_parts_group(path),
            service_guess='(error)',
            file_count=0,
            photos=0,
            videos=0,
            json_sidecars=0,
            other=0,
            compressed_size=size,
        )

    photos, videos, jsons, other = tally_exts(members)
    svc = guess_service_from_members(members)
    parts_group = derive_parts_group(path)
    
    # Detect media pairs (Live Photos, photo+JSON pairs)
    file_list_for_pairing = [(fd.path, fd.size) for fd in file_details_list]
    media_pairs, paired_files = detect_media_pairs(file_list_for_pairing)
    
    # Count different pair types
    pair_counts = {'live_photo': 0, 'motion_photo': 0, 'photo_json': 0}
    for pair in media_pairs:
        pair_counts[pair.pair_type] = pair_counts.get(pair.pair_type, 0) + 1
    
    # Save discovery information if requested
    if save_discovery:
        try:
            # Check if this is a rescan
            existing = load_takeout_discovery(path)
            now = datetime.now().isoformat()
            
            discovery = TakeoutDiscovery(
                source_path=str(path.resolve()),
                source_type=source_type,
                first_discovered=existing.first_discovered if existing else now,
                last_scanned=now,
                parts_group=parts_group,
                service_guess=svc,
                file_count=len(members),
                photos=photos,
                videos=videos,
                json_sidecars=jsons,
                other=other,
                compressed_size=size,
                photos_with_exif=metadata_stats['exif'],
                photos_with_gps=metadata_stats['gps'],
                photos_with_datetime=metadata_stats['datetime'],
                photos_checked=metadata_stats['checked'],
                live_photos=pair_counts['live_photo'],
                motion_photos=pair_counts['motion_photo'],
                photo_json_pairs=pair_counts['photo_json'],
                scan_count=(existing.scan_count + 1) if existing else 1,
                file_details=[fd.to_dict() for fd in file_details_list],
                media_pairs=[mp.to_dict() for mp in media_pairs],
                notes=existing.notes if existing else '',
            )
            
            save_takeout_discovery(discovery)
        except Exception as e:
            logger.exception(f"Failed to save discovery for {path}: {e}")
    
    return ArchiveSummary(
        path=str(path),
        parts_group=parts_group,
        service_guess=svc,
        file_count=len(members),
        photos=photos,
        videos=videos,
        json_sidecars=jsons,
        other=other,
        compressed_size=size,
        photos_with_exif=metadata_stats['exif'],
        photos_with_gps=metadata_stats['gps'],
        photos_with_datetime=metadata_stats['datetime'],
        photos_checked=metadata_stats['checked'],
        live_photos=pair_counts['live_photo'],
        motion_photos=pair_counts['motion_photo'],
        photo_json_pairs=pair_counts['photo_json'],
    )


def scan_directory(path: Path, save_discovery: bool = True) -> ArchiveSummary:
    """Scan an uncompressed directory and return a summary.
    
    Args:
        path: Path to the directory
        save_discovery: If True, saves detailed tracking info to JSON
    """
    try:
        files: List[str] = []
        total_size = 0
        metadata_stats = {'exif': 0, 'gps': 0, 'datetime': 0, 'checked': 0}
        file_details_list: List[FileDetails] = []
        
        for root, _dirs, filenames in os.walk(path):
            for name in filenames:
                file_path = Path(root) / name
                file_size = 0
                try:
                    file_size = file_path.stat().st_size
                    total_size += file_size
                except Exception:
                    pass
                
                # Make relative path for service detection
                rel_path = str(file_path.relative_to(path))
                files.append(rel_path)
                
                # Determine file type
                ext = file_path.suffix.lower()
                if ext in MEDIA_PHOTO_EXT:
                    file_type = 'photo'
                elif ext in MEDIA_VIDEO_EXT:
                    file_type = 'video'
                elif ext in JSON_EXT:
                    file_type = 'json'
                else:
                    file_type = 'other'
                
                # Create file detail record
                file_detail = FileDetails(
                    path=rel_path,
                    size=file_size,
                    file_type=file_type,
                    extension=ext,
                )
                
                # Extract metadata from photo files
                if _HAS_PIL and file_type == 'photo':
                    try:
                        with open(file_path, 'rb') as f:
                            file_data = f.read()
                        metadata = extract_photo_metadata(file_data, name)
                        if metadata:
                            metadata_stats['checked'] += 1
                            if metadata.has_exif:
                                metadata_stats['exif'] += 1
                            if metadata.has_gps:
                                metadata_stats['gps'] += 1
                            if metadata.has_datetime:
                                metadata_stats['datetime'] += 1
                            
                            # Store metadata in file detail
                            file_detail.metadata = {
                                'has_exif': metadata.has_exif,
                                'has_gps': metadata.has_gps,
                                'has_datetime': metadata.has_datetime,
                                'datetime_original': metadata.datetime_original,
                                'camera_make': metadata.camera_make,
                                'camera_model': metadata.camera_model,
                                'width': metadata.width,
                                'height': metadata.height,
                            }
                    except Exception as e:
                        logger.debug(f'Failed to read metadata from {file_path}: {e}')
                
                file_details_list.append(file_detail)
        
        photos, videos, jsons, other = tally_exts(files)
        svc = guess_service_from_members(files)
        
        # Detect media pairs (Live Photos, photo+JSON pairs)
        file_list_for_pairing = [(fd.path, fd.size) for fd in file_details_list]
        media_pairs, paired_files = detect_media_pairs(file_list_for_pairing)
        
        # Count different pair types
        pair_counts = {'live_photo': 0, 'motion_photo': 0, 'photo_json': 0}
        for pair in media_pairs:
            pair_counts[pair.pair_type] = pair_counts.get(pair.pair_type, 0) + 1
        
        # Save discovery information if requested
        if save_discovery:
            try:
                # Check if this is a rescan
                existing = load_takeout_discovery(path)
                now = datetime.now().isoformat()
                
                discovery = TakeoutDiscovery(
                    source_path=str(path.resolve()),
                    source_type='directory',
                    first_discovered=existing.first_discovered if existing else now,
                    last_scanned=now,
                    parts_group=path.name,
                    service_guess=svc,
                    file_count=len(files),
                    photos=photos,
                    videos=videos,
                    json_sidecars=jsons,
                    other=other,
                    compressed_size=total_size,
                    photos_with_exif=metadata_stats['exif'],
                    photos_with_gps=metadata_stats['gps'],
                    photos_with_datetime=metadata_stats['datetime'],
                    photos_checked=metadata_stats['checked'],
                    live_photos=pair_counts['live_photo'],
                    motion_photos=pair_counts['motion_photo'],
                    photo_json_pairs=pair_counts['photo_json'],
                    scan_count=(existing.scan_count + 1) if existing else 1,
                    file_details=[fd.to_dict() for fd in file_details_list],
                    media_pairs=[mp.to_dict() for mp in media_pairs],
                    notes=existing.notes if existing else '',
                )
                
                save_takeout_discovery(discovery)
            except Exception as e:
                logger.exception(f"Failed to save discovery for {path}: {e}")
        
        return ArchiveSummary(
            path=str(path),
            parts_group=path.name,
            service_guess=svc,
            file_count=len(files),
            photos=photos,
            videos=videos,
            json_sidecars=jsons,
            other=other,
            compressed_size=total_size,
            photos_with_exif=metadata_stats['exif'],
            photos_with_gps=metadata_stats['gps'],
            photos_with_datetime=metadata_stats['datetime'],
            photos_checked=metadata_stats['checked'],
            live_photos=pair_counts['live_photo'],
            motion_photos=pair_counts['motion_photo'],
            photo_json_pairs=pair_counts['photo_json'],
        )
    except Exception as e:
        logger.exception(f"Failed to scan directory {path}: {e}")
        return ArchiveSummary(
            path=str(path),
            parts_group=path.name,
            service_guess='(error)',
            file_count=0,
            photos=0,
            videos=0,
            json_sidecars=0,
            other=0,
            compressed_size=0,
        )


def find_archives_and_dirs(root: Path) -> Tuple[List[Path], List[Path]]:
    """Find both archives and Takeout directories.
    Returns (archives, directories)
    """
    archives: List[Path] = []
    directories: List[Path] = []
    
    # Check if the root itself is a Takeout directory
    if root.is_dir():
        # Look for telltale signs of a Takeout folder
        root_contents = list(root.iterdir())
        has_takeout_marker = any(
            'takeout' in item.name.lower() or 
            item.name in {'Google Photos', 'Google Drive', 'Google Maps'}
            for item in root_contents if item.is_dir()
        )
        
        if has_takeout_marker:
            directories.append(root)
            logger.info(f"Root folder appears to be a Takeout directory: {root}")
    
    # Walk the tree for archives and subdirectories
    for dirpath, dirnames, filenames in os.walk(root):
        current_dir = Path(dirpath)
        
        # Find archives
        for name in filenames:
            lower = name.lower()
            if lower.endswith('.zip') or lower.endswith('.tgz') or lower.endswith('.tar.gz'):
                archives.append(current_dir / name)
        
        # Find Takeout directories (one level deep to avoid duplicates)
        if current_dir == root:
            for dirname in dirnames:
                subdir = current_dir / dirname
                # Check if it looks like a Takeout folder
                if 'takeout' in dirname.lower():
                    directories.append(subdir)
    
    return sorted(archives), sorted(directories)


# --- GUI ---------------------------------------------------------------------
import tkinter as tk
from tkinter import ttk, filedialog, messagebox

class TakeoutScoutGUI(tk.Tk):
    def __init__(self) -> None:
        super().__init__()
        self.title('Takeout Scout — Google Takeout Scanner (MVP)')
        self.geometry('1000x600')
        self.minsize(800, 500)
        self._root_dir: Optional[Path] = None
        self._selected_files: Optional[List[Path]] = None
        self._rows: List[ArchiveSummary] = []
        self._prev_index: Dict[str, Dict[str, float]] = load_index()
        self._new_paths: set[str] = set()
        self._missing_paths: set[str] = set()
        self._cancel_evt = threading.Event()
        self._build_widgets()

    # UI construction
    def _build_widgets(self) -> None:
        top = ttk.Frame(self, padding=(10, 10))
        top.pack(side=tk.TOP, fill=tk.X)

        self.dir_var = tk.StringVar(value='(Choose a folder with Takeout archives or uncompressed Takeout data)')
        dir_label = ttk.Label(top, textvariable=self.dir_var, wraplength=500)
        dir_label.pack(side=tk.TOP, fill=tk.X, pady=(0, 5))

        btn_frame = ttk.Frame(top)
        btn_frame.pack(side=tk.TOP, fill=tk.X)
        
        btn_choose = ttk.Button(btn_frame, text='Choose Folder…', command=self.on_choose_folder)
        btn_choose.pack(side=tk.LEFT)

        btn_choose_files = ttk.Button(btn_frame, text='Choose Files…', command=self.on_choose_files)
        btn_choose_files.pack(side=tk.LEFT, padx=(5, 0))

        self.btn_scan = ttk.Button(btn_frame, text='Scan All', command=self.on_scan, state=tk.DISABLED)
        self.btn_scan.pack(side=tk.LEFT, padx=(10, 0))

        self.btn_export = ttk.Button(btn_frame, text='Export CSV', command=self.on_export, state=tk.DISABLED)
        self.btn_export.pack(side=tk.LEFT, padx=(10, 0))

        self.btn_logs = ttk.Button(btn_frame, text='Open Logs…', command=self.on_open_logs)
        self.btn_logs.pack(side=tk.RIGHT)

        # Create frame for tree and scrollbars
        tree_frame = ttk.Frame(self)
        tree_frame.pack(side=tk.TOP, fill=tk.BOTH, expand=True, padx=10, pady=(0, 10))
        
        cols = ('action', 'archive', 'parts', 'service', 'files', 'photos', 'videos', 'json', 'other', 'size', 'exif', 'gps', 'datetime', 'checked', 'live', 'motion', 'pjson')
        self.tree = ttk.Treeview(tree_frame, columns=cols, show='headings')
        for key, title, width, minwidth, anchor, stretch in (
            ('action','Action',70,70,tk.CENTER,False),
            ('archive','Source',250,150,tk.W,True),
            ('parts','Group/Name',150,100,tk.W,True),
            ('service','Service',100,80,tk.W,False),
            ('files','Files',60,50,tk.E,False),
            ('photos','Photos',60,50,tk.E,False),
            ('videos','Videos',60,50,tk.E,False),
            ('json','JSON',60,50,tk.E,False),
            ('other','Other',60,50,tk.E,False),
            ('size','Size',100,80,tk.E,False),
            ('exif','w/EXIF',60,50,tk.E,False),
            ('gps','w/GPS',60,50,tk.E,False),
            ('datetime','w/Date',60,50,tk.E,False),
            ('checked','Checked',60,50,tk.E,False),
            ('live','Live',50,45,tk.E,False),
            ('motion','Motion',60,50,tk.E,False),
            ('pjson','P+J',50,45,tk.E,False),
        ):
            self.tree.heading(key, text=title)
            self.tree.column(key, width=width, minwidth=minwidth, anchor=anchor, stretch=stretch)

        # Bind click event to handle scan button clicks
        self.tree.bind('<Button-1>', self._on_tree_click)

        # Vertical scrollbar
        vsb = ttk.Scrollbar(tree_frame, orient='vertical', command=self.tree.yview)
        self.tree.configure(yscrollcommand=vsb.set)
        
        # Horizontal scrollbar
        hsb = ttk.Scrollbar(tree_frame, orient='horizontal', command=self.tree.xview)
        self.tree.configure(xscrollcommand=hsb.set)
        
        # Grid layout for tree and scrollbars
        self.tree.grid(row=0, column=0, sticky='nsew')
        vsb.grid(row=0, column=1, sticky='ns')
        hsb.grid(row=1, column=0, sticky='ew')
        
        tree_frame.grid_rowconfigure(0, weight=1)
        tree_frame.grid_columnconfigure(0, weight=1)

        # Progress area
        prog = ttk.Frame(self, padding=(10, 0))
        prog.pack(side=tk.TOP, fill=tk.X)
        self.overall_label_var = tk.StringVar(value='Overall: 0/0')
        ttk.Label(prog, textvariable=self.overall_label_var).pack(anchor=tk.W)
        self.pb_overall = ttk.Progressbar(prog, mode='determinate', maximum=1, value=0)
        self.pb_overall.pack(fill=tk.X, pady=(2, 8))

        self.current_label_var = tk.StringVar(value='Current archive: —')
        ttk.Label(prog, textvariable=self.current_label_var).pack(anchor=tk.W)
        self.pb_current = ttk.Progressbar(prog, mode='determinate', maximum=1, value=0)
        self.pb_current.pack(fill=tk.X, pady=(2, 8))

        self.btn_cancel = ttk.Button(prog, text='Cancel Scan', command=self.on_cancel, state=tk.DISABLED)
        self.btn_cancel.pack(anchor=tk.E)

        self.status_var = tk.StringVar(value='Ready')
        status = ttk.Label(self, textvariable=self.status_var, relief=tk.SUNKEN, anchor=tk.W, padding=(8, 4))
        status.pack(side=tk.BOTTOM, fill=tk.X)

    # Handlers
    def on_choose_folder(self) -> None:
        """Let user select a folder."""
        chosen = filedialog.askdirectory(title='Select folder with Google Takeout archives or data')
        if not chosen:
            return
        
        # User selected a folder
        self._root_dir = Path(chosen)
        self._selected_files = None
        self.dir_var.set(str(self._root_dir))
        self.btn_scan.config(state=tk.NORMAL)
        
        # Immediately show the selected folder in the table
        self._show_selected_folder()
        
        self.status('Folder selected. Click "Scan" to analyze contents.')
        logger.info(f"Chosen folder: {self._root_dir}")

    def on_choose_files(self) -> None:
        """Let user select individual archive files."""
        files = filedialog.askopenfilenames(
            title='Select Google Takeout archive files',
            filetypes=[
                ('Archive files', '*.zip *.tgz *.tar.gz'),
                ('ZIP files', '*.zip'),
                ('TGZ files', '*.tgz *.tar.gz'),
                ('All files', '*.*')
            ]
        )
        if not files:
            return
        
        # If they selected files, use the parent directory as root
        # and we'll scan those specific files
        self._root_dir = Path(files[0]).parent
        self._selected_files = [Path(f) for f in files]
        self.dir_var.set(f"{len(files)} file(s) selected: {self._root_dir}")
        self.btn_scan.config(state=tk.NORMAL)
        self._show_selected_files()
        self.status(f'{len(files)} file(s) selected. Click "Scan All" or individual [Scan] buttons.')
        logger.info(f"Chosen files: {files}")

    def _show_selected_folder(self) -> None:
        """Display the selected folder immediately in the table."""
        if not self._root_dir:
            return
        
        # Clear existing rows
        for item in self.tree.get_children():
            self.tree.delete(item)
        
        # Show the selected folder as pending scan
        self.tree.insert('', tk.END, values=(
            '[Scan]',
            str(self._root_dir.name),
            '(pending scan)',
            '(pending scan)',
            '—',
            '—',
            '—',
            '—',
            '—',
            '—',
            '—',
            '—',
            '—',
            '—',
        ), tags=('scannable',))

    def _show_selected_files(self) -> None:
        """Display the selected files immediately in the table."""
        if not self._selected_files:
            return
        
        # Clear existing rows
        for item in self.tree.get_children():
            self.tree.delete(item)
        
        # Show each selected file as pending scan
        for file_path in self._selected_files:
            self.tree.insert('', tk.END, values=(
                '[Scan]',
                str(file_path.name),
                '(pending scan)',
                '(pending scan)',
                '—',
                '—',
                '—',
                '—',
                '—',
                '—',
                '—',
                '—',
                '—',
                '—',
            ), tags=('scannable',))

    def _on_tree_click(self, event) -> None:
        """Handle clicks on the tree, specifically on the Action column."""
        region = self.tree.identify_region(event.x, event.y)
        if region == 'cell':
            column = self.tree.identify_column(event.x)
            row = self.tree.identify_row(event.y)
            
            # Check if clicked on Action column (column #0 is the first column)
            if column == '#1' and row:  # #1 is the 'action' column
                # Check if this row has a scan button
                values = self.tree.item(row, 'values')
                if values and values[0] == '[Scan]':
                    # Trigger scan for this specific item
                    self._scan_single_item(row)

    def _scan_single_item(self, item_id: str) -> None:
        """Scan a single item from the tree."""
        values = self.tree.item(item_id, 'values')
        if not values:
            return
        
        # Get the path from the row (second column is the source)
        source_name = values[1]
        
        # Find the actual path
        target_path = None
        
        # Check if we have selected files
        if self._selected_files:
            for file_path in self._selected_files:
                if file_path.name == source_name:
                    target_path = file_path
                    break
        
        # Otherwise check root directory
        if not target_path:
            if self._root_dir and self._root_dir.name == source_name:
                target_path = self._root_dir
            elif self._root_dir:
                potential = self._root_dir / source_name
                if potential.exists():
                    target_path = potential
        
        if not target_path:
            self.status('Could not find item to scan.')
            return
        
        # Update the row to show scanning
        self.tree.item(item_id, values=(
            '[...]',
            values[1],
            '(scanning...)',
            values[3],
            '—',
            '—',
            '—',
            '—',
            '—',
            '—',
        ))
        
        # Launch scan in background thread
        threading.Thread(target=self._scan_single_item_thread, args=(item_id, target_path), daemon=True).start()

    def _scan_single_item_thread(self, item_id: str, path: Path) -> None:
        """Background thread to scan a single item."""
        try:
            if path.is_file():
                # It's an archive
                summary = scan_archive(path)
            else:
                # It's a directory
                summary = scan_directory(path)
            
            # Update the tree with results
            def update():
                self.tree.item(item_id, values=(
                    '[✓]',  # Checkmark to show it's been scanned
                    Path(summary.path).name,
                    summary.parts_group if summary.parts_group != Path(summary.path).name else '—',
                    summary.service_guess,
                    summary.file_count,
                    summary.photos,
                    summary.videos,
                    summary.json_sidecars,
                    summary.other,
                    human_size(summary.compressed_size),
                ))
                self.status(f'Scanned: {Path(summary.path).name}')
            
            self._ui(update)
            
        except Exception as e:
            logger.exception(f"Failed to scan {path}: {e}")
            def error_update():
                values = self.tree.item(item_id, 'values')
                self.tree.item(item_id, values=(
                    '[X]',
                    values[1] if values else str(path.name),
                    '(error)',
                    str(e)[:50],
                    '—',
                    '—',
                    '—',
                    '—',
                    '—',
                    '—',
                ))
                self.status(f'Error scanning: {path.name}')
            self._ui(error_update)

    def on_scan(self) -> None:
        if not self._root_dir:
            return
        self.btn_scan.config(state=tk.DISABLED)
        self.btn_export.config(state=tk.DISABLED)
        self.btn_cancel.config(state=tk.NORMAL)
        self._cancel_evt.clear()
        self.status('Scanning…')
        self._prev_index = load_index()
        self._set_overall_progress(0, 1)
        self._set_current_label('Current archive: —')
        threading.Thread(target=self._scan_thread, daemon=True).start()

    def _scan_thread(self) -> None:
        start = time.time()
        try:
            # If specific files were selected, scan only those
            if self._selected_files:
                archives = [f for f in self._selected_files if f.is_file()]
                directories = [f for f in self._selected_files if f.is_dir()]
                total = len(archives) + len(directories)
                logger.info(f"Scanning {len(archives)} selected archive(s) and {len(directories)} selected directory(ies).")
            else:
                # Otherwise scan everything in the root directory
                archives, directories = find_archives_and_dirs(self._root_dir or Path('.'))
                total = len(archives) + len(directories)
                logger.info(f"Found {len(archives)} archive(s) and {len(directories)} directory(ies).")
            
            if total == 0:
                # No Takeout content found - show the folder with appropriate label
                self._show_no_takeout_found()
                self._set_status('No Takeout content found in the selected folder.')
                self._enable_scan_buttons()
                return
            
            rows: List[ArchiveSummary] = []
            current_index: Dict[str, Dict[str, float]] = {}
            self._set_overall_progress(0, max(1, total))
            
            # Scan directories first
            item_count = 0
            for i, d in enumerate(directories, 1):
                if self._cancel_evt.is_set():
                    logger.info('Scan canceled by user.')
                    break
                item_count += 1
                self._set_current_label(f'Current directory: {d.name} ({item_count}/{total})')
                self._current_progress_start(0)  # Indeterminate for directories
                
                r = scan_directory(d)
                rows.append(r)
                
                try:
                    st = d.stat()
                    current_index[str(d.resolve())] = {'size': float(r.compressed_size), 'mtime': float(st.st_mtime)}
                except Exception:
                    pass
                
                # Update overall progress and ETA
                self._set_overall_progress(item_count, max(1, total))
                elapsed = time.time() - start
                rate = item_count / elapsed if elapsed > 0 else 0
                remaining = (total - item_count) / rate if rate > 0 else 0
                eta = time.strftime('%M:%S', time.gmtime(max(0, int(remaining))))
                self._set_status(f'Scanned {item_count}/{total} items • ETA ~ {eta}')
            
            # Then scan archives
            for i, a in enumerate(archives, 1):
                if self._cancel_evt.is_set():
                    logger.info('Scan canceled by user.')
                    break
                item_count += 1
                self._set_current_label(f'Current archive: {a.name} ({item_count}/{total})')
                # Per-archive progress
                members = iter_members_with_progress(a, self._current_progress_start, self._current_progress_tick)
                # Build summary from members
                try:
                    size_bytes = a.stat().st_size
                except Exception:
                    size_bytes = 0
                photos, videos, jsons, other = tally_exts(members)
                svc = guess_service_from_members(members)
                r = ArchiveSummary(
                    path=str(a),
                    parts_group=derive_parts_group(a),
                    service_guess=svc,
                    file_count=len(members),
                    photos=photos,
                    videos=videos,
                    json_sidecars=jsons,
                    other=other,
                    compressed_size=size_bytes,
                )
                rows.append(r)
                try:
                    st = a.stat()
                    current_index[str(a.resolve())] = {'size': float(st.st_size), 'mtime': float(st.st_mtime)}
                except Exception:
                    pass
                # Update overall progress and ETA
                self._set_overall_progress(item_count, max(1, total))
                elapsed = time.time() - start
                rate = item_count / elapsed if elapsed > 0 else 0
                remaining = (total - item_count) / rate if rate > 0 else 0
                eta = time.strftime('%M:%S', time.gmtime(max(0, int(remaining))))
                self._set_status(f'Scanned {item_count}/{total} items • ETA ~ {eta}')
            # Diff and persist index
            prev_paths = set(self._prev_index.keys())
            curr_paths = set(current_index.keys())
            self._new_paths = curr_paths - prev_paths
            self._missing_paths = prev_paths - curr_paths
            save_index(current_index)
            self._rows = rows
            self._populate_tree()
            if self._new_paths or self._missing_paths:
                added = '\n'.join(Path(p).name for p in sorted(self._new_paths)) or '(none)'
                missing = '\n'.join(Path(p).name for p in sorted(self._missing_paths)) or '(none)'
                message = (
                    f"New archives: {len(self._new_paths)}\n{added}\n\n"
                    f"Missing since last scan: {len(self._missing_paths)}\n{missing}"
                )
                self._info_dialog('Changes since last scan', message)
            final_msg = 'Scan canceled.' if self._cancel_evt.is_set() else f'Scan complete. {len(rows)} archive(s) summarized.'
            self._set_status(final_msg)
            self._enable_export(len(rows) > 0)
        except Exception as e:
            logger.exception(f"Scan failed: {e}")
            self._error_dialog('Error', f'Scan failed: {e}')
        finally:
            self._enable_scan_buttons()
            self._stop_current_spinner()

    def _show_no_takeout_found(self) -> None:
        """Display the folder when no Takeout content is found."""
        if not self._root_dir:
            return
        
        # Quick scan to get basic file stats
        try:
            file_count = 0
            total_size = 0
            for root, _dirs, files in os.walk(self._root_dir):
                file_count += len(files)
                for f in files:
                    try:
                        total_size += (Path(root) / f).stat().st_size
                    except Exception:
                        pass
            
            # Create a summary row
            summary = ArchiveSummary(
                path=str(self._root_dir),
                parts_group=self._root_dir.name,
                service_guess='(no Takeout found)',
                file_count=file_count,
                photos=0,
                videos=0,
                json_sidecars=0,
                other=file_count,
                compressed_size=total_size,
            )
            self._rows = [summary]
            self._populate_tree()
        except Exception as e:
            logger.exception(f"Failed to scan folder: {e}")
            # Just show basic info
            for item in self.tree.get_children():
                self.tree.delete(item)
            self.tree.insert('', tk.END, values=(
                '[✓]',
                str(self._root_dir.name),
                self._root_dir.name,
                '(no Takeout found)',
                '—',
                '—',
                '—',
                '—',
                '—',
                '—',
                '—',
                '—',
                '—',
                '—',
            ))

    def _populate_tree(self) -> None:
        for item in self.tree.get_children():
            self.tree.delete(item)
        # Summaries by parts group (for visual cue of multi-part exports)
        parts_counter: Dict[str, int] = Counter(r.parts_group for r in self._rows)
        new_basenames = {Path(p).name for p in self._new_paths}
        for r in self._rows:
            part_suffix = ''
            if parts_counter[r.parts_group] > 1:
                part_suffix = f' ({parts_counter[r.parts_group]} files)'
            self.tree.insert('', tk.END, values=(
                '[✓]',  # Checkmark for already scanned
                (lambda b: f"{b}  [NEW]" if b in new_basenames else b)(Path(r.path).name),
                f'{r.parts_group}{part_suffix}',
                r.service_guess,
                r.file_count,
                r.photos,
                r.videos,
                r.json_sidecars,
                r.other,
                human_size(r.compressed_size),
                r.photos_with_exif,
                r.photos_with_gps,
                r.photos_with_datetime,
                r.photos_checked,
            ))

    def on_export(self) -> None:
        if not self._rows:
            return
        ts = datetime.now().strftime('%Y%m%d-%H%M%S')
        default_name = f'takeout_scout_summary_{ts}.csv'
        dest = filedialog.asksaveasfilename(
            title='Export summary to CSV',
            defaultextension='.csv',
            initialfile=default_name,
            filetypes=[('CSV', '*.csv'), ('All files', '*.*')]
        )
        if not dest:
            return
        try:
            with open(dest, 'w', newline='', encoding='utf-8') as f:
                w = csv.writer(f)
                w.writerow(['Archive','Parts Group','Service Guess','Files','Photos','Videos','JSON Sidecars','Other','Compressed Size (bytes)','Photos w/EXIF','Photos w/GPS','Photos w/DateTime','Photos Checked','Live Photos','Motion Photos','Photo+JSON'])
                for r in self._rows:
                    w.writerow([
                        Path(r.path).name,
                        r.parts_group,
                        r.service_guess,
                        r.file_count,
                        r.photos,
                        r.videos,
                        r.json_sidecars,
                        r.other,
                        r.compressed_size,
                        r.photos_with_exif,
                        r.photos_with_gps,
                        r.photos_with_datetime,
                        r.photos_checked,
                        r.live_photos,
                        r.motion_photos,
                        r.photo_json_pairs,
                    ])
            logger.info(f"Exported CSV: {dest}")
            messagebox.showinfo('Export complete', f'CSV saved to:\n{dest}')
        except Exception as e:
            logger.exception(f"Export failed: {e}")
            messagebox.showerror('Error', f'Export failed: {e}')

    def on_open_logs(self) -> None:
        try:
            path = LOG_DIR / 'takeout_scout.log'
            if not path.exists():
                messagebox.showinfo('Logs', 'No log file yet. Run a scan first.')
                return
            if sys.platform.startswith('win'):
                os.startfile(str(path))  # type: ignore
            elif sys.platform == 'darwin':
                os.system(f'open "{path}"')
            else:
                os.system(f'xdg-open "{path}"')
        except Exception as e:
            logger.exception(f"Open logs failed: {e}")
            messagebox.showerror('Error', f'Open logs failed: {e}')

    def status(self, text: str) -> None:
        # legacy; prefer _set_status from worker thread
        self.status_var.set(text)
        self.update_idletasks()

    # --- UI thread-safe helpers ---
    def _ui(self, fn) -> None:
        try:
            self.after(0, fn)
        except Exception:
            pass

    def _set_status(self, text: str) -> None:
        self._ui(lambda: self.status_var.set(text))

    def _set_overall_progress(self, value: int, maximum: int) -> None:
        def _apply():
            self.overall_label_var.set(f'Overall: {value}/{maximum}')
            self.pb_overall.config(maximum=maximum, value=value)
        self._ui(_apply)

    def _start_current_spinner(self) -> None:
        # no-op (spinner replaced with determinate progress)
        pass

    def _stop_current_spinner(self) -> None:
        # no-op (spinner replaced with determinate progress)
        pass

    def _current_progress_start(self, total: int) -> None:
        def _apply():
            self.pb_current.config(mode='determinate', maximum=max(1, total), value=0)
        self._ui(_apply)

    def _current_progress_tick(self) -> None:
        self._ui(lambda: self.pb_current.step(1))

    def _set_current_label(self, text: str) -> None:
        self._ui(lambda: self.current_label_var.set(text))

    def _enable_export(self, enable: bool) -> None:
        self._ui(lambda: self.btn_export.config(state=tk.NORMAL if enable else tk.DISABLED))

    def _enable_scan_buttons(self) -> None:
        def _apply():
            self.btn_scan.config(state=tk.NORMAL)
            self.btn_cancel.config(state=tk.DISABLED)
        self._ui(_apply)

    def _info_dialog(self, title: str, message: str) -> None:
        self._ui(lambda: messagebox.showinfo(title, message))

    def _error_dialog(self, title: str, message: str) -> None:
        self._ui(lambda: messagebox.showerror(title, message))

    def on_cancel(self) -> None:
        self._cancel_evt.set()
        self._set_status('Canceling…')


def main() -> None:
    logger.info('Takeout Scout started.')
    app = TakeoutScoutGUI()
    app.mainloop()


if __name__ == '__main__':
    main()
