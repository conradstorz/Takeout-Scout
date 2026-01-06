"""
Constants and configuration for Takeout Scout.

Defines file extension sets, service detection patterns, and path configurations.
"""
from __future__ import annotations

import re
from pathlib import Path
from typing import Dict, Pattern, Set


# --- File Extensions ---------------------------------------------------------

MEDIA_PHOTO_EXT: Set[str] = {
    # Common formats
    '.jpg', '.jpeg', '.jfif', '.png', '.heic', '.heif', '.webp', '.gif', '.bmp',
    '.tif', '.tiff', '.avif', '.jxl',
    # RAW formats
    '.raw', '.dng', '.arw', '.cr2', '.cr3', '.nef', '.nrw', '.orf', '.rw2',
    '.raf', '.srf', '.sr2', '.pef', '.srw',
    # Other
    '.psd', '.svg',
}

MEDIA_VIDEO_EXT: Set[str] = {
    '.mp4', '.mov', '.m4v', '.avi', '.mts', '.m2ts', '.wmv', '.3gp', '.mkv',
    '.webm', '.mpg', '.mpeg', '.flv', '.ogv', '.vob', '.ts', '.mxf',
}

JSON_EXT: Set[str] = {'.json'}


# --- Service Detection -------------------------------------------------------

SERVICE_HINTS: Dict[str, Pattern[str]] = {
    'Google Photos': re.compile(r'Google Photos', re.I),
    'Google Drive': re.compile(r'Google Drive', re.I),
    'Google Maps': re.compile(r'Maps|Location|Contributions', re.I),
    'Hangouts/Chat': re.compile(r'Hangouts|Chat', re.I),
    'Blogger/Album Archive': re.compile(r'Blogger|Album Archive|Picasa', re.I),
    'Contacts': re.compile(r'Contacts', re.I),
    'Calendar': re.compile(r'Calendar', re.I),
    'Mail': re.compile(r'Mail', re.I),
    'YouTube': re.compile(r'YouTube', re.I),
    'Keep': re.compile(r'Keep', re.I),
}

# Pattern for detecting multi-part archives
PARTS_PAT: Pattern[str] = re.compile(
    r"^(?P<prefix>.+?)-(?:\d{3,})(?:\.zip|\.tgz|\.tar\.gz)$", 
    re.I
)


# --- Path Configuration ------------------------------------------------------

def get_default_paths() -> Dict[str, Path]:
    """Get default paths relative to current working directory.
    
    Returns:
        Dictionary with 'log_dir', 'state_dir', 'discoveries_dir', etc.
    """
    base = Path('.')
    return {
        'log_dir': base / 'logs',
        'state_dir': base / 'state',
        'discoveries_dir': base / 'takeouts_discovered',
        'index_path': base / 'state' / 'takeout_index.json',
        'discoveries_index_path': base / 'discoveries_index.json',
    }


def ensure_directories() -> Dict[str, Path]:
    """Create necessary directories if they don't exist.
    
    Returns:
        Dictionary of created paths
    """
    paths = get_default_paths()
    paths['log_dir'].mkdir(parents=True, exist_ok=True)
    paths['state_dir'].mkdir(parents=True, exist_ok=True)
    paths['discoveries_dir'].mkdir(parents=True, exist_ok=True)
    return paths


# --- File Type Classification ------------------------------------------------

def classify_file(path: str) -> str:
    """Classify a file by its extension.
    
    Args:
        path: File path or name
        
    Returns:
        Classification string: 'photo', 'video', 'json', or 'other'
    """
    ext = Path(path).suffix.lower()
    
    if ext in MEDIA_PHOTO_EXT:
        return 'photo'
    elif ext in MEDIA_VIDEO_EXT:
        return 'video'
    elif ext in JSON_EXT:
        return 'json'
    else:
        return 'other'
