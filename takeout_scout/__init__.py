"""
Takeout Scout - Google Takeout Scanner Package

A modular library for scanning and analyzing Google Takeout archives.
"""
from __future__ import annotations

__version__ = "0.4.0"
__author__ = "Conrad"

# Re-export main components for convenient imports
from takeout_scout.models import (
    PhotoMetadata,
    FileDetails,
    MediaPair,
    TakeoutDiscovery,
    ArchiveSummary,
)
from takeout_scout.scanner import (
    scan_archive,
    scan_directory,
    find_archives_and_dirs,
)
from takeout_scout.discovery import (
    load_takeout_discovery,
    save_takeout_discovery,
    load_discoveries_index,
    save_discoveries_index,
    get_takeout_id,
    get_takeout_json_path,
)
from takeout_scout.metadata import (
    extract_photo_metadata,
    extract_metadata_from_zip,
    extract_metadata_from_tar,
    detect_media_pairs,
)
from takeout_scout.hashing import (
    calculate_hash,
    hash_file,
    hash_zip_member,
    hash_tar_member,
    HashIndex,
)
from takeout_scout.sidecar import (
    SidecarMetadata,
    GeoLocation,
    DateAnalysis,
    parse_sidecar,
    parse_sidecar_from_file,
    parse_sidecar_from_zip,
    parse_sidecar_from_tar,
    find_sidecar_for_media,
)
from takeout_scout.utils import human_size

__all__ = [
    # Version info
    "__version__",
    "__author__",
    # Models
    "PhotoMetadata",
    "FileDetails", 
    "MediaPair",
    "TakeoutDiscovery",
    "ArchiveSummary",
    # Scanner
    "scan_archive",
    "scan_directory",
    "find_archives_and_dirs",
    # Discovery
    "load_takeout_discovery",
    "save_takeout_discovery",
    "load_discoveries_index",
    "save_discoveries_index",
    "get_takeout_id",
    "get_takeout_json_path",
    # Metadata
    "extract_photo_metadata",
    "extract_metadata_from_zip",
    "extract_metadata_from_tar",
    "detect_media_pairs",
    # Hashing
    "calculate_hash",
    "hash_file",
    "hash_zip_member",
    "hash_tar_member",
    "HashIndex",
    # Sidecar parsing
    "SidecarMetadata",
    "GeoLocation",
    "DateAnalysis",
    "parse_sidecar",
    "parse_sidecar_from_file",
    "parse_sidecar_from_zip",
    "parse_sidecar_from_tar",
    "find_sidecar_for_media",
    # Utils
    "human_size",
]
