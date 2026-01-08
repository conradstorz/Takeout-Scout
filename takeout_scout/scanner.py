"""
Archive and directory scanning for Takeout Scout.

Core scanning functionality for ZIP archives, TGZ archives, and directories.
"""
from __future__ import annotations

import os
import re
import tarfile
import zipfile
from datetime import datetime
from pathlib import Path
from typing import Callable, Dict, Iterable, List, Optional, Tuple

from takeout_scout.constants import (
    MEDIA_PHOTO_EXT,
    MEDIA_VIDEO_EXT,
    JSON_EXT,
    SERVICE_HINTS,
    PARTS_PAT,
    classify_file,
)
from takeout_scout.models import ArchiveSummary, FileDetails, TakeoutDiscovery
from takeout_scout.metadata import (
    has_pil,
    extract_photo_metadata,
    extract_metadata_from_zip,
    extract_metadata_from_tar,
    detect_media_pairs,
)
from takeout_scout.discovery import load_takeout_discovery, save_takeout_discovery
from takeout_scout.logging import logger


# Type alias for progress callbacks
ProgressCallback = Callable[[int], None]

# Import hashing functions lazily to avoid circular imports
def _get_hash_functions():
    from takeout_scout.hashing import hash_zip_member, hash_tar_member, hash_file
    return hash_zip_member, hash_tar_member, hash_file


def _get_sidecar_functions():
    """Import sidecar functions lazily to avoid circular imports."""
    from takeout_scout.sidecar import (
        parse_sidecar_from_zip,
        parse_sidecar_from_tar,
        parse_sidecar_from_file,
        find_sidecar_for_media,
    )
    return parse_sidecar_from_zip, parse_sidecar_from_tar, parse_sidecar_from_file, find_sidecar_for_media


def guess_service_from_members(members: Iterable[str]) -> str:
    """Guess which Google service a takeout contains based on file paths.
    
    Args:
        members: Iterable of file paths within the archive
        
    Returns:
        Service name or 'Unknown' if not detected
    """
    joined = '\n'.join(members)
    for name, pattern in SERVICE_HINTS.items():
        if pattern.search(joined):
            return name
    return 'Unknown'


def iter_zip_members(zf: zipfile.ZipFile) -> Iterable[str]:
    """Iterate over non-directory members of a ZIP file."""
    for info in zf.infolist():
        if not info.is_dir():
            yield info.filename


def iter_tar_members(tf: tarfile.TarFile) -> Iterable[str]:
    """Iterate over file members of a TAR archive."""
    for member in tf.getmembers():
        if member.isfile():
            yield member.name.lstrip('./')


def tally_exts(paths: Iterable[str]) -> Tuple[int, int, int, int]:
    """Count files by type based on extension.
    
    Args:
        paths: Iterable of file paths
        
    Returns:
        Tuple of (photos, videos, jsons, other) counts
    """
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


def derive_parts_group(archive_path: Path) -> str:
    """Derive the parts group name for multi-part archives.
    
    For archives like "takeout-001.zip", "takeout-002.zip", returns "takeout".
    
    Args:
        archive_path: Path to the archive file
        
    Returns:
        Group name or the archive stem if not a multi-part archive
    """
    # Try standard pattern: prefix-NNN.ext
    m = PARTS_PAT.match(archive_path.name)
    if m:
        return m.group('prefix')
    
    # Try Google's Takeout-YYYYMMDD...-NNN.zip pattern
    m2 = re.match(r'^(Takeout-\d{8}T\d{6}Z-\w+?)-(?:\d{3,})$', archive_path.stem)
    if m2:
        return m2.group(1)
    
    return archive_path.stem


def iter_members_with_progress(
    path: Path,
    start_cb: ProgressCallback,
    tick_cb: Callable[[], None]
) -> List[str]:
    """Iterate archive members while calling progress callbacks.
    
    Args:
        path: Path to the archive
        start_cb: Called once with total count of files
        tick_cb: Called for each file processed
        
    Returns:
        List of member file paths
    """
    members: List[str] = []
    
    if path.suffix.lower() == '.zip':
        with zipfile.ZipFile(path) as zf:
            infos = [i for i in zf.infolist() if not i.is_dir()]
            start_cb(len(infos))
            for info in infos:
                members.append(info.filename)
                tick_cb()
    
    elif path.suffix.lower() in {'.tgz', '.gz'} or path.name.lower().endswith('.tar.gz'):
        with tarfile.open(path, 'r:*') as tf:
            files = [m for m in tf.getmembers() if m.isfile()]
            start_cb(len(files))
            for member in files:
                members.append(member.name.lstrip('./'))
                tick_cb()
    else:
        start_cb(0)
    
    return members


def _create_error_summary(path: Path, error_type: str, size: int = 0) -> ArchiveSummary:
    """Create an ArchiveSummary for error cases."""
    return ArchiveSummary(
        path=str(path),
        parts_group=derive_parts_group(path) if path.suffix else path.name,
        service_guess=error_type,
        file_count=0,
        photos=0,
        videos=0,
        json_sidecars=0,
        other=0,
        compressed_size=size,
    )


def _process_file_metadata(
    file_data: bytes,
    filename: str,
    metadata_stats: Dict[str, int]
) -> Optional[Dict]:
    """Process photo metadata and update statistics.
    
    Returns metadata dict if successful, None otherwise.
    """
    if not has_pil():
        return None
    
    metadata = extract_photo_metadata(file_data, filename)
    if not metadata:
        return None
    
    metadata_stats['checked'] += 1
    if metadata.has_exif:
        metadata_stats['exif'] += 1
    if metadata.has_gps:
        metadata_stats['gps'] += 1
    if metadata.has_datetime:
        metadata_stats['datetime'] += 1
    
    return {
        'has_exif': metadata.has_exif,
        'has_gps': metadata.has_gps,
        'has_datetime': metadata.has_datetime,
        'datetime_original': metadata.datetime_original,
        'camera_make': metadata.camera_make,
        'camera_model': metadata.camera_model,
        'width': metadata.width,
        'height': metadata.height,
    }


def scan_archive(
    path: Path,
    save_discovery: bool = True,
    compute_hashes: bool = False,
    parse_sidecars: bool = False,
) -> ArchiveSummary:
    """Scan an archive and optionally save detailed discovery information.
    
    Supports ZIP (.zip) and TAR (.tgz, .tar.gz) archives.
    
    Args:
        path: Path to the archive file
        save_discovery: If True, saves detailed tracking info to JSON
        compute_hashes: If True, calculate content hashes for duplicate detection
        parse_sidecars: If True, parse JSON sidecars to extract authoritative timestamps
        
    Returns:
        ArchiveSummary with aggregate statistics
    """
    try:
        size = path.stat().st_size
    except Exception:
        size = 0

    members: List[str] = []
    metadata_stats: Dict[str, int] = {'exif': 0, 'gps': 0, 'datetime': 0, 'checked': 0}
    file_details_list: List[FileDetails] = []
    
    # Determine source type
    suffix_lower = path.suffix.lower()
    name_lower = path.name.lower()
    
    if suffix_lower == '.zip':
        source_type = 'zip'
    elif suffix_lower in {'.tgz', '.gz'} or name_lower.endswith('.tar.gz'):
        source_type = 'tgz'
    else:
        logger.warning(f"Skipping unsupported archive: {path}")
        return _create_error_summary(path, '(unsupported)', size)
    
    try:
        if source_type == 'zip':
            file_details_list, members = _scan_zip_archive(path, metadata_stats, compute_hashes, parse_sidecars)
        else:
            file_details_list, members = _scan_tar_archive(path, metadata_stats, compute_hashes, parse_sidecars)
    
    except Exception as e:
        logger.exception(f"Failed to read archive {path}: {e}")
        return _create_error_summary(path, '(error)', size)

    # Calculate statistics
    photos, videos, jsons, other = tally_exts(members)
    service = guess_service_from_members(members)
    parts_group = derive_parts_group(path)
    
    # Detect media pairs
    file_list_for_pairing = [(fd.path, fd.size) for fd in file_details_list]
    media_pairs, _ = detect_media_pairs(file_list_for_pairing)
    
    pair_counts = _count_pair_types(media_pairs)
    
    # Save discovery if requested
    if save_discovery:
        _save_discovery_record(
            path=path,
            source_type=source_type,
            parts_group=parts_group,
            service=service,
            members=members,
            size=size,
            metadata_stats=metadata_stats,
            pair_counts=pair_counts,
            file_details_list=file_details_list,
            media_pairs=media_pairs,
        )
    
    return ArchiveSummary(
        path=str(path),
        parts_group=parts_group,
        service_guess=service,
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


def _scan_zip_archive(
    path: Path,
    metadata_stats: Dict[str, int],
    compute_hashes: bool = False,
    parse_sidecars: bool = False,
) -> Tuple[List[FileDetails], List[str]]:
    """Scan a ZIP archive and extract file details."""
    file_details_list: List[FileDetails] = []
    members: List[str] = []
    
    # Get hash function if needed
    hash_zip_member = None
    if compute_hashes:
        hash_zip_member, _, _ = _get_hash_functions()
    
    # Get sidecar functions if needed
    parse_sidecar_from_zip = None
    find_sidecar_for_media = None
    if parse_sidecars:
        parse_sidecar_from_zip, _, _, find_sidecar_for_media = _get_sidecar_functions()
    
    with zipfile.ZipFile(path) as zf:
        # First pass: collect all member paths
        for info in zf.infolist():
            if not info.is_dir():
                members.append(info.filename)
        
        # Create set for sidecar lookup
        members_set = set(members)
        
        # Second pass: create file details
        for info in zf.infolist():
            if info.is_dir():
                continue
            
            member_path = info.filename
            
            file_type = classify_file(member_path)
            ext = Path(member_path).suffix.lower()
            
            file_detail = FileDetails(
                path=member_path,
                size=info.file_size,
                file_type=file_type,
                extension=ext,
            )
            
            # Calculate hash if requested
            if compute_hashes and hash_zip_member:
                file_detail.file_hash = hash_zip_member(zf, member_path)
            
            # Parse sidecar for media files
            if parse_sidecars and file_type in ('photo', 'video') and find_sidecar_for_media and parse_sidecar_from_zip:
                sidecar_path = find_sidecar_for_media(member_path, members_set)
                if sidecar_path:
                    file_detail.sidecar_path = sidecar_path
                    sidecar_meta = parse_sidecar_from_zip(zf, sidecar_path)
                    if sidecar_meta:
                        if sidecar_meta.photo_taken_time:
                            file_detail.photo_taken_time = sidecar_meta.photo_taken_time.isoformat()
                        if sidecar_meta.creation_time:
                            file_detail.creation_time = sidecar_meta.creation_time.isoformat()
            
            # Extract metadata from photos
            if has_pil() and file_type == 'photo':
                metadata = extract_metadata_from_zip(zf, member_path)
                if metadata:
                    metadata_stats['checked'] += 1
                    if metadata.has_exif:
                        metadata_stats['exif'] += 1
                    if metadata.has_gps:
                        metadata_stats['gps'] += 1
                    if metadata.has_datetime:
                        metadata_stats['datetime'] += 1
                    
                    file_detail.metadata = metadata.to_dict()
            
            file_details_list.append(file_detail)
    
    return file_details_list, members


def _scan_tar_archive(
    path: Path,
    metadata_stats: Dict[str, int],
    compute_hashes: bool = False,
    parse_sidecars: bool = False,
) -> Tuple[List[FileDetails], List[str]]:
    """Scan a TAR archive and extract file details."""
    file_details_list: List[FileDetails] = []
    members: List[str] = []
    
    # Get hash function if needed
    hash_tar_member = None
    if compute_hashes:
        _, hash_tar_member, _ = _get_hash_functions()
    
    # Get sidecar functions if needed
    parse_sidecar_from_tar = None
    find_sidecar_for_media = None
    if parse_sidecars:
        _, parse_sidecar_from_tar, _, find_sidecar_for_media = _get_sidecar_functions()
    
    with tarfile.open(path, 'r:*') as tf:
        # First pass: collect all member paths and create lookup dict
        tar_members_dict = {}
        for tar_member in tf.getmembers():
            if tar_member.isfile():
                member_path = tar_member.name.lstrip('./')
                members.append(member_path)
                tar_members_dict[member_path] = tar_member
        
        # Create set for sidecar lookup
        members_set = set(members)
        
        # Second pass: create file details
        for member_path, tar_member in tar_members_dict.items():
            file_type = classify_file(member_path)
            ext = Path(member_path).suffix.lower()
            
            file_detail = FileDetails(
                path=member_path,
                size=tar_member.size,
                file_type=file_type,
                extension=ext,
            )
            
            # Calculate hash if requested
            if compute_hashes and hash_tar_member:
                file_detail.file_hash = hash_tar_member(tf, member_path)
            
            # Parse sidecar for media files
            if parse_sidecars and file_type in ('photo', 'video') and find_sidecar_for_media and parse_sidecar_from_tar:
                sidecar_path = find_sidecar_for_media(member_path, members_set)
                if sidecar_path and sidecar_path in tar_members_dict:
                    file_detail.sidecar_path = sidecar_path
                    sidecar_member = tar_members_dict[sidecar_path]
                    sidecar_meta = parse_sidecar_from_tar(tf, sidecar_member)
                    if sidecar_meta:
                        if sidecar_meta.photo_taken_time:
                            file_detail.photo_taken_time = sidecar_meta.photo_taken_time.isoformat()
                        if sidecar_meta.creation_time:
                            file_detail.creation_time = sidecar_meta.creation_time.isoformat()
            
            # Extract metadata from photos
            if has_pil() and file_type == 'photo':
                metadata = extract_metadata_from_tar(tf, member_path)
                if metadata:
                    metadata_stats['checked'] += 1
                    if metadata.has_exif:
                        metadata_stats['exif'] += 1
                    if metadata.has_gps:
                        metadata_stats['gps'] += 1
                    if metadata.has_datetime:
                        metadata_stats['datetime'] += 1
                    
                    file_detail.metadata = metadata.to_dict()
            
            file_details_list.append(file_detail)
    
    return file_details_list, members
    
    return file_details_list, members


def scan_directory(
    path: Path,
    save_discovery: bool = True,
    compute_hashes: bool = False,
    parse_sidecars: bool = False,
) -> ArchiveSummary:
    """Scan an uncompressed directory and return a summary.
    
    Recursively walks the directory tree, analyzing all files.
    
    Args:
        path: Path to the directory
        save_discovery: If True, saves detailed tracking info to JSON
        compute_hashes: If True, calculate content hashes for duplicate detection
        parse_sidecars: If True, parse JSON sidecars to extract authoritative timestamps
        
    Returns:
        ArchiveSummary with aggregate statistics
    """
    # Get hash function if needed
    hash_file_fn = None
    if compute_hashes:
        _, _, hash_file_fn = _get_hash_functions()
    
    # Get sidecar functions if needed
    parse_sidecar_from_file = None
    find_sidecar_for_media = None
    if parse_sidecars:
        _, _, parse_sidecar_from_file, find_sidecar_for_media = _get_sidecar_functions()
    
    try:
        files: List[str] = []
        total_size = 0
        metadata_stats: Dict[str, int] = {'exif': 0, 'gps': 0, 'datetime': 0, 'checked': 0}
        file_details_list: List[FileDetails] = []
        
        # First pass: collect all relative paths
        for root, _dirs, filenames in os.walk(path):
            for name in filenames:
                file_path = Path(root) / name
                rel_path = str(file_path.relative_to(path))
                files.append(rel_path)
        
        # Create set for sidecar lookup
        files_set = set(files)
        
        # Second pass: create file details
        for root, _dirs, filenames in os.walk(path):
            for name in filenames:
                file_path = Path(root) / name
                
                # Get file size
                try:
                    file_size = file_path.stat().st_size
                    total_size += file_size
                except Exception:
                    file_size = 0
                
                # Make relative path for consistency
                rel_path = str(file_path.relative_to(path))
                
                file_type = classify_file(rel_path)
                ext = file_path.suffix.lower()
                
                file_detail = FileDetails(
                    path=rel_path,
                    size=file_size,
                    file_type=file_type,
                    extension=ext,
                )
                
                # Calculate hash if requested
                if compute_hashes and hash_file_fn:
                    file_detail.file_hash = hash_file_fn(file_path)
                
                # Parse sidecar for media files
                if parse_sidecars and file_type in ('photo', 'video') and find_sidecar_for_media and parse_sidecar_from_file:
                    sidecar_rel_path = find_sidecar_for_media(rel_path, files_set)
                    if sidecar_rel_path:
                        file_detail.sidecar_path = sidecar_rel_path
                        sidecar_full_path = path / sidecar_rel_path
                        sidecar_meta = parse_sidecar_from_file(sidecar_full_path)
                        if sidecar_meta:
                            if sidecar_meta.photo_taken_time:
                                file_detail.photo_taken_time = sidecar_meta.photo_taken_time.isoformat()
                            if sidecar_meta.creation_time:
                                file_detail.creation_time = sidecar_meta.creation_time.isoformat()
                
                # Extract metadata from photos
                if has_pil() and file_type == 'photo':
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
                            
                            file_detail.metadata = metadata.to_dict()
                    except Exception as e:
                        logger.debug(f'Failed to read metadata from {file_path}: {e}')
                
                file_details_list.append(file_detail)
        
        # Calculate statistics
        photos, videos, jsons, other = tally_exts(files)
        service = guess_service_from_members(files)
        
        # Detect media pairs
        file_list_for_pairing = [(fd.path, fd.size) for fd in file_details_list]
        media_pairs, _ = detect_media_pairs(file_list_for_pairing)
        
        pair_counts = _count_pair_types(media_pairs)
        
        # Save discovery if requested
        if save_discovery:
            _save_discovery_record(
                path=path,
                source_type='directory',
                parts_group=path.name,
                service=service,
                members=files,
                size=total_size,
                metadata_stats=metadata_stats,
                pair_counts=pair_counts,
                file_details_list=file_details_list,
                media_pairs=media_pairs,
            )
        
        return ArchiveSummary(
            path=str(path),
            parts_group=path.name,
            service_guess=service,
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
        return _create_error_summary(path, '(error)')


def _count_pair_types(media_pairs: List) -> Dict[str, int]:
    """Count media pairs by type."""
    counts: Dict[str, int] = {'live_photo': 0, 'motion_photo': 0, 'photo_json': 0}
    for pair in media_pairs:
        pair_type = pair.pair_type
        counts[pair_type] = counts.get(pair_type, 0) + 1
    return counts


def _save_discovery_record(
    path: Path,
    source_type: str,
    parts_group: str,
    service: str,
    members: List[str],
    size: int,
    metadata_stats: Dict[str, int],
    pair_counts: Dict[str, int],
    file_details_list: List[FileDetails],
    media_pairs: List,
) -> None:
    """Save a discovery record for the scanned source."""
    try:
        existing = load_takeout_discovery(path)
        now = datetime.now().isoformat()
        
        photos, videos, jsons, other = tally_exts(members)
        
        discovery = TakeoutDiscovery(
            source_path=str(path.resolve()),
            source_type=source_type,
            first_discovered=existing.first_discovered if existing else now,
            last_scanned=now,
            parts_group=parts_group,
            service_guess=service,
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


def find_archives_and_dirs(root: Path) -> Tuple[List[Path], List[Path]]:
    """Find both archives and Takeout directories within a root path.
    
    Args:
        root: Root directory to search
        
    Returns:
        Tuple of (archives, directories) as sorted lists of Paths
    """
    archives: List[Path] = []
    directories: List[Path] = []
    
    if not root.is_dir():
        return archives, directories
    
    # Check if the root itself is a Takeout directory
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
        
        # Find Takeout directories (only at root level to avoid duplicates)
        if current_dir == root:
            for dirname in dirnames:
                if 'takeout' in dirname.lower():
                    directories.append(current_dir / dirname)
    
    return sorted(archives), sorted(directories)
