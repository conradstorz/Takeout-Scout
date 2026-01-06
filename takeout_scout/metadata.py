"""
Metadata extraction for Takeout Scout.

Handles EXIF metadata extraction from photos and detection of 
media pairs (Live Photos, Motion Photos, photo+JSON sidecars).
"""
from __future__ import annotations

import io
import tarfile
import zipfile
from collections import defaultdict
from pathlib import Path
from typing import Dict, List, Optional, Tuple

from takeout_scout.models import MediaPair, PhotoMetadata
from takeout_scout.logging import logger


# --- PIL/Pillow availability -------------------------------------------------

try:
    from PIL import Image
    from PIL.ExifTags import TAGS
    _HAS_PIL = True
except ImportError:
    _HAS_PIL = False
    Image = None  # type: ignore
    TAGS = {}  # type: ignore


def has_pil() -> bool:
    """Check if PIL/Pillow is available for metadata extraction."""
    return _HAS_PIL


# --- Photo Metadata Extraction -----------------------------------------------

def extract_photo_metadata(file_data: bytes, filename: str) -> Optional[PhotoMetadata]:
    """Extract EXIF metadata from photo file bytes.
    
    Args:
        file_data: Raw bytes of the image file
        filename: Name of the file (for logging)
    
    Returns:
        PhotoMetadata object if extraction succeeds, None otherwise
        
    Note:
        Requires PIL/Pillow to be installed. Returns None if not available.
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
                # Fallback to DateTime if DateTimeOriginal not present
                metadata.datetime_original = str(value)
                metadata.has_datetime = True
            elif tag_name == 'GPSInfo':
                metadata.has_gps = True
                # GPS data parsing could be added here for coordinates
                # For now, just mark presence
        
        return metadata
    
    except Exception as e:
        logger.debug(f'Failed to extract metadata from {filename}: {e}')
        return None


def extract_metadata_from_zip(
    zf: zipfile.ZipFile, 
    member_path: str
) -> Optional[PhotoMetadata]:
    """Extract metadata from a photo inside a ZIP archive.
    
    Args:
        zf: Open ZipFile object
        member_path: Path to the member within the archive
        
    Returns:
        PhotoMetadata if successful, None otherwise
    """
    try:
        with zf.open(member_path) as f:
            file_data = f.read()
        return extract_photo_metadata(file_data, member_path)
    except Exception as e:
        logger.debug(f'Failed to read {member_path} from ZIP: {e}')
        return None


def extract_metadata_from_tar(
    tf: tarfile.TarFile, 
    member_path: str
) -> Optional[PhotoMetadata]:
    """Extract metadata from a photo inside a TAR archive.
    
    Args:
        tf: Open TarFile object
        member_path: Path to the member within the archive
        
    Returns:
        PhotoMetadata if successful, None otherwise
    """
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


# --- Media Pair Detection ----------------------------------------------------

def detect_media_pairs(
    file_list: List[Tuple[str, int]]
) -> Tuple[List[MediaPair], Dict[str, str]]:
    """Detect paired files that represent single media entities.
    
    Identifies:
    - Apple Live Photos (HEIC/JPG + MOV pairs with same base name)
    - Photo + JSON sidecar pairs (Google Photos metadata files)
    
    Args:
        file_list: List of (path, size) tuples for all files
    
    Returns:
        Tuple of:
        - List of MediaPair objects
        - Dict mapping file paths to their pair type (for flagging)
        
    Examples:
        >>> files = [
        ...     ("photos/IMG_001.HEIC", 2000000),
        ...     ("photos/IMG_001.MOV", 5000000),
        ...     ("photos/IMG_002.jpg", 1500000),
        ...     ("photos/IMG_002.jpg.json", 1200),
        ... ]
        >>> pairs, paired = detect_media_pairs(files)
        >>> len(pairs)
        2
    """
    pairs: List[MediaPair] = []
    paired_files: Dict[str, str] = {}  # Maps file path to pair type
    
    # Group files by directory + base name
    files_by_base: Dict[str, List[Tuple[str, int, str]]] = defaultdict(list)
    
    for path, size in file_list:
        path_obj = Path(path)
        parent = str(path_obj.parent)
        name = path_obj.name
        ext = path_obj.suffix.lower()
        
        # Handle JSON sidecars like IMG_1234.jpg.json
        if name.lower().endswith('.json') and name != '.json':
            # Remove .json to get potential photo filename
            potential_photo_name = name[:-5]  # Remove .json
            base = Path(potential_photo_name).stem
        else:
            base = path_obj.stem
        
        # Create unique key for grouping
        full_base = f"{parent}/{base}"
        files_by_base[full_base].append((path, size, ext))
    
    # Look for pairs in each group
    for base_path, files in files_by_base.items():
        if len(files) < 2:
            continue
        
        # Group by extension for easier lookup
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
        for pext in photo_exts:
            if pext not in by_ext:
                continue
            
            photo_path, photo_size = by_ext[pext]
            
            # Skip if already paired as live photo
            if photo_path in paired_files:
                continue
            
            # Look for JSON sidecar (e.g., .jpg.json)
            json_ext_key = pext + '.json'
            json_path = None
            json_size = 0
            
            # Check for specific sidecar pattern
            for path, size, ext in files:
                if path.lower().endswith(f'{pext}.json'):
                    json_path = path
                    json_size = size
                    break
            
            if json_path:
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
    
    Motion Photos (Samsung/Google) have video embedded in the same file,
    indicated by specific EXIF tags.
    
    Args:
        metadata: PhotoMetadata from the image
        
    Returns:
        True if Motion Photo detected, False otherwise
        
    Note:
        This is currently a stub. Full implementation would parse:
        - Samsung: MotionPhoto=1
        - Google: MicroVideo=1, GCamera tags
    """
    if not metadata or not metadata.has_exif:
        return False
    
    # TODO: Implement actual EXIF tag parsing for motion photo detection
    # This requires reading extended EXIF/XMP data which PIL doesn't fully support
    return False
