"""
Discovery tracking system for Takeout Scout.

Handles persistence of scanned takeout information to JSON files,
maintaining an index of all discovered sources and their details.
"""
from __future__ import annotations

import hashlib
import json
import re
from pathlib import Path
from typing import Dict, Optional

from takeout_scout.models import TakeoutDiscovery
from takeout_scout.constants import ensure_directories, get_default_paths
from takeout_scout.logging import logger


def get_takeout_id(path: Path) -> str:
    """Generate a unique ID for a takeout source.
    
    Uses the absolute path to create a consistent, filesystem-safe identifier.
    The ID combines a sanitized base name with a hash suffix for uniqueness.
    
    Args:
        path: Path to the takeout source (archive or directory)
        
    Returns:
        Unique identifier string like "MyTakeout_a1b2c3d4e5f6"
        
    Examples:
        >>> get_takeout_id(Path("D:/Takeouts/photos-001.zip"))
        'photos-001_8f14e45fceea'
    """
    abs_path = str(path.resolve())
    # Use first 12 chars of MD5 hash for uniqueness while keeping readability
    hash_suffix = hashlib.md5(abs_path.encode()).hexdigest()[:12]
    
    # Get a clean base name
    if path.is_dir():
        base = path.name
    else:
        base = path.stem
    
    # Sanitize for filesystem safety
    safe_base = re.sub(r'[<>:"/\\|?*]', '_', base)
    return f"{safe_base}_{hash_suffix}"


def get_takeout_json_path(path: Path) -> Path:
    """Get the JSON file path for a takeout discovery.
    
    Args:
        path: Path to the takeout source
        
    Returns:
        Path to the .takeout_scout JSON file
    """
    paths = get_default_paths()
    takeout_id = get_takeout_id(path)
    return paths['discoveries_dir'] / f"{takeout_id}.takeout_scout"


def load_discoveries_index() -> Dict[str, str]:
    """Load the main discoveries index.
    
    The index maps source paths to their discovery JSON filenames,
    allowing quick lookup of whether a source has been scanned before.
    
    Returns:
        Dictionary mapping source paths to discovery filenames
        
    Example:
        {
            "D:\\Takeouts\\photos-001.zip": "photos-001_a1b2c3d4.takeout_scout",
            "D:\\Takeouts\\photos-002.zip": "photos-002_e5f6g7h8.takeout_scout"
        }
    """
    paths = get_default_paths()
    index_path = paths['discoveries_index_path']
    
    if index_path.exists():
        try:
            with open(index_path, 'r', encoding='utf-8') as f:
                return json.load(f)
        except json.JSONDecodeError as e:
            logger.warning(f'Discoveries index corrupted: {e}. Starting fresh.')
        except Exception as e:
            logger.warning(f'Failed to read discoveries index: {e}. Starting fresh.')
    
    return {}


def save_discoveries_index(index: Dict[str, str]) -> None:
    """Save the main discoveries index.
    
    Args:
        index: Dictionary mapping source paths to discovery filenames
    """
    ensure_directories()
    paths = get_default_paths()
    index_path = paths['discoveries_index_path']
    
    try:
        with open(index_path, 'w', encoding='utf-8') as f:
            json.dump(index, f, indent=2)
    except Exception as e:
        logger.exception(f'Failed to save discoveries index: {e}')
        raise


def load_takeout_discovery(path: Path) -> Optional[TakeoutDiscovery]:
    """Load an existing takeout discovery record.
    
    Args:
        path: Path to the takeout source (not the JSON file)
        
    Returns:
        TakeoutDiscovery object if found, None otherwise
    """
    json_path = get_takeout_json_path(path)
    
    if not json_path.exists():
        return None
    
    try:
        with open(json_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        return TakeoutDiscovery.from_dict(data)
    
    except json.JSONDecodeError as e:
        logger.error(f'Discovery file corrupted: {json_path}: {e}')
        return None
    except KeyError as e:
        logger.error(f'Discovery file missing required field: {json_path}: {e}')
        return None
    except Exception as e:
        logger.exception(f'Failed to load takeout discovery from {json_path}: {e}')
        return None


def save_takeout_discovery(discovery: TakeoutDiscovery) -> Path:
    """Save a takeout discovery record.
    
    Creates or updates the discovery JSON file and updates the main index.
    
    Args:
        discovery: TakeoutDiscovery object to save
        
    Returns:
        Path to the saved JSON file
        
    Raises:
        IOError: If writing fails
    """
    ensure_directories()
    json_path = get_takeout_json_path(Path(discovery.source_path))
    
    try:
        with open(json_path, 'w', encoding='utf-8') as f:
            json.dump(discovery.to_dict(), f, indent=2)
        
        # Update the main index
        index = load_discoveries_index()
        index[discovery.source_path] = json_path.name
        save_discoveries_index(index)
        
        logger.info(f'Saved takeout discovery: {json_path.name}')
        return json_path
    
    except Exception as e:
        logger.exception(f'Failed to save takeout discovery to {json_path}: {e}')
        raise


def delete_takeout_discovery(path: Path) -> bool:
    """Delete a takeout discovery record.
    
    Removes both the JSON file and the index entry.
    
    Args:
        path: Path to the takeout source
        
    Returns:
        True if deleted, False if not found
    """
    json_path = get_takeout_json_path(path)
    abs_path = str(path.resolve())
    
    deleted = False
    
    # Remove the JSON file
    if json_path.exists():
        try:
            json_path.unlink()
            deleted = True
            logger.info(f'Deleted discovery file: {json_path.name}')
        except Exception as e:
            logger.error(f'Failed to delete discovery file: {e}')
            return False
    
    # Remove from index
    index = load_discoveries_index()
    if abs_path in index:
        del index[abs_path]
        save_discoveries_index(index)
        deleted = True
    
    return deleted


def list_all_discoveries() -> Dict[str, TakeoutDiscovery]:
    """Load all discovery records.
    
    Returns:
        Dictionary mapping source paths to TakeoutDiscovery objects
    """
    index = load_discoveries_index()
    discoveries: Dict[str, TakeoutDiscovery] = {}
    
    for source_path, json_filename in index.items():
        try:
            discovery = load_takeout_discovery(Path(source_path))
            if discovery:
                discoveries[source_path] = discovery
        except Exception as e:
            logger.warning(f'Failed to load discovery for {source_path}: {e}')
    
    return discoveries
