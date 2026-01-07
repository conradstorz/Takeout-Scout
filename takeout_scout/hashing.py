"""
File hashing utilities for Takeout Scout.

Provides hash calculation for duplicate detection across archives.
Supports streaming hashes for memory efficiency with large files.
"""
from __future__ import annotations

import hashlib
import io
import tarfile
import zipfile
from pathlib import Path
from typing import BinaryIO, Callable, Dict, List, Optional, Tuple

from takeout_scout.logging import logger


# Default chunk size for streaming hash calculation (64KB)
HASH_CHUNK_SIZE = 65536


def calculate_hash(
    data: bytes | BinaryIO,
    algorithm: str = 'md5',
    chunk_size: int = HASH_CHUNK_SIZE,
) -> str:
    """Calculate hash of file data.
    
    Args:
        data: File bytes or file-like object to hash
        algorithm: Hash algorithm ('md5', 'sha256', 'sha1')
        chunk_size: Chunk size for streaming reads
        
    Returns:
        Hex digest string of the hash
    """
    hasher = hashlib.new(algorithm)
    
    if isinstance(data, bytes):
        hasher.update(data)
    else:
        # Stream from file-like object
        while True:
            chunk = data.read(chunk_size)
            if not chunk:
                break
            hasher.update(chunk)
    
    return hasher.hexdigest()


def hash_file(
    path: Path,
    algorithm: str = 'md5',
    chunk_size: int = HASH_CHUNK_SIZE,
) -> Optional[str]:
    """Calculate hash of a file on disk.
    
    Args:
        path: Path to the file
        algorithm: Hash algorithm to use
        chunk_size: Chunk size for reading
        
    Returns:
        Hex digest string, or None if file cannot be read
    """
    try:
        with open(path, 'rb') as f:
            return calculate_hash(f, algorithm, chunk_size)
    except Exception as e:
        logger.debug(f"Failed to hash file {path}: {e}")
        return None


def hash_zip_member(
    zf: zipfile.ZipFile,
    member_path: str,
    algorithm: str = 'md5',
) -> Optional[str]:
    """Calculate hash of a file inside a ZIP archive.
    
    Args:
        zf: Open ZipFile object
        member_path: Path to the member within the ZIP
        algorithm: Hash algorithm to use
        
    Returns:
        Hex digest string, or None if member cannot be read
    """
    try:
        with zf.open(member_path) as f:
            return calculate_hash(f, algorithm)
    except Exception as e:
        logger.debug(f"Failed to hash ZIP member {member_path}: {e}")
        return None


def hash_tar_member(
    tf: tarfile.TarFile,
    member_path: str,
    algorithm: str = 'md5',
) -> Optional[str]:
    """Calculate hash of a file inside a TAR archive.
    
    Args:
        tf: Open TarFile object
        member_path: Path to the member within the TAR
        algorithm: Hash algorithm to use
        
    Returns:
        Hex digest string, or None if member cannot be read
    """
    try:
        member = tf.getmember(member_path)
        f = tf.extractfile(member)
        if f:
            result = calculate_hash(f, algorithm)
            f.close()
            return result
        return None
    except Exception as e:
        logger.debug(f"Failed to hash TAR member {member_path}: {e}")
        return None


class HashIndex:
    """Index for tracking file hashes across multiple sources.
    
    Used for duplicate detection without modifying source files.
    """
    
    def __init__(self) -> None:
        # hash -> list of (source_path, file_path, size)
        self._by_hash: Dict[str, List[Tuple[str, str, int]]] = {}
        # (source_path, file_path) -> hash
        self._by_path: Dict[Tuple[str, str], str] = {}
    
    def add(
        self,
        file_hash: str,
        source_path: str,
        file_path: str,
        size: int,
    ) -> None:
        """Add a file to the index.
        
        Args:
            file_hash: Hash of the file content
            source_path: Path to the archive/directory containing the file
            file_path: Path to the file within the source
            size: File size in bytes
        """
        key = (source_path, file_path)
        
        # Store by hash for duplicate lookup
        if file_hash not in self._by_hash:
            self._by_hash[file_hash] = []
        self._by_hash[file_hash].append((source_path, file_path, size))
        
        # Store by path for reverse lookup
        self._by_path[key] = file_hash
    
    def get_hash(self, source_path: str, file_path: str) -> Optional[str]:
        """Get the hash for a specific file."""
        return self._by_path.get((source_path, file_path))
    
    def get_duplicates(self, file_hash: str) -> List[Tuple[str, str, int]]:
        """Get all files with the given hash."""
        return self._by_hash.get(file_hash, [])
    
    def find_all_duplicates(self) -> Dict[str, List[Tuple[str, str, int]]]:
        """Find all hashes that have more than one file.
        
        Returns:
            Dict mapping hash -> list of (source_path, file_path, size)
            Only includes hashes with 2+ files.
        """
        return {
            h: files for h, files in self._by_hash.items()
            if len(files) > 1
        }
    
    def get_duplicate_stats(self) -> Dict[str, int]:
        """Get statistics about duplicates.
        
        Returns:
            Dict with keys:
                - total_files: Total files indexed
                - unique_hashes: Number of unique file contents
                - duplicate_sets: Number of hashes with duplicates
                - duplicate_files: Total files that are duplicates
                - wasted_bytes: Bytes that could be saved by deduping
        """
        total_files = len(self._by_path)
        unique_hashes = len(self._by_hash)
        
        duplicate_sets = 0
        duplicate_files = 0
        wasted_bytes = 0
        
        for file_hash, files in self._by_hash.items():
            if len(files) > 1:
                duplicate_sets += 1
                # All but one are "wasted"
                duplicate_files += len(files) - 1
                # Sum sizes of duplicates (keeping largest)
                sizes = [size for _, _, size in files]
                sizes.sort(reverse=True)
                wasted_bytes += sum(sizes[1:])  # All but largest
        
        return {
            'total_files': total_files,
            'unique_hashes': unique_hashes,
            'duplicate_sets': duplicate_sets,
            'duplicate_files': duplicate_files,
            'wasted_bytes': wasted_bytes,
        }
    
    def to_dict(self) -> Dict:
        """Serialize to dictionary for JSON storage."""
        return {
            'by_hash': self._by_hash,
        }
    
    @classmethod
    def from_dict(cls, data: Dict) -> 'HashIndex':
        """Create HashIndex from dictionary."""
        index = cls()
        for file_hash, files in data.get('by_hash', {}).items():
            for source_path, file_path, size in files:
                index.add(file_hash, source_path, file_path, size)
        return index
    
    def merge(self, other: 'HashIndex') -> None:
        """Merge another HashIndex into this one."""
        for file_hash, files in other._by_hash.items():
            for source_path, file_path, size in files:
                index.add(file_hash, source_path, file_path, size)
