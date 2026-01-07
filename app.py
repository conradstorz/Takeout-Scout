#!/usr/bin/env python3
"""
Takeout Scout — Google Takeout Scanner (Streamlit Web UI)

A web-based utility that:
  • Lets you select folders or files containing Google Takeout archives
  • Scans archives non-destructively and summarizes their contents
  • Presents an interactive table with counts of photos/videos/JSON sidecars
  • Supports individual or batch scanning
  • Exports the summary to CSV
  • Logs all actions to ./logs/takeout_scout.log (rotated)

Author: ChatGPT for Conrad
License: MIT
"""
from __future__ import annotations

import os
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import List, Optional

import streamlit as st
import pandas as pd

# Import from our package
from takeout_scout import (
    ArchiveSummary,
    scan_archive,
    scan_directory,
    find_archives_and_dirs,
    human_size,
    HashIndex,
)
from takeout_scout.constants import ensure_directories
from takeout_scout.logging import logger
from takeout_scout.discovery import load_takeout_discovery


# Ensure directories exist on import
ensure_directories()


# --- File status enum --------------------------------------------------------
class FileStatus(Enum):
    """Status of a file during validation and scanning."""
    PENDING = "pending"
    VALID = "valid"
    INVALID = "invalid"
    SCANNING = "scanning"
    SCANNED = "scanned"
    ERROR = "error"


class FileInfo:
    """Quick metadata about a file without deep scanning."""
    
    def __init__(
        self,
        path: Path,
        name: str,
        size: int,
        status: FileStatus,
        is_valid: bool,
        error_message: Optional[str] = None,
        file_type: Optional[str] = None,
    ) -> None:
        self.path = path
        self.name = name
        self.size = size
        self.status = status
        self.is_valid = is_valid
        self.error_message = error_message
        self.file_type = file_type
    
    def to_dict(self) -> dict:
        return {
            'path': str(self.path),
            'name': self.name,
            'size': self.size,
            'size_human': human_size(self.size),
            'status': self.status.value,
            'is_valid': self.is_valid,
            'error_message': self.error_message,
            'file_type': self.file_type,
        }


# --- Quick validation functions ----------------------------------------------
def validate_zip(path: Path) -> bool:
    """Validate a ZIP file without extracting it."""
    import zipfile
    try:
        with zipfile.ZipFile(path, 'r') as zf:
            infolist = zf.infolist()
            return len(infolist) > 0
    except zipfile.BadZipFile:
        return False
    except Exception as e:
        logger.warning(f"ZIP validation error for {path}: {e}")
        return False


def validate_tar(path: Path) -> bool:
    """Validate a TAR/TGZ file without extracting it."""
    import tarfile
    try:
        with tarfile.open(path, 'r:*') as tf:
            _ = tf.getmembers()
            return True
    except tarfile.TarError:
        return False
    except Exception as e:
        logger.warning(f"TAR validation error for {path}: {e}")
        return False


def validate_and_get_info(path: Path) -> FileInfo:
    """Quickly validate a file and get basic metadata without deep scanning."""
    try:
        if not path.exists():
            return FileInfo(
                path=path,
                name=path.name,
                size=0,
                status=FileStatus.INVALID,
                is_valid=False,
                error_message="File not found",
                file_type=None
            )
        
        size = path.stat().st_size
        
        if path.is_dir():
            return FileInfo(
                path=path,
                name=path.name,
                size=size,
                status=FileStatus.VALID,
                is_valid=True,
                file_type='directory'
            )
        
        if path.suffix.lower() == '.zip':
            is_valid = validate_zip(path)
            return FileInfo(
                path=path,
                name=path.name,
                size=size,
                status=FileStatus.VALID if is_valid else FileStatus.INVALID,
                is_valid=is_valid,
                error_message=None if is_valid else "Corrupt or invalid ZIP file",
                file_type='zip'
            )
        
        if path.suffix.lower() in {'.tgz', '.gz'} or path.name.lower().endswith('.tar.gz'):
            is_valid = validate_tar(path)
            return FileInfo(
                path=path,
                name=path.name,
                size=size,
                status=FileStatus.VALID if is_valid else FileStatus.INVALID,
                is_valid=is_valid,
                error_message=None if is_valid else "Corrupt or invalid TAR/TGZ file",
                file_type='tgz'
            )
        
        return FileInfo(
            path=path,
            name=path.name,
            size=size,
            status=FileStatus.INVALID,
            is_valid=False,
            error_message="Unsupported file type (only ZIP, TGZ supported)",
            file_type='unknown'
        )
        
    except Exception as e:
        logger.exception(f"Error validating {path}: {e}")
        return FileInfo(
            path=path,
            name=path.name if path else "Unknown",
            size=0,
            status=FileStatus.ERROR,
            is_valid=False,
            error_message=str(e),
            file_type=None
        )


# --- Main Streamlit App ------------------------------------------------------
def main():
    """Main Streamlit application."""
    st.set_page_config(
        page_title="Takeout Scout",
        page_icon="📦",
        layout="wide",
        initial_sidebar_state="expanded"
    )
    
    st.title("📦 Takeout Scout")
    st.markdown("*Scan and analyze Google Takeout archives*")
    
    # Initialize session state
    if 'results' not in st.session_state:
        st.session_state.results = []
    if 'scanned_paths' not in st.session_state:
        st.session_state.scanned_paths = set()
    if 'pending_files' not in st.session_state:
        st.session_state.pending_files = []
    if 'compute_hashes' not in st.session_state:
        st.session_state.compute_hashes = False
    if 'hash_index' not in st.session_state:
        st.session_state.hash_index = HashIndex()
    
    # Sidebar for controls
    with st.sidebar:
        st.header("📂 Input")
        
        # Folder input
        folder_path = st.text_input(
            "Enter folder path",
            placeholder="/path/to/takeout/folder",
            help="Path to a folder containing Takeout archives"
        )
        
        col1, col2 = st.columns(2)
        with col1:
            if st.button("🔍 Scan Folder", disabled=not folder_path):
                process_folder(Path(folder_path))
        
        st.divider()
        
        # File input
        st.subheader("Or enter file paths")
        file_paths_text = st.text_area(
            "Enter file paths (one per line)",
            placeholder="/path/to/archive1.zip\n/path/to/archive2.tgz",
            help="Enter paths to individual archive files"
        )
        
        if st.button("📄 Add Files", disabled=not file_paths_text):
            paths = [Path(p.strip()) for p in file_paths_text.strip().split('\n') if p.strip()]
            add_files_to_pending(paths)
        
        st.divider()
        
        # Scan options
        st.header("⚙️ Options")
        st.session_state.compute_hashes = st.checkbox(
            "Compute file hashes",
            value=st.session_state.compute_hashes,
            help="Calculate MD5 hashes for duplicate detection (slower but enables duplicate analysis)"
        )
        
        st.divider()
        
        # Export button
        if st.session_state.results:
            st.header("📊 Export")
            export_csv()
        
        # Clear button
        st.divider()
        if st.button("🗑️ Clear Results"):
            st.session_state.results = []
            st.session_state.scanned_paths = set()
            st.session_state.pending_files = []
            st.session_state.hash_index = HashIndex()
            st.rerun()
    
    # Main content area
    show_pending_files()
    show_results()
    
    # Show duplicate analysis if hashes were computed
    if st.session_state.compute_hashes:
        show_duplicate_analysis()


def add_files_to_pending(paths: List[Path]):
    """Add files to the pending list with validation."""
    for path in paths:
        # Skip if already in pending or scanned
        if str(path) in st.session_state.scanned_paths:
            continue
        if any(f.path == path for f in st.session_state.pending_files):
            continue
        
        file_info = validate_and_get_info(path)
        st.session_state.pending_files.append(file_info)
    
    st.rerun()


def show_pending_files():
    """Show the list of pending files with scan buttons."""
    if not st.session_state.pending_files:
        return
    
    st.header("📋 Pending Files")
    
    # Scan All button
    valid_count = sum(1 for f in st.session_state.pending_files if f.is_valid and f.status != FileStatus.SCANNED)
    if valid_count > 0:
        if st.button(f"⚡ Scan All ({valid_count} files)", type="primary"):
            scan_all_pending()
    
    # Show each file
    for index, file_info in enumerate(st.session_state.pending_files):
        with st.container():
            col1, col2, col3, col4 = st.columns([3, 1, 1, 1])
            
            with col1:
                if file_info.status == FileStatus.SCANNED:
                    st.markdown(f"✅ **{file_info.name}**")
                elif file_info.status == FileStatus.ERROR:
                    st.markdown(f"❌ **{file_info.name}**")
                elif not file_info.is_valid:
                    st.markdown(f"⚠️ **{file_info.name}**")
                else:
                    st.markdown(f"📄 **{file_info.name}**")
            
            with col2:
                st.text(human_size(file_info.size))
            
            with col3:
                st.text(file_info.file_type or "—")
            
            with col4:
                if file_info.is_valid and file_info.status != FileStatus.SCANNED:
                    if st.button("Scan", key=f"scan_{index}"):
                        scan_single_file(index, file_info)
                elif file_info.error_message:
                    st.text(file_info.error_message[:20])


def scan_single_file(index: int, file_info: FileInfo):
    """Scan a single file."""
    try:
        file_info.status = FileStatus.SCANNING
        st.session_state.pending_files[index] = file_info
        
        compute_hashes = st.session_state.compute_hashes
        
        with st.spinner(f"Scanning {file_info.name}..."):
            if file_info.file_type == 'directory':
                summary = scan_directory(file_info.path, compute_hashes=compute_hashes)
            else:
                summary = scan_archive(file_info.path, compute_hashes=compute_hashes)
        
        st.session_state.results.append(summary)
        st.session_state.scanned_paths.add(str(file_info.path))
        file_info.status = FileStatus.SCANNED
        st.session_state.pending_files[index] = file_info
        
        # Update hash index if hashes were computed
        if compute_hashes:
            _update_hash_index(file_info.path)
        
        st.rerun()
        
    except Exception as e:
        logger.exception(f"Failed to scan {file_info.path}: {e}")
        file_info.status = FileStatus.ERROR
        file_info.error_message = str(e)
        st.session_state.pending_files[index] = file_info
        st.error(f"❌ Error scanning {file_info.name}: {e}")


def _update_hash_index(path: Path):
    """Update the hash index from scanned file data."""
    try:
        discovery = load_takeout_discovery()
        if discovery is None:
            return
        
        source_name = str(path)
        
        # Check both archives and directories for matching path
        for archive in discovery.archives:
            if str(archive.path) == source_name:
                for file_detail in archive.files:
                    if file_detail.file_hash:
                        st.session_state.hash_index.add(
                            file_detail.file_hash,
                            str(archive.path),
                            file_detail.path
                        )
                return
        
        for directory in discovery.directories:
            if str(directory.path) == source_name:
                for file_detail in directory.files:
                    if file_detail.file_hash:
                        st.session_state.hash_index.add(
                            file_detail.file_hash,
                            str(directory.path),
                            file_detail.path
                        )
                return
    except Exception as e:
        logger.warning(f"Failed to update hash index: {e}")


def scan_all_pending():
    """Scan all pending valid files."""
    valid_files = [
        (i, f) for i, f in enumerate(st.session_state.pending_files)
        if f.is_valid and f.status != FileStatus.SCANNED
    ]
    
    if not valid_files:
        st.warning("No files to scan")
        return
    
    compute_hashes = st.session_state.compute_hashes
    progress_bar = st.progress(0, text=f"Scanning 0/{len(valid_files)} files...")
    
    for count, (index, file_info) in enumerate(valid_files, 1):
        try:
            if file_info.file_type == 'directory':
                summary = scan_directory(file_info.path, compute_hashes=compute_hashes)
            else:
                summary = scan_archive(file_info.path, compute_hashes=compute_hashes)
            
            st.session_state.results.append(summary)
            st.session_state.scanned_paths.add(str(file_info.path))
            file_info.status = FileStatus.SCANNED
            st.session_state.pending_files[index] = file_info
            
            # Update hash index if hashes were computed
            if compute_hashes:
                _update_hash_index(file_info.path)
            
        except Exception as e:
            logger.exception(f"Failed to scan {file_info.path}: {e}")
            file_info.status = FileStatus.ERROR
            file_info.error_message = str(e)
            st.session_state.pending_files[index] = file_info
        
        progress_bar.progress(count / len(valid_files), text=f"Scanning {count}/{len(valid_files)} files...")
    
    progress_bar.empty()
    st.success(f"✅ Scanned {len(valid_files)} files")
    st.rerun()


def process_folder(folder_path: Path):
    """Process a folder by finding and scanning all archives/directories."""
    if not folder_path.exists():
        st.error(f"❌ Folder not found: {folder_path}")
        return
    
    compute_hashes = st.session_state.compute_hashes
    
    with st.spinner(f"Scanning {folder_path.name}..."):
        archives, directories = find_archives_and_dirs(folder_path)
        total = len(archives) + len(directories)
        
        if total == 0:
            st.warning(f"⚠️ No Takeout archives or directories found in {folder_path}")
            summary = scan_directory(folder_path, compute_hashes=compute_hashes)
            if summary.file_count > 0:
                st.session_state.results.append(summary)
                st.session_state.scanned_paths.add(str(folder_path))
                if compute_hashes:
                    _update_hash_index(folder_path)
            return
        
        progress_bar = st.progress(0, text=f"Scanning 0/{total} items...")
        
        count = 0
        for directory in directories:
            if str(directory) not in st.session_state.scanned_paths:
                summary = scan_directory(directory, compute_hashes=compute_hashes)
                st.session_state.results.append(summary)
                st.session_state.scanned_paths.add(str(directory))
                if compute_hashes:
                    _update_hash_index(directory)
            count += 1
            progress_bar.progress(count / total, text=f"Scanning {count}/{total} items...")
        
        for archive in archives:
            if str(archive) not in st.session_state.scanned_paths:
                summary = scan_archive(archive, compute_hashes=compute_hashes)
                st.session_state.results.append(summary)
                st.session_state.scanned_paths.add(str(archive))
                if compute_hashes:
                    _update_hash_index(archive)
            count += 1
            progress_bar.progress(count / total, text=f"Scanning {count}/{total} items...")
        
        progress_bar.empty()
        st.success(f"✅ Scanned {total} items from {folder_path.name}")
        st.rerun()


def show_duplicate_analysis():
    """Display duplicate file analysis."""
    hash_index = st.session_state.hash_index
    stats = hash_index.get_duplicate_stats()
    
    if stats['duplicate_hashes'] == 0:
        if st.session_state.results:
            st.info("No duplicates detected in scanned files.")
        return
    
    st.header("🔍 Duplicate Analysis")
    
    col1, col2, col3 = st.columns(3)
    col1.metric("Duplicate Groups", stats['duplicate_hashes'])
    col2.metric("Duplicate Files", stats['duplicate_files'])
    col3.metric("Wasted Space", human_size(stats['wasted_bytes']))
    
    # Show detailed duplicate list
    with st.expander("📋 View Duplicate Details", expanded=False):
        duplicates = hash_index.find_all_duplicates()
        
        for i, (file_hash, locations) in enumerate(duplicates.items(), 1):
            if i > 50:  # Limit display to 50 groups
                st.info(f"... and {len(duplicates) - 50} more duplicate groups")
                break
            
            # Get file size from first location
            first_loc = locations[0]
            
            st.markdown(f"**Group {i}** ({len(locations)} copies)")
            for source, path in locations:
                source_name = Path(source).name
                st.markdown(f"- `{source_name}` → `{path}`")
            st.divider()


def show_results():
    """Display scan results in a table."""
    if not st.session_state.results:
        st.info("👆 Select a folder or files to scan")
        return
    
    st.header("📊 Results")
    
    # Summary stats
    total_files = sum(r.file_count for r in st.session_state.results)
    total_photos = sum(r.photos for r in st.session_state.results)
    total_videos = sum(r.videos for r in st.session_state.results)
    total_size = sum(r.compressed_size for r in st.session_state.results)
    
    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Total Archives", len(st.session_state.results))
    col2.metric("Total Files", f"{total_files:,}")
    col3.metric("Photos/Videos", f"{total_photos:,} / {total_videos:,}")
    col4.metric("Total Size", human_size(total_size))
    
    st.divider()
    
    # Results table
    df = pd.DataFrame([r.to_dict() for r in st.session_state.results])
    
    # ArchiveSummary.to_dict() already uses display-friendly keys
    # Just need to format the Path column to show filename only
    if 'Path' in df.columns:
        df['Path'] = df['Path'].apply(lambda x: Path(x).name)
    
    # Display table
    st.dataframe(
        df,
        width="stretch",
        hide_index=True,
    )


def export_csv():
    """Export results to CSV."""
    if not st.session_state.results:
        st.warning("No results to export")
        return
    
    df = pd.DataFrame([r.to_dict() for r in st.session_state.results])
    timestamp = datetime.now().strftime('%Y%m%d-%H%M%S')
    filename = f'takeout_scout_summary_{timestamp}.csv'
    
    csv = df.to_csv(index=False)
    st.download_button(
        label="⬇️ Download CSV",
        data=csv,
        file_name=filename,
        mime='text/csv',
        type="primary"
    )


if __name__ == '__main__':
    main()
