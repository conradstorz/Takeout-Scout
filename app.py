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
import re
import tarfile
import time
import zipfile
from collections import Counter, defaultdict
from dataclasses import dataclass, asdict
from datetime import datetime
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple
from enum import Enum

import streamlit as st
import pandas as pd


# --- File status enum --------------------------------------------------------
class FileStatus(Enum):
    PENDING = "pending"
    VALID = "valid"
    INVALID = "invalid"
    SCANNING = "scanning"
    SCANNED = "scanned"
    ERROR = "error"


# --- File info dataclass -----------------------------------------------------
@dataclass
class FileInfo:
    """Quick metadata about a file without deep scanning."""
    path: Path
    name: str
    size: int
    status: FileStatus
    is_valid: bool
    error_message: Optional[str] = None
    file_type: Optional[str] = None  # 'zip', 'tgz', 'directory'
    
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

# --- Logging setup -----------------------------------------------------------
try:
    from loguru import logger
    _HAS_LOGURU = True
except Exception:
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
    logger = _Shim()
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

# --- State (persistent index) -----------------------------------------------
STATE_DIR = Path('state')
STATE_DIR.mkdir(parents=True, exist_ok=True)
INDEX_PATH = STATE_DIR / 'takeout_index.json'

# --- File type definitions ---------------------------------------------------
MEDIA_PHOTO_EXT = {
    '.jpg', '.jpeg', '.png', '.heic', '.heif', '.webp', '.gif', '.bmp', '.tif', '.tiff', '.raw', '.dng', '.arw', '.cr2', '.nef'
}
MEDIA_VIDEO_EXT = {
    '.mp4', '.mov', '.m4v', '.avi', '.mts', '.m2ts', '.wmv', '.3gp', '.mkv'
}
JSON_EXT = {'.json'}

SERVICE_HINTS = {
    'Google Photos': re.compile(r'^Takeout/Google Photos/|Google Photos/', re.I),
    'Google Drive': re.compile(r'^Takeout/Google Drive/|Google Drive/', re.I),
    'Google Maps': re.compile(r'Maps|Location|Contributions', re.I),
    'Hangouts/Chat': re.compile(r'Hangouts|Chat', re.I),
    'Blogger/Album Archive': re.compile(r'Blogger|Album Archive|Picasa', re.I),
}

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

    def to_dict(self) -> dict:
        return {
            'Path': Path(self.path).name,
            'Parts Group': self.parts_group,
            'Service': self.service_guess,
            'Files': self.file_count,
            'Photos': self.photos,
            'Videos': self.videos,
            'JSON': self.json_sidecars,
            'Other': self.other,
            'Size': human_size(self.compressed_size),
        }


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


def derive_parts_group(archive_path: Path) -> str:
    m = PARTS_PAT.match(archive_path.stem)
    if m:
        return m.group('prefix')
    m2 = re.match(r'^(Takeout-\d{8}T\d{6}Z-\w+?)-(?:\d{3,})$', archive_path.stem)
    if m2:
        return m2.group(1)
    return archive_path.stem


# --- Quick validation functions ----------------------------------------------
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
        
        # Determine file type
        if path.is_dir():
            return FileInfo(
                path=path,
                name=path.name,
                size=size,
                status=FileStatus.VALID,
                is_valid=True,
                file_type='directory'
            )
        
        # Check if it's a zip file
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
        
        # Check if it's a tar/tgz file
        elif path.suffix.lower() in {'.tgz', '.gz'} or path.name.lower().endswith('.tar.gz'):
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
        
        # Unsupported file type
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


def validate_zip(path: Path) -> bool:
    """Validate a ZIP file without extracting it."""
    try:
        with zipfile.ZipFile(path, 'r') as zf:
            # Just verify we can open the ZIP - don't read the full file list
            # for large archives as it can be slow. Just check the first entry.
            infolist = zf.infolist()
            if not infolist:
                return False  # Empty ZIP
            # Successfully opened and has at least one file
            return True
    except zipfile.BadZipFile:
        return False
    except Exception as e:
        logger.warning(f"ZIP validation error for {path}: {e}")
        return False


def validate_tar(path: Path) -> bool:
    """Validate a TAR/TGZ file without extracting it."""
    try:
        with tarfile.open(path, 'r:*') as tf:
            # Try to read the member list
            _ = tf.getmembers()
            return True
    except tarfile.TarError:
        return False
    except Exception as e:
        logger.warning(f"TAR validation error for {path}: {e}")
        return False


def scan_archive(path: Path) -> ArchiveSummary:
    try:
        size = path.stat().st_size
    except Exception:
        size = 0

    members: List[str] = []
    try:
        if path.suffix.lower() == '.zip':
            with zipfile.ZipFile(path) as zf:
                members = list(iter_zip_members(zf))
        elif path.suffix.lower() in {'.tgz', '.gz'} or path.name.lower().endswith('.tar.gz'):
            with tarfile.open(path, 'r:*') as tf:
                members = list(iter_tar_members(tf))
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
    return ArchiveSummary(
        path=str(path),
        parts_group=derive_parts_group(path),
        service_guess=svc,
        file_count=len(members),
        photos=photos,
        videos=videos,
        json_sidecars=jsons,
        other=other,
        compressed_size=size,
    )


def scan_directory(path: Path) -> ArchiveSummary:
    """Scan an uncompressed directory and return a summary."""
    try:
        files: List[str] = []
        total_size = 0
        for root, _dirs, filenames in os.walk(path):
            for name in filenames:
                file_path = Path(root) / name
                try:
                    total_size += file_path.stat().st_size
                except Exception:
                    pass
                rel_path = str(file_path.relative_to(path))
                files.append(rel_path)
        
        photos, videos, jsons, other = tally_exts(files)
        svc = guess_service_from_members(files)
        
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
    """Find both archives and Takeout directories."""
    archives: List[Path] = []
    directories: List[Path] = []
    
    if root.is_dir():
        root_contents = list(root.iterdir())
        has_takeout_marker = any(
            'takeout' in item.name.lower() or 
            item.name in {'Google Photos', 'Google Drive', 'Google Maps'}
            for item in root_contents if item.is_dir()
        )
        
        if has_takeout_marker:
            directories.append(root)
            logger.info(f"Root folder appears to be a Takeout directory: {root}")
    
    for dirpath, dirnames, filenames in os.walk(root):
        current_dir = Path(dirpath)
        
        for name in filenames:
            lower = name.lower()
            if lower.endswith('.zip') or lower.endswith('.tgz') or lower.endswith('.tar.gz'):
                archives.append(current_dir / name)
        
        if current_dir == root:
            for dirname in dirnames:
                subdir = current_dir / dirname
                if 'takeout' in dirname.lower():
                    directories.append(subdir)
    
    return sorted(archives), sorted(directories)


# --- Streamlit App -----------------------------------------------------------
def main():
    st.set_page_config(
        page_title="Takeout Scout",
        page_icon="📦",
        layout="wide",
        initial_sidebar_state="expanded"
    )
    
    st.title("📦 Takeout Scout")
    st.markdown("*Google Takeout Scanner - Analyze your archives without extraction*")
    
    # Initialize session state
    if 'results' not in st.session_state:
        st.session_state.results = []
    if 'scanned_paths' not in st.session_state:
        st.session_state.scanned_paths = set()
    if 'pending_files' not in st.session_state:
        st.session_state.pending_files = []  # List of FileInfo objects
    
    # Sidebar for selection
    with st.sidebar:
        st.header("Select Source")
        
        selection_mode = st.radio(
            "Choose how to select:",
            ["Folder", "Files"],
            help="Select a folder to scan everything inside, or choose specific files"
        )
        
        if selection_mode == "Folder":
            folder_path = st.text_input(
                "Folder Path",
                placeholder="Enter folder path or paste from file explorer",
                help="Paste the full path to a folder containing Takeout archives or data"
            )
            
            if folder_path and st.button("📁 Load Folder", type="primary"):
                cleaned_path = clean_file_path(folder_path)
                load_folder(Path(cleaned_path))
        
        else:  # Files mode
            st.info("💡 Enter file paths (one per line) or paste from file explorer")
            file_paths_text = st.text_area(
                "File Paths",
                placeholder="C:\\path\\to\\file1.zip\nC:\\path\\to\\file2.zip",
                height=150,
                help="Paste full paths to ZIP or TGZ files, one per line"
            )
            
            if file_paths_text and st.button("📄 Load Files", type="primary"):
                raw_paths = [line.strip() for line in file_paths_text.split('\n') if line.strip()]
                cleaned_paths = [clean_file_path(p) for p in raw_paths]
                load_files([Path(p) for p in cleaned_paths])
        
        st.divider()
        
        # Bulk actions
        if st.session_state.pending_files:
            st.subheader("Bulk Actions")
            if st.button("🔍 Scan All Pending", type="primary", width="stretch"):
                scan_all_pending()
        
        if st.session_state.results:
            st.success(f"✅ {len(st.session_state.results)} items scanned")
            
            if st.button("🔄 Clear All", width="stretch"):
                st.session_state.results = []
                st.session_state.scanned_paths = set()
                st.session_state.pending_files = []
                st.rerun()
            
            if st.button("💾 Export CSV", width="stretch"):
                export_csv()
    
    # Main area - Show pending files and results
    if st.session_state.pending_files or st.session_state.results:
        
        # Pending files section
        if st.session_state.pending_files:
            st.subheader("📋 Files Ready to Scan")
            display_file_cards()
            st.divider()
        
        # Scanned results section
        if st.session_state.results:
            st.subheader("✅ Scan Results")
            display_results_table()
    
    else:
        show_welcome_screen()


# --- UI Display Functions ----------------------------------------------------
def display_file_cards():
    """Display beautiful cards for each pending file."""
    for idx, file_info in enumerate(st.session_state.pending_files):
        with st.container():
            # Create a nice border using columns
            col1, col2, col3 = st.columns([3, 2, 1])
            
            with col1:
                # File name and type
                icon = get_file_icon(file_info)
                st.markdown(f"### {icon} {file_info.name}")
                st.caption(f"`{file_info.path.parent}`")
            
            with col2:
                # Status and size info
                status_color = get_status_color(file_info.status)
                status_text = get_status_text(file_info)
                st.markdown(f"**Status:** :{status_color}[{status_text}]")
                st.markdown(f"**Size:** {human_size(file_info.size)}")
                st.markdown(f"**Type:** {file_info.file_type or 'Unknown'}")
            
            with col3:
                # Scan button
                if file_info.is_valid and file_info.status != FileStatus.SCANNED:
                    if st.button(f"🔍 Scan", key=f"scan_{idx}", type="primary"):
                        scan_single_file(idx)
                elif not file_info.is_valid:
                    st.error("❌ Invalid")
                else:
                    st.success("✅ Done")
            
            # Error message if any
            if file_info.error_message:
                st.error(f"⚠️ {file_info.error_message}")
            
            st.divider()


def display_results_table():
    """Display the scanned results in a table."""
    df = pd.DataFrame([r.to_dict() for r in st.session_state.results])
    
    # Display interactive table
    st.dataframe(
        df,
        width="stretch",
        hide_index=True,
        column_config={
            "Path": st.column_config.TextColumn("Path", width="large"),
            "Service": st.column_config.TextColumn("Service", width="medium"),
            "Size": st.column_config.TextColumn("Size", width="small"),
        }
    )
    
    # Summary stats
    col1, col2, col3, col4, col5 = st.columns(5)
    total_files = sum(r.file_count for r in st.session_state.results)
    total_photos = sum(r.photos for r in st.session_state.results)
    total_videos = sum(r.videos for r in st.session_state.results)
    total_json = sum(r.json_sidecars for r in st.session_state.results)
    total_size = sum(r.compressed_size for r in st.session_state.results)
    
    col1.metric("Total Files", f"{total_files:,}")
    col2.metric("Photos", f"{total_photos:,}")
    col3.metric("Videos", f"{total_videos:,}")
    col4.metric("JSON", f"{total_json:,}")
    col5.metric("Total Size", human_size(total_size))


def show_welcome_screen():
    """Show welcome screen when no files are loaded."""
    st.info("👈 Select a folder or files from the sidebar to begin")
    
    with st.expander("ℹ️ How to use"):
        st.markdown("""
        **Folder Mode:**
        1. Copy a folder path from File Explorer
        2. Paste it in the 'Folder Path' box
        3. Click 'Load Folder' to validate files
        4. Click individual 'Scan' buttons or 'Scan All Pending'
        
        **Files Mode:**
        1. Select files in File Explorer
        2. Shift+Right-Click and choose 'Copy as path'
        3. Paste into the 'File Paths' box
        4. Click 'Load Files' to validate
        5. Click individual 'Scan' buttons or 'Scan All Pending'
        
        **Features:**
        - Instant file validation
        - Non-destructive scanning (files are never modified)
        - Detects Google Photos, Drive, Maps, and more
        - Supports ZIP and TGZ archives
        - Scans uncompressed Takeout folders
        - Export results to CSV
        """)


def get_file_icon(file_info: FileInfo) -> str:
    """Get an appropriate icon for the file type."""
    if file_info.file_type == 'zip':
        return '📦'
    elif file_info.file_type == 'tgz':
        return '📚'
    elif file_info.file_type == 'directory':
        return '📁'
    else:
        return '📄'


def get_status_color(status: FileStatus) -> str:
    """Get color for status badge."""
    if status == FileStatus.VALID:
        return 'green'
    elif status == FileStatus.INVALID:
        return 'red'
    elif status == FileStatus.SCANNING:
        return 'orange'
    elif status == FileStatus.SCANNED:
        return 'blue'
    elif status == FileStatus.ERROR:
        return 'red'
    else:
        return 'gray'


def get_status_text(file_info: FileInfo) -> str:
    """Get human-readable status text."""
    if file_info.status == FileStatus.VALID:
        return "Valid & Ready"
    elif file_info.status == FileStatus.INVALID:
        return "Invalid File"
    elif file_info.status == FileStatus.SCANNING:
        return "Scanning..."
    elif file_info.status == FileStatus.SCANNED:
        return "Scanned"
    elif file_info.status == FileStatus.ERROR:
        return "Error"
    else:
        return "Pending"


# --- File Loading Functions --------------------------------------------------
def clean_file_path(path_str: str) -> str:
    """Clean up a file path string from various sources."""
    # Remove quotes that Windows adds when you copy as path
    path_str = path_str.strip()
    if path_str.startswith('"') and path_str.endswith('"'):
        path_str = path_str[1:-1]
    # Also handle single quotes
    if path_str.startswith("'") and path_str.endswith("'"):
        path_str = path_str[1:-1]
    return path_str.strip()


def load_folder(folder_path: Path):
    """Load and validate files from a folder."""
    if not folder_path.exists():
        st.error(f"❌ Folder not found: `{folder_path}`")
        st.caption(f"Resolved path: `{folder_path.resolve()}`")
        return
    
    with st.spinner(f"Loading files from {folder_path.name}..."):
        archives, directories = find_archives_and_dirs(folder_path)
        all_items = list(archives) + list(directories)
        
        if not all_items:
            st.warning(f"⚠️ No archives or Takeout directories found in {folder_path}")
            # Still validate the folder itself
            file_info = validate_and_get_info(folder_path)
            st.session_state.pending_files.append(file_info)
            st.rerun()
            return
        
        # Validate all found files
        progress_bar = st.progress(0, text=f"Validating 0/{len(all_items)} files...")
        for i, item in enumerate(all_items, 1):
            file_info = validate_and_get_info(item)
            st.session_state.pending_files.append(file_info)
            progress_bar.progress(i / len(all_items), text=f"Validating {i}/{len(all_items)} files...")
        
        progress_bar.empty()
        st.success(f"✅ Loaded {len(all_items)} files")
        st.rerun()


def load_files(file_paths: List[Path]):
    """Load and validate individual files."""
    if not file_paths:
        st.warning("⚠️ No file paths provided")
        return
    
    valid_count = 0
    
    with st.spinner(f"Validating {len(file_paths)} file(s)..."):
        progress_bar = st.progress(0, text=f"Validating 0/{len(file_paths)} files...")
        
        for i, file_path in enumerate(file_paths, 1):
            # Debug: show what path we're trying to validate
            logger.info(f"Validating path: {file_path} (exists: {file_path.exists()})")
            
            file_info = validate_and_get_info(file_path)
            st.session_state.pending_files.append(file_info)
            if file_info.is_valid:
                valid_count += 1
            progress_bar.progress(i / len(file_paths), text=f"Validating {i}/{len(file_paths)} files...")
        
        progress_bar.empty()
        
        if valid_count == 0:
            st.error(f"❌ No valid files found")
            # Show the first error for debugging
            if st.session_state.pending_files:
                first = st.session_state.pending_files[-len(file_paths)]
                if first.error_message:
                    st.caption(f"First error: {first.error_message}")
                st.caption(f"Path tried: `{first.path}`")
        elif valid_count < len(file_paths):
            st.warning(f"⚠️ Loaded {valid_count}/{len(file_paths)} valid files")
        else:
            st.success(f"✅ All {valid_count} files are valid")
        
        st.rerun()


def scan_single_file(index: int):
    """Scan a single file from the pending list."""
    file_info = st.session_state.pending_files[index]
    
    with st.spinner(f"Scanning {file_info.name}..."):
        try:
            if file_info.file_type == 'directory':
                summary = scan_directory(file_info.path)
            else:
                summary = scan_archive(file_info.path)
            
            st.session_state.results.append(summary)
            st.session_state.scanned_paths.add(str(file_info.path))
            
            # Update status
            file_info.status = FileStatus.SCANNED
            st.session_state.pending_files[index] = file_info
            
            st.success(f"✅ Scanned {file_info.name}")
            st.rerun()
            
        except Exception as e:
            logger.exception(f"Failed to scan {file_info.path}: {e}")
            file_info.status = FileStatus.ERROR
            file_info.error_message = str(e)
            st.session_state.pending_files[index] = file_info
            st.error(f"❌ Error scanning {file_info.name}: {e}")


def scan_all_pending():
    """Scan all pending valid files."""
    valid_files = [
        (i, f) for i, f in enumerate(st.session_state.pending_files)
        if f.is_valid and f.status != FileStatus.SCANNED
    ]
    
    if not valid_files:
        st.warning("No files to scan")
        return
    
    progress_bar = st.progress(0, text=f"Scanning 0/{len(valid_files)} files...")
    
    for count, (index, file_info) in enumerate(valid_files, 1):
        try:
            if file_info.file_type == 'directory':
                summary = scan_directory(file_info.path)
            else:
                summary = scan_archive(file_info.path)
            
            st.session_state.results.append(summary)
            st.session_state.scanned_paths.add(str(file_info.path))
            file_info.status = FileStatus.SCANNED
            st.session_state.pending_files[index] = file_info
            
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
    
    with st.spinner(f"Scanning {folder_path.name}..."):
        archives, directories = find_archives_and_dirs(folder_path)
        total = len(archives) + len(directories)
        
        if total == 0:
            st.warning(f"⚠️ No Takeout archives or directories found in {folder_path}")
            # Still show the folder with basic stats
            summary = scan_directory(folder_path)
            if summary.file_count > 0:
                st.session_state.results.append(summary)
                st.session_state.scanned_paths.add(str(folder_path))
            return
        
        progress_bar = st.progress(0, text=f"Scanning 0/{total} items...")
        
        count = 0
        for directory in directories:
            if str(directory) not in st.session_state.scanned_paths:
                summary = scan_directory(directory)
                st.session_state.results.append(summary)
                st.session_state.scanned_paths.add(str(directory))
            count += 1
            progress_bar.progress(count / total, text=f"Scanning {count}/{total} items...")
        
        for archive in archives:
            if str(archive) not in st.session_state.scanned_paths:
                summary = scan_archive(archive)
                st.session_state.results.append(summary)
                st.session_state.scanned_paths.add(str(archive))
            count += 1
            progress_bar.progress(count / total, text=f"Scanning {count}/{total} items...")
        
        progress_bar.empty()
        st.success(f"✅ Scanned {total} items from {folder_path.name}")
        st.rerun()


def process_files(file_paths: List[Path]):
    """Process a list of specific files."""
    valid_files = [f for f in file_paths if f.exists()]
    
    if not valid_files:
        st.error("❌ No valid files found")
        return
    
    invalid = len(file_paths) - len(valid_files)
    if invalid > 0:
        st.warning(f"⚠️ Skipped {invalid} invalid path(s)")
    
    with st.spinner(f"Scanning {len(valid_files)} file(s)..."):
        progress_bar = st.progress(0, text=f"Scanning 0/{len(valid_files)} files...")
        
        for i, file_path in enumerate(valid_files, 1):
            if str(file_path) not in st.session_state.scanned_paths:
                if file_path.is_file():
                    summary = scan_archive(file_path)
                else:
                    summary = scan_directory(file_path)
                st.session_state.results.append(summary)
                st.session_state.scanned_paths.add(str(file_path))
            
            progress_bar.progress(i / len(valid_files), text=f"Scanning {i}/{len(valid_files)} files...")
        
        progress_bar.empty()
        st.success(f"✅ Scanned {len(valid_files)} file(s)")
        st.rerun()


def export_csv():
    """Export results to CSV."""
    if not st.session_state.results:
        st.warning("No results to export")
        return
    
    df = pd.DataFrame([r.to_dict() for r in st.session_state.results])
    timestamp = datetime.now().strftime('%Y%m%d-%H%M%S')
    filename = f'takeout_scout_summary_{timestamp}.csv'
    
    csv = df.to_csv(index=False)
    st.sidebar.download_button(
        label="⬇️ Download CSV",
        data=csv,
        file_name=filename,
        mime='text/csv',
        type="primary"
    )


if __name__ == '__main__':
    main()
