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
License: GNU GPL v3
"""
from __future__ import annotations

import json
import os
import re
import tarfile
import zipfile
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import Dict, List, Optional, Tuple

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
    summarize_sources,
)
from takeout_scout.utils import partition_known_paths, remove_dirs, unreferenced_dirs
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


@dataclass
class DeepScanResult:
    """Results from deep analysis of a Takeout archive's folder structure."""
    path: str

    # Photo/JSON pairing analysis
    paired_photos: int  # Photos with matching JSON
    unpaired_photos: int  # Photos without JSON
    orphaned_json: int  # JSON without matching photo

    # Folder organization
    organization_type: str  # 'by_year', 'by_album', 'by_date', 'flat', 'mixed'
    folder_structure: Dict[str, int]  # Folder path -> file count
    year_distribution: Dict[str, int]  # Year -> count

    # Content details
    albums: List[str]  # Album names found
    date_range: Optional[Tuple[str, str]]  # Earliest, Latest dates

    # Issues found
    issues: List[str]  # List of problems discovered

    def to_dict(self) -> dict:
        return {
            'Path': Path(self.path).name,
            'Paired Photos': self.paired_photos,
            'Unpaired Photos': self.unpaired_photos,
            'Orphaned JSON': self.orphaned_json,
            'Organization': self.organization_type,
            'Folders': len(self.folder_structure),
            'Years': ', '.join(sorted(self.year_distribution.keys())) if self.year_distribution else 'Unknown',
            'Albums': len(self.albums),
            'Date Range': f"{self.date_range[0]} to {self.date_range[1]}" if self.date_range else 'Unknown',
            'Issues': len(self.issues),
        }


# --- State (persistent index) -----------------------------------------------
STATE_DIR = Path('state')
STATE_DIR.mkdir(parents=True, exist_ok=True)
RECENT_FOLDERS_PATH = STATE_DIR / 'recent_folders.json'
MAX_RECENT_FOLDERS = 10


# --- Recent folders helpers --------------------------------------------------
def load_recent_folders() -> List[str]:
    """Load list of recently accessed folders."""
    if RECENT_FOLDERS_PATH.exists():
        try:
            with open(RECENT_FOLDERS_PATH, 'r', encoding='utf-8') as f:
                folders = json.load(f)
                # Filter to only existing folders
                return [f for f in folders if Path(f).exists()]
        except Exception:
            logger.warning('Recent folders file unreadable; starting fresh.')
    return []


def save_recent_folders(folders: List[str]) -> None:
    """Save list of recent folders."""
    try:
        with open(RECENT_FOLDERS_PATH, 'w', encoding='utf-8') as f:
            json.dump(folders, f, indent=2)
    except Exception as e:
        logger.exception(f'Failed to save recent folders: {e}')


def add_recent_folder(folder_path: str) -> None:
    """Add a folder to recent folders list."""
    recent = st.session_state.recent_folders
    folder_path = str(Path(folder_path).resolve())
    
    # Remove if already in list
    if folder_path in recent:
        recent.remove(folder_path)
    
    # Add to front
    recent.insert(0, folder_path)
    
    # Keep only MAX_RECENT_FOLDERS
    recent = recent[:MAX_RECENT_FOLDERS]
    
    st.session_state.recent_folders = recent
    save_recent_folders(recent)


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
        st.session_state.pending_files = []  # List of FileInfo objects
    if 'compute_hashes' not in st.session_state:
        st.session_state.compute_hashes = False
    if 'hash_index' not in st.session_state:
        st.session_state.hash_index = HashIndex()
    if 'parse_sidecars' not in st.session_state:
        st.session_state.parse_sidecars = True  # Default on since it's so useful
    if 'recent_folders' not in st.session_state:
        st.session_state.recent_folders = load_recent_folders()
    if 'current_browse_path' not in st.session_state:
        st.session_state.current_browse_path = Path.home()
    if 'deep_scan_results' not in st.session_state:
        st.session_state.deep_scan_results = {}  # path -> DeepScanResult
    if 'upload_dirs' not in st.session_state:
        st.session_state.upload_dirs = []  # Temp dirs from previous uploads, pending cleanup

    # Sidebar for controls
    with st.sidebar:
        st.header("📂 Input")

        # Create tabs for different selection methods
        tab1, tab2, tab3 = st.tabs(["📁 Browse", "📋 Paste", "⬆️ Upload"])
        
        with tab1:
            st.markdown("**Browse for folder:**")
            
            # Recent folders quick access
            if st.session_state.recent_folders:
                recent_folder = st.selectbox(
                    "Recent folders:",
                    options=['-- Select recent --'] + st.session_state.recent_folders,
                    key='recent_select'
                )
                if recent_folder != '-- Select recent --':
                    st.session_state.current_browse_path = Path(recent_folder)
            
            # Manual path entry with current path display
            folder_path = st.text_input(
                "Folder Path",
                value=str(st.session_state.current_browse_path),
                placeholder="Enter or edit folder path",
                help="Type or paste a folder path"
            )
            
            col1, col2 = st.columns(2)
            with col1:
                if st.button("📁 Load Folder", type="primary", width="stretch"):
                    if folder_path:
                        cleaned_path = clean_file_path(folder_path)
                        path_obj = Path(cleaned_path)
                        if path_obj.exists() and path_obj.is_dir():
                            add_recent_folder(str(path_obj))
                            load_folder(path_obj)
                        else:
                            st.error("Invalid folder path")
            
            with col2:
                # Parent directory navigation
                if st.button("⬆️ Up", width="stretch"):
                    current = Path(folder_path) if folder_path else st.session_state.current_browse_path
                    if current.parent != current:
                        st.session_state.current_browse_path = current.parent
                        st.rerun()
            
            # Browse current directory
            if folder_path:
                browse_path = Path(folder_path)
                if browse_path.exists() and browse_path.is_dir():
                    with st.expander("📂 Browse subfolders", expanded=False):
                        try:
                            subdirs = [d for d in browse_path.iterdir() if d.is_dir()]
                            if subdirs:
                                for subdir in sorted(subdirs)[:20]:  # Limit to 20
                                    if st.button(f"📁 {subdir.name}", key=f"sub_{subdir}", width="stretch"):
                                        st.session_state.current_browse_path = subdir
                                        st.rerun()
                                if len(subdirs) > 20:
                                    st.caption(f"... and {len(subdirs) - 20} more")
                            else:
                                st.caption("No subfolders")
                        except PermissionError:
                            st.warning("⚠️ Permission denied")
        
        with tab2:
            st.markdown("**Paste paths manually:**")
            
            paste_mode = st.radio(
                "Type:",
                ["Folder", "Files"],
                horizontal=True,
                label_visibility="collapsed"
            )
            
            if paste_mode == "Folder":
                folder_path_paste = st.text_input(
                    "Folder Path",
                    placeholder="Paste folder path from file explorer",
                    help="Right-click folder → Copy as path",
                    key="paste_folder"
                )
                if folder_path_paste and st.button("📁 Load Pasted Folder", type="primary", width="stretch"):
                    cleaned_path = clean_file_path(folder_path_paste)
                    path_obj = Path(cleaned_path)
                    if path_obj.exists():
                        add_recent_folder(str(path_obj))
                    load_folder(path_obj)
            else:
                st.info("💡 One path per line")
                file_paths_text = st.text_area(
                    "File Paths",
                    placeholder="C:\\path\\to\\file1.zip\nC:\\path\\to\\file2.zip",
                    height=150,
                    help="Shift+Right-click files → Copy as path",
                    key="paste_files"
                )
                if file_paths_text and st.button("📄 Load Pasted Files", type="primary", width="stretch"):
                    raw_paths = [line.strip() for line in file_paths_text.split('\n') if line.strip()]
                    cleaned_paths = [clean_file_path(p) for p in raw_paths]
                    load_files([Path(p) for p in cleaned_paths])
        
        with tab3:
            st.markdown("**Upload archives:**")
            st.info("⚠️ Large files may take time to upload")
            
            uploaded_files = st.file_uploader(
                "Choose ZIP/TGZ files",
                type=['zip', 'tgz', 'tar.gz'],
                accept_multiple_files=True,
                help="Select one or more archive files to upload and scan",
                label_visibility="collapsed"
            )
            
            if uploaded_files:
                st.write(f"Selected: {len(uploaded_files)} file(s)")
                if st.button("⬆️ Process Uploads", type="primary", width="stretch"):
                    process_uploaded_files(uploaded_files)

        st.divider()

        # Scan options
        st.header("⚙️ Options")
        st.session_state.parse_sidecars = st.checkbox(
            "Parse JSON sidecars",
            value=st.session_state.parse_sidecars,
            help="Extract authoritative timestamps from Google Photos JSON files (recommended)"
        )
        st.session_state.compute_hashes = st.checkbox(
            "Compute file hashes",
            value=st.session_state.compute_hashes,
            help="Calculate MD5 hashes for duplicate detection (slower but enables duplicate analysis)"
        )

        st.divider()

        # Bulk actions
        valid_count = sum(1 for f in st.session_state.pending_files if f.is_valid and f.status != FileStatus.SCANNED)
        if valid_count > 0:
            st.subheader("Bulk Actions")
            if st.button(f"⚡ Scan All Pending ({valid_count})", type="primary", width="stretch"):
                scan_all_pending()

        if st.session_state.results:
            st.divider()
            st.success(f"✅ {len(st.session_state.results)} items scanned")
            st.header("📊 Export")
            export_csv()

            if st.button("🗑️ Clear Results", width="stretch"):
                st.session_state.results = []
                st.session_state.scanned_paths = set()
                st.session_state.pending_files = []
                st.session_state.hash_index = HashIndex()
                st.session_state.parse_sidecars = True
                st.session_state.deep_scan_results = {}
                remove_dirs(st.session_state.upload_dirs)
                st.session_state.upload_dirs = []
                st.rerun()

    # Main content area
    if st.session_state.pending_files:
        st.subheader("📋 Files Ready to Scan")
        display_file_cards()
        st.divider()

    if st.session_state.results:
        st.subheader("✅ Scan Results")
        display_results_table()
    elif not st.session_state.pending_files:
        show_welcome_screen()

    # Show date analysis if sidecars were parsed
    if st.session_state.parse_sidecars and st.session_state.results:
        show_date_analysis()

    # Show duplicate analysis if hashes were computed
    if st.session_state.compute_hashes:
        show_duplicate_analysis()

    # Show timeline analysis
    if st.session_state.results:
        show_timeline_analysis()

    # Show orphan analysis if sidecars were parsed
    if st.session_state.parse_sidecars and st.session_state.results:
        show_orphan_analysis()

    # Show cross-archive analysis if multiple archives
    if len(st.session_state.results) > 1 and st.session_state.compute_hashes:
        show_cross_archive_analysis()

    # Show full inventory
    if st.session_state.results:
        show_full_inventory()


# --- UI Display Functions ----------------------------------------------------
def display_file_cards():
    """Display cards for each pending file, with scan/ignore actions."""
    for idx, file_info in enumerate(st.session_state.pending_files):
        with st.container():
            col1, col2, col3 = st.columns([3, 2, 1])

            with col1:
                icon = get_file_icon(file_info)
                st.markdown(f"### {icon} {file_info.name}")
                st.caption(f"`{file_info.path.parent}`")

            with col2:
                status_color = get_status_color(file_info.status)
                status_text = get_status_text(file_info)
                st.markdown(f"**Status:** :{status_color}[{status_text}]")
                st.markdown(f"**Size:** {human_size(file_info.size)}")
                st.markdown(f"**Type:** {file_info.file_type or 'Unknown'}")

            with col3:
                if file_info.is_valid and file_info.status != FileStatus.SCANNED:
                    btn_col1, btn_col2 = st.columns(2)
                    with btn_col1:
                        if st.button("🔍", key=f"scan_{idx}", type="primary", help="Scan this file", width="stretch"):
                            scan_single_file(idx)
                    with btn_col2:
                        if st.button("🚫", key=f"ignore_{idx}", help="Ignore this file", width="stretch"):
                            ignore_file(idx)
                elif not file_info.is_valid:
                    st.error("❌ Invalid")
                else:
                    st.success("✅ Done")

            if file_info.error_message:
                st.error(f"⚠️ {file_info.error_message}")

            st.divider()


def display_results_table():
    """Display the scanned results in a table, with optional deep scan analysis."""
    df = pd.DataFrame([r.to_dict() for r in st.session_state.results])

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

    # Deep scan section
    st.divider()
    st.subheader("🔬 Deep Scan Analysis")

    for idx, result in enumerate(st.session_state.results):
        result_path = result.path
        has_deep_scan = result_path in st.session_state.deep_scan_results

        with st.container():
            col1, col2 = st.columns([4, 1])
            with col1:
                st.markdown(f"**{Path(result_path).name}**")
            with col2:
                if not has_deep_scan:
                    if st.button("🔬 Deep Scan", key=f"deep_{idx}", width="stretch"):
                        perform_deep_scan(result_path, idx)
                else:
                    st.success("✅ Analyzed")

            if has_deep_scan:
                display_deep_scan_card(st.session_state.deep_scan_results[result_path])

            st.divider()


def show_welcome_screen():
    """Show welcome screen when no files are loaded."""
    st.info("👈 Select a folder or files from the sidebar to begin")

    with st.expander("ℹ️ How to use"):
        st.markdown("""
        **Browse tab:**
        1. Pick a recent folder or type/paste a path
        2. Click 'Load Folder' to find archives and Takeout directories
        3. Click individual scan buttons or 'Scan All Pending'

        **Paste tab:**
        1. Copy a folder path, or Shift+Right-Click files and choose 'Copy as path'
        2. Paste into the box and load

        **Upload tab:**
        1. Choose ZIP/TGZ files to upload directly
        2. Click 'Process Uploads'

        **Features:**
        - Instant file validation
        - Non-destructive scanning (files are never modified)
        - JSON sidecar parsing for authoritative photo dates
        - Optional hash-based duplicate detection
        - Detects Google Photos, Drive, Maps, and more
        - Supports ZIP and TGZ archives, and uncompressed Takeout folders
        - Deep scan for folder/album organization and pairing analysis
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

    with st.spinner(f"Searching {folder_path.name}..."):
        archives, directories = find_archives_and_dirs(folder_path)
    all_items = list(archives) + list(directories)

    if not all_items:
        st.warning(f"⚠️ No archives or Takeout directories found in {folder_path}")
        # Still validate the folder itself
        file_info = validate_and_get_info(folder_path)
        st.session_state.pending_files.append(file_info)
        st.rerun()
        return

    known = set(st.session_state.scanned_paths)
    known.update(str(f.path) for f in st.session_state.pending_files)
    new_items, already = partition_known_paths(all_items, known)

    if already:
        st.info(f"ℹ️ Skipped {len(already)} item(s) already queued or already scanned")

    if not new_items:
        return

    progress_bar = st.progress(0, text=f"Validating 0/{len(new_items)} files...")
    for i, item in enumerate(new_items, 1):
        file_info = validate_and_get_info(item)
        st.session_state.pending_files.append(file_info)
        progress_bar.progress(i / len(new_items), text=f"Validating {i}/{len(new_items)} files...")

    progress_bar.empty()
    st.success(f"✅ Loaded {len(new_items)} files")
    st.rerun()


def load_files(file_paths: List[Path]):
    """Load and validate individual files."""
    if not file_paths:
        st.warning("⚠️ No file paths provided")
        return

    known = set(st.session_state.scanned_paths)
    known.update(str(f.path) for f in st.session_state.pending_files)
    new_items, already = partition_known_paths(file_paths, known)

    if already:
        st.info(f"ℹ️ Skipped {len(already)} file(s) already queued or already scanned")

    if not new_items:
        return

    valid_count = 0
    first_appended = None

    with st.spinner(f"Validating {len(new_items)} file(s)..."):
        progress_bar = st.progress(0, text=f"Validating 0/{len(new_items)} files...")

        for i, file_path in enumerate(new_items, 1):
            file_info = validate_and_get_info(file_path)
            st.session_state.pending_files.append(file_info)
            if first_appended is None:
                first_appended = file_info
            if file_info.is_valid:
                valid_count += 1
            progress_bar.progress(i / len(new_items), text=f"Validating {i}/{len(new_items)} files...")

        progress_bar.empty()

        if valid_count == 0:
            st.error("❌ No valid files found")
            if first_appended is not None:
                if first_appended.error_message:
                    st.caption(f"First error: {first_appended.error_message}")
                st.caption(f"Path tried: `{first_appended.path}`")
        elif valid_count < len(new_items):
            st.warning(f"⚠️ Loaded {valid_count}/{len(new_items)} valid files")
        else:
            st.success(f"✅ All {valid_count} files are valid")

        st.rerun()


def ignore_file(index: int):
    """Remove a file from the pending list without scanning it."""
    file_info = st.session_state.pending_files[index]
    logger.info(f"Ignoring file: {file_info.path}")
    st.session_state.pending_files.pop(index)
    st.success(f"🚫 Ignored {file_info.name}")
    st.rerun()


def scan_single_file(index: int):
    """Scan a single file from the pending list."""
    file_info = st.session_state.pending_files[index]
    file_info.status = FileStatus.SCANNING
    st.session_state.pending_files[index] = file_info

    compute_hashes = st.session_state.compute_hashes
    parse_sidecars = st.session_state.parse_sidecars

    try:
        with st.spinner(f"Scanning {file_info.name}..."):
            if file_info.file_type == 'directory':
                summary = scan_directory(file_info.path, compute_hashes=compute_hashes, parse_sidecars=parse_sidecars)
            else:
                summary = scan_archive(file_info.path, compute_hashes=compute_hashes, parse_sidecars=parse_sidecars)

        st.session_state.results.append(summary)
        st.session_state.scanned_paths.add(str(file_info.path))

        # Update hash index if hashes were computed
        if compute_hashes:
            _update_hash_index(file_info.path)

        # Remove from pending files after a successful scan
        st.session_state.pending_files.pop(index)

        st.success(f"✅ Scanned {file_info.name}")
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
        discovery = load_takeout_discovery(path)
        if discovery is None:
            return

        for file_detail in discovery.file_details:
            file_hash = file_detail.get('file_hash')
            if file_hash:
                st.session_state.hash_index.add(
                    file_hash,
                    discovery.source_path,
                    file_detail.get('path'),
                    file_detail.get('size', 0),
                )
    except Exception as e:
        logger.warning(f"Failed to update hash index: {e}")


def scan_all_pending():
    """Scan all pending valid files."""
    valid_files = [
        f for f in st.session_state.pending_files
        if f.is_valid and f.status != FileStatus.SCANNED
    ]

    if not valid_files:
        st.warning("No files to scan")
        return

    compute_hashes = st.session_state.compute_hashes
    parse_sidecars = st.session_state.parse_sidecars
    progress_bar = st.progress(0, text=f"Scanning 0/{len(valid_files)} files...")

    scanned_count = 0
    error_count = 0

    for count, file_info in enumerate(valid_files, 1):
        try:
            if file_info.file_type == 'directory':
                summary = scan_directory(file_info.path, compute_hashes=compute_hashes, parse_sidecars=parse_sidecars)
            else:
                summary = scan_archive(file_info.path, compute_hashes=compute_hashes, parse_sidecars=parse_sidecars)

            st.session_state.results.append(summary)
            st.session_state.scanned_paths.add(str(file_info.path))

            if compute_hashes:
                _update_hash_index(file_info.path)

            # Remove from pending files after a successful scan
            st.session_state.pending_files.remove(file_info)
            scanned_count += 1

        except Exception as e:
            logger.exception(f"Failed to scan {file_info.path}: {e}")
            file_info.status = FileStatus.ERROR
            file_info.error_message = str(e)
            # Keep error files in the list but update their status
            for i, f in enumerate(st.session_state.pending_files):
                if f.path == file_info.path:
                    st.session_state.pending_files[i] = file_info
                    break
            error_count += 1

        progress_bar.progress(count / len(valid_files), text=f"Completed {count}/{len(valid_files)} files")

    progress_bar.empty()

    if error_count > 0:
        st.warning(f"✅ Scanned {scanned_count} files, ⚠️ {error_count} errors")
    else:
        st.success(f"✅ Scanned {scanned_count} files")
    st.rerun()


# --- Deep Scan Functions -----------------------------------------------------
def deep_scan_archive(path: Path, progress_callback=None) -> DeepScanResult:
    """Perform deep analysis of an archive's structure and contents."""
    from takeout_scout.scanner import iter_zip_members, iter_tar_members

    if progress_callback:
        progress_callback("Starting deep scan...", 0.1)

    members: List[str] = []
    try:
        if path.suffix.lower() == '.zip':
            with zipfile.ZipFile(path) as zf:
                members = list(iter_zip_members(zf))
        elif path.suffix.lower() in {'.tgz', '.gz'} or path.name.lower().endswith('.tar.gz'):
            with tarfile.open(path, 'r:*') as tf:
                members = list(iter_tar_members(tf))
    except Exception as e:
        logger.exception(f"Failed to read archive for deep scan: {e}")
        return DeepScanResult(
            path=str(path),
            paired_photos=0, unpaired_photos=0, orphaned_json=0,
            organization_type='error',
            folder_structure={}, year_distribution={},
            albums=[], date_range=None,
            issues=[f"Failed to read archive: {e}"]
        )

    if progress_callback:
        progress_callback(f"Analyzing {len(members)} files...", 0.3)

    return analyze_file_structure(members, str(path), progress_callback)


def deep_scan_directory(path: Path, progress_callback=None) -> DeepScanResult:
    """Perform deep analysis of a directory's structure and contents."""
    if progress_callback:
        progress_callback("Collecting files...", 0.1)

    members: List[str] = []
    for root, _dirs, filenames in os.walk(path):
        for name in filenames:
            file_path = Path(root) / name
            rel_path = str(file_path.relative_to(path))
            members.append(rel_path)

    if progress_callback:
        progress_callback(f"Analyzing {len(members)} files...", 0.3)

    return analyze_file_structure(members, str(path), progress_callback)


def analyze_file_structure(members: List[str], base_path: str, progress_callback=None) -> DeepScanResult:
    """Analyze the structure and organization of files."""
    from collections import defaultdict
    from takeout_scout.constants import MEDIA_PHOTO_EXT

    photos: Dict[str, str] = {}
    jsons: Dict[str, str] = {}

    folder_structure: Dict[str, int] = defaultdict(int)
    year_distribution: Dict[str, int] = defaultdict(int)
    albums = set()
    dates = []
    issues = []

    date_pattern = re.compile(r'(\d{4})[_-]?(\d{2})[_-]?(\d{2})')
    year_pattern = re.compile(r'/(\d{4})/')

    for idx, member in enumerate(members):
        if progress_callback and idx % 500 == 0:
            progress_callback(f"Analyzing file {idx}/{len(members)}...", 0.3 + (idx / len(members)) * 0.5)

        # Normalize separators up front: zip members always use '/', but a
        # directory scan on Windows hands back backslashes, and folder.split('/')
        # plus year_pattern below both assume '/'. Path(member).parent is not
        # enough by itself on Windows: WindowsPath.__str__ re-emits '\\'
        # regardless of the separator it was built from, so the derived
        # `folder` needs the same normalization as `member`.
        member = member.replace("\\", "/")

        member_lower = member.lower()
        folder = str(Path(member).parent).replace("\\", "/")
        folder_structure[folder] += 1

        folder_parts = folder.split('/')
        for part in folder_parts:
            # '.' is what Path(...).parent gives a root-level member — the
            # absence of a folder, not a folder named '.' — so skip it like
            # an empty part rather than let it register as an album.
            if part in ("", "."):
                continue
            if part and not part.isdigit() and part.lower() not in {'google photos', 'takeout', 'photos', 'archive'}:
                if not re.match(r'^\d{4}$', part):
                    albums.add(part)

        year_match = year_pattern.search(member)
        if year_match:
            year_distribution[year_match.group(1)] += 1

        date_match = date_pattern.search(member)
        if date_match:
            try:
                date_str = f"{date_match.group(1)}-{date_match.group(2)}-{date_match.group(3)}"
                dates.append(date_str)
            except Exception:
                pass

        if any(member_lower.endswith(ext) for ext in MEDIA_PHOTO_EXT):
            base = Path(member).stem
            photos[f"{folder}/{base}"] = member
        elif member_lower.endswith('.json'):
            base = Path(member).stem
            if '.' in base:
                base = base.rsplit('.', 1)[0]
            jsons[f"{folder}/{base}"] = member

    if progress_callback:
        progress_callback("Analyzing pairing and organization...", 0.9)

    photo_bases = set(photos.keys())
    json_bases = set(jsons.keys())

    paired = photo_bases & json_bases
    unpaired_photos = photo_bases - json_bases
    orphaned_json = json_bases - photo_bases

    org_type = detect_organization_type(folder_structure, year_distribution, albums)

    date_range = None
    if dates:
        sorted_dates = sorted(dates)
        date_range = (sorted_dates[0], sorted_dates[-1])

    if len(unpaired_photos) > len(paired) * 0.5:
        issues.append(f"Many photos missing JSON metadata ({len(unpaired_photos)} unpaired)")

    if len(orphaned_json) > 100:
        issues.append(f"{len(orphaned_json)} JSON files without matching photos")

    if len(folder_structure) > 1000:
        issues.append(f"Highly fragmented: {len(folder_structure)} folders")

    if not year_distribution:
        issues.append("No year information found in folder structure")

    return DeepScanResult(
        path=base_path,
        paired_photos=len(paired),
        unpaired_photos=len(unpaired_photos),
        orphaned_json=len(orphaned_json),
        organization_type=org_type,
        folder_structure=dict(folder_structure),
        year_distribution=dict(year_distribution),
        albums=sorted(list(albums))[:50],
        date_range=date_range,
        issues=issues
    )


def detect_organization_type(folder_structure: Dict[str, int], year_dist: Dict[str, int], albums: set) -> str:
    """Detect how the content is organized."""
    if not folder_structure or len(folder_structure) == 1:
        return 'flat'

    year_folders = sum(1 for folder in folder_structure.keys() if any(year in folder for year in year_dist.keys()))
    if year_folders > len(folder_structure) * 0.5:
        return 'by_year'

    if len(albums) > 5 and len(folder_structure) > 3:
        return 'by_album'

    date_folders = sum(1 for folder in folder_structure.keys() if re.search(r'\d{4}-\d{2}-\d{2}', folder))
    if date_folders > len(folder_structure) * 0.3:
        return 'by_date'

    return 'mixed'


def perform_deep_scan(file_path: str, index: int):
    """Perform deep scan on a previously scanned file."""
    path = Path(file_path)

    progress_bar = st.progress(0, text=f"Deep scanning {path.name}...")
    status_text = st.empty()

    def update_progress(message: str, progress: float):
        progress_bar.progress(progress, text=message)
        status_text.text(message)

    try:
        if path.is_file():
            result = deep_scan_archive(path, progress_callback=update_progress)
        else:
            result = deep_scan_directory(path, progress_callback=update_progress)

        st.session_state.deep_scan_results[file_path] = result

        progress_bar.empty()
        status_text.empty()
        st.success(f"🔬 Deep scan complete for {path.name}")
        st.rerun()

    except Exception as e:
        logger.exception(f"Failed to deep scan {file_path}: {e}")
        progress_bar.empty()
        status_text.empty()
        st.error(f"❌ Deep scan failed: {e}")


def build_folder_tree(folder_paths: List[str]) -> dict:
    """Build a hierarchical tree structure from folder paths."""
    tree: dict = {}

    for path in sorted(folder_paths):
        parts = path.split('/') if '/' in path else path.split('\\')
        current = tree

        for part in parts:
            if part:
                if part not in current:
                    current[part] = {}
                current = current[part]

    return tree


def display_folder_tree(tree: dict, indent: int = 0):
    """Display folder tree in a hierarchical format."""
    for name, subtree in sorted(tree.items()):
        prefix = "  " * indent + ("└─ " if indent > 0 else "📁 ")
        st.markdown(f"`{prefix}{name}`")
        if subtree:
            display_folder_tree(subtree, indent + 1)


def display_deep_scan_card(result: DeepScanResult):
    """Display detailed deep scan results in an expandable card."""
    with st.expander("📊 View Deep Scan Details", expanded=True):
        st.markdown("#### 📷 Photo & Metadata Pairing")
        col1, col2, col3 = st.columns(3)
        col1.metric("✅ Paired Photos", f"{result.paired_photos:,}",
                   help="Photos with matching JSON metadata files")
        col2.metric("📸 Unpaired Photos", f"{result.unpaired_photos:,}",
                   help="Photos without JSON metadata")
        col3.metric("🗒️ Orphaned JSON", f"{result.orphaned_json:,}",
                   help="JSON files without matching photos")

        st.markdown("#### 📁 Folder Organization")
        col1, col2 = st.columns(2)
        with col1:
            st.markdown(f"**Type:** `{result.organization_type}`")
            st.markdown(f"**Total Folders:** {len(result.folder_structure):,}")
        with col2:
            if result.date_range:
                st.markdown("**Date Range:**")
                st.caption(f"{result.date_range[0]} → {result.date_range[1]}")

        if result.year_distribution:
            st.markdown("#### 📅 Content by Year")
            year_data = pd.DataFrame([
                {"Year": year, "Files": count}
                for year, count in sorted(result.year_distribution.items())
            ])
            st.bar_chart(year_data.set_index("Year"))

        if result.albums:
            st.markdown(f"#### 📚 Albums ({len(result.albums)})")
            if len(result.albums) <= 10:
                for album in result.albums:
                    st.markdown(f"- {album}")
            else:
                with st.expander(f"Show all {len(result.albums)} albums"):
                    for album in result.albums:
                        st.markdown(f"- {album}")

        if result.folder_structure:
            st.markdown("#### 📂 Folder Structure")

            top_folders = sorted(result.folder_structure.items(), key=lambda x: x[1], reverse=True)[:10]
            folder_df = pd.DataFrame(top_folders, columns=["Folder", "Files"])
            st.dataframe(folder_df, hide_index=True, width="stretch")

            if len(result.folder_structure) > 10:
                with st.expander(f"📂 Show all {len(result.folder_structure)} folders"):
                    folder_tree = build_folder_tree(list(result.folder_structure.keys()))
                    display_folder_tree(folder_tree)

        if result.issues:
            st.markdown("#### ⚠️ Issues Detected")
            for issue in result.issues:
                st.warning(issue)



def show_date_analysis():
    """Display date recovery analysis from JSON sidecars."""
    from datetime import datetime
    from takeout_scout.sidecar import DateComparison, DateComparisonSummary
    
    # Gather date statistics from all discoveries
    total_media = 0
    with_sidecar = 0
    with_photo_taken_time = 0
    with_creation_time = 0
    all_dates = []
    missing_dates = []
    
    # EXIF vs Sidecar comparison data
    comparisons = []
    
    for result in st.session_state.results:
        try:
            discovery = load_takeout_discovery(Path(result.path))
            if not discovery:
                continue
            
            # Process the flat file list
            for fd in discovery.iter_file_details():
                if fd.file_type in ('photo', 'video'):
                    total_media += 1

                    # Parse dates
                    sidecar_dt = None
                    exif_dt = None

                    if fd.sidecar_path:
                        with_sidecar += 1

                    if fd.photo_taken_time:
                        with_photo_taken_time += 1
                        try:
                            sidecar_dt = datetime.fromisoformat(fd.photo_taken_time)
                            all_dates.append(sidecar_dt)
                        except ValueError:
                            pass
                    elif fd.creation_time:
                        with_creation_time += 1
                        try:
                            sidecar_dt = datetime.fromisoformat(fd.creation_time)
                        except ValueError:
                            pass

                    # Get EXIF date if available
                    if fd.metadata and fd.metadata.get('datetime_original'):
                        try:
                            exif_str = fd.metadata['datetime_original']
                            # EXIF format: "2019:07:15 14:00:05"
                            exif_dt = datetime.strptime(exif_str, "%Y:%m:%d %H:%M:%S")
                        except (ValueError, TypeError):
                            pass

                    # Create comparison
                    diff_seconds = None
                    if exif_dt and sidecar_dt:
                        # Make both naive for comparison
                        exif_naive = exif_dt.replace(tzinfo=None) if exif_dt.tzinfo else exif_dt
                        sidecar_naive = sidecar_dt.replace(tzinfo=None) if sidecar_dt.tzinfo else sidecar_dt
                        diff_seconds = (exif_naive - sidecar_naive).total_seconds()

                    comparison = DateComparison(
                        file_path=fd.path,
                        exif_date=exif_dt,
                        sidecar_date=sidecar_dt,
                        difference_seconds=diff_seconds,
                        source=str(discovery.source_path),
                    )
                    comparisons.append(comparison)

                    if not sidecar_dt and not exif_dt:
                        missing_dates.append(fd.path)
        except Exception:
            logger.exception(f"Date analysis failed for {result.path}")
            continue
    
    if total_media == 0:
        return
    
    st.header("📅 Date Analysis")
    
    # Calculate coverage percentages
    sidecar_pct = (with_sidecar / total_media * 100) if total_media else 0
    date_recovery_pct = ((with_photo_taken_time + with_creation_time) / total_media * 100) if total_media else 0
    
    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Total Media", f"{total_media:,}")
    col2.metric("With Sidecar", f"{with_sidecar:,} ({sidecar_pct:.1f}%)")
    col3.metric("Date Recoverable", f"{with_photo_taken_time + with_creation_time:,} ({date_recovery_pct:.1f}%)")
    col4.metric("Missing Dates", f"{len(missing_dates):,}")
    
    # Date range if we have dates
    if all_dates:
        all_dates.sort()
        earliest = all_dates[0]
        latest = all_dates[-1]
        
        st.markdown(f"**Date Range:** {earliest.strftime('%Y-%m-%d')} to {latest.strftime('%Y-%m-%d')}")
    
    # EXIF vs Sidecar comparison
    st.subheader("🔍 EXIF vs Sidecar Comparison")
    
    # Calculate comparison stats
    with_both = sum(1 for c in comparisons if c.has_both)
    matching = sum(1 for c in comparisons if c.dates_match)
    mismatched = sum(1 for c in comparisons if c.status == "mismatch")
    exif_only = sum(1 for c in comparisons if c.status == "exif_only")
    sidecar_only = sum(1 for c in comparisons if c.status == "sidecar_only")
    
    if with_both > 0:
        match_pct = (matching / with_both * 100)
        
        col1, col2, col3, col4 = st.columns(4)
        col1.metric("Both Dates Available", f"{with_both:,}")
        col2.metric("Matching", f"{matching:,} ({match_pct:.1f}%)")
        col3.metric("Mismatched", f"{mismatched:,}")
        col4.metric("EXIF Only / Sidecar Only", f"{exif_only:,} / {sidecar_only:,}")
        
        # Show mismatched files
        if mismatched > 0:
            mismatches = [c for c in comparisons if c.status == "mismatch"]
            mismatches.sort(key=lambda c: abs(c.difference_seconds or 0), reverse=True)
            
            with st.expander(f"⚠️ {mismatched} files with date mismatches", expanded=False):
                for comp in mismatches[:50]:
                    diff_hours = abs(comp.difference_seconds or 0) / 3600
                    direction = "EXIF later" if (comp.difference_seconds or 0) > 0 else "Sidecar later"
                    st.markdown(f"**{Path(comp.file_path).name}** - {diff_hours:.1f}h difference ({direction})")
                    st.text(f"  EXIF: {comp.exif_date}")
                    st.text(f"  Sidecar: {comp.sidecar_date}")
                    st.divider()
                if len(mismatches) > 50:
                    st.info(f"... and {len(mismatches) - 50} more")
    else:
        st.info("No files with both EXIF and sidecar dates to compare")
    
    # Show files missing dates
    if missing_dates:
        with st.expander(f"⚠️ {len(missing_dates)} files without any recoverable dates", expanded=False):
            for path in missing_dates[:100]:
                st.text(path)
            if len(missing_dates) > 100:
                st.info(f"... and {len(missing_dates) - 100} more")
    
    # Export button
    st.divider()
    _export_date_analysis(comparisons, total_media, all_dates, missing_dates)


def _export_date_analysis(comparisons, total_media, all_dates, missing_dates):
    """Export date analysis to CSV."""
    from datetime import datetime
    
    # Build export data
    export_rows = []
    for comp in comparisons:
        export_rows.append({
            'file_path': comp.file_path,
            'source': comp.source,
            'exif_date': comp.exif_date.isoformat() if comp.exif_date else '',
            'sidecar_date': comp.sidecar_date.isoformat() if comp.sidecar_date else '',
            'difference_seconds': comp.difference_seconds if comp.difference_seconds is not None else '',
            'status': comp.status,
        })
    
    if not export_rows:
        return
    
    df = pd.DataFrame(export_rows)
    timestamp = datetime.now().strftime('%Y%m%d-%H%M%S')
    
    col1, col2 = st.columns(2)
    
    with col1:
        # Full report
        csv_full = df.to_csv(index=False)
        st.download_button(
            label="📥 Export Full Date Report (CSV)",
            data=csv_full,
            file_name=f'date_analysis_full_{timestamp}.csv',
            mime='text/csv',
        )
    
    with col2:
        # Mismatches only
        df_mismatches = df[df['status'] == 'mismatch']
        if not df_mismatches.empty:
            csv_mismatches = df_mismatches.to_csv(index=False)
            st.download_button(
                label="⚠️ Export Mismatches Only (CSV)",
                data=csv_mismatches,
                file_name=f'date_mismatches_{timestamp}.csv',
                mime='text/csv',
            )


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
    
    # Export duplicate report
    st.divider()
    _export_duplicate_report(duplicates, stats)


def _export_duplicate_report(duplicates: dict, stats: dict):
    """Export duplicate analysis to CSV."""
    export_rows = []
    
    for file_hash, locations in duplicates.items():
        for i, (source, path) in enumerate(locations):
            export_rows.append({
                'hash': file_hash,
                'source': source,
                'file_path': path,
                'is_first': i == 0,  # First occurrence (keep), rest are duplicates
                'copy_number': i + 1,
                'total_copies': len(locations),
            })
    
    if not export_rows:
        return
    
    df = pd.DataFrame(export_rows)
    timestamp = datetime.now().strftime('%Y%m%d-%H%M%S')
    
    col1, col2 = st.columns(2)
    
    with col1:
        csv_full = df.to_csv(index=False)
        st.download_button(
            label="📥 Export Full Duplicate Report (CSV)",
            data=csv_full,
            file_name=f'duplicates_full_{timestamp}.csv',
            mime='text/csv',
        )
    
    with col2:
        # Only duplicates (not first occurrence)
        df_dupes = df[~df['is_first']]
        if not df_dupes.empty:
            csv_dupes = df_dupes.to_csv(index=False)
            st.download_button(
                label="🗑️ Export Duplicates Only (CSV)",
                data=csv_dupes,
                file_name=f'duplicates_only_{timestamp}.csv',
                mime='text/csv',
            )


def show_timeline_analysis():
    """Display timeline visualization of photos by date."""
    from collections import Counter
    
    # Gather all dates
    dates_by_month = Counter()
    dates_by_year = Counter()
    
    for result in st.session_state.results:
        try:
            discovery = load_takeout_discovery(Path(result.path))
            if not discovery:
                continue
            
            for fd in discovery.iter_file_details():
                if fd.file_type in ('photo', 'video') and fd.photo_taken_time:
                    try:
                        dt = datetime.fromisoformat(fd.photo_taken_time)
                        dates_by_year[dt.year] += 1
                        dates_by_month[f"{dt.year}-{dt.month:02d}"] += 1
                    except ValueError:
                        pass
        except Exception:
            logger.exception(f"Timeline analysis failed for {result.path}")
            continue
    
    if not dates_by_year:
        return
    
    st.header("📈 Timeline")
    
    # Year view
    years = sorted(dates_by_year.keys())
    year_counts = [dates_by_year[y] for y in years]
    
    year_df = pd.DataFrame({
        'Year': years,
        'Files': year_counts
    })
    
    st.subheader("Photos by Year")
    st.bar_chart(year_df.set_index('Year'))
    
    # Month view (last 5 years or all if less)
    with st.expander("📅 Monthly Breakdown", expanded=False):
        months = sorted(dates_by_month.keys())
        if len(months) > 60:  # Limit to last 60 months
            months = months[-60:]
        month_counts = [dates_by_month[m] for m in months]
        
        month_df = pd.DataFrame({
            'Month': months,
            'Files': month_counts
        })
        st.bar_chart(month_df.set_index('Month'))


def show_orphan_analysis():
    """Detect orphaned sidecars and media without sidecars."""
    orphan_sidecars = []  # JSON files without matching media
    orphan_media = []  # Media files without matching JSON
    paired_count = 0
    
    for result in st.session_state.results:
        try:
            discovery = load_takeout_discovery(Path(result.path))
            if not discovery:
                continue
            
            # Build sets for lookup
            json_files = {fd.path for fd in discovery.iter_file_details() if fd.file_type == 'json'}
            media_files = {fd.path for fd in discovery.iter_file_details() if fd.file_type in ('photo', 'video')}

            # Check each media file for sidecar
            for fd in discovery.iter_file_details():
                if fd.file_type in ('photo', 'video'):
                    expected_sidecar = f"{fd.path}.json"
                    if expected_sidecar in json_files or fd.sidecar_path:
                        paired_count += 1
                    else:
                        orphan_media.append({
                            'source': str(discovery.source_path),
                            'path': fd.path,
                            'type': 'media_without_sidecar',
                        })

            # Check each JSON for matching media
            for fd in discovery.iter_file_details():
                if fd.file_type == 'json' and fd.path.endswith('.json'):
                    # Expected media path: remove .json suffix
                    if fd.path.endswith('.json'):
                        expected_media = fd.path[:-5]  # Remove .json
                        if expected_media not in media_files:
                            orphan_sidecars.append({
                                'source': str(discovery.source_path),
                                'path': fd.path,
                                'type': 'sidecar_without_media',
                            })
        except Exception:
            logger.exception(f"Orphan analysis failed for {result.path}")
            continue
    
    total_orphans = len(orphan_sidecars) + len(orphan_media)
    
    if total_orphans == 0 and paired_count == 0:
        return
    
    st.header("🔗 Pairing Analysis")
    
    col1, col2, col3 = st.columns(3)
    col1.metric("Properly Paired", f"{paired_count:,}")
    col2.metric("Media Without Sidecar", f"{len(orphan_media):,}")
    col3.metric("Orphan Sidecars", f"{len(orphan_sidecars):,}")
    
    if orphan_media:
        with st.expander(f"⚠️ {len(orphan_media)} media files without sidecars", expanded=False):
            for item in orphan_media[:100]:
                st.text(f"{Path(item['source']).name}: {item['path']}")
            if len(orphan_media) > 100:
                st.info(f"... and {len(orphan_media) - 100} more")
    
    if orphan_sidecars:
        with st.expander(f"⚠️ {len(orphan_sidecars)} sidecars without media", expanded=False):
            for item in orphan_sidecars[:100]:
                st.text(f"{Path(item['source']).name}: {item['path']}")
            if len(orphan_sidecars) > 100:
                st.info(f"... and {len(orphan_sidecars) - 100} more")


def show_cross_archive_analysis():
    """Analyze unique and shared files across archives."""
    if not st.session_state.compute_hashes:
        return
    
    hash_index = st.session_state.hash_index
    entries = hash_index.entries()
    if not entries:
        return

    files_by_source, hash_to_sources = summarize_sources(entries)

    if len(files_by_source) < 2:
        return  # Need at least 2 sources to compare
    
    st.header("📊 Cross-Archive Analysis")
    
    # Calculate unique vs shared for each source
    analysis_rows = []
    for source, hashes in files_by_source.items():
        unique = sum(1 for h in hashes if len(hash_to_sources[h]) == 1)
        shared = len(hashes) - unique
        analysis_rows.append({
            'Archive': source,
            'Total Files': len(hashes),
            'Unique Files': unique,
            'Shared Files': shared,
            'Unique %': f"{(unique/len(hashes)*100):.1f}%" if hashes else "0%",
        })
    
    df = pd.DataFrame(analysis_rows)
    st.dataframe(df, hide_index=True, width="stretch")
    
    # Show overlap matrix
    if len(files_by_source) <= 10:  # Only show matrix for reasonable number of sources
        with st.expander("🔀 Overlap Matrix", expanded=False):
            sources = list(files_by_source.keys())
            matrix_data = []
            
            for s1 in sources:
                row = {'Archive': s1}
                for s2 in sources:
                    if s1 == s2:
                        row[s2] = len(files_by_source[s1])
                    else:
                        overlap = len(files_by_source[s1] & files_by_source[s2])
                        row[s2] = overlap
                matrix_data.append(row)
            
            matrix_df = pd.DataFrame(matrix_data)
            st.dataframe(matrix_df.set_index('Archive'), width="stretch")


def show_full_inventory():
    """Display and export full file inventory."""
    inventory = []
    
    for result in st.session_state.results:
        try:
            discovery = load_takeout_discovery(Path(result.path))
            if not discovery:
                continue
            
            for fd in discovery.iter_file_details():
                inventory.append({
                    'source': Path(discovery.source_path).name,
                    'file_path': fd.path,
                    'file_type': fd.file_type,
                    'extension': fd.extension,
                    'size_bytes': fd.size,
                    'size_human': human_size(fd.size),
                    'file_hash': fd.file_hash or '',
                    'sidecar_path': fd.sidecar_path or '',
                    'photo_taken_time': fd.photo_taken_time or '',
                    'creation_time': fd.creation_time or '',
                    'has_exif': fd.metadata.get('has_exif', False) if fd.metadata else False,
                    'has_gps': fd.metadata.get('has_gps', False) if fd.metadata else False,
                })
        except Exception:
            logger.exception(f"Inventory build failed for {result.path}")
            continue
    
    if not inventory:
        return
    
    st.header("📋 Full Inventory")
    
    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Total Files", f"{len(inventory):,}")
    
    photos = sum(1 for f in inventory if f['file_type'] == 'photo')
    videos = sum(1 for f in inventory if f['file_type'] == 'video')
    jsons = sum(1 for f in inventory if f['file_type'] == 'json')
    
    col2.metric("Photos", f"{photos:,}")
    col3.metric("Videos", f"{videos:,}")
    col4.metric("JSON Sidecars", f"{jsons:,}")
    
    # Export button
    df = pd.DataFrame(inventory)
    timestamp = datetime.now().strftime('%Y%m%d-%H%M%S')
    
    csv = df.to_csv(index=False)
    st.download_button(
        label="📥 Export Full Inventory (CSV)",
        data=csv,
        file_name=f'file_inventory_{timestamp}.csv',
        mime='text/csv',
        type="primary",
    )
    
    # Preview
    with st.expander("👀 Preview Inventory", expanded=False):
        st.dataframe(df.head(100), hide_index=True, width="stretch")
        if len(df) > 100:
            st.info(f"Showing first 100 of {len(df):,} files")


def process_uploaded_files(uploaded_files):
    """Process files uploaded through Streamlit's file uploader."""
    import tempfile

    if not uploaded_files:
        return

    # Clean up temp dirs left behind by previous uploads — but only ones
    # nothing still points at. Uploaded files aren't scanned immediately;
    # they're queued in pending_files until the user clicks Scan All, so an
    # earlier upload's directory can still be referenced when a later one
    # arrives. Deleting it unconditionally (the previous fix for the leak)
    # traded the leak for silently losing queued files. See the `finally`
    # block below for why we can't clean up *this* upload's dir here too.
    in_use = {str(f.path) for f in st.session_state.pending_files}
    stale = unreferenced_dirs(st.session_state.upload_dirs, in_use)
    remove_dirs(stale)
    st.session_state.upload_dirs = [
        d for d in st.session_state.upload_dirs if d not in stale
    ]

    # Create temp directory for uploads
    temp_dir = Path(tempfile.mkdtemp(prefix='takeout_scout_'))
    st.session_state.upload_dirs.append(temp_dir)

    try:
        with st.spinner(f"Processing {len(uploaded_files)} uploaded file(s)..."):
            file_paths = []
            
            for uploaded_file in uploaded_files:
                # Save uploaded file to temp location
                temp_path = temp_dir / uploaded_file.name
                with open(temp_path, 'wb') as f:
                    f.write(uploaded_file.getbuffer())
                file_paths.append(temp_path)
                logger.info(f"Saved upload: {uploaded_file.name} ({human_size(uploaded_file.size)})")
            
            # Now scan them
            load_files(file_paths)

    except Exception as e:
        logger.exception(f"Error processing uploads: {e}")
        st.error(f"❌ Error processing uploads: {e}")
    finally:
        # Cleanup temp directory after a delay (files might still be in use)
        # Note: In production, you might want a better cleanup strategy
        pass


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
    # If launched directly with `python app.py` (not via `streamlit run`),
    # re-invoke ourselves under Streamlit so the app is self-launching.
    try:
        from streamlit.runtime.scriptrunner import get_script_run_ctx
        if get_script_run_ctx() is None:
            import subprocess
            import sys as _sys

            script_path = Path(__file__).resolve()
            cmd = [_sys.executable, "-m", "streamlit", "run", str(script_path)] + _sys.argv[1:]
            subprocess.run(cmd)
            _sys.exit(0)
    except ImportError:
        pass

    # Running via streamlit, proceed normally
    main()
