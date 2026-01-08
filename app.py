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
    if 'parse_sidecars' not in st.session_state:
        st.session_state.parse_sidecars = True  # Default on since it's so useful
    
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
            st.session_state.parse_sidecars = True
            st.rerun()
    
    # Main content area
    show_pending_files()
    show_results()
    
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
        parse_sidecars = st.session_state.parse_sidecars
        
        with st.spinner(f"Scanning {file_info.name}..."):
            if file_info.file_type == 'directory':
                summary = scan_directory(file_info.path, compute_hashes=compute_hashes, parse_sidecars=parse_sidecars)
            else:
                summary = scan_archive(file_info.path, compute_hashes=compute_hashes, parse_sidecars=parse_sidecars)
        
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
    parse_sidecars = st.session_state.parse_sidecars
    progress_bar = st.progress(0, text=f"Scanning 0/{len(valid_files)} files...")
    
    for count, (index, file_info) in enumerate(valid_files, 1):
        try:
            if file_info.file_type == 'directory':
                summary = scan_directory(file_info.path, compute_hashes=compute_hashes, parse_sidecars=parse_sidecars)
            else:
                summary = scan_archive(file_info.path, compute_hashes=compute_hashes, parse_sidecars=parse_sidecars)
            
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
    parse_sidecars = st.session_state.parse_sidecars
    
    with st.spinner(f"Scanning {folder_path.name}..."):
        archives, directories = find_archives_and_dirs(folder_path)
        total = len(archives) + len(directories)
        
        if total == 0:
            st.warning(f"⚠️ No Takeout archives or directories found in {folder_path}")
            summary = scan_directory(folder_path, compute_hashes=compute_hashes, parse_sidecars=parse_sidecars)
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
                summary = scan_directory(directory, compute_hashes=compute_hashes, parse_sidecars=parse_sidecars)
                st.session_state.results.append(summary)
                st.session_state.scanned_paths.add(str(directory))
                if compute_hashes:
                    _update_hash_index(directory)
            count += 1
            progress_bar.progress(count / total, text=f"Scanning {count}/{total} items...")
        
        for archive in archives:
            if str(archive) not in st.session_state.scanned_paths:
                summary = scan_archive(archive, compute_hashes=compute_hashes, parse_sidecars=parse_sidecars)
                st.session_state.results.append(summary)
                st.session_state.scanned_paths.add(str(archive))
                if compute_hashes:
                    _update_hash_index(archive)
            count += 1
            progress_bar.progress(count / total, text=f"Scanning {count}/{total} items...")
        
        progress_bar.empty()
        st.success(f"✅ Scanned {total} items from {folder_path.name}")
        st.rerun()


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
            
            # Process archives and directories
            for source_list in [discovery.archives, discovery.directories]:
                for source in source_list:
                    for fd in source.files:
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
                                source=str(source.path),
                            )
                            comparisons.append(comparison)
                            
                            if not sidecar_dt and not exif_dt:
                                missing_dates.append(fd.path)
        except Exception:
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
            
            for source_list in [discovery.archives, discovery.directories]:
                for source in source_list:
                    for fd in source.files:
                        if fd.file_type in ('photo', 'video') and fd.photo_taken_time:
                            try:
                                dt = datetime.fromisoformat(fd.photo_taken_time)
                                dates_by_year[dt.year] += 1
                                dates_by_month[f"{dt.year}-{dt.month:02d}"] += 1
                            except ValueError:
                                pass
        except Exception:
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
            
            for source_list in [discovery.archives, discovery.directories]:
                for source in source_list:
                    # Build sets for lookup
                    all_paths = {fd.path for fd in source.files}
                    json_files = {fd.path for fd in source.files if fd.file_type == 'json'}
                    media_files = {fd.path for fd in source.files if fd.file_type in ('photo', 'video')}
                    
                    # Check each media file for sidecar
                    for fd in source.files:
                        if fd.file_type in ('photo', 'video'):
                            expected_sidecar = f"{fd.path}.json"
                            if expected_sidecar in json_files or fd.sidecar_path:
                                paired_count += 1
                            else:
                                orphan_media.append({
                                    'source': str(source.path),
                                    'path': fd.path,
                                    'type': 'media_without_sidecar',
                                })
                    
                    # Check each JSON for matching media
                    for fd in source.files:
                        if fd.file_type == 'json' and fd.path.endswith('.json'):
                            # Expected media path: remove .json suffix
                            if fd.path.endswith('.json'):
                                expected_media = fd.path[:-5]  # Remove .json
                                if expected_media not in media_files:
                                    orphan_sidecars.append({
                                        'source': str(source.path),
                                        'path': fd.path,
                                        'type': 'sidecar_without_media',
                                    })
        except Exception:
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
    if not hash_index._index:
        return
    
    # Analyze file distribution across sources
    files_by_source = {}  # source -> set of hashes
    hash_to_sources = {}  # hash -> set of sources
    
    for file_hash, locations in hash_index._index.items():
        sources = set()
        for source, path in locations:
            source_name = Path(source).name
            sources.add(source_name)
            
            if source_name not in files_by_source:
                files_by_source[source_name] = set()
            files_by_source[source_name].add(file_hash)
        
        hash_to_sources[file_hash] = sources
    
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
    st.dataframe(df, hide_index=True, use_container_width=True)
    
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
            st.dataframe(matrix_df.set_index('Archive'), use_container_width=True)


def show_full_inventory():
    """Display and export full file inventory."""
    inventory = []
    
    for result in st.session_state.results:
        try:
            discovery = load_takeout_discovery(Path(result.path))
            if not discovery:
                continue
            
            for source_list in [discovery.archives, discovery.directories]:
                for source in source_list:
                    for fd in source.files:
                        inventory.append({
                            'source': Path(source.path).name,
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
        st.dataframe(df.head(100), hide_index=True, use_container_width=True)
        if len(df) > 100:
            st.info(f"Showing first 100 of {len(df):,} files")


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
