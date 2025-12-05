#!/usr/bin/env python3
"""
Takeout Scout — Google Takeout Scanner (MVP)

A small, re-runnable GUI utility that:
  • Asks where to look for Google Takeout archives (ZIP/TGZ)
  • Scans archives non-destructively and summarizes their contents
  • Presents a prettified table (per-archive) with counts of photos/videos/JSON sidecars
  • Exports the summary to CSV
  • Logs all actions to ./logs/takeout_scout.log (rotated)

Design notes:
  • Pure standard library + loguru (optional but recommended). If loguru is not installed,
    it falls back to a minimal logger.
  • Idempotent and safe to re-run; does not modify archives.
  • Future steps (unpack, merge JSON→EXIF, dedupe, organize) can be added as additional
    buttons without changing the scan step. Each step should write to its own output
    directory so that runs are “restful.”

Author: ChatGPT for Conrad
License: MIT
"""
from __future__ import annotations

import csv
import os
import re
import sys
import tarfile
import threading
import time
import zipfile
from collections import Counter, defaultdict
from dataclasses import dataclass, asdict
from datetime import datetime
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple

# --- Logging setup -----------------------------------------------------------
try:
    from loguru import logger  # type: ignore
    _HAS_LOGURU = True
except Exception:  # pragma: no cover
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
    logger = _Shim()  # type: ignore
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

# --- Simple size helpers -----------------------------------------------------

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

    def to_row(self) -> List[str]:
        return [
            self.path,
            self.parts_group,
            self.service_guess,
            str(self.file_count),
            str(self.photos),
            str(self.videos),
            str(self.json_sidecars),
            str(self.other),
            human_size(self.compressed_size),
        ]


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

# --- Archive iteration with per-archive progress ----------------------------

def iter_members_with_progress(path: Path, start_cb, tick_cb) -> List[str]:
    """Return a list of file members while calling progress callbacks.
    start_cb(total) is called once with the number of file entries.
    tick_cb() is called for each file entry.
    """
    members: List[str] = []
    if path.suffix.lower() == '.zip':
        with zipfile.ZipFile(path) as zf:
            infos = [i for i in zf.infolist() if not i.is_dir()]
            start_cb(len(infos))
            for i in infos:
                members.append(i.filename)
                tick_cb()
    elif path.suffix.lower() in {'.tgz', '.gz'} or path.name.lower().endswith('.tar.gz'):
        with tarfile.open(path, 'r:*') as tf:
            files = [m for m in tf.getmembers() if m.isfile()]
            start_cb(len(files))
            for m in files:
                members.append(m.name.lstrip('./'))
                tick_cb()
    else:
        start_cb(0)
    return members


def derive_parts_group(archive_path: Path) -> str:
    m = PARTS_PAT.match(archive_path.stem)
    if m:
        return m.group('prefix')
    # Also handle Google’s common Takeout-YYYYMMDD…-001.zip style
    m2 = re.match(r'^(Takeout-\d{8}T\d{6}Z-\w+?)-(?:\d{3,})$', archive_path.stem)
    if m2:
        return m2.group(1)
    return archive_path.stem


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


def find_archives(root: Path) -> List[Path]:
    pats = {'.zip', '.tgz'}
    results: List[Path] = []
    for dirpath, _dirnames, filenames in os.walk(root):
        for name in filenames:
            lower = name.lower()
            if lower.endswith('.zip') or lower.endswith('.tgz') or lower.endswith('.tar.gz'):
                results.append(Path(dirpath) / name)
    return sorted(results)


# --- GUI ---------------------------------------------------------------------
import tkinter as tk
from tkinter import ttk, filedialog, messagebox

class TakeoutScoutGUI(tk.Tk):
    def __init__(self) -> None:
        super().__init__()
        self.title('Takeout Scout — Google Takeout Scanner (MVP)')
        self.geometry('1100x600')
        self.minsize(960, 540)
        self._root_dir: Optional[Path] = None
        self._rows: List[ArchiveSummary] = []
        self._prev_index: Dict[str, Dict[str, float]] = load_index()
        self._new_paths: set[str] = set()
        self._missing_paths: set[str] = set()
        self._cancel_evt = threading.Event()
        self._build_widgets()

    # UI construction
    def _build_widgets(self) -> None:
        top = ttk.Frame(self, padding=(10, 10))
        top.pack(side=tk.TOP, fill=tk.X)

        self.dir_var = tk.StringVar(value='(Choose a folder that contains your Takeout archives)')
        dir_label = ttk.Label(top, textvariable=self.dir_var)
        dir_label.pack(side=tk.LEFT, padx=(0, 10))

        btn_choose = ttk.Button(top, text='Choose Folder…', command=self.on_choose)
        btn_choose.pack(side=tk.LEFT)

        self.btn_scan = ttk.Button(top, text='Scan', command=self.on_scan, state=tk.DISABLED)
        self.btn_scan.pack(side=tk.LEFT, padx=(10, 0))

        self.btn_export = ttk.Button(top, text='Export CSV', command=self.on_export, state=tk.DISABLED)
        self.btn_export.pack(side=tk.LEFT, padx=(10, 0))

        self.btn_logs = ttk.Button(top, text='Open Logs…', command=self.on_open_logs)
        self.btn_logs.pack(side=tk.RIGHT)

        cols = ('archive', 'parts', 'service', 'files', 'photos', 'videos', 'json', 'other', 'size')
        self.tree = ttk.Treeview(self, columns=cols, show='headings')
        for key, title, width, anchor in (
            ('archive','Archive',380,tk.W),
            ('parts','Parts Group',220,tk.W),
            ('service','Service Guess',160,tk.W),
            ('files','Files',70,tk.E),
            ('photos','Photos',70,tk.E),
            ('videos','Videos',70,tk.E),
            ('json','JSON Sidecars',110,tk.E),
            ('other','Other',70,tk.E),
            ('size','Compressed Size',140,tk.E),
        ):
            self.tree.heading(key, text=title)
            self.tree.column(key, width=width, anchor=anchor)

        vsb = ttk.Scrollbar(self, orient='vertical', command=self.tree.yview)
        self.tree.configure(yscrollcommand=vsb.set)
        self.tree.pack(side=tk.LEFT, fill=tk.BOTH, expand=True, padx=(10, 0), pady=(0, 10))
        vsb.pack(side=tk.RIGHT, fill=tk.Y, pady=(0, 10))

        # Progress area
        prog = ttk.Frame(self, padding=(10, 0))
        prog.pack(side=tk.TOP, fill=tk.X)
        self.overall_label_var = tk.StringVar(value='Overall: 0/0')
        ttk.Label(prog, textvariable=self.overall_label_var).pack(anchor=tk.W)
        self.pb_overall = ttk.Progressbar(prog, mode='determinate', maximum=1, value=0)
        self.pb_overall.pack(fill=tk.X, pady=(2, 8))

        self.current_label_var = tk.StringVar(value='Current archive: —')
        ttk.Label(prog, textvariable=self.current_label_var).pack(anchor=tk.W)
        self.pb_current = ttk.Progressbar(prog, mode='determinate', maximum=1, value=0)
        self.pb_current.pack(fill=tk.X, pady=(2, 8))

        self.btn_cancel = ttk.Button(prog, text='Cancel Scan', command=self.on_cancel, state=tk.DISABLED)
        self.btn_cancel.pack(anchor=tk.E)

        self.status_var = tk.StringVar(value='Ready')
        status = ttk.Label(self, textvariable=self.status_var, relief=tk.SUNKEN, anchor=tk.W, padding=(8, 4))
        status.pack(side=tk.BOTTOM, fill=tk.X)

    # Handlers
    def on_choose(self) -> None:
        chosen = filedialog.askdirectory(title='Select folder containing Google Takeout archives')
        if chosen:
            self._root_dir = Path(chosen)
            self.dir_var.set(str(self._root_dir))
            self.btn_scan.config(state=tk.NORMAL)
            self.status('Folder selected. Click “Scan”.')
            logger.info(f"Chosen folder: {self._root_dir}")

    def on_scan(self) -> None:
        if not self._root_dir:
            return
        self.btn_scan.config(state=tk.DISABLED)
        self.btn_export.config(state=tk.DISABLED)
        self.btn_cancel.config(state=tk.NORMAL)
        self._cancel_evt.clear()
        self.status('Scanning…')
        self._prev_index = load_index()
        self._set_overall_progress(0, 1)
        self._set_current_label('Current archive: —')
        threading.Thread(target=self._scan_thread, daemon=True).start()

    def _scan_thread(self) -> None:
        start = time.time()
        try:
            archives = find_archives(self._root_dir or Path('.'))
            total = len(archives)
            logger.info(f"Found {total} archive(s).")
            rows: List[ArchiveSummary] = []
            current_index: Dict[str, Dict[str, float]] = {}
            self._set_overall_progress(0, max(1, total))
            for i, a in enumerate(archives, 1):
                if self._cancel_evt.is_set():
                    logger.info('Scan canceled by user.')
                    break
                self._set_current_label(f'Current archive: {a.name} ({i}/{total})')
                # Per-archive progress
                members = iter_members_with_progress(a, self._current_progress_start, self._current_progress_tick)
                # Build summary from members
                try:
                    size_bytes = a.stat().st_size
                except Exception:
                    size_bytes = 0
                photos, videos, jsons, other = tally_exts(members)
                svc = guess_service_from_members(members)
                r = ArchiveSummary(
                    path=str(a),
                    parts_group=derive_parts_group(a),
                    service_guess=svc,
                    file_count=len(members),
                    photos=photos,
                    videos=videos,
                    json_sidecars=jsons,
                    other=other,
                    compressed_size=size_bytes,
                )
                rows.append(r)
                try:
                    st = a.stat()
                    current_index[str(a.resolve())] = {'size': float(st.st_size), 'mtime': float(st.st_mtime)}
                except Exception:
                    pass
                # Update overall progress and ETA
                self._set_overall_progress(i, max(1, total))
                elapsed = time.time() - start
                rate = i / elapsed if elapsed > 0 else 0
                remaining = (total - i) / rate if rate > 0 else 0
                eta = time.strftime('%M:%S', time.gmtime(max(0, int(remaining))))
                self._set_status(f'Scanned {i}/{total} archives • ETA ~ {eta}')
            # Diff and persist index
            prev_paths = set(self._prev_index.keys())
            curr_paths = set(current_index.keys())
            self._new_paths = curr_paths - prev_paths
            self._missing_paths = prev_paths - curr_paths
            save_index(current_index)
            self._rows = rows
            self._populate_tree()
            if self._new_paths or self._missing_paths:
                added = '\n'.join(Path(p).name for p in sorted(self._new_paths)) or '(none)'
                missing = '\n'.join(Path(p).name for p in sorted(self._missing_paths)) or '(none)'
                message = (
                    f"New archives: {len(self._new_paths)}\n{added}\n\n"
                    f"Missing since last scan: {len(self._missing_paths)}\n{missing}"
                )
                self._info_dialog('Changes since last scan', message)
            final_msg = 'Scan canceled.' if self._cancel_evt.is_set() else f'Scan complete. {len(rows)} archive(s) summarized.'
            self._set_status(final_msg)
            self._enable_export(len(rows) > 0)
        except Exception as e:
            logger.exception(f"Scan failed: {e}")
            self._error_dialog('Error', f'Scan failed: {e}')
        finally:
            self._enable_scan_buttons()
            self._stop_current_spinner()

    def _populate_tree(self) -> None:
        for item in self.tree.get_children():
            self.tree.delete(item)
        # Summaries by parts group (for visual cue of multi-part exports)
        parts_counter: Dict[str, int] = Counter(r.parts_group for r in self._rows)
        new_basenames = {Path(p).name for p in self._new_paths}
        for r in self._rows:
            part_suffix = ''
            if parts_counter[r.parts_group] > 1:
                part_suffix = f' ({parts_counter[r.parts_group]} files)'
            self.tree.insert('', tk.END, values=(
                (lambda b: f"{b}  [NEW]" if b in new_basenames else b)(Path(r.path).name),
                f'{r.parts_group}{part_suffix}',
                r.service_guess,
                r.file_count,
                r.photos,
                r.videos,
                r.json_sidecars,
                r.other,
                human_size(r.compressed_size),
            ))

    def on_export(self) -> None:
        if not self._rows:
            return
        ts = datetime.now().strftime('%Y%m%d-%H%M%S')
        default_name = f'takeout_scout_summary_{ts}.csv'
        dest = filedialog.asksaveasfilename(
            title='Export summary to CSV',
            defaultextension='.csv',
            initialfile=default_name,
            filetypes=[('CSV', '*.csv'), ('All files', '*.*')]
        )
        if not dest:
            return
        try:
            with open(dest, 'w', newline='', encoding='utf-8') as f:
                w = csv.writer(f)
                w.writerow(['Archive','Parts Group','Service Guess','Files','Photos','Videos','JSON Sidecars','Other','Compressed Size (bytes)'])
                for r in self._rows:
                    w.writerow([
                        Path(r.path).name,
                        r.parts_group,
                        r.service_guess,
                        r.file_count,
                        r.photos,
                        r.videos,
                        r.json_sidecars,
                        r.other,
                        r.compressed_size,
                    ])
            logger.info(f"Exported CSV: {dest}")
            messagebox.showinfo('Export complete', f'CSV saved to:\n{dest}')
        except Exception as e:
            logger.exception(f"Export failed: {e}")
            messagebox.showerror('Error', f'Export failed: {e}')

    def on_open_logs(self) -> None:
        try:
            path = LOG_DIR / 'takeout_scout.log'
            if not path.exists():
                messagebox.showinfo('Logs', 'No log file yet. Run a scan first.')
                return
            if sys.platform.startswith('win'):
                os.startfile(str(path))  # type: ignore
            elif sys.platform == 'darwin':
                os.system(f'open "{path}"')
            else:
                os.system(f'xdg-open "{path}"')
        except Exception as e:
            logger.exception(f"Open logs failed: {e}")
            messagebox.showerror('Error', f'Open logs failed: {e}')

    def status(self, text: str) -> None:
        # legacy; prefer _set_status from worker thread
        self.status_var.set(text)
        self.update_idletasks()

    # --- UI thread-safe helpers ---
    def _ui(self, fn) -> None:
        try:
            self.after(0, fn)
        except Exception:
            pass

    def _set_status(self, text: str) -> None:
        self._ui(lambda: self.status_var.set(text))

    def _set_overall_progress(self, value: int, maximum: int) -> None:
        def _apply():
            self.overall_label_var.set(f'Overall: {value}/{maximum}')
            self.pb_overall.config(maximum=maximum, value=value)
        self._ui(_apply)

    def _start_current_spinner(self) -> None:
        # no-op (spinner replaced with determinate progress)
        pass

    def _stop_current_spinner(self) -> None:
        # no-op (spinner replaced with determinate progress)
        pass

    def _current_progress_start(self, total: int) -> None:
        def _apply():
            self.pb_current.config(mode='determinate', maximum=max(1, total), value=0)
        self._ui(_apply)

    def _current_progress_tick(self) -> None:
        self._ui(lambda: self.pb_current.step(1))

    def _set_current_label(self, text: str) -> None:
        self._ui(lambda: self.current_label_var.set(text))

    def _enable_export(self, enable: bool) -> None:
        self._ui(lambda: self.btn_export.config(state=tk.NORMAL if enable else tk.DISABLED))

    def _enable_scan_buttons(self) -> None:
        def _apply():
            self.btn_scan.config(state=tk.NORMAL)
            self.btn_cancel.config(state=tk.DISABLED)
        self._ui(_apply)

    def _info_dialog(self, title: str, message: str) -> None:
        self._ui(lambda: messagebox.showinfo(title, message))

    def _error_dialog(self, title: str, message: str) -> None:
        self._ui(lambda: messagebox.showerror(title, message))

    def on_cancel(self) -> None:
        self._cancel_evt.set()
        self._set_status('Canceling…')


def main() -> None:
    logger.info('Takeout Scout started.')
    app = TakeoutScoutGUI()
    app.mainloop()


if __name__ == '__main__':
    main()
