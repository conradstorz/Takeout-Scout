#!/usr/bin/env python3
"""
Takeout Scout — Google Takeout Scanner (Tkinter GUI)

A desktop GUI utility that:
  • Asks where to look for Google Takeout archives (ZIP/TGZ)
  • Scans archives non-destructively and summarizes their contents
  • Presents a prettified table (per-archive) with counts of photos/videos/JSON sidecars
  • Exports the summary to CSV
  • Logs all actions to ./logs/takeout_scout.log (rotated)

Author: ChatGPT for Conrad
License: GNU GPL v3
"""
from __future__ import annotations

import csv
import os
import sys
import threading
import time
import tkinter as tk
from datetime import datetime
from pathlib import Path
from tkinter import ttk, filedialog, messagebox
from typing import List, Optional

# Import from our package
from takeout_scout import (
    ArchiveSummary,
    scan_archive,
    scan_directory,
    find_archives_and_dirs,
    human_size,
)
from takeout_scout.constants import get_default_paths, ensure_directories
from takeout_scout.logging import logger, LOG_DIR


# Ensure directories exist on import
ensure_directories()


class TakeoutScoutGUI(tk.Tk):
    """Main application window for Takeout Scout."""
    
    def __init__(self) -> None:
        super().__init__()
        self.title('Takeout Scout')
        self.geometry('1300x700')
        self.minsize(900, 500)
        
        self._paths: List[Path] = []
        self._rows: List[ArchiveSummary] = []
        self._scanning = False
        self._cancel_evt = threading.Event()
        
        self._build_widgets()
    
    def _build_widgets(self) -> None:
        """Build all UI widgets."""
        # Status bar at top
        status_frame = ttk.Frame(self)
        status_frame.pack(side=tk.TOP, fill=tk.X, padx=10, pady=5)
        
        self.status_var = tk.StringVar(value='Select a folder or files to begin.')
        ttk.Label(status_frame, textvariable=self.status_var).pack(side=tk.LEFT)

        # Toolbar buttons
        btn_frame = ttk.Frame(self)
        btn_frame.pack(side=tk.TOP, fill=tk.X, padx=10, pady=5)

        btn_choose_folder = ttk.Button(
            btn_frame, text='📂 Choose Folder…', 
            command=self.on_choose_folder
        )
        btn_choose_folder.pack(side=tk.LEFT)

        btn_choose_files = ttk.Button(
            btn_frame, text='📄 Choose Files…', 
            command=self.on_choose_files
        )
        btn_choose_files.pack(side=tk.LEFT, padx=(5, 0))

        self.btn_scan = ttk.Button(
            btn_frame, text='Scan All', 
            command=self.on_scan, state=tk.DISABLED
        )
        self.btn_scan.pack(side=tk.LEFT, padx=(10, 0))

        self.btn_export = ttk.Button(
            btn_frame, text='Export CSV', 
            command=self.on_export, state=tk.DISABLED
        )
        self.btn_export.pack(side=tk.LEFT, padx=(10, 0))

        self.btn_logs = ttk.Button(
            btn_frame, text='Open Logs…', 
            command=self.on_open_logs
        )
        self.btn_logs.pack(side=tk.RIGHT)

        # Treeview for results
        tree_frame = ttk.Frame(self)
        tree_frame.pack(side=tk.TOP, fill=tk.BOTH, expand=True, padx=10, pady=(0, 10))
        
        cols = (
            'action', 'archive', 'parts', 'service', 'files', 'photos', 
            'videos', 'json', 'other', 'size', 'exif', 'gps', 'datetime', 
            'checked', 'live', 'motion', 'pjson'
        )
        self.tree = ttk.Treeview(tree_frame, columns=cols, show='headings')
        
        # Column configuration
        column_config = [
            ('action', 'Action', 70, 70, tk.CENTER, False),
            ('archive', 'Source', 250, 150, tk.W, True),
            ('parts', 'Group/Name', 150, 100, tk.W, True),
            ('service', 'Service', 100, 80, tk.W, False),
            ('files', 'Files', 60, 50, tk.E, False),
            ('photos', 'Photos', 60, 50, tk.E, False),
            ('videos', 'Videos', 60, 50, tk.E, False),
            ('json', 'JSON', 60, 50, tk.E, False),
            ('other', 'Other', 60, 50, tk.E, False),
            ('size', 'Size', 100, 80, tk.E, False),
            ('exif', 'w/EXIF', 60, 50, tk.E, False),
            ('gps', 'w/GPS', 60, 50, tk.E, False),
            ('datetime', 'w/Date', 60, 50, tk.E, False),
            ('checked', 'Checked', 60, 50, tk.E, False),
            ('live', 'Live', 50, 45, tk.E, False),
            ('motion', 'Motion', 60, 50, tk.E, False),
            ('pjson', 'P+J', 50, 45, tk.E, False),
        ]
        
        for key, title, width, minwidth, anchor, stretch in column_config:
            self.tree.heading(key, text=title)
            self.tree.column(key, width=width, minwidth=minwidth, anchor=anchor, stretch=stretch)

        self.tree.bind('<Button-1>', self._on_tree_click)

        # Scrollbars
        vsb = ttk.Scrollbar(tree_frame, orient='vertical', command=self.tree.yview)
        self.tree.configure(yscrollcommand=vsb.set)
        
        hsb = ttk.Scrollbar(tree_frame, orient='horizontal', command=self.tree.xview)
        self.tree.configure(xscrollcommand=hsb.set)
        
        # Grid layout
        self.tree.grid(row=0, column=0, sticky='nsew')
        vsb.grid(row=0, column=1, sticky='ns')
        hsb.grid(row=1, column=0, sticky='ew')
        
        tree_frame.grid_rowconfigure(0, weight=1)
        tree_frame.grid_columnconfigure(0, weight=1)

        # Progress area
        prog = ttk.Frame(self, padding=(10, 0))
        prog.pack(side=tk.TOP, fill=tk.X)
        
        self.overall_label_var = tk.StringVar(value='Overall: 0/0')
        ttk.Label(prog, textvariable=self.overall_label_var).pack(anchor=tk.W)
        self.pb_overall = ttk.Progressbar(prog, mode='determinate', maximum=1, value=0)
        self.pb_overall.pack(fill=tk.X, pady=2)

        self.current_label_var = tk.StringVar(value='Current: (none)')
        ttk.Label(prog, textvariable=self.current_label_var).pack(anchor=tk.W)
        self.pb_current = ttk.Progressbar(prog, mode='determinate', maximum=1, value=0)
        self.pb_current.pack(fill=tk.X, pady=2)

        # Cancel button
        cancel_frame = ttk.Frame(self)
        cancel_frame.pack(side=tk.BOTTOM, fill=tk.X, padx=10, pady=5)
        
        self.btn_cancel = ttk.Button(
            cancel_frame, text='Cancel', 
            command=self.on_cancel, state=tk.DISABLED
        )
        self.btn_cancel.pack(side=tk.RIGHT)

    def on_choose_folder(self) -> None:
        """Handle folder selection."""
        folder = filedialog.askdirectory(title='Select folder containing Takeout archives')
        if folder:
            root = Path(folder)
            archives, directories = find_archives_and_dirs(root)
            
            self._paths = archives + directories
            if self._paths:
                self._show_selected_folder()
                self.btn_scan.config(state=tk.NORMAL)
                logger.info(f"Found {len(archives)} archives and {len(directories)} directories in {root}")
            else:
                self.status_var.set('No archives or Takeout directories found.')
                logger.warning(f"No archives or directories found in {root}")

    def on_choose_files(self) -> None:
        """Handle file selection."""
        files = filedialog.askopenfilenames(
            title='Select Takeout archive files',
            filetypes=[
                ('Archives', '*.zip *.tgz *.tar.gz'),
                ('ZIP files', '*.zip'),
                ('TGZ files', '*.tgz *.tar.gz'),
                ('All files', '*.*')
            ]
        )
        if files:
            self._paths = [Path(f) for f in files]
            self._show_selected_files()
            self.btn_scan.config(state=tk.NORMAL)
            logger.info(f"Selected {len(self._paths)} files")

    def _show_selected_folder(self) -> None:
        """Display selected folder contents in tree."""
        for item in self.tree.get_children():
            self.tree.delete(item)
        self._rows.clear()
        
        for p in self._paths:
            # Show '▶ Scan' as clickable action
            is_dir = p.is_dir()
            values = (
                '▶ Scan',
                p.name,
                p.name if is_dir else '',
                '',
                '',
                '',
                '',
                '',
                '',
                human_size(p.stat().st_size) if not is_dir else '(directory)',
                '', '', '', '', '', '', ''
            )
            self.tree.insert('', tk.END, iid=str(p), values=values)
        
        self.status_var.set(f'Found {len(self._paths)} items. Click "Scan All" or click individual items.')

    def _show_selected_files(self) -> None:
        """Display selected files in tree."""
        for item in self.tree.get_children():
            self.tree.delete(item)
        self._rows.clear()
        
        for p in self._paths:
            try:
                size = p.stat().st_size
            except Exception:
                size = 0
            
            values = (
                '▶ Scan',
                p.name,
                '',
                '',
                '',
                '',
                '',
                '',
                '',
                human_size(size),
                '', '', '', '', '', '', ''
            )
            self.tree.insert('', tk.END, iid=str(p), values=values)
        
        self.status_var.set(f'Selected {len(self._paths)} files. Click "Scan All" or click individual items.')

    def _on_tree_click(self, event) -> None:
        """Handle clicks on tree items."""
        region = self.tree.identify_region(event.x, event.y)
        if region != 'cell':
            return
        
        col = self.tree.identify_column(event.x)
        if col != '#1':  # Not the action column
            return
        
        item_id = self.tree.identify_row(event.y)
        if not item_id:
            return
        
        # Check if it's scannable
        values = self.tree.item(item_id, 'values')
        if values and values[0] == '▶ Scan':
            self._scan_single_item(item_id)

    def _scan_single_item(self, item_id: str) -> None:
        """Scan a single item."""
        if self._scanning:
            return
        
        path = Path(item_id)
        if not path.exists():
            messagebox.showerror('Error', f'Path not found: {path}')
            return
        
        self._scanning = True
        self.tree.item(item_id, values=('⏳ Scanning…',) + self.tree.item(item_id, 'values')[1:])
        
        def scan_thread():
            try:
                if path.is_dir():
                    summary = scan_directory(path)
                else:
                    summary = scan_archive(path)
                
                self._rows.append(summary)
                
                def update_ui():
                    row = summary.to_row()
                    values = ('✓ Done', Path(row[0]).name, *row[1:])
                    self.tree.item(item_id, values=values)
                    self.btn_export.config(state=tk.NORMAL)
                    self._scanning = False
                
                self.after(0, update_ui)
                
            except Exception as e:
                logger.exception(f"Scan failed for {path}: {e}")
                
                def error_update():
                    self.tree.item(item_id, values=('✗ Error',) + self.tree.item(item_id, 'values')[1:])
                    self._scanning = False
                
                self.after(0, error_update)
        
        threading.Thread(target=scan_thread, daemon=True).start()

    def on_scan(self) -> None:
        """Scan all items."""
        if self._scanning or not self._paths:
            return
        
        self._scanning = True
        self._cancel_evt.clear()
        self.btn_scan.config(state=tk.DISABLED)
        self.btn_cancel.config(state=tk.NORMAL)
        self._rows.clear()
        
        threading.Thread(target=self._scan_thread, daemon=True).start()

    def _scan_thread(self) -> None:
        """Background thread for scanning all items."""
        total = len(self._paths)
        
        for idx, path in enumerate(self._paths, 1):
            if self._cancel_evt.is_set():
                break
            
            self._set_status(f'Scanning {idx}/{total}: {path.name}')
            self._set_overall_progress(idx - 1, total)
            self._set_current_label(f'Current: {path.name}')
            
            try:
                if path.is_dir():
                    summary = scan_directory(path)
                else:
                    summary = scan_archive(path)
                
                self._rows.append(summary)
                
                def update_row(p=path, s=summary):
                    row = s.to_row()
                    values = ('✓ Done', Path(row[0]).name, *row[1:])
                    self.tree.item(str(p), values=values)
                
                self.after(0, update_row)
                
            except Exception as e:
                logger.exception(f"Scan failed for {path}: {e}")
                
                def error_row(p=path):
                    current = self.tree.item(str(p), 'values')
                    self.tree.item(str(p), values=('✗ Error',) + current[1:])
                
                self.after(0, error_row)
        
        self._set_overall_progress(total, total)
        self._set_current_label('Current: (done)')
        self._set_status('Scan complete.' if not self._cancel_evt.is_set() else 'Scan cancelled.')
        
        self._scanning = False
        self._enable_scan_buttons()
        self._enable_export(bool(self._rows))

    def on_export(self) -> None:
        """Export results to CSV."""
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
                w.writerow([
                    'Archive', 'Parts Group', 'Service Guess', 'Files', 'Photos', 
                    'Videos', 'JSON Sidecars', 'Other', 'Compressed Size (bytes)',
                    'Photos w/EXIF', 'Photos w/GPS', 'Photos w/DateTime', 'Photos Checked',
                    'Live Photos', 'Motion Photos', 'Photo+JSON'
                ])
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
                        r.photos_with_exif,
                        r.photos_with_gps,
                        r.photos_with_datetime,
                        r.photos_checked,
                        r.live_photos,
                        r.motion_photos,
                        r.photo_json_pairs,
                    ])
            
            logger.info(f"Exported CSV: {dest}")
            messagebox.showinfo('Export complete', f'CSV saved to:\n{dest}')
        except Exception as e:
            logger.exception(f"Export failed: {e}")
            messagebox.showerror('Error', f'Export failed: {e}')

    def on_open_logs(self) -> None:
        """Open the log file."""
        try:
            log_path = LOG_DIR / 'takeout_scout.log'
            if not log_path.exists():
                messagebox.showinfo('Logs', 'No log file yet. Run a scan first.')
                return
            
            if sys.platform.startswith('win'):
                os.startfile(str(log_path))  # type: ignore
            elif sys.platform == 'darwin':
                os.system(f'open "{log_path}"')
            else:
                os.system(f'xdg-open "{log_path}"')
        except Exception as e:
            logger.exception(f"Open logs failed: {e}")
            messagebox.showerror('Error', f'Open logs failed: {e}')

    def on_cancel(self) -> None:
        """Cancel the current scan operation."""
        self._cancel_evt.set()
        self._set_status('Canceling…')

    # --- UI thread-safe helpers ---
    
    def _ui(self, fn) -> None:
        """Run a function on the UI thread."""
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

    def _set_current_label(self, text: str) -> None:
        self._ui(lambda: self.current_label_var.set(text))

    def _enable_export(self, enable: bool) -> None:
        self._ui(lambda: self.btn_export.config(state=tk.NORMAL if enable else tk.DISABLED))

    def _enable_scan_buttons(self) -> None:
        def _apply():
            self.btn_scan.config(state=tk.NORMAL)
            self.btn_cancel.config(state=tk.DISABLED)
        self._ui(_apply)


def main() -> None:
    """Entry point for the GUI application."""
    logger.info('Takeout Scout GUI started.')
    app = TakeoutScoutGUI()
    app.mainloop()


if __name__ == '__main__':
    main()
