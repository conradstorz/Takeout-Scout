# Takeout Scout

A modern, web-based tool for scanning and analyzing Google Takeout archives without extraction.

> **Where this fits.** [`Google_Takeout_Downloader`](https://github.com/conradstorz/Google_Takeout_Downloader)
> fetches the archives. **Takeout Scout** explores them interactively in a
> browser. [`Takeout_Inventory`](https://github.com/conradstorz/Takeout_Inventory)
> produces a machine-readable pairing index and a static HTML report. Scout and
> Inventory scan independently today — they share no code.

## Features

- **🌐 Web Interface** - Clean, modern UI that runs in your browser
- **Non-destructive scanning** - Analyzes ZIP and TGZ archives without extracting
- **Smart detection** - Identifies content types (photos, videos, JSON sidecars) and Google services
- **Multi-part support** - Groups related archive parts together
- **Flexible input** - Select folders or individual files
- **Real-time progress** - See scanning progress as it happens
- **Interactive tables** - Sort and filter results
- **CSV export** - Export summaries for further analysis
- **Rotating logs** - Comprehensive logging with automatic rotation

## Deep pass with Takeout Inventory

Scout pairs each photo with its `.json` sidecar within a single archive. On a
multi-part export that is usually the wrong answer — in one measured export,
71.7% of photos had their sidecar in a *different* archive.

If [`Takeout_Inventory`](https://github.com/conradstorz/Takeout_Inventory) is
available, Scout offers to run it after a scan. It resolves pairings across
every archive at once and publishes an index, which Scout reads back as a work
list: orphaned media, orphaned sidecars, pairings whose location data cannot be
trusted, and every place Scout's own answer was wrong.

Inventory is run as a separate program, never imported — Scout is GPL-3.0 and
Inventory is AGPL-3.0. Scout works fully without it; the offer simply does not
appear.

**Nothing in your archives is modified.** Inventory writes its index and
cache alongside them, in the export directory: `takeout-index.sqlite`,
`takeout-index.json`, `inventory.json` and a cache directory. The output
Scout shows you is a list.

## Requirements

- Python 3.10+
- Streamlit and pandas (required dependencies, installed automatically)
- Optional: `loguru` for enhanced logging (falls back to standard logging)
- Optional: `Pillow` for reading photo EXIF metadata

## Installation

### Using uv (Recommended)

1. Clone this repository:
```bash
git clone https://github.com/conradstorz/Takeout-Scout.git
cd Takeout-Scout
```

2. Install with uv:
```bash
# Base install (streamlit + pandas)
uv pip install -e .

# With enhanced logging
uv pip install -e ".[logging]"

# With EXIF metadata support
uv pip install -e ".[exif]"

# With everything
uv pip install -e ".[full]"
```

### Using pip

1. Clone this repository:
```bash
git clone https://github.com/conradstorz/Takeout-Scout.git
cd Takeout-Scout
```

2. Install dependencies:
```bash
# Base install (streamlit + pandas)
pip install -e .

# With enhanced logging
pip install -e ".[logging]"

# With EXIF metadata support
pip install -e ".[exif]"

# With everything
pip install -e ".[full]"
```

## Usage

Run the web application:
```bash
streamlit run takeout_scout/app.py
```

Or use uv:
```bash
uv run streamlit run takeout_scout/app.py
```

Or use the convenience launcher (after installation):
```bash
uv run takeout-scout
```

The app will automatically open in your default web browser at `http://localhost:8501`.

### Workflow

**Folder Mode:**
1. Copy a folder path from File Explorer (e.g., `D:\My Takeout\`)
2. Paste it in the sidebar "Folder Path" field
3. Click "📁 Scan Folder"

**Files Mode:**
1. Select "Files" mode in the sidebar
2. Select files in File Explorer, Shift+Right-Click and choose "Copy as path"
3. Paste paths into the text area (one per line)
4. Click "📄 Scan Files"

### Features in the Web UI

- **Interactive Table** - Sort columns, view all results at a glance
- **Real-time Progress** - Progress bar shows scanning status
- **Summary Stats** - Total counts for files, photos, videos, JSON, and size
- **CSV Export** - Download results with timestamp
- **Clear Results** - Start fresh with one click

## Project Structure

```
Takeout-Scout/
├── takeout_scout/             # Scanning engine + UI (importable package)
│   ├── app.py                 # Streamlit web application
│   ├── cli.py                 # Launcher: starts Streamlit on app.py
│   ├── scanner.py             # Archive and directory scanning
│   ├── sidecar.py             # Google Takeout JSON sidecar parsing
│   ├── hashing.py             # File hashing utilities
│   ├── metadata.py            # EXIF metadata extraction
│   ├── discovery.py           # Discovery tracking system
│   ├── models.py              # Data models
│   ├── constants.py           # Constants and configuration
│   ├── logging.py             # Logging configuration
│   └── utils.py               # Utility functions
├── tests/                     # pytest suite for takeout_scout/
├── docs/superpowers/          # Design specs and implementation plans
├── logs/                      # Log files (auto-created)
│   └── takeout_scout.log
├── state/                     # Persistent state (auto-created)
│   └── takeout_index.json
├── discoveries_index.json    # Main index of all discoveries (auto-created)
├── takeouts_discovered/       # Per-source discovery records (auto-created)
├── README.md                  # This file
├── DISCOVERY_TRACKING.md      # How discovery records work
├── METADATA_FEATURES.md       # EXIF extraction details
├── LICENSE                    # GNU GPL v3 License
├── pyproject.toml             # Project configuration
├── requirements.txt           # Dependency list for pip users
└── .gitignore                 # Git ignore rules
```

## Design Philosophy

- **Idempotent** - Safe to run multiple times without side effects
- **Restful** - Each operation writes to its own directory
- **Incremental** - Future features (unpack, merge, dedupe) can be added without changing scan logic
- **User-friendly** - Clear GUI with progress indicators and helpful messages

## Future Enhancements

Planned features for future releases:
- Archive extraction with smart output organization
- JSON sidecar → EXIF metadata merging
- Duplicate detection and reporting
- File organization by date/service
- Batch processing automation

## Logging

All operations are logged to `logs/takeout_scout.log` with automatic rotation at 5MB.

## License

GNU General Public License v3 (GPLv3) - See LICENSE file for details

## Author

Created by ChatGPT for Conrad
