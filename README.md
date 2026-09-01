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

## Requirements

- Python 3.8+
- Streamlit (included in dependencies)
- Optional: `loguru` for enhanced logging (falls back to standard logging)

## Installation

### Using uv (Recommended)

1. Clone this repository:
```bash
git clone https://github.com/conradstorz/Takeout-Scout.git
cd Takeout-Scout
```

2. Install with uv:
```bash
# Install with enhanced logging
uv pip install -e ".[logging]"

# Or install minimal version (standard library only)
uv pip install -e .
```

### Using pip

1. Clone this repository:
```bash
git clone https://github.com/conradstorz/Takeout-Scout.git
cd Takeout-Scout
```

2. Install dependencies:
```bash
# With enhanced logging
pip install -e ".[logging]"

# Or minimal install
pip install -e .
```

## Usage

Run the web application:
```bash
streamlit run app.py
```

Or use uv:
```bash
uv run streamlit run app.py
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
├── app.py                     # Streamlit web application
├── run_app.py                 # Launcher: starts Streamlit on app.py
├── takeout_scout/             # Scanning engine (importable package)
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
