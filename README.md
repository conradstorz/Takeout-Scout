# Takeout Scout

A lightweight, GUI-based tool for scanning and analyzing Google Takeout archives without extraction.

## Features

- **Non-destructive scanning** - Analyzes ZIP and TGZ archives without extracting
- **Smart detection** - Identifies content types (photos, videos, JSON sidecars) and Google services
- **Multi-part support** - Groups related archive parts together
- **Change tracking** - Detects new and missing archives between scans
- **CSV export** - Export summaries for further analysis
- **Rotating logs** - Comprehensive logging with automatic rotation

## Requirements

- Python 3.8+
- Standard library only (tkinter included with Python)
- Optional: `loguru` for enhanced logging (falls back to standard logging)

## Installation

1. Clone this repository:
```bash
git clone https://github.com/YOUR_USERNAME/Takeout_Scout.git
cd Takeout_Scout
```

2. (Optional) Install loguru for better logging:
```bash
pip install loguru
```

## Usage

Run the application:
```bash
python ts.py
```

### Workflow

1. **Choose Folder** - Select the directory containing your Google Takeout archives
2. **Scan** - The tool will scan all ZIP/TGZ files and summarize their contents
3. **Review** - View the summary table showing:
   - Archive names (multi-part archives are grouped)
   - Service detection (Google Photos, Drive, etc.)
   - File counts by type (photos, videos, JSON sidecars)
   - Compressed sizes
4. **Export CSV** - Save the summary for record-keeping or further analysis

### Change Detection

On subsequent scans, Takeout Scout will:
- Highlight newly added archives with a **[NEW]** tag
- Report missing archives since the last scan
- Maintain a persistent index in `state/takeout_index.json`

## Project Structure

```
Takeout_Scout/
├── ts.py                      # Main application
├── logs/                      # Log files (auto-created)
│   └── takeout_scout.log
├── state/                     # Persistent state (auto-created)
│   └── takeout_index.json
├── README.md                  # This file
├── LICENSE                    # MIT License
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

All operations are logged to `logs/takeout_scout.log` with automatic rotation at 5MB. Access logs via the "Open Logs..." button in the GUI.

## License

MIT License - See LICENSE file for details

## Author

Created by ChatGPT for Conrad
