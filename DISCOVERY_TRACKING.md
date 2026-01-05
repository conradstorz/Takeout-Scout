# Discovery Tracking System

## Overview
The Takeout Scout now includes a comprehensive discovery tracking system that persistently records detailed information about every Google Takeout source (ZIP, TGZ, or directory) that has been scanned.

## Directory Structure

```
takeout_scout/
├── app.py
├── ts.py
├── discoveries_index.json          # Main index of all discoveries
├── takeouts_discovered/            # Directory containing individual takeout records
│   ├── MyTakeout_a1b2c3d4e5f6.takeout_scout
│   ├── GooglePhotos_789abc123def.takeout_scout
│   └── ...
├── state/
│   └── takeout_index.json
└── logs/
    └── takeout_scout.log
```

## Files Created

### 1. `discoveries_index.json` (Main Index)
Located in the same directory where app.py/ts.py is run from.
Maps source paths to their discovery JSON filenames:

```json
{
  "C:\\Users\\You\\Takeouts\\takeout-20240101.zip": "takeout-20240101_a1b2c3d4e5f6.takeout_scout",
  "D:\\Archives\\Google Photos": "Google Photos_789abc123def.takeout_scout"
}
```

### 2. `takeouts_discovered/` Directory
Subdirectory containing individual JSON files for each discovered takeout.

Each file is named: `{original_name}_{hash}.takeout_scout`

Example: `takeout-20240101_a1b2c3d4e5f6.takeout_scout`

## Takeout Scout JSON Format

Each `.takeout_scout` file contains comprehensive information about a discovered takeout:

```json
{
  "source_path": "C:\\Users\\You\\Takeouts\\takeout-20240101.zip",
  "source_type": "zip",
  "first_discovered": "2026-01-02T14:30:00.123456",
  "last_scanned": "2026-01-02T15:45:00.789012",
  "parts_group": "takeout-20240101",
  "service_guess": "Google Photos",
  "file_count": 1523,
  "photos": 1250,
  "videos": 180,
  "json_sidecars": 1430,
  "other": 93,
  "compressed_size": 5368709120,
  "photos_with_exif": 1150,
  "photos_with_gps": 890,
  "photos_with_datetime": 1200,
  "photos_checked": 1250,
  "scan_count": 3,
  "file_details": [
    {
      "path": "Takeout/Google Photos/2023/IMG_1234.jpg",
      "size": 2456789,
      "file_type": "photo",
      "extension": ".jpg",
      "metadata": {
        "has_exif": true,
        "has_gps": true,
        "has_datetime": true,
        "datetime_original": "2023:05:15 14:32:10",
        "camera_make": "Apple",
        "camera_model": "iPhone 13 Pro",
        "width": 4032,
        "height": 3024
      }
    },
    {
      "path": "Takeout/Google Photos/2023/VID_5678.mp4",
      "size": 45678901,
      "file_type": "video",
      "extension": ".mp4",
      "metadata": null
    },
    {
      "path": "Takeout/Google Photos/2023/IMG_1234.jpg.json",
      "size": 1234,
      "file_type": "json",
      "extension": ".json",
      "metadata": null
    }
  ],
  "notes": ""
}
```

## Fields Explained

### Core Information
- **source_path**: Absolute path to the takeout source (file or directory)
- **source_type**: Type of source - `"zip"`, `"tgz"`, or `"directory"`
- **first_discovered**: ISO format timestamp of first scan
- **last_scanned**: ISO format timestamp of most recent scan
- **scan_count**: Number of times this takeout has been scanned

### Content Summary
- **parts_group**: Grouping name for multi-part archives
- **service_guess**: Best guess of Google service (Google Photos, Drive, etc.)
- **file_count**: Total number of files
- **photos**: Count of photo files
- **videos**: Count of video files
- **json_sidecars**: Count of JSON metadata files
- **other**: Count of other files
- **compressed_size**: Total size in bytes

### EXIF Metadata Statistics
- **photos_with_exif**: Number of photos containing EXIF data
- **photos_with_gps**: Number of photos with GPS coordinates
- **photos_with_datetime**: Number of photos with original date/time
- **photos_checked**: Number of photos analyzed for metadata

### Detailed File Information
- **file_details**: Array of objects, one per file in the takeout

Each file detail includes:
- `path`: Relative path within the archive/directory
- `size`: File size in bytes
- `file_type`: Classification - `"photo"`, `"video"`, `"json"`, or `"other"`
- `extension`: File extension (e.g., `.jpg`, `.mp4`)
- `metadata`: For photos, contains extracted EXIF data (or `null` for non-photos)

### User Notes
- **notes**: Free-form text field for user annotations (currently empty by default)

## How It Works

### Automatic Tracking
When you scan a takeout:

1. **Scan function** extracts all file information and metadata
2. **Unique ID** is generated using the source path (hashed for uniqueness)
3. **Check for existing record**:
   - If this is a rescan, preserves `first_discovered` and increments `scan_count`
   - If new, creates fresh discovery record
4. **Save JSON file** to `takeouts_discovered/` directory
5. **Update main index** in `discoveries_index.json`

### ID Generation
Each takeout gets a unique ID based on its absolute path:
```python
{sanitized_name}_{12_char_hash}.takeout_scout
```

This ensures:
- Consistent naming across rescans
- No collisions even with same filename in different locations
- Human-readable base name for easy identification

## Usage

### In Code

Both `scan_archive()` and `scan_directory()` now accept a `save_discovery` parameter:

```python
# Save discovery info (default)
summary = scan_archive(Path("takeout.zip"))

# Skip discovery tracking
summary = scan_archive(Path("takeout.zip"), save_discovery=False)
```

### Loading Existing Discoveries

```python
from pathlib import Path

# Load a specific takeout's discovery record
discovery = load_takeout_discovery(Path("C:\\path\\to\\takeout.zip"))

if discovery:
    print(f"First scanned: {discovery.first_discovered}")
    print(f"Times scanned: {discovery.scan_count}")
    print(f"Total photos: {discovery.photos}")
    print(f"Photos with GPS: {discovery.photos_with_gps}")
    
    # Access detailed file list
    for file in discovery.file_details:
        if file['file_type'] == 'photo' and file['metadata']:
            print(f"{file['path']}: {file['metadata']['camera_model']}")
```

### Querying All Discoveries

```python
# Load the main index
index = load_discoveries_index()

# Iterate through all discovered takeouts
for source_path, json_filename in index.items():
    discovery = load_takeout_discovery(Path(source_path))
    if discovery:
        print(f"{discovery.parts_group}: {discovery.photos} photos")
```

## Benefits

### 1. **Persistent Knowledge**
Never lose track of what you've scanned. Even if you move or rename archives, the discovery records preserve the original information.

### 2. **Rescan Detection**
Automatically identifies when you're scanning the same takeout again and updates accordingly.

### 3. **Complete File Inventory**
Every file is cataloged with its path, size, type, and metadata.

### 4. **Metadata Preservation**
EXIF data from photos is extracted and stored, even if the original archive is later deleted.

### 5. **Analysis Ready**
JSON format makes it easy to:
- Query with scripts
- Import into databases
- Generate reports
- Build dashboards

### 6. **Privacy Audits**
Quickly identify which takeouts contain GPS-tagged photos or other sensitive metadata.

## Future Enhancements

Potential features for future versions:
- Web UI to browse all discoveries
- Search across all takeouts
- Compare changes between scans
- Export discoveries to CSV/SQLite
- Merge metadata from JSON sidecars into discovery records
- Tag and categorize takeouts
- Track file movements/deduplication
- Generate timeline visualizations from photo dates

## Technical Details

### Performance
- Discovery saving is optional (can be disabled for quick scans)
- JSON files are written only after successful scans
- File details are collected during the existing scan pass (no extra processing)

### Storage
- Each discovery file is typically 1-10 KB for small takeouts
- Large takeouts (10,000+ files) may create 100-500 KB discovery files
- The `takeouts_discovered/` directory will grow with each unique takeout scanned

### Compatibility
- Works with both GUI (ts.py) and Web (app.py) versions
- All discovery files are portable (can be copied to other systems)
- JSON format ensures long-term readability
