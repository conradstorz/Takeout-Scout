# Photo Metadata Extraction Features

## Overview
The Takeout Scout application now includes photo metadata extraction capabilities. When scanning Google Takeout archives, the tool will analyze EXIF data from photo files and provide statistics about:

- Photos with EXIF metadata
- Photos with GPS location data
- Photos with original date/time information
- Total photos checked for metadata

## Requirements

Install Pillow to enable metadata extraction:

```bash
pip install Pillow>=10.0.0
```

If Pillow is not installed, the application will still work but metadata extraction will be disabled.

## Features Added

### 1. EXIF Metadata Extraction
The application extracts the following metadata from photos:
- **EXIF presence**: Whether the photo has any EXIF data
- **GPS data**: Whether the photo contains GPS coordinates
- **DateTime**: Original capture date/time from the camera
- **Camera info**: Make and model (extracted but not displayed in summary)
- **Dimensions**: Width and height (extracted but not displayed in summary)

### 2. Supported Photo Formats
Metadata extraction works with these formats:
- JPEG (.jpg, .jpeg)
- PNG (.png)
- HEIC/HEIF (.heic, .heif)
- TIFF (.tif, .tiff)
- WebP (.webp)
- GIF (.gif)
- BMP (.bmp)
- RAW formats (.raw, .dng, .arw, .cr2, .nef)

### 3. Archive Support
Metadata can be extracted from photos in:
- ZIP archives (.zip)
- Gzipped tar archives (.tgz, .tar.gz)
- Uncompressed directories

### 4. Updated Display Columns

#### GUI (ts.py)
New columns in the table view:
- **w/EXIF**: Count of photos with EXIF data
- **w/GPS**: Count of photos with GPS coordinates
- **w/Date**: Count of photos with original date/time
- **Checked**: Total photos analyzed for metadata

#### Web UI (app.py)
Same columns added to the Streamlit interface

#### CSV Export
CSV files now include these additional columns:
- Photos w/EXIF
- Photos w/GPS
- Photos w/DateTime
- Photos Checked

## Technical Details

### PhotoMetadata Class
A new dataclass stores extracted metadata:
```python
@dataclass
class PhotoMetadata:
    has_exif: bool = False
    has_gps: bool = False
    has_datetime: bool = False
    camera_make: Optional[str] = None
    camera_model: Optional[str] = None
    datetime_original: Optional[str] = None
    gps_latitude: Optional[float] = None
    gps_longitude: Optional[float] = None
    width: Optional[int] = None
    height: Optional[int] = None
```

### Extraction Functions
- `extract_photo_metadata(file_data, filename)`: Core extraction from bytes
- `extract_metadata_from_zip(zf, member_path)`: Extract from ZIP archive members
- `extract_metadata_from_tar(tf, member_path)`: Extract from TAR archive members

### Performance Considerations
- Metadata extraction is performed during the scan phase
- Each photo file is read once to extract metadata
- Debug logging tracks extraction failures without disrupting the scan
- Large archives may take longer to scan with metadata extraction enabled

## Use Cases

### Finding Photos Without Metadata
Use the metadata columns to identify:
- Photos missing EXIF data (might be screenshots or edited images)
- Photos without GPS data (useful for organizing by location)
- Photos without date/time info (might need manual organization)

### Data Quality Assessment
Compare the "Photos" count with "Checked" to see:
- How many photos were successfully analyzed
- Whether any photos failed to load

### GPS-Tagged Photos
The "w/GPS" column helps identify which archives contain location-tagged photos, useful for:
- Privacy review before sharing
- Creating location-based albums
- Travel photo organization

## Future Enhancements
Potential improvements for future versions:
- Display GPS coordinates in detail view
- Extract and display specific date ranges
- Filter/search by camera make/model
- Export detailed metadata to separate JSON/CSV
- Analyze metadata discrepancies with JSON sidecars
- Merge JSON sidecar data into EXIF tags
