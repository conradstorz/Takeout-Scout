"""
Utility functions for Takeout Scout.

Common helper functions used across the application.
"""
from __future__ import annotations


def human_size(n: int) -> str:
    """Convert bytes to human-readable size string.
    
    Args:
        n: Size in bytes
        
    Returns:
        Human-readable string (e.g., "1.23 GB")
        
    Examples:
        >>> human_size(1024)
        '1.00 KB'
        >>> human_size(1536)
        '1.50 KB'
        >>> human_size(1073741824)
        '1.00 GB'
    """
    if n < 0:
        return f"-{human_size(-n)}"
    
    units = ['B', 'KB', 'MB', 'GB', 'TB']
    size = float(n)
    
    for unit in units:
        if size < 1024 or unit == 'TB':
            return f"{size:.2f} {unit}"
        size /= 1024
    
    return f"{size:.2f} TB"
