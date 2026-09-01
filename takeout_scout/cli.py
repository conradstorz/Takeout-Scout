#!/usr/bin/env python3
"""
Takeout Scout launcher.

Starts the Streamlit web interface (app.py). Lives inside the takeout_scout/
package so it ships in the wheel and the takeout-scout console script can
find it after a non-editable install.
"""
import sys
import subprocess
from pathlib import Path


def main():
    """Launch the Streamlit app."""
    script_path = Path(__file__).parent / "app.py"
    
    # Run streamlit with the app file
    cmd = [sys.executable, "-m", "streamlit", "run", str(script_path)] + sys.argv[1:]
    
    try:
        subprocess.run(cmd, check=True)
    except KeyboardInterrupt:
        print("\n\nShutting down Takeout Scout...")
        sys.exit(0)
    except subprocess.CalledProcessError as e:
        print(f"Error launching Streamlit: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
