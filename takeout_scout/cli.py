#!/usr/bin/env python3
"""
Takeout Scout launcher.

Starts the Streamlit web interface (app.py). Lives inside the takeout_scout/
package so it ships in the wheel and the takeout-scout console script can
find it after a non-editable install.
"""
from __future__ import annotations

import sys
import subprocess
from pathlib import Path


def main():
    """Launch the Streamlit app."""
    # resolve() because __file__ can be relative depending on how Python was
    # invoked, and Streamlit is handed this path as a string to open.
    script_path = Path(__file__).resolve().parent / "app.py"

    # Run streamlit with the app file
    cmd = [sys.executable, "-m", "streamlit", "run", str(script_path)] + sys.argv[1:]

    try:
        completed = subprocess.run(cmd)
    except KeyboardInterrupt:
        print("\n\nShutting down Takeout Scout...")
        sys.exit(0)

    # Exit with Streamlit's own code rather than collapsing every failure to 1.
    # A supervisor or CI step reading this needs to tell "port already in use"
    # from "killed" from "bad argument", and check=True threw all of that away.
    if completed.returncode != 0:
        print(f"Streamlit exited with code {completed.returncode}")
    sys.exit(completed.returncode)


if __name__ == "__main__":
    main()
