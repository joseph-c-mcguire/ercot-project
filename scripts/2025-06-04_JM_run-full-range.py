"""
Script: 2025-06-04_JM_run-full-range.py

Purpose:
    Downloads DAM (Day-Ahead Market) and SPP (Settlement Point Prices) data
    for a specified date range, then merges them into the FINAL table using
    the ERCOT CLI tool.

How to Use:
    1. Set the DB_FILE, PYTHON, START_DATE, and END_DATE variables at the top
       of this script to match your environment and desired date range.
    2. Run the script:
        python scripts/2025-06-04_JM_run-full-range.py
    3. The script will use the unified `download` command to fetch and merge
       all data in batches with checkpointing.
    4. The script uses your local Python environment and the CLI module
       ercot_scraping.run.

Notes:
    - Make sure your .env file is set up with ERCOT API credentials.
    - If you encounter file path errors on Windows, ensure you include the
      drive letter (e.g., 'Z:').
"""

import subprocess

DB_FILE = "Z:\\Databases\\000_ERCOT\\ercot_data.db"
# DB_FILE = "_data/ercot_data.db"  # Relative path to the database file
# Change this to your Python executable path if needed
PYTHON = "Z:\\programming (PYTHON)\\JOSEPH-MCGUIRE\\GITHUB\\ercot-project\\.venv\\Scripts\\python.exe"
# Use 'python' if running in a virtual environment or system Python
# PYTHON = "C:\\Users\\bigme\\OneDrive\\Documents\\GitHub\\ercot-project\\.venv_3.9\\Scripts\\python.exe"

# These test the archive and current APIs for the DAM data.
# START_DATE = "2024-01-30"
# END_DATE = "2024-01-30"
# These test the archive and current APIs for the SPP data.
# START_DATE = "2023-12-10"
# END_DATE = "2023-12-12"
# Final Run
# START_DATE = "2020-01-01"
# END_DATE = "2025-06-04"

# DB_FILE = "_data/ercot_data.db"
# PYTHON = (
#     "C:\\Users\\bigme\\OneDrive\\Documents\\GitHub\\ercot-project"
#     "\\.venv_3.9\\Scripts\\python.exe"
# )
START_DATE = "2023-02-01"
END_DATE = "2024-02-02"  # Adjust the date range as needed
BATCH_DAYS = "100"  # Use 1 day batches for full range


def run_command(args):
    """
    Execute a system command by printing and running the given command
    arguments.
    """
    print(f"Running: {' '.join(args)}")
    subprocess.run(args, check=True)


# --- Unified download and merge using the new CLI ---
run_command([
    PYTHON, "-m", "ercot_scraping.run", "download",
    "--start", START_DATE, "--end", END_DATE,
    "--db", DB_FILE,
    "--batch-days", BATCH_DAYS,  # Use 1 day batches for full range
])
