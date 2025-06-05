"""
Script: 2025-06-04_JM_run-full-range.py

Purpose:
    Downloads DAM (Day-Ahead Market) and SPP (Settlement Point Prices) data for a specified date range, then merges them into the FINAL table using the ERCOT CLI tool.

How to Use:
    1. Set the DB_FILE, PYTHON, START_DATE, and END_DATE variables at the top of this script to match your environment and desired date range.
    2. Run the script:
        python scripts/2025-06-04_JM_run-full-range.py
    3. The script will sequentially download DAM and SPP data for the given range, then merge the data into the FINAL table.
    4. The script uses your local Python environment and the CLI module ercot_scraping.run.

Notes:
    - Make sure your .env file is set up with ERCOT API credentials.
    - If you encounter file path errors on Windows, ensure you include the drive letter (e.g., 'Z:').
"""

# run_full_range.py
import subprocess
from datetime import datetime, timedelta

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
START_DATE = "2000-01-01"
END_DATE = "2025-06-04"


def run_command(args):
    """Execute a system
    command by printing
    and running the given
    command arguments.

    Parameters:
        args (list of str): The command and its arguments to be executed.

    Raises:
        subprocess.CalledProcessError: If the command exits with a non-zero status.
    """
    print(f"Running: {' '.join(args)}")
    subprocess.run(args, check=True)


def offset_date(date_str, days):
    """Offsets a given
    date by subtracting a
    specified number of
    days.

    Parameters:
        date_str (str): A date string in the format 'YYYY-MM-DD'.
        days (int): The number of days to subtract from the given date.

    Returns:
        str: The new date, formatted as 'YYYY-MM-DD'.

    Raises:
        ValueError: If the provided date_str does not match the expected format.
    """
    return (datetime.strptime(date_str, "%Y-%m-%d") - timedelta(days=days)).strftime("%Y-%m-%d")


SPP_START_DATE = offset_date(START_DATE, 60)
SPP_END_DATE = offset_date(END_DATE, 60)

run_command([
    PYTHON, "-m", "ercot_scraping.run", "historical-dam",
    "--start", START_DATE, "--end", END_DATE,
    "--db", DB_FILE
])

run_command([
    PYTHON, "-m", "ercot_scraping.run", "historical-spp",
    "--start", SPP_START_DATE, "--end", SPP_END_DATE,
    "--db", DB_FILE
])

run_command([
    PYTHON, "-m", "ercot_scraping.run", "merge-data",
    "--start", START_DATE, "--end", END_DATE,
    "--db", DB_FILE
])
