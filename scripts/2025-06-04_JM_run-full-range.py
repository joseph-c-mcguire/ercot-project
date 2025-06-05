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
# DB_FILE = "Z:\\Databases\\000_ERCOT\\ercot_data.db"
DB_FILE = "_data/ercot_data.db"  # Relative path to the database file
# Change this to your Python executable path if needed
# PYTHON = "Z:\\programming (PYTHON)\\JOSEPH-MCGUIRE\\GITHUB\\ercot-project\\.venv\\Scripts\\python.exe"
# Use 'python' if running in a virtual environment or system Python
PYTHON = "C:\\Users\\bigme\\OneDrive\\Documents\\GitHub\\ercot-project\\.venv_3.9\\Scripts\\python.exe"
START_DATE = "2025-01-31"
END_DATE = "2025-02-28"


def run_command(args):
    """
    Execute a system command by printing and running the given command arguments.

    Parameters:
        args (list of str): The command and its arguments to be executed.

    Raises:
        subprocess.CalledProcessError: If the command exits with a non-zero status.
    """
    print(f"Running: {' '.join(args)}")
    subprocess.run(args, check=True)


run_command([
    PYTHON, "-m", "ercot_scraping.run", "historical-dam",
    "--start", START_DATE, "--end", END_DATE,
    "--db", DB_FILE
])


run_command([
    PYTHON, "-m", "ercot_scraping.run", "historical-spp",
    "--start", START_DATE, "--end", END_DATE,
    "--db", DB_FILE
])

run_command([
    PYTHON, "-m", "ercot_scraping.run", "merge-data",
    "--start", START_DATE, "--end", END_DATE,
    "--db", DB_FILE
])
