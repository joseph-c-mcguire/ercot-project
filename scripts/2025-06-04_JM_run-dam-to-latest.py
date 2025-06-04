"""
Script: 2025-06-04_JM_run-dam-to-latest.py

Purpose:
    Downloads DAM (Day-Ahead Market) data from the latest date in your database up
    to today using the ERCOT CLI tool.

How to Use:
    1. Set the DB_FILE and PYTHON variables at the top of this script to match your
        environment.
    2. Ensure your database already exists and contains the BID_AWARDS table.
    3. Run the script:
        python scripts/2025-06-04_JM_run-dam-to-latest.py
    4. The script will automatically determine the latest date in BID_AWARDS and
        fetch new DAM data up to today.
    5. The script uses your local Python environment and the CLI module
        ercot_scraping.run.

Notes:
    - Make sure your .env file is set up with ERCOT API credentials.
    - If you encounter file path errors on Windows, ensure you include the
        drive letter (e.g., 'Z:').
"""
# run_dam_to_latest.py
import subprocess
from datetime import datetime
import sqlite3
DB_FILE = "Z:\\Databases\\000_ERCOT\\ercot_data.db"
# Change this to your Python executable path if needed
PYTHON = "Z:\\programming (PYTHON)\\JOSEPH-MCGUIRE\\GITHUB\\ercot-project\\.venv\\Scripts\\python.exe"


def get_latest_date(db_file):
    """
    Retrieves the latest date from the BID_AWARDS table in the provided SQLite
    database.

    Parameters:
        db_file (str): The path to the SQLite database file.

    Returns:
        The latest date found in the BID_AWARDS table (the type depends on how
        dates are stored in the database).

    Raises:
        Exception: If no entries are found in the BID_AWARDS table.
    """
    conn = sqlite3.connect(db_file)
    cursor = conn.cursor()
    try:
        cursor.execute("SELECT MAX(date) FROM BID_AWARDS")
        result = cursor.fetchone()
        if result and result[0]:
            return result[0]
        raise ValueError("No entries found in BID_AWARDS table.")
    finally:
        conn.close()


def run_command(args):
    """
    Executes the given command using subprocess.run and raises an exception if
    the command exits with a non-zero status.
    """
    print(f"Running: {' '.join(args)}")
    subprocess.run(args, check=True)


today = datetime.now().strftime("%Y-%m-%d")
latest_date = get_latest_date(DB_FILE)
run_command([
    PYTHON, "-m", "ercot_scraping.run", "historical-dam",
    "--start", latest_date, "--end", today,
    "--db", DB_FILE
])
