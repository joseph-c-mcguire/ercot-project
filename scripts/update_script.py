import subprocess
import sqlite3
from datetime import datetime, timedelta

DB_FILE = r"Z:\Databases\000_ERCOT\ERCOT DBs\2025-06-25-11-15.db"
PYTHON = r"Z:\programming (PYTHON)\JOSEPH-MCGUIRE\GITHUB\ercot-project\.venv_3.9\Scripts\python.exe"

print(f"Connecting to database: {DB_FILE}")
current_year = datetime.now().year
db_file_name = DB_FILE.rstrip('.db')
year_db_file = f"{db_file_name}_{current_year}.db"
with sqlite3.connect(year_db_file) as conn:
    cur = conn.cursor()
    print("Querying for all unique DELIVERYDATE values in COMBINED_BIDS...")
    cur.execute("SELECT DISTINCT DELIVERYDATE FROM COMBINED_BIDS")
    date_rows = cur.fetchall()
    if not date_rows:
        raise ValueError(
            "No DELIVERYDATE values found in COMBINED_BIDS table.")
    print(f"Found {len(date_rows)} unique DELIVERYDATE values.")

    parsed_dates = []
    for row in date_rows:
        date_str = row[0]
        parsed = False
        for fmt in ("%Y-%m-%d", "%m/%d/%Y"):
            try:
                dt = datetime.strptime(date_str, fmt)
                parsed_dates.append(dt)
                parsed = True
                break
            except Exception:
                continue
        if not parsed:
            try:
                dt = datetime.strptime(date_str[:10], "%Y-%m-%d")
                parsed_dates.append(dt)
            except Exception:
                print(f"Warning: Could not parse DELIVERYDATE: {date_str}")

    if not parsed_dates:
        raise ValueError("No valid DELIVERYDATE values could be parsed.")

    latest_date = max(parsed_dates)
    print(f"Latest parsed DELIVERYDATE: {latest_date}")

start_date = latest_date.replace(day=1)
print(f"Start date (first of month): {start_date.strftime('%Y-%m-%d')}")

sixty_days_prior = (datetime.today() - timedelta(days=60)).replace(day=1)
end_date = sixty_days_prior
print(
    f"End date (first of month, 60 days prior): {end_date.strftime('%Y-%m-%d')}")

if start_date.year == end_date.year and start_date.month == end_date.month:
    print("Date range is present in database, skipping.")
else:
    start_str = start_date.strftime("%Y-%m-%d")
    end_str = end_date.strftime("%Y-%m-%d")
    print(f"Preparing to run pipeline from {start_str} to {end_str}...")
    cmd = [
        PYTHON,
        "-m",
        "scripts.improved_ercot_pipeline",
        "--start", start_str,
        "--end", end_str,
        "--db", DB_FILE,
        "--clear-cache"
    ]
    print("Running command:")
    print(" ".join(cmd))
    subprocess.run(cmd, check=True)
    print("Pipeline script completed.")
