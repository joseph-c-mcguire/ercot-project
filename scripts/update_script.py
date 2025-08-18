import subprocess
import sqlite3
import logging
from datetime import datetime, timedelta

DB_FILE = r"\\COHESION-NAS\z_work\Databases\000_ERCOT\ERCOT DBs\2025-06-25-11-15.db"
PYTHON = r"\\COHESION-NAS\z_work\programming (PYTHON)\JOSEPH-MCGUIRE\GITHUB\ercot-project\.venv_3.9\Scripts\python.exe"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("ercot_update_script.log",
                            mode='a', encoding='utf-8'),
        logging.StreamHandler()
    ]
)

logging.info(f"Connecting to database: {DB_FILE}")
current_year = datetime.now().year
db_file_name = DB_FILE.rstrip('.db')
year_db_file = f"{db_file_name}_{current_year}.db"
with sqlite3.connect(year_db_file) as conn:
    cur = conn.cursor()
    logging.info(
        "Querying for all unique DELIVERYDATE values in COMBINED_BIDS...")
    cur.execute("SELECT DISTINCT DELIVERYDATE FROM COMBINED_BIDS")
    date_rows = cur.fetchall()
    if not date_rows:
        logging.error("No DELIVERYDATE values found in COMBINED_BIDS table.")
        raise ValueError(
            "No DELIVERYDATE values found in COMBINED_BIDS table.")
    logging.info(f"Found {len(date_rows)} unique DELIVERYDATE values.")

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
                logging.warning(f"Could not parse DELIVERYDATE: {date_str}")

    if not parsed_dates:
        logging.error("No valid DELIVERYDATE values could be parsed.")
        raise ValueError("No valid DELIVERYDATE values could be parsed.")

    latest_date = max(parsed_dates)
    logging.info(f"Latest parsed DELIVERYDATE: {latest_date}")

start_date = latest_date.replace(day=1)
logging.info(f"Start date (first of month): {start_date.strftime('%Y-%m-%d')}")

sixty_days_prior = (datetime.today() - timedelta(days=60)).replace(day=1)
end_date = sixty_days_prior
logging.info(
    f"End date (first of month, 60 days prior): {end_date.strftime('%Y-%m-%d')}")

if start_date.year == end_date.year and start_date.month == end_date.month:
    logging.info("Date range is present in database, skipping.")
else:
    start_str = start_date.strftime("%Y-%m-%d")
    end_str = end_date.strftime("%Y-%m-%d")
    logging.info(f"Preparing to run pipeline from {start_str} to {end_str}...")
    cmd = [
        PYTHON,
        "-m",
        "scripts.improved_ercot_pipeline",
        "--start", start_str,
        "--end", end_str,
        "--db", DB_FILE,
        "--clear-cache"
    ]
    logging.info("Running command:")
    logging.info(" ".join(cmd))
    subprocess.run(cmd, check=True)
    logging.info("Pipeline script executed successfully.")
