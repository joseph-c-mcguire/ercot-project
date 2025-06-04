# run_full_range.py
import subprocess
DB_FILE = "\\Cohesion-nas\\z_work\\Databases\\000_ERCOT\\ercot_data.db"
# Change this to your Python executable path if needed
PYTHON = "Z:\\programming (PYTHON)\\JOSEPH-MCGUIRE\\GITHUB\\ercot-project\\.venv\\Scripts\\python.exe"
START_DATE = "2025-01-31"
END_DATE = "2025-02-04"


def run_command(args):
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
