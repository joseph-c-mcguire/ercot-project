import os
import sqlite3
import subprocess
import tempfile
import pytest

# Path to the CLI module and test DB
CLI_MODULE = "ercot_scraping.run"


@pytest.mark.e2e
def test_full_cli_workflow_produces_non_empty_final_table():
    """
    End-to-end test: Runs the download command and all other CLI commands in run.py,
    then verifies that the FINAL table is non-empty.
    """
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = os.path.join(tmpdir, "test_e2e.db")
        python_exe = os.environ.get("PYTHON", "python")
        # Use a small date range for speed
        start_date = "2024-01-01"
        end_date = "2024-01-03"
        batch_days = "1"

        # 1. Run the download command
        subprocess.run([
            python_exe, "-m", CLI_MODULE, "download",
            "--start", start_date, "--end", end_date,
            "--db", db_path, "--batch-days", batch_days
        ], check=True)

        # 2. Run update-dam
        subprocess.run([
            python_exe, "-m", CLI_MODULE, "update-dam", "--db", db_path], check=True)
        # 3. Run update-spp
        subprocess.run([
            python_exe, "-m", CLI_MODULE, "update-spp", "--db", db_path], check=True)
        # 4. Run merge-data
        subprocess.run([
            python_exe, "-m", CLI_MODULE, "merge-data", "--db", db_path], check=True)

        # 5. Check FINAL table is non-empty
        conn = sqlite3.connect(db_path)
        try:
            cur = conn.cursor()
            cur.execute("SELECT COUNT(*) FROM FINAL")
            count = cur.fetchone()[0]
            assert count > 0, "FINAL table should not be empty after full workflow"
        finally:
            conn.close()
