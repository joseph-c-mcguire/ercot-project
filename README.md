# ERCOT Data Scraper

This tool allows you to fetch historical and current ERCOT market data including Day Ahead Market (DAM) energy bids/offers and Settlement Point Prices (SPP).

## Getting Started

> **Note for Windows users:**  
> If you encounter issues with file paths (e.g., "file not found" errors), ensure you include the drive letter (such as `Z:`) in your path. For example:  
> `Z:\Databases\000_ERCOT\ercot_data.db`  
> Omitting the drive letter may cause Python or scripts to fail to locate files.

### 1. Clone the Repository

```bash
# Clone the repository
git clone https://github.com/joseph-c-mcguire/ercot-project.git
cd ercot-project
```

### 2. Install as a Local Python Package

```bash
pip install .
# Or for development:
pip install -e .
```

### 3. Set Up Environment Variables

Create a `.env` file in the root directory with your ERCOT API credentials:

```env
ERCOT_API_USERNAME=your_username
ERCOT_API_PASSWORD=your_password
ERCOT_API_SUBSCRIPTION_KEY=your_subscription_key
```

---

## CLI Usage

You can run the CLI using the Python module syntax:

```bash
python -m ercot_scraping.run <command> [options]
```

### CLI Commands

| Command            | Description                                                                                      |
|--------------------|--------------------------------------------------------------------------------------------------|
| `historical-dam`   | Download historical Day-Ahead Market (DAM) data (bids, offers, awards) for a date range.         |
| `historical-spp`   | Download historical Settlement Point Prices (SPP) for a date range.                              |
| `update-dam`       | Download and update DAM data for the most recent day(s).                                         |
| `update-spp`       | Download and update SPP data for the most recent day(s).                                         |
| `merge-data`       | Merge data from BID_AWARDS, BIDS, and SETTLEMENT_POINT_PRICES into the FINAL table.              |

### Common Arguments

| Argument                | Description                                                                                 | Example Input                |
|-------------------------|---------------------------------------------------------------------------------------------|------------------------------|
| `--db <filename>`       | Path to SQLite database file (default: `_data/ercot_data.db`)                               | `--db mydata.db`             |
| `--start <YYYY-MM-DD>`  | Start date for data download/merge (required for historical commands)                       | `--start 2024-01-01`         |
| `--end <YYYY-MM-DD>`    | End date for data download/merge (optional; defaults to today for download commands)        | `--end 2024-01-31`           |
| `--qse-filter <csv/list>` | QSE filter as CSV file or comma-separated list (optional)                                 | `--qse-filter qses.csv`      |
| `--debug`               | Enable detailed debug logging                                                               | `--debug`                    |
| `--quick-test`          | Run a quick test with a small QSE set and short date range                                 | `--quick-test`               |

#### Example Commands

```bash
python -m ercot_scraping.run historical-dam --start 2024-01-01 --end 2024-01-02 --db mydata.db
python -m ercot_scraping.run historical-spp --start 2024-01-01 --end 2024-01-02 --db mydata.db
python -m ercot_scraping.run merge-data --start 2024-01-01 --end 2024-01-02 --db mydata.db
```

---

## Expected Inputs

- **Dates**: Must be in `YYYY-MM-DD` format.
- **QSE Filter**: Either a CSV file with QSE names or a comma-separated string.
- **Database**: Path to a writable SQLite file.

## Expected Outputs

- **Database Tables**:  
  - `SETTLEMENT_POINT_PRICES`
  - `BID_AWARDS`
  - `BIDS`
  - `OFFER_AWARDS`
  - `OFFERS`
  - `FINAL` (after merge)
- **Logs**: Console and file logs (if enabled) with progress, errors, and debug info.
- **Normalized Data**: All tables use consistent column names and types as defined in `data_models.py`.

---

## Module Details

- **data_models.py**: Defines the data structure for each table, ensuring consistent field names and types.
- **ercot_api.py**: Handles live API requests, batching, pagination, and normalization.
- **batched_api.py**: Splits large date ranges into batches, manages QSE filtering, and enforces API rate limits.
- **archive_api.py**: Downloads and processes historical archive files, normalizes CSVs, and stores data in the database.

---

## Script Usage

### `scripts/2025-06-04_JM_run-dam-to-latest.py`

**Purpose**:  
Automatically downloads DAM data from the latest date in your database up to today.

**How to use**:
1. Set `DB_FILE` and `PYTHON` path as needed.
2. Run:
   ```powershell
   python scripts/2025-06-04_JM_run-dam-to-latest.py
   ```

---

### `scripts/2025-06-04_JM_run-full-range.py`

**Purpose**:  
Downloads DAM and SPP data for a specified date range, then merges them into the FINAL table.

**How to use**:
1. Set `DB_FILE`, `PYTHON`, `START_DATE`, and `END_DATE` as needed.
2. Run:
   ```powershell
   python scripts/2025-06-04_JM_run-full-range.py
   ```

---

## Environment Variables

Required in `.env`:

| Variable | Description |
|----------|-------------|
| ERCOT_API_USERNAME | Your ERCOT API username |
| ERCOT_API_PASSWORD | Your ERCOT API password |
| ERCOT_API_SUBSCRIPTION_KEY | Your ERCOT API subscription key |

---

## Output

The tool creates/updates a SQLite database (`_data/ercot_data.db` by default) with the following tables:

- SETTLEMENT_POINT_PRICES
- BID_AWARDS
- BIDS  
- OFFER_AWARDS
- OFFERS
- FINAL 

Data is stored in a normalized format with consistent column names and data types.

---

## Debug Mode

Add `--debug` flag to any command for detailed logging:

```powershell
python -m ercot_scraping.run historical-dam --start 2024-01-01 --end 2024-01-02 --debug
```

---

## CLI Defaults and Argument Details

- `--db <filename>`: SQLite database file (default: `_data/ercot_data.db`)
- `--start <YYYY-MM-DD>`: Start date for data download or merge (required for `historical-dam`, `historical-spp`; optional for `merge-data`)
- `--end <YYYY-MM-DD>`: End date for data download or merge (optional; defaults to today for download commands, not set for `merge-data`)
- `--qse-filter <csv or list>`: QSE filter as CSV file or comma-separated list (optional)
- `--debug`: Enable detailed debug logging (optional)
- `--quick-test`: Run a quick test with a small QSE set and short date range (optional)

See the help output (`python -m ercot_scraping.run --help`) for full details on all arguments and their defaults.
