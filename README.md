# ERCOT Data Scraper

This tool allows you to fetch historical and current ERCOT market data including Day Ahead Market (DAM) energy bids/offers and Settlement Point Prices (SPP).

## Getting Started

### 1. Clone the Repository

First, download the code from GitHub:

```bash
git clone https://github.com/joseph-c-mcguire/ercot-project.git
cd ercot-project
```

### 2. Install as a Local Python Package

Install the dependencies and the package itself (recommended in a virtual environment):

```bash
pip install .
```

If you want to make edits and have them reflected immediately, use editable mode:

```bash
pip install -e .
```

### 3. Set Up Environment Variables

Create a `.env` file in the root directory with your ERCOT API credentials:

```env
ERCOT_API_USERNAME=your_username
ERCOT_API_PASSWORD=your_password
ERCOT_API_SUBSCRIPTION_KEY=your_subscription_key
```

## CLI Usage

You can run the CLI using the Python module syntax:

```bash
python -m ercot_scraping.run <command> [options]
```

Or, if you have an entry point script, you can use:

```bash
python ercot_scraping <command> [options]
```

### Example Commands

- **Download historical DAM data:**

  ```bash
  python -m ercot_scraping.run historical-dam --start 2024-01-01 --end 2024-01-02
  ```

- **Download historical SPP data:**

  ```bash
  python -m ercot_scraping.run historical-spp --start 2024-01-01 --end 2024-01-02
  ```

- **Update daily DAM data:**

  ```bash
  python -m ercot_scraping.run update-dam
  ```

- **Update daily SPP data:**

  ```bash
  python -m ercot_scraping.run update-spp
  ```

- **Merge data into the FINAL table:**

  ```bash
  python -m ercot_scraping.run merge-data
  ```

### Additional CLI Options

- `--db <filename>`: Specify a custom SQLite database file (default: `_data/ercot_data.db`)
- `--qse-filter <csv or list>`: Filter by QSEs using a CSV file or comma-separated list
- `--debug`: Enable detailed debug logging
- `--quick-test`: Run a quick test with a small QSE set and short date range

For full help and all options, run:

```bash
python -m ercot_scraping.run --help
```

## CLI Functions

- `historical-dam`: Downloads historical DAM data including:
  - Energy Bid Awards
  - Energy Bids
  - Energy Only Offer Awards
  - Energy Only Offers

- `historical-spp`: Downloads historical Settlement Point Prices for:
  - Nodes
  - Hubs
  - Load Zones

- `merge-data`: Combines data from multiple database files into a single database

## Important Notes on Data Availability and Merging

- The `merge-data` command creates the `FINAL` table by merging data from the BID_AWARDS, BIDS, and SETTLEMENT_POINT_PRICES tables.
- **You must have already downloaded data for the desired date range using the `historical-dam` and `historical-spp` commands before running `merge-data`.**
- If you run `merge-data` for a date range where data is missing in any of the required tables, the `FINAL` table may be incomplete or empty for those dates.

## Environment Variables

Required variables in `.env`:

| Variable | Description |
|----------|-------------|
| ERCOT_API_USERNAME | Your ERCOT API username |
| ERCOT_API_PASSWORD | Your ERCOT API password |
| ERCOT_API_SUBSCRIPTION_KEY | Your ERCOT API subscription key |

## Output

The tool creates/updates a SQLite database (`_data/ercot_data.db`) with the following tables:

- SETTLEMENT_POINT_PRICES
- BID_AWARDS
- BIDS  
- OFFER_AWARDS
- OFFERS
- FINAL 

Data is stored in a normalized format with consistent column names and data types.

## Debug Mode

Add `--debug` flag to any command for detailed logging:

```bash
python -m ercot_scraping.run historical-dam --start 2024-01-01 --end 2024-01-02 --debug
```

## CLI Defaults and Argument Details

- `--db <filename>`: SQLite database file (default: `_data/ercot_data.db`)
- `--start <YYYY-MM-DD>`: Start date for data download or merge (required for `historical-dam`, `historical-spp`; optional for `merge-data`)
- `--end <YYYY-MM-DD>`: End date for data download or merge (optional; defaults to today for download commands, not set for `merge-data`)
- `--qse-filter <csv or list>`: QSE filter as CSV file or comma-separated list (optional)
- `--debug`: Enable detailed debug logging (optional)
- `--quick-test`: Run a quick test with a small QSE set and short date range (optional)

See the help output (`python -m ercot_scraping.run --help`) for full details on all arguments and their defaults.
