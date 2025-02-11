# ercot_scraping
## Description
A python module to scrape data from the ERCOT(Electric Reliability Council of Texas) API for pulling all Day Ahead Market data and Settlement Point Prices data both historical as well as enabling daily updates. 


## Setting up Git LFS

To track .csv files with Git LFS, run the following commands:

```sh
git lfs install
git lfs track "*.csv"
```

Remember to commit the changes to the .gitattributes file:

```sh
git add .gitattributes
git commit -m "Track .csv files with Git LFS"
```

## Setting up the SQLite Database

To set up the SQLite database with the required tables, run the following script:

```sh
python src/setup-database.py
```

## Fetching and Storing Settlement Point Prices

To fetch settlement point prices from the ERCOT API and store them in the SQLite database, you need to set up your API key and URL in a `.env` file.

1. Create a `.env` file in the root directory of the project.
2. Add your ERCOT API key and URL to the `.env` file:

```env
ERCOT_API_SUBSCRIPTION_KEY=your_api_key_here
ERCOT_API_BASE_URL=https://api.ercot.com/api/public-reports/np6-905-cd/spp_node_zone_hub
```

Ensure that the API key is valid and has the necessary permissions.

Then, run the following script:

```sh
python -m ercot_scraping
```

## Fetching Data from New Endpoints

To fetch data from the new ERCOT API endpoints, you can use the following functions:

- `fetch_dam_energy_bid_awards(start_date, end_date)`
- `fetch_dam_energy_bids(start_date, end_date)`
- `fetch_dam_energy_only_offer_awards(start_date, end_date)`
- `fetch_dam_energy_only_offers(start_date, end_date)`

Example usage:

```python
from ercot_scraping.ercot_api import (
    fetch_dam_energy_bid_awards,
    fetch_dam_energy_bids,
    fetch_dam_energy_only_offer_awards,
    fetch_dam_energy_only_offers,
)

data_bid_awards = fetch_dam_energy_bid_awards(start_date="2023-10-01", end_date="2023-10-02")
data_bids = fetch_dam_energy_bids(start_date="2023-10-01", end_date="2023-10-02")
data_offer_awards = fetch_dam_energy_only_offer_awards(start_date="2023-10-01", end_date="2023-10-02")
data_offers = fetch_dam_energy_only_offers(start_date="2023-10-01", end_date="2023-10-02")
```
<<<<<<< HEAD

## Command Line Interface (CLI)

The package provides a command-line interface with four main commands for downloading and updating ERCOT data.

### Installation

After installation, you can use the CLI in two ways:

1. Using the installed script:
   ```sh
   ercot-scraping <command> [options]
   ```

2. Running as a module:
   ```sh
   python -m ercot_scraping <command> [options]
   ```

### Available Commands

- `download-historical-dam`: Download Historical DAM Data
- `download-historical-spp`: Download Historical SPP Data
- `update-daily-dam`: Update Daily DAM Data
- `update-daily-spp`: Update Daily SPP Data

### Examples

- Download one month of historical DAM data:
  ```sh
  ercot-scraping download-historical-dam --start-date 2023-01-01 --end-date 2023-01-31
  ```

- Download historical SPP data with custom database:
  ```sh
  ercot-scraping download-historical-spp --start-date 2023-01-01 --end-date 2023-01-31 --database custom.db
  ```

- Update daily DAM data with QSE filtering:
  ```sh
  ercot-scraping update-daily-dam --qse-filter qse_filter.csv
  ```

- Update daily SPP data:
  ```sh
  ercot-scraping update-daily-spp
  ```

### QSE Filtering

The --qse-filter option allows you to filter DAM data by QSE (Qualified Scheduling Entity) names. The filter file should be a CSV with a "SHORT NAME" column containing the QSE short names to include:

Only data for the listed QSEs will be downloaded and stored in the database.

// ...existing code...

=======
>>>>>>> origin/main
