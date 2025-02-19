# ERCOT Data Scraper

This tool allows you to fetch historical and current ERCOT market data including Day Ahead Market (DAM) energy bids/offers and Settlement Point Prices (SPP).

## Setup

1. Install dependencies:
```bash
pip install .
```
If you want to make edits run it in editable mode:
```bash
pip install -e .
```

2. Create a `.env` file in the root directory with the following variables:
```env
ERCOT_API_USERNAME=your_username
ERCOT_API_PASSWORD=your_password 
ERCOT_API_SUBSCRIPTION_KEY=your_subscription_key
```

## CLI Usage

The CLI provides several commands to fetch and manage ERCOT data:

### Historical DAM Data

Fetches historical Day Ahead Market data including bids, offers, and awards.

```bash
python ercot_scraping historical-dam --start YYYY-MM-DD --end YYYY-MM-DD [--debug]
```

Example:
```bash
python ercot_scraping historical-dam --start 2024-01-01 --end 2024-01-02
```

### Historical Settlement Point Prices

Fetches historical Settlement Point Prices data.

```bash
python ercot_scraping historical-spp --start YYYY-MM-DD --end YYYY-MM-DD [--debug]
```

Example:
```bash
python ercot_scraping historical-spp --start 2024-01-01 --end 2024-01-02
```

### Merge Data

Merges data from multiple databases into a single database.

```bash
python ercot_scraping merge-data
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
python main.py historical-dam --start 2024-01-01 --end 2024-01-02 --debug
```
