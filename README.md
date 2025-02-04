# ercot_scraping

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
ERCOT_API_PRIMARY_KEY=your_api_key_here
ERCOT_API_URL=https://api.ercot.com/api/public-reports/np6-905-cd/spp_node_zone_hub
```

Then, run the following script:

```sh
python -m ercot_scraping
```