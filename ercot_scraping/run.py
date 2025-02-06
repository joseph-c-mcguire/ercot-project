import logging
from datetime import datetime, timedelta
from typing import Optional, Set
from ercot_scraping.ercot_api import (
    fetch_settlement_point_prices,
    fetch_dam_energy_bid_awards,
    fetch_dam_energy_bids,
    fetch_dam_energy_only_offers,
    fetch_dam_energy_only_offer_awards,
)
from ercot_scraping.store_data import (
    store_prices_to_db,
    store_bid_awards_to_db,
    store_bids_to_db,
    store_offers_to_db,
    store_offer_awards_to_db,
)
from ercot_scraping.filters import load_qse_shortnames

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def download_historical_dam_data(
    start_date: str,
    end_date: Optional[str] = None,
    db_name: str = "ercot.db",
    qse_filter: Optional[Set[str]] = None,
) -> None:
    """
    Downloads all historical DAM data from start_date to end_date.
    Includes bids, bid awards, offers, and offer awards.

    Args:
        start_date (str): Start date in YYYY-MM-DD format
        end_date (Optional[str]): End date in YYYY-MM-DD format. Defaults to current date
        db_name (str): Database filename
        qse_filter (Optional[Set[str]]): Set of QSE names to filter by
    """
    if end_date is None:
        end_date = datetime.now().strftime("%Y-%m-%d")

    logger.info(f"Downloading historical DAM data from {start_date} to {end_date}")

    try:
        # Fetch and store bid awards
        logger.info("Fetching bid awards...")
        bid_awards = fetch_dam_energy_bid_awards(start_date, end_date)
        store_bid_awards_to_db(bid_awards, db_name, qse_filter)

        # Fetch and store bids
        logger.info("Fetching bids...")
        bids = fetch_dam_energy_bids(start_date, end_date)
        store_bids_to_db(bids, db_name, qse_filter)

        # Fetch and store offer awards
        logger.info("Fetching offer awards...")
        offer_awards = fetch_dam_energy_only_offer_awards(start_date, end_date)
        store_offer_awards_to_db(offer_awards, db_name, qse_filter)

        # Fetch and store offers
        logger.info("Fetching offers...")
        offers = fetch_dam_energy_only_offers(start_date, end_date)
        store_offers_to_db(offers, db_name, qse_filter)

        logger.info("Historical DAM data download completed successfully")

    except Exception as e:
        logger.error(f"Error downloading historical DAM data: {str(e)}")
        raise


def download_historical_spp_data(
    start_date: str, end_date: Optional[str] = None, db_name: str = "ercot.db"
) -> None:
    """
    Downloads all historical Settlement Point Price data from start_date to end_date.

    Args:
        start_date (str): Start date in YYYY-MM-DD format
        end_date (Optional[str]): End date in YYYY-MM-DD format. Defaults to current date
        db_name (str): Database filename
    """
    if end_date is None:
        end_date = datetime.now().strftime("%Y-%m-%d")

    logger.info(f"Downloading historical SPP data from {start_date} to {end_date}")

    try:
        prices = fetch_settlement_point_prices(start_date, end_date)
        store_prices_to_db(prices, db_name)
        logger.info("Historical SPP data download completed successfully")

    except Exception as e:
        logger.error(f"Error downloading historical SPP data: {str(e)}")
        raise


def update_daily_dam_data(
    db_name: str = "ercot.db", qse_filter: Optional[Set[str]] = None
) -> None:
    """
    Downloads DAM data for the previous day.

    Args:
        db_name (str): Database filename
        qse_filter (Optional[Set[str]]): Set of QSE names to filter by
    """
    yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
    logger.info(f"Updating DAM data for {yesterday}")

    try:
        download_historical_dam_data(yesterday, yesterday, db_name, qse_filter)
        logger.info("Daily DAM data update completed successfully")

    except Exception as e:
        logger.error(f"Error updating daily DAM data: {str(e)}")
        raise


def update_daily_spp_data(db_name: str = "ercot.db") -> None:
    """
    Downloads Settlement Point Price data for the previous day.

    Args:
        db_name (str): Database filename
    """
    yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
    logger.info(f"Updating SPP data for {yesterday}")

    try:
        download_historical_spp_data(yesterday, yesterday, db_name)
        logger.info("Daily SPP data update completed successfully")

    except Exception as e:
        logger.error(f"Error updating daily SPP data: {str(e)}")
        raise


def main():
    """
    Main entry point for the script.
    This can be customized based on needs, but provides example usage of the functions.
    """
    db_name = "ercot.db"

    # Optional: Load QSE filter from CSV
    # qse_filter = load_qse_shortnames("path/to/qse_names.csv")
    qse_filter = None

    try:
        # Example: Download one month of historical data
        start_date = (datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d")
        download_historical_dam_data(start_date, db_name=db_name, qse_filter=qse_filter)
        download_historical_spp_data(start_date, db_name=db_name)

        # Example: Update daily data
        update_daily_dam_data(db_name, qse_filter)
        update_daily_spp_data(db_name)

    except Exception as e:
        logger.error(f"Error in main execution: {str(e)}")
        raise


if __name__ == "__main__":
    main()
