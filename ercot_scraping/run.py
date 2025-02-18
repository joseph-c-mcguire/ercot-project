import logging
import requests
from datetime import datetime, timedelta
from typing import Optional, Set
from pathlib import Path
import argparse

from ercot_scraping.config import ERCOT_API_REQUEST_HEADERS, ERCOT_DB_NAME, ERCOT_ARCHIVE_PRODUCT_IDS
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
from ercot_scraping.utils import should_use_archive_api
from ercot_scraping.filters import load_qse_shortnames
from ercot_scraping.archive_api import (
    get_archive_document_ids,
    download_dam_archive_files,
    download_spp_archive_files
)

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def download_historical_dam_data(
    start_date: str,
    end_date: Optional[str] = None,
    db_name: str = ERCOT_DB_NAME,
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

    logger.info(
        f"Downloading historical DAM data from {start_date} to {end_date}")

    try:
        if should_use_archive_api(start_date, end_date):
            logger.info("Using archive API for historical DAM data")
            doc_ids = get_archive_document_ids(
                ERCOT_ARCHIVE_PRODUCT_IDS["DAM_BIDS"],
                start_date,
                end_date
            )
            logger.info(f"Found {len(doc_ids)} documents in archive")
            download_dam_archive_files(
                ERCOT_ARCHIVE_PRODUCT_IDS["DAM_BIDS"],
                doc_ids,
                db_name
            )
        else:
            logger.info("Using regular API for historical DAM data")
            # Fetch and store bid awards
            logger.info(f"Fetching bid awards for {start_date} to {end_date}...")
            bid_awards = fetch_dam_energy_bid_awards(
                start_date, end_date, header=ERCOT_API_REQUEST_HEADERS
            )
            if not bid_awards or "data" not in bid_awards:
                logger.error("No bid awards data found.")
            else:
                logger.info(f"Found {len(bid_awards.get('data', []))} bid awards")
                store_bid_awards_to_db(bid_awards, db_name, qse_filter)

            # Fetch and store bids
            logger.info(f"Fetching bids for {start_date} to {end_date}...")
            bids = fetch_dam_energy_bids(
                start_date, end_date, header=ERCOT_API_REQUEST_HEADERS
            )
            if bids and "data" in bids:
                logger.info(f"Found {len(bids.get('data', []))} bids")
                store_bids_to_db(bids, db_name, qse_filter)
            else:
                logger.error("No bids data found.")

            # Fetch and store offer awards
            logger.info(f"Fetching offer awards for {start_date} to {end_date}...")
            offer_awards = fetch_dam_energy_only_offer_awards(
                start_date, end_date, header=ERCOT_API_REQUEST_HEADERS
            )
            if offer_awards and "data" in offer_awards:
                logger.info(f"Found {len(offer_awards.get('data', []))} offer awards")
                store_offer_awards_to_db(offer_awards, db_name, qse_filter)
            else:
                logger.error("No offer awards data found.")

            # Fetch and store offers
            logger.info(f"Fetching offers for {start_date} to {end_date}...")
            offers = fetch_dam_energy_only_offers(
                start_date, end_date, header=ERCOT_API_REQUEST_HEADERS
            )
            if offers and "data" in offers:
                logger.info(f"Found {len(offers.get('data', []))} offers")
                store_offers_to_db(offers, db_name, qse_filter)
            else:
                logger.error("No offers data found.")

        logger.info("Historical DAM data download completed successfully")

    except Exception as e:
        logger.error(f"Error downloading historical DAM data: {str(e)}")
        raise


def download_historical_spp_data(
    start_date: str, end_date: Optional[str] = None, db_name: str = ERCOT_DB_NAME
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

    logger.info(
        f"Downloading historical SPP data from {start_date} to {end_date}")
    if should_use_archive_api(start_date, end_date):
            doc_ids = get_archive_document_ids(
                ERCOT_ARCHIVE_PRODUCT_IDS["SPP"],
                start_date,
                end_date
            )
            download_spp_archive_files(ERCOT_ARCHIVE_PRODUCT_IDS["SPP"], doc_ids, db_name=ERCOT_DB_NAME)
            return
    try:
        prices = fetch_settlement_point_prices(start_date, end_date)
        store_prices_to_db(prices, db_name)
        logger.info("Historical SPP data download completed successfully")
        return
    except Exception as e:
        logger.error(f"Error downloading historical SPP data: {str(e)}")
        raise


def update_daily_dam_data(
    db_name: str = ERCOT_DB_NAME, qse_filter: Optional[Set[str]] = None
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


def update_daily_spp_data(db_name: str = ERCOT_DB_NAME) -> None:
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


def parse_args() -> argparse.Namespace:
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description="ERCOT data downloading tool",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
    Examples:
    # Download historical DAM data for a date range
    python -m ercot_scraping.run historical-dam --start 2023-01-01 --end 2023-12-31
    
    # Download historical SPP data
    python -m ercot_scraping.run historical-spp --start 2023-01-01
    
    # Update daily DAM data
    python -m ercot_scraping.run update-dam
    
    # Update daily SPP data
    python -m ercot_scraping.run update-spp
    """,
    )

    subparsers = parser.add_subparsers(
        dest="command", help="Command to execute")

    # Historical DAM data command
    historical_dam = subparsers.add_parser(
        "historical-dam", help="Download historical DAM data"
    )
    historical_dam.add_argument(
        "--start", required=True, help="Start date (YYYY-MM-DD)"
    )
    historical_dam.add_argument("--end", help="End date (YYYY-MM-DD)")
    historical_dam.add_argument(
        "--db", default=ERCOT_DB_NAME, help="Database filename")
    historical_dam.add_argument(
        "--qse-filter", type=Path, help="Path to QSE filter CSV file"
    )

    # Historical SPP data command
    historical_spp = subparsers.add_parser(
        "historical-spp", help="Download historical SPP data"
    )
    historical_spp.add_argument(
        "--start", required=True, help="Start date (YYYY-MM-DD)"
    )
    historical_spp.add_argument("--end", help="End date (YYYY-MM-DD)")
    historical_spp.add_argument(
        "--db", default=ERCOT_DB_NAME, help="Database filename")

    # Update DAM data command
    update_dam = subparsers.add_parser(
        "update-dam", help="Update daily DAM data")
    update_dam.add_argument("--db", default=ERCOT_DB_NAME,
                            help="Database filename")
    update_dam.add_argument(
        "--qse-filter", type=Path, help="Path to QSE filter CSV file"
    )

    # Update SPP data command
    update_spp = subparsers.add_parser(
        "update-spp", help="Update daily SPP data")
    update_spp.add_argument("--db", default=ERCOT_DB_NAME,
                            help="Database filename")

    # Add debug flag
    parser.add_argument(
        "--debug", action="store_true", help="Enable debug logging"
    )

    return parser.parse_args()


def main():
    """Main entry point for the CLI."""
    args = parse_args()

    # Set logging level to DEBUG if --debug flag is specified
    if args.debug:
        logger.setLevel(logging.DEBUG)

    if not args.command:
        logger.error("No command specified. Use -h for help.")
        return

    try:
        # Load QSE filter if specified
        qse_filter = None
        if hasattr(args, "qse_filter") and args.qse_filter:
            qse_filter = load_qse_shortnames(args.qse_filter)

        # Execute the requested command
        if args.command == "historical-dam":
            download_historical_dam_data(
                start_date=args.start,
                end_date=args.end,
                db_name=args.db,
                qse_filter=qse_filter,
            )

        elif args.command == "historical-spp":
            download_historical_spp_data(
                start_date=args.start, end_date=args.end, db_name=args.db
            )

        elif args.command == "update-dam":
            update_daily_dam_data(db_name=args.db, qse_filter=qse_filter)

        elif args.command == "update-spp":
            update_daily_spp_data(db_name=args.db)

    except requests.exceptions.HTTPError as e:
        if e.response.status_code == 404:
            logger.error(f"API endpoint not found: {e.response.url}")
        else:
            logger.error(f"HTTP error occurred: {str(e)}")
        raise

    except Exception as e:
        logger.error(f"Error executing command: {str(e)}")
        raise


if __name__ == "__main__":
    main()
