import logging
import requests
from datetime import datetime, timedelta
from typing import Optional, Set
from pathlib import Path
import argparse

from ercot_scraping.config import ERCOT_API_REQUEST_HEADERS, ERCOT_DB_NAME, ERCOT_ARCHIVE_PRODUCT_IDS, QSE_FILTER_CSV
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
from ercot_scraping.merge_data import merge_data

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

    qse_filter = _load_qse_filter(qse_filter)

    logger.info(
        f"Downloading historical DAM data from {start_date} to {end_date}")
    logger.info(f"Filtering for QSEs: {sorted(qse_filter)}")

    try:
        if should_use_archive_api(start_date, end_date):
            _download_dam_data_from_archive(start_date, end_date, db_name)
        else:
            _fetch_and_store_historical_dam_data(
                start_date, end_date, qse_filter, db_name
            )
        logger.info("Historical DAM data download completed successfully")

    except Exception as e:
        logger.error(f"Error downloading historical DAM data: {str(e)}")
        raise


def _load_qse_filter(qse_filter: Optional[Set[str]]) -> Set[str]:
    """
    Loads the QSE filter from the provided set or from the tracking list.

    Args:
        qse_filter (Optional[Set[str]]): Set of QSE names to filter by

    Returns:
        Set[str]: Loaded QSE filter
    """
    if qse_filter is None:
        qse_filter = load_qse_shortnames(QSE_FILTER_CSV)
        if qse_filter:
            logger.info(f"Loaded {len(qse_filter)} QSEs from tracking list")
        else:
            logger.warning("No QSEs found in tracking list")
            return set()
    return qse_filter


def _download_dam_data_from_archive(start_date: str, end_date: str, db_name: str) -> None:
    """
    Downloads DAM data from the archive API.

    Args:
        start_date (str): Start date in YYYY-MM-DD format
        end_date (str): End date in YYYY-MM-DD format
        db_name (str): Database filename
    """
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


def _fetch_and_store_historical_dam_data(
    start_date: str, end_date: str, qse_filter: Set[str], db_name: str
) -> None:
    """
    Fetches and stores historical DAM (Day-Ahead Market) data for a given date range and QSE (Qualified Scheduling Entity) filter.

    This function uses the regular API to fetch and store various types of DAM data, including bid awards, bids, offer awards, and offers.

    Args:
        start_date (str): The start date for the data fetching period in 'YYYY-MM-DD' format.
        end_date (str): The end date for the data fetching period in 'YYYY-MM-DD' format.
        qse_filter (Set[str]): A set of QSE identifiers to filter the data.
        db_name (str): The name of the database where the data will be stored.

    Returns:
        None
    """
    logger.info("Using regular API for historical DAM data")
    _fetch_and_store_bid_awards(start_date, end_date, qse_filter, db_name)
    _fetch_and_store_bids(start_date, end_date, qse_filter, db_name)
    _fetch_and_store_offer_awards(start_date, end_date, qse_filter, db_name)
    _fetch_and_store_offers(start_date, end_date, qse_filter, db_name)


def _fetch_and_store_bid_awards(start_date: str, end_date: str, qse_filter: Set[str], db_name: str) -> None:
    """
    Fetches bid awards data from the ERCOT API for the specified date range and stores it in the database.

    Args:
        start_date (str): The start date for fetching bid awards in the format 'YYYY-MM-DD'.
        end_date (str): The end date for fetching bid awards in the format 'YYYY-MM-DD'.
        qse_filter (Set[str]): A set of Qualified Scheduling Entities (QSE) names to filter the bid awards.
        db_name (str): The name of the database where the bid awards data will be stored.

    Returns:
        None

    Logs:
        - Info: When the fetching process starts and ends, and the number of bid awards found.
        - Error: If no bid awards data is found.
    """
    logger.info(f"Fetching bid awards for {start_date} to {end_date}...")
    bid_awards = fetch_dam_energy_bid_awards(
        start_date, end_date,
        header=ERCOT_API_REQUEST_HEADERS,
        qse_names=qse_filter
    )
    if not bid_awards or "data" not in bid_awards:
        logger.error("No bid awards data found.")
    else:
        logger.info(f"Found {len(bid_awards.get('data', []))} bid awards")
        store_bid_awards_to_db(bid_awards, db_name, qse_filter)


def _fetch_and_store_bids(start_date: str, end_date: str, qse_filter: Set[str], db_name: str) -> None:
    """
    Fetches and stores DAM energy bids for a given date range and QSE filter.

    This function retrieves DAM energy bids from the ERCOT API for the specified
    date range and QSE filter, and stores the retrieved bids in the specified database.

    Args:
        start_date (str): The start date for fetching bids in 'YYYY-MM-DD' format.
        end_date (str): The end date for fetching bids in 'YYYY-MM-DD' format.
        qse_filter (Set[str]): A set of QSE names to filter the bids.
        db_name (str): The name of the database where the bids will be stored.

    Returns:
        None
    """
    logger.info(f"Fetching bids for {start_date} to {end_date}...")
    bids = fetch_dam_energy_bids(
        start_date, end_date,
        header=ERCOT_API_REQUEST_HEADERS,
        qse_names=qse_filter
    )
    if bids and "data" in bids:
        logger.info(f"Found {len(bids.get('data', []))} bids")
        store_bids_to_db(bids, db_name, qse_filter)
    else:
        logger.error("No bids data found.")


def _fetch_and_store_offer_awards(start_date, end_date, qse_filter, db_name):
    """
    Fetches and stores offer awards data from the ERCOT API for a given date range and QSE filter.

    Args:
        start_date (str): The start date for fetching offer awards in the format 'YYYY-MM-DD'.
        end_date (str): The end date for fetching offer awards in the format 'YYYY-MM-DD'.
        qse_filter (list): A list of QSE (Qualified Scheduling Entity) names to filter the offer awards.
        db_name (str): The name of the database where the offer awards data will be stored.

    Returns:
        None
    """
    logger.info(f"Fetching offer awards for {start_date} to {end_date}...")
    offer_awards = fetch_dam_energy_only_offer_awards(
        start_date, end_date,
        header=ERCOT_API_REQUEST_HEADERS,
        qse_names=qse_filter
    )
    if offer_awards and "data" in offer_awards:
        logger.info(f"Found {len(offer_awards.get('data', []))} offer awards")
        store_offer_awards_to_db(offer_awards, db_name, qse_filter)
    else:
        logger.error("No offer awards data found.")


def _fetch_and_store_offers(start_date: str, end_date: str, qse_filter: Set[str], db_name: str) -> None:
    """
    Fetches and stores energy offers from the ERCOT API within the specified date range.

    Args:
        start_date (str): The start date for fetching offers in the format 'YYYY-MM-DD'.
        end_date (str): The end date for fetching offers in the format 'YYYY-MM-DD'.
        qse_filter (Set[str]): A set of Qualified Scheduling Entities (QSE) names to filter the offers.
        db_name (str): The name of the database where the offers will be stored.

    Returns:
        None
    """
    logger.info(f"Fetching offers for {start_date} to {end_date}...")
    offers = fetch_dam_energy_only_offers(
        start_date, end_date,
        header=ERCOT_API_REQUEST_HEADERS,
        qse_names=qse_filter
    )
    if offers and "data" in offers:
        logger.info(f"Found {len(offers.get('data', []))} offers")
        store_offers_to_db(offers, db_name, qse_filter)
    else:
        logger.error("No offers data found.")


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

    try:
        # Archive API check
        if should_use_archive_api(start_date, end_date):
            doc_ids = get_archive_document_ids(
                ERCOT_ARCHIVE_PRODUCT_IDS["SPP"],
                start_date,
                end_date
            )
            download_spp_archive_files(
                ERCOT_ARCHIVE_PRODUCT_IDS["SPP"], doc_ids, db_name=db_name)  # Changed to use keyword arg
            return

        # Use regular API with headers
        prices = fetch_settlement_point_prices(
            start_date,
            end_date,
            header=ERCOT_API_REQUEST_HEADERS
        )
        # Changed to use keyword arg
        store_prices_to_db(prices, db_name=db_name)
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
    """
    Parse command-line arguments for the ERCOT data downloading tool.
    Returns:
        argparse.Namespace: Parsed command-line arguments.
    The following commands are supported:
        - historical-dam: Download historical DAM data for a date range.
        - historical-spp: Download historical SPP data.
        - update-dam: Update daily DAM data.
        - update-spp: Update daily SPP data.
        - merge-data: Merge downloaded data.
    Examples:
    Additional arguments:
        --debug: Enable debug logging.
    """
    parser = argparse.ArgumentParser(
        description="ERCOT data downloading tool",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""Examples:
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

    _add_historical_dam_parser(subparsers)
    _add_historical_spp_parser(subparsers)
    _add_update_dam_parser(subparsers)
    _add_update_spp_parser(subparsers)
    _add_merge_data_parser(subparsers)

    parser.add_argument("--debug", action="store_true",
                        help="Enable debug logging")

    return parser.parse_args()


def _add_historical_dam_parser(subparsers: argparse._SubParsersAction) -> None:
    """
    Adds a subparser for the 'historical-dam' command to the given subparsers.

    This command is used to download historical Day-Ahead Market (DAM) data.

    Args:
        subparsers (argparse._SubParsersAction): The subparsers action object to which the 
                                                 'historical-dam' subparser will be added.

    The 'historical-dam' subparser includes the following argument:
        --qse-filter (Path): Optional; Path to a CSV file containing the QSE filter.
    """
    historical_dam = _setup_command_parser(
        subparsers, "historical-dam", "Download historical DAM data"
    )
    historical_dam.add_argument(
        "--qse-filter", type=Path, help="Path to QSE filter CSV file"
    )


def _add_historical_spp_parser(subparsers: argparse._SubParsersAction) -> None:
    """
    Add parser for the historical SPP data command.

    This function sets up a command parser for downloading historical SPP (Settlement Point Prices) data.

    Args:
        subparsers (argparse._SubParsersAction): The subparsers action object to which the new parser will be added.
    """
    _setup_command_parser(subparsers, "historical-spp",
                          "Download historical SPP data")


def _add_update_dam_parser(subparsers: argparse._SubParsersAction) -> None:
    """
    Add parser for the 'update-dam' command to update daily DAM data.

    Args:
        subparsers (argparse._SubParsersAction): The subparsers action object to which the new parser will be added.

    The 'update-dam' command supports the following arguments:
        --db: The filename of the database to update. Defaults to the value of ERCOT_DB_NAME.
        --qse-filter: The path to a CSV file containing QSE filter data.
    """
    update_dam = subparsers.add_parser(
        "update-dam", help="Update daily DAM data")
    update_dam.add_argument(
        "--db", default=ERCOT_DB_NAME, help="Database filename")
    update_dam.add_argument(
        "--qse-filter", type=Path, help="Path to QSE filter CSV file"
    )


def _add_update_spp_parser(subparsers: argparse._SubParsersAction) -> None:
    """
    Add a subparser for the 'update-spp' command to the given subparsers.

    The 'update-spp' command is used to update daily SPP (Southwest Power Pool) data.
    This function adds the necessary arguments and help descriptions for the command.

    Args:
        subparsers (argparse._SubParsersAction): The subparsers action object to which 
                                                 the 'update-spp' subparser will be added.

    Returns:
        None
    """
    """Add parser for updating daily SPP data command."""
    update_spp = subparsers.add_parser(
        "update-spp", help="Update daily SPP data")
    update_spp.add_argument(
        "--db", default=ERCOT_DB_NAME, help="Database filename")


def _add_merge_data_parser(subparsers: argparse._SubParsersAction) -> None:
    """
    Adds a subparser for the 'merge-data' command to the provided subparsers object.

    Args:
        subparsers (argparse._SubParsersAction): The subparsers object to which the 'merge-data' command parser will be added.

    The 'merge-data' command parser includes the following argument:
        --db: The filename of the database. Defaults to the value of ERCOT_DB_NAME.
    """
    merge_cmd = subparsers.add_parser(
        "merge-data", help="Merge data into FINAL table")
    merge_cmd.add_argument("--db", default=ERCOT_DB_NAME,
                           help="Database filename")


def _setup_command_parser(subparsers: argparse._SubParsersAction, arg1: str, help: str) -> argparse.ArgumentParser:
    """
    Sets up a command parser for historical DAM data.

    Args:
        subparsers (argparse._SubParsersAction): The subparsers object to add the parser to.
        arg1 (str): The name of the command.
        help (str): The help message for the command.

    Returns:
        argparse.ArgumentParser: The argument parser for the command.
    """
    # Historical DAM data command
    result = subparsers.add_parser(arg1, help=help)
    result.add_argument("--start", required=True,
                        help="Start date (YYYY-MM-DD)")
    result.add_argument("--end", help="End date (YYYY-MM-DD)")
    result.add_argument("--db", default=ERCOT_DB_NAME,
                        help="Database filename")
    return result


def main():
    """
    This function performs the following steps:
    1. Parses command-line arguments using the `parse_args` function.
    2. Sets the logging level to DEBUG if the `--debug` flag is specified.
    3. Checks if a command is specified in the arguments. If not, logs an error and exits.
    4. Attempts to execute the specified command using the `execute_command` function.
    5. Handles HTTP errors specifically by calling `handle_http_error`.
    6. Logs any other exceptions that occur during command execution and re-raises them.
    Raises:
        requests.exceptions.HTTPError: If an HTTP error occurs during command execution.
        Exception: If any other error occurs during command execution.
    """
    args = parse_args()

    # Set logging level to DEBUG if --debug flag is specified
    if args.debug:
        logger.setLevel(logging.DEBUG)

    if not args.command:
        logger.error("No command specified. Use -h for help.")
        return

    try:
        execute_command(args)
    except requests.exceptions.HTTPError as e:
        handle_http_error(e)
    except Exception as e:
        logger.error(f"Error executing command: {str(e)}")
        raise


def execute_command(args: argparse.Namespace) -> None:
    """
    Execute the specified command based on parsed arguments.

    Args:
        args (argparse.Namespace): Parsed command-line arguments.
    """
    qse_filter = load_qse_filter_if_specified(args)

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
    elif args.command == "merge-data":
        merge_data(db_name=args.db)


def load_qse_filter_if_specified(args: argparse.Namespace) -> Optional[Set[str]]:
    """
    Load QSE filter if specified in the arguments.

    Args:
        args (argparse.Namespace): Parsed command-line arguments.

    Returns:
        Optional[Set[str]]: Loaded QSE filter or None.
    """
    if hasattr(args, "qse_filter") and args.qse_filter:
        return load_qse_shortnames(args.qse_filter)
    return None


def handle_http_error(e: requests.exceptions.HTTPError) -> None:
    """
    Handle HTTP errors during command execution.

    Args:
        e (requests.exceptions.HTTPError): The HTTP error to handle.
    """
    if e.response.status_code == 404:
        logger.error(f"API endpoint not found: {e.response.url}")
    else:
        logger.error(f"HTTP error occurred: {str(e)}")
    raise


if __name__ == "__main__":
    main()
