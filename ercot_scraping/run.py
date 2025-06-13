"""
ERCOT Data Pipeline CLI and Batch Processing Module.

This module provides command-line tools and batch processing logic for
 downloading, merging, and checkpointing ERCOT DAM and SPP data, supporting
 both historical and daily updates, as well as robust batch/merge pipelines
 with checkpointing and error handling.
"""

# noqa: E501
import argparse
from typing import Optional, Set
from datetime import datetime, timedelta
import logging
from pathlib import Path
import os
import json

import requests

from ercot_scraping.config.config import (
    DAM_ARCHIVE_CUTOFF_DATE,
    SPP_ARCHIVE_CUTOFF_DATE,
    ERCOT_API_REQUEST_HEADERS,
    ERCOT_DB_NAME,
    ERCOT_ARCHIVE_PRODUCT_IDS,
    QSE_FILTER_CSV,
    FILE_LIMITS,  # <-- use this for batch sizes
)
from ercot_scraping.apis.archive_api import (
    get_archive_document_ids,
    download_dam_archive_files,
    download_spp_archive_files,
)
from ercot_scraping.apis.ercot_api import (
    fetch_settlement_point_prices,
    fetch_dam_energy_bid_awards,
    fetch_dam_energy_bids,
    fetch_dam_energy_only_offers,
    fetch_dam_energy_only_offer_awards,
)
from ercot_scraping.database.merge_data import merge_data

from ercot_scraping.utils.filters import load_qse_shortnames
from ercot_scraping.utils.logging_utils import setup_module_logging

# Configure logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
per_run_handler = setup_module_logging(__name__)

# Ensure logger outputs to console if no handlers exist
if not logger.hasHandlers():
    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter(
        '%(asctime)s - %(levelname)s - %(message)s'))
    logger.addHandler(handler)

CHECKPOINT_FILE = "_data/ercot_download_checkpoint.json"


def validate_checkpoint(data):
    """
    Validate checkpoint structure and content.
    Returns True if valid, False otherwise.
    """
    required_keys = {
        "stage",
        "details"
    }
    if not isinstance(data, dict):
        return False
    if not required_keys.issubset(data.keys()):
        return False
    if not isinstance(data["details"], dict):
        return False
    return True


def save_checkpoint_atomic(data, path=CHECKPOINT_FILE):
    """
    Atomically save checkpoint data to disk.
    """
    tmp_path = path + ".tmp"
    bak_path = path + ".bak"
    try:
        if os.path.exists(path):
            os.replace(path, bak_path)
        with open(tmp_path, "w", encoding="utf-8") as f:
            json.dump(data, f)
        os.replace(tmp_path, path)
        logger.info("Checkpoint saved: %s", data)
    except OSError as e:
        logger.error("Failed to save checkpoint: %s", e)


def load_checkpoint_safe(path=CHECKPOINT_FILE):
    """
    Safely load checkpoint data from disk, returning an empty dict if not
    found or invalid.
    """
    try:
        if not os.path.exists(path):
            return {}
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        if validate_checkpoint(data):
            logger.info("Loaded checkpoint: %s", data)
            return data
        logger.warning("Checkpoint validation failed. Ignoring checkpoint.")
        return {}
    except (OSError, json.JSONDecodeError) as e:
        logger.warning("Failed to load checkpoint: %s", e)
        return {}


def clear_checkpoint(path=CHECKPOINT_FILE):
    """
    Remove the checkpoint file if it exists.
    """
    try:
        os.remove(path)
        logger.info("Checkpoint cleared.")
    except FileNotFoundError:
        pass
    except OSError as e:
        logger.error("Failed to clear checkpoint: %s", e)


def split_date_range_by_cutoff(
        start_date: str,
        end_date: str,
        cutoff_date: str):
    """
    Splits the date range into (archive_range, regular_range) based on
    cutoff_date. Returns tuple of (archive_range, regular_range), where each
    is (start, end) or None.
    """
    fmt = "%Y-%m-%d"
    start_dt = datetime.strptime(start_date, fmt)
    end_dt = datetime.strptime(end_date, fmt)
    cutoff_dt = datetime.strptime(cutoff_date, fmt)
    if end_dt < cutoff_dt:
        return ((start_date, end_date), None)
    elif start_dt >= cutoff_dt:
        return (None, (start_date, end_date))
    else:
        archive_end = (cutoff_dt - timedelta(days=1)).strftime(fmt)
        return ((start_date, archive_end), (cutoff_date, end_date))


def download_historical_dam_data(
        start_date: str,
        end_date: Optional[str] = None,
        db_name: str = ERCOT_DB_NAME,
        qse_filter: Optional[Set[str]] = None) -> None:
    """
    Downloads historical DAM (Day-Ahead Market) data within the specified
    date range.

    This function fetches historical DAM data across two different API
    endpoints based on a cutoff date. The date range is split into two
    segments:
        - Archive API: For dates before the cutoff.
        - Regular API: For dates on or after the cutoff.
    If the end date is not provided, the current date is used. The function
    also applies a filter for specific QSEs (Qualified Scheduling Entities)
    if provided.

    Parameters:
        start_date (str): The starting date for downloading data, in
            "YYYY-MM-DD" format.
        end_date (Optional[str]): The ending date for downloading data, in
            "YYYY-MM-DD" format; if None, defaults to the current date.
        db_name (str): The name of the database where the downloaded data
            will be stored.
        qse_filter (Optional[Set[str]]): A set of QSE identifiers; if
            provided, used to filter the downloaded data.

    Raises:
        Exception: If an error occurs during the data fetching, processing,
            or storing process.
    """

    if end_date is None:
        end_date = datetime.now().strftime("%Y-%m-%d")
    qse_filter = _load_qse_filter(qse_filter)
    logger.info(
        "Downloading historical DAM data from %s to %s",
        start_date,
        end_date)
    logger.info("Filtering for QSEs: %s", sorted(qse_filter))
    try:
        archive_range, regular_range = split_date_range_by_cutoff(
            start_date, end_date, DAM_ARCHIVE_CUTOFF_DATE)
        if archive_range:
            logger.info(
                "Fetching DAM data from archive API for %s to %s",
                archive_range[0],
                archive_range[1]
            )
            _download_dam_data_from_archive(
                archive_range[0], archive_range[1], db_name)
        if regular_range:
            logger.info(
                "Fetching DAM data from regular API for %s to %s",
                regular_range[0],
                regular_range[1]
            )
            _fetch_and_store_historical_dam_data(
                regular_range[0], regular_range[1], qse_filter, db_name)
        logger.info("Historical DAM data download completed successfully")
    except Exception as e:
        logger.error("Error downloading historical DAM data: %s", str(e))
        raise


def _load_qse_filter(qse_filter: Optional[object]) -> Set[str]:
    """
    Loads the QSE filter from the provided set, comma-separated string, or
    from the tracking list file.

    Args:
        qse_filter (Optional[object]): Set of QSE names, comma-separated
            string, or Path to CSV file

    Returns:
        Set[str]: Loaded QSE filter
    """
    if qse_filter is None:
        qse_filter = load_qse_shortnames(QSE_FILTER_CSV)
        if qse_filter:
            logger.info("Loaded %d QSEs from tracking list", len(qse_filter))
        else:
            logger.warning("No QSEs found in tracking list")
            return set()
    elif isinstance(qse_filter, set):
        return qse_filter
    elif isinstance(qse_filter, str):
        return set(q.strip() for q in qse_filter.split(',') if q.strip())
        # Try to interpret as a file path
        path = Path(qse_filter)
        if path.exists():
            return load_qse_shortnames(path)
        else:
            logger.warning(
                "QSE filter string provided but not a file or comma-list: %s",
                qse_filter
            )
            return set()
    elif hasattr(qse_filter, 'exists') and qse_filter.exists():
        # Path object
        return load_qse_shortnames(qse_filter)
    else:
        logger.warning("Unrecognized qse_filter type: %s", type(qse_filter))
        return set()
    return qse_filter


def _download_dam_data_from_archive(
        start_date: str,
        end_date: str,
        db_name: str) -> None:
    logger.info("Using archive API for historical DAM data")
    logger.info(
        "Calling get_archive_document_ids with: product_id=DAM_BIDS, "
        "start_date=%s, end_date=%s",
        start_date,
        end_date
    )
    doc_ids = get_archive_document_ids(
        ERCOT_ARCHIVE_PRODUCT_IDS["DAM_BIDS"],
        start_date,
        end_date
    )
    logger.info("Found %d documents in archive", len(doc_ids))
    logger.info(
        "Calling download_dam_archive_files with %d doc_ids", len(doc_ids)
    )
    for i in range(0, len(doc_ids), FILE_LIMITS["DAM"]):
        batch = doc_ids[i:i + FILE_LIMITS["DAM"]]
        logger.info(
            "DAM archive docIds %s-%s (batch %d/%d)",
            batch[0], batch[-1],
            i // FILE_LIMITS["DAM"] + 1,
            (len(doc_ids) + FILE_LIMITS["DAM"] - 1) // FILE_LIMITS["DAM"]
        )
        download_dam_archive_files(
            ERCOT_ARCHIVE_PRODUCT_IDS["DAM_BIDS"],
            batch,
            db_name,
            batch_size=FILE_LIMITS["DAM"]
        )
    logger.info(
        "Completed download_dam_archive_files for product_id=%s",
        ERCOT_ARCHIVE_PRODUCT_IDS["DAM_BIDS"]
    )


def _fetch_and_store_historical_dam_data(
    start_date: str, end_date: str, qse_filter: Set[str], db_name: str
) -> None:
    """
    Fetches and stores historical DAM (Day-Ahead Market) data for a given
    date range and QSE (Qualified Scheduling Entity) filter.

    This function uses the regular API to fetch and store various types of
    DAM data, including bid awards, bids, offer awards, and offers.

    Args:
        start_date (str): The start date for the data fetching period in
            'YYYY-MM-DD' format.
        end_date (str): The end date for the data fetching period in
            'YYYY-MM-DD' format.
        qse_filter (Set[str]): A set of QSE identifiers to filter the data.
        db_name (str): The name of the database where the data will be stored.

    Returns:
        None
    """
    logger.info("Using regular API for historical DAM data")
    _fetch_and_store_bids(start_date, end_date, qse_filter, db_name)
    _fetch_and_store_bid_awards(start_date, end_date, qse_filter, db_name)
    _fetch_and_store_offers(start_date, end_date, qse_filter, db_name)
    _fetch_and_store_offer_awards(start_date, end_date, qse_filter, db_name)
    logger.info(
        "Fetching Settlement Point Prices for %s to %s...",
        start_date, end_date
    )
    fetch_settlement_point_prices(
        start_date, end_date,
        header=ERCOT_API_REQUEST_HEADERS,
        db_name=db_name
    )
    logger.info("Merging data after all fetches...")
    merge_data(db_name)


def _fetch_and_store_bid_awards(
        start_date: str,
        end_date: str,
        qse_filter: Set[str],
        db_name: str) -> None:
    logger.info("Fetching bid awards for %s to %s...", start_date, end_date)
    fetch_dam_energy_bid_awards(
        start_date, end_date,
        header=ERCOT_API_REQUEST_HEADERS,
        qse_names=qse_filter,
        db_name=db_name
    )
    # No assignment or membership test; function does not return data


def _fetch_and_store_bids(
        start_date: str,
        end_date: str,
        qse_filter: Set[str],
        db_name: str) -> None:
    logger.info("Fetching bids for %s to %s...", start_date, end_date)
    fetch_dam_energy_bids(
        start_date, end_date,
        header=ERCOT_API_REQUEST_HEADERS,
        qse_names=qse_filter,
        db_name=db_name
    )
    # No assignment or membership test; function does not return data


def _fetch_and_store_offer_awards(start_date, end_date, qse_filter, db_name):
    logger.info("Fetching offer awards for %s to %s...", start_date, end_date)
    fetch_dam_energy_only_offer_awards(
        start_date, end_date,
        header=ERCOT_API_REQUEST_HEADERS,
        qse_names=qse_filter,
        db_name=db_name
    )
    # No assignment or membership test; function does not return data


def _fetch_and_store_offers(
        start_date: str,
        end_date: str,
        qse_filter: Set[str],
        db_name: str) -> None:
    logger.info("Fetching offers for %s to %s...", start_date, end_date)
    fetch_dam_energy_only_offers(
        start_date, end_date,
        header=ERCOT_API_REQUEST_HEADERS,
        qse_names=qse_filter,
        db_name=db_name
    )
    # No assignment or membership test; function does not return data


def download_historical_spp_data(
    start_date: str,
    end_date: Optional[str] = None,
    db_name: str = ERCOT_DB_NAME,
) -> None:
    """
    Download historical SPP (Settlement Point Price) data for the given date
    range.
    """
    if end_date is None:
        end_date = datetime.now().strftime("%Y-%m-%d")
    logger.info(
        "Downloading historical SPP data from %s to %s", start_date, end_date)
    try:
        archive_range, regular_range = split_date_range_by_cutoff(
            start_date, end_date, SPP_ARCHIVE_CUTOFF_DATE
        )
        if archive_range:
            logger.info(
                "Fetching SPP data from archive API for %s to %s",
                archive_range[0],
                archive_range[1]
            )
            doc_ids = get_archive_document_ids(
                ERCOT_ARCHIVE_PRODUCT_IDS["SPP"],
                archive_range[0],
                archive_range[1]
            )
            logger.info("Found %d documents in archive", len(doc_ids))
            download_spp_archive_files(
                ERCOT_ARCHIVE_PRODUCT_IDS["SPP"],
                doc_ids,
                db_name
            )
        if regular_range:
            logger.info(
                "Fetching SPP data from regular API for %s to %s",
                regular_range[0],
                regular_range[1]
            )
            fetch_settlement_point_prices(
                regular_range[0],
                regular_range[1],
                header=ERCOT_API_REQUEST_HEADERS,
                db_name=db_name
            )
        logger.info("Historical SPP data download completed successfully")
    except Exception as e:
        logger.error("Error downloading historical SPP data: %s", str(e))
        raise


def update_daily_dam_data(
    db_name: str = ERCOT_DB_NAME, qse_filter: Optional[Set[str]] = None
) -> None:
    """
    Update daily DAM (Day-Ahead Market) data for the most recent available
    date.
    """
    target_date = (datetime.now() - timedelta(days=60)).strftime("%Y-%m-%d")
    logger.info("Updating DAM data for %s (60 days before today)", target_date)
    try:
        download_historical_dam_data(
            target_date, target_date, db_name, qse_filter)
        logger.info("Daily DAM data update completed successfully")
    except Exception as e:
        logger.error("Error updating daily DAM data: %s", str(e))
        raise


def update_daily_spp_data(db_name: str = ERCOT_DB_NAME) -> None:
    """
    Update daily SPP (Settlement Point Price) data for the most recent
    available date.
    """
    yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
    logger.info("Updating SPP data for %s", yesterday)
    try:
        download_historical_spp_data(yesterday, yesterday, db_name)
        logger.info("Daily SPP data update completed successfully")
    except Exception as e:
        logger.error("Error updating daily SPP data: %s", str(e))
        raise


def parse_args() -> argparse.Namespace:
    """
    Parse command-line arguments for the ERCOT data downloading tool.
    Returns:
        argparse.Namespace: Parsed command-line arguments.
    """
    parser = argparse.ArgumentParser(
        description="ERCOT data downloading tool",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""Examples:
    # Download historical DAM data for a date range
    python -m ercot_scraping.run historical-dam --start 2023-01-01 \
        --end 2023-12-31

    # Download historical SPP data
    python -m ercot_scraping.run historical-spp --start 2023-01-01

    # Update daily DAM data
    python -m ercot_scraping.run update-dam

    # Update daily SPP data
    python -m ercot_scraping.run update-spp

    # Download all data in batches
    python -m ercot_scraping.run download --start 2024-01-01 \
        --end 2024-01-31 --db _data/ercot_data.db
    """,
    )

    subparsers = parser.add_subparsers(
        dest="command", help="Command to execute")

    _add_historical_dam_parser(subparsers)
    _add_historical_spp_parser(subparsers)
    _add_update_dam_parser(subparsers)
    _add_update_spp_parser(subparsers)
    _add_merge_data_parser(subparsers)
    _add_download_and_merge_parser(subparsers)
    _add_download_parser(subparsers)
    # Add --quick-test flag to all commands
    parser.add_argument(
        "--quick-test",
        action="store_true",
        help="Run a quick test with a small QSE set and short date range")
    # Add --debug flag to enable debug logging
    parser.add_argument(
        "--debug",
        action="store_true",
        help="Enable debug logging.")
    return parser.parse_args()


def _add_historical_dam_parser(subparsers: argparse._SubParsersAction) -> None:
    """
    Adds a subparser for the 'historical-dam' command to the given subparsers.

    This command is used to download historical Day-Ahead Market (DAM) data.

    Args:
        subparsers (argparse._SubParsersAction): The subparsers action object
            to which the 'historical-dam' subparser will be added.

    The 'historical-dam' subparser includes the following argument:
        --qse-filter (Path): Optional; Path to a CSV file containing the QSE
            filter.
    """
    historical_dam = _setup_command_parser(
        subparsers, "historical-dam", "Download historical DAM data"
    )
    historical_dam.add_argument(
        "--qse-filter",
        type=str,
        help="Path to QSE filter CSV file or comma-separated QSE names")


def _add_historical_spp_parser(subparsers: argparse._SubParsersAction) -> None:
    """
    Add parser for the historical SPP data command.

    This function sets up a command parser for downloading historical SPP
    (Settlement Point Prices) data.

    Args:
        subparsers (argparse._SubParsersAction): The subparsers action object
            to which the new parser will be added.
    """
    _setup_command_parser(subparsers, "historical-spp",
                          "Download historical SPP data")


def _add_update_dam_parser(subparsers: argparse._SubParsersAction) -> None:
    """
    Add parser for the 'update-dam' command to update daily DAM data.

    Args:
        subparsers (argparse._SubParsersAction): The subparsers action object
            to which the new parser will be added.

    The 'update-dam' command supports the following arguments:
        --db: The filename of the database to update. Defaults to the value of
            ERCOT_DB_NAME.
        --qse-filter: The path to a CSV file containing QSE filter data.
    """
    update_dam = subparsers.add_parser(
        "update-dam", help="Update daily DAM data")
    update_dam.add_argument(
        "--db", default=ERCOT_DB_NAME, help="Database filename")
    update_dam.add_argument(
        "--qse-filter",
        type=str,
        help="Path to QSE filter CSV file or comma-separated QSE names")


def _add_update_spp_parser(subparsers: argparse._SubParsersAction) -> None:
    """
    Add a subparser for the 'update-spp' command to the given subparsers.

    The 'update-spp' command is used to update daily SPP (Southwest Power
    Pool) data. This function adds the necessary arguments and help
    descriptions for the command.

    Args:
        subparsers (argparse._SubParsersAction): The subparsers action object
            to which the 'update-spp' subparser will be added.

    Returns:
        None
    """
    update_spp = subparsers.add_parser(
        "update-spp", help="Update daily SPP data")
    update_spp.add_argument(
        "--db", default=ERCOT_DB_NAME, help="Database filename")


def _add_merge_data_parser(subparsers: argparse._SubParsersAction) -> None:
    """
    Adds a subparser for the 'merge-data' command to the provided subparsers
    object.

    Args:
        subparsers (argparse._SubParsersAction): The subparsers object to
            which the 'merge-data' command parser will be added.

    The 'merge-data' command parser includes the following argument:
        --db: The filename of the database. Defaults to the value of
            ERCOT_DB_NAME.
        --start: Optional; Start date for merge (YYYY-MM-DD)
        --end: Optional; End date for merge (YYYY-MM-DD)
    """
    merge_cmd = subparsers.add_parser(
        "merge-data", help="Merge data into FINAL table")
    merge_cmd.add_argument("--db", default=ERCOT_DB_NAME,
                           help="Database filename")
    merge_cmd.add_argument("--start", help="Start date for merge (YYYY-MM-DD)")
    merge_cmd.add_argument("--end", help="End date for merge (YYYY-MM-DD)")


def _add_download_and_merge_parser(
    subparsers: argparse._SubParsersAction
) -> None:
    """
    Adds a subparser for the 'download-and-merge' command.
    Args:
        subparsers (argparse._SubParsersAction):
            The subparsers object to add the parser to.
    """
    cmd = subparsers.add_parser(
        "download-and-merge",
        help="Download all data and merge into FINAL table"
    )
    cmd.add_argument("--start", required=True, help="Start date (YYYY-MM-DD)")
    cmd.add_argument("--end", help="End date (YYYY-MM-DD)")
    cmd.add_argument("--db", default=ERCOT_DB_NAME, help="Database filename")
    cmd.add_argument(
        "--qse-filter",
        type=str,
        help="Path to QSE filter CSV file or comma-separated QSE names"
    )
    cmd.add_argument(
        "--merge-every",
        type=int,
        default=100,
        help="Merge data every N requests (default: 100)"
    )


def _add_download_parser(subparsers: argparse._SubParsersAction) -> None:
    """
    Adds a subparser for the 'download' command.
    """
    download = subparsers.add_parser(
        "download",
        help="Download DAM and SPP data in batches with checkpointing. "
             "The --start/--end dates refer to DAM; SPP is always lagged by -60 days (SPP = DAM - 60d)."
    )
    download.add_argument(
        "--start", required=True,
        help="Start date (YYYY-MM-DD, DAM date)")
    download.add_argument(
        "--end", required=True,
        help="End date (YYYY-MM-DD, DAM date)")
    download.add_argument(
        "--batch-days", type=int, default=1,
        help="Batch size in days (default: 1)")
    download.add_argument(
        "--db", default=ERCOT_DB_NAME,
        help="Database filename")
    download.add_argument(
        "--resume", action="store_true",
        default=True,
        help="Resume from checkpoint (default: True)")
    download.add_argument(
        "--no-merge", action="store_true",
        help="Skip merging after each batch")


def _setup_command_parser(
        subparsers: argparse._SubParsersAction,
        arg1: str,
        help_text: str) -> argparse.ArgumentParser:
    """
    Sets up a command parser for historical DAM data.

    Args:
        subparsers (argparse._SubParsersAction):
            The subparsers object to add the parser to.
        arg1 (str): The name of the command.
        help_text (str): The help message for the command.

    Returns:
        argparse.ArgumentParser: The argument parser for the command.
    """
    # Historical DAM data command
    result = subparsers.add_parser(arg1, help=help_text)
    result.add_argument("--start", required=True,
                        help="Start date (YYYY-MM-DD)")
    result.add_argument("--end", help="End date (YYYY-MM-DD)")
    result.add_argument("--db", default=ERCOT_DB_NAME,
                        help="Database filename")
    return result


def main():
    """
    Main entry point for the ERCOT data pipeline CLI.
    Parses arguments, sets up logging, and dispatches commands.
    """
    args = parse_args()
    logger.info("[DEBUG] Parsed CLI args: %s", args)

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
        logger.error("Error executing command: %s", str(e))
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
            args.start, args.end, args.db, qse_filter)
    elif args.command == "historical-spp":
        download_historical_spp_data(
            args.start, args.end, args.db)
    elif args.command == "update-dam":
        update_daily_dam_data(
            db_name=args.db, qse_filter=qse_filter)
    elif args.command == "update-spp":
        update_daily_spp_data(
            db_name=args.db)
    elif args.command == "merge-data":
        merge_data(
            args.db, args.start, args.end)
    elif args.command == "download-and-merge":
        download_and_merge_all_data(
            args.start, args.end, args.db, qse_filter, args.merge_every)
    elif args.command == "download":
        download_batched_data(
            start_date=args.start,
            end_date=args.end,
            batch_days=args.batch_days,
            db_name=args.db,
        )
    elif args.command == "quick-test":
        # Use a very short date range and a small QSE set for fast test
        test_start = (datetime.now() - timedelta(days=2)).strftime("%Y-%m-%d")
        test_end = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
        test_qses = {"QSE1", "QSE2"}  # Replace with real QSEs if needed
        logger.info(
            "Running quick-test from %s to %s for QSEs: %s",
            test_start,
            test_end,
            test_qses)
        download_historical_dam_data(
            start_date=test_start,
            end_date=test_end,
            db_name=args.db,
            qse_filter=test_qses,
        )
        logger.info("Quick test completed.")


def load_qse_filter_if_specified(
        args: argparse.Namespace) -> Optional[Set[str]]:
    """
    Load QSE filter from args if specified.
    """
    if hasattr(args, "qse_filter") and args.qse_filter:
        return args.qse_filter
    return None


def handle_http_error(e: requests.exceptions.HTTPError) -> None:
    """
    Handle HTTP errors from requests, logging details.
    """
    if e.response.status_code == 404:
        logger.error("API endpoint not found: %s", e.response.url)
    else:
        logger.error("HTTP error occurred: %s", str(e))


def download_and_merge_all_data(
    start_date: str,
    end_date: Optional[str] = None,
    db_name: str = ERCOT_DB_NAME,
    qse_filter: Optional[Set[str]] = None,
    merge_every: int = 100,
) -> None:
    """
    Download and merge all DAM and SPP data for the specified date range,
    with checkpointing.
    NOTE: The user-supplied start/end dates refer to DAM. SPP is always lagged by -60 days (SPP = DAM - 60d).
    """
    if end_date is None:
        end_date = datetime.now().strftime("%Y-%m-%d")
    qse_filter = _load_qse_filter(qse_filter)
    checkpoint = load_checkpoint_safe()
    try:
        # DAM data (user-supplied dates)
        archive_range, regular_range = split_date_range_by_cutoff(
            start_date, end_date, DAM_ARCHIVE_CUTOFF_DATE
        )
        # Archive DAM
        if archive_range:
            stage = "dam_archive"
            last_idx = checkpoint.get("details", {}). \
                get("dam_archive_idx", 0) \
                if checkpoint.get("stage") == stage else 0
            logger.info(
                "Fetching DAM data from archive API for %s to %s (resume idx %d)",
                archive_range[0], archive_range[1], last_idx
            )
            if last_idx == 0:
                _download_dam_data_from_archive(
                    archive_range[0], archive_range[1], db_name
                )
                merge_data(db_name)
                save_checkpoint_atomic(
                    {"stage": stage, "details": {"dam_archive_idx": 1}})
        # Regular DAM
        if regular_range:
            stage = "dam_regular"
            last_func = checkpoint.get("details", {}).get("dam_regular_func", 0) \
                if checkpoint.get("stage") == stage else 0
            logger.info(
                "Fetching DAM data from regular API for %s to %s (resume func %d)",
                regular_range[0], regular_range[1], last_func
            )
            dam_funcs = [
                fetch_dam_energy_bid_awards,
                fetch_dam_energy_bids,
                fetch_dam_energy_only_offer_awards,
                fetch_dam_energy_only_offers,
            ]
            for i, func in enumerate(dam_funcs):
                if i < last_func:
                    continue
                func(
                    regular_range[0], regular_range[1],
                    header=ERCOT_API_REQUEST_HEADERS,
                    qse_names=qse_filter,
                    db_name=db_name,
                    batch_size=100,
                    log_every=merge_every,
                )
                merge_data(db_name)
                save_checkpoint_atomic(
                    {"stage": stage, "details": {"dam_regular_func": i + 1}})
        # SPP data (lagged by -60 days)
        fmt = "%Y-%m-%d"
        spp_start = (
            datetime.strptime(start_date, fmt) - timedelta(days=60)
        ).strftime(fmt)
        spp_end = (
            datetime.strptime(end_date, fmt) - timedelta(days=60)
        ).strftime(fmt)
        archive_range, regular_range = split_date_range_by_cutoff(
            spp_start, spp_end, SPP_ARCHIVE_CUTOFF_DATE
        )
        # Archive SPP
        if archive_range:
            stage = "spp_archive"
            doc_ids = get_archive_document_ids(
                ERCOT_ARCHIVE_PRODUCT_IDS["SPP"],
                archive_range[0], archive_range[1]
            )
            last_idx = checkpoint.get("details", {}).get("spp_archive_idx", 0) \
                if checkpoint.get("stage") == stage else 0
            logger.info(
                "Fetching SPP data from archive API for %s to %s (resume idx %d)",
                archive_range[0], archive_range[1], last_idx
            )
            for i in range(last_idx, len(doc_ids), merge_every):
                batch_ids = doc_ids[i:i+merge_every]
                download_spp_archive_files(
                    ERCOT_ARCHIVE_PRODUCT_IDS["SPP"],
                    batch_ids, db_name
                )
                merge_data(db_name)
                save_checkpoint_atomic(
                    {"stage": stage, "details": {"spp_archive_idx":
                                                 i + merge_every}}
                )
        # Regular SPP
        if regular_range:
            stage = "spp_regular"
            last_done = checkpoint.get("details", {}).get("spp_regular_done", False) \
                if checkpoint.get("stage") == stage else False
            if not last_done:
                logger.info(
                    "Fetching SPP data from regular API for %s to %s",
                    regular_range[0], regular_range[1]
                )
                fetch_settlement_point_prices(
                    regular_range[0], regular_range[1],
                    header=ERCOT_API_REQUEST_HEADERS,
                    db_name=db_name,
                    batch_size=100,
                    log_every=merge_every,
                )
                merge_data(db_name)
                save_checkpoint_atomic(
                    {"stage": stage, "details": {"spp_regular_done": True}})
        logger.info("All data downloaded and stored. Final merge...")
        merge_data(db_name)
        clear_checkpoint()
        logger.info("Data merge completed successfully.")
    except Exception as e:
        logger.error("Error in download_and_merge_all_data: %s", e)
        logger.error(
            "Job interrupted. Resume will pick up from last checkpoint.")
        raise


def download_batched_data(
    start_date: str,
    end_date: str,
    batch_days: int,
    db_name: str,
):
    """
    Downloads DAM and SPP data in batches, using archive or current API as
    appropriate. The CLI start/end dates refer to DAM; SPP is shifted -60 days (SPP = DAM - 60d).
    """
    fmt = "%Y-%m-%d"
    dam_start = datetime.strptime(start_date, fmt)
    dam_end = datetime.strptime(end_date, fmt)
    # Prepare DAM batches (user input), newest to oldest
    batches = []
    current = dam_end
    while current >= dam_start:
        batch_start = max(current - timedelta(days=batch_days - 1), dam_start)
        batches.append((batch_start.strftime(fmt), current.strftime(fmt)))
        current = batch_start - timedelta(days=1)
    # Now batches is newest to oldest

    checkpoint = load_checkpoint_safe()
    last_batch_idx = checkpoint.get("details", {}).get(
        "batch_idx", 0) if checkpoint.get("stage") == "dam_spp_download" else 0

    logger.info("Total batches: %d", len(batches))
    for idx, (dam_batch_start, dam_batch_end) in enumerate(batches):
        if idx < last_batch_idx:
            logger.info(
                "Skipping completed batch %d (%s to %s)",
                idx, dam_batch_start, dam_batch_end)
            continue

        spp_batch_start = (
            datetime.strptime(dam_batch_start, fmt) - timedelta(days=60)
        ).strftime(fmt)
        spp_batch_end = (
            datetime.strptime(dam_batch_end, fmt) - timedelta(days=60)
        ).strftime(fmt)

        logger.info(
            "Batch %d/%d: DAM %s to %s | SPP (lagged -60d) %s to %s",
            idx+1,
            len(batches),
            dam_batch_start,
            dam_batch_end,
            spp_batch_start,
            spp_batch_end
        )
        logger.debug(
            "[DEBUG] Batch %d/%d: DAM batch start=%s, DAM batch end=%s, "
            "SPP batch start=%s, SPP batch end=%s, db_name=%s",
            idx + 1,
            len(batches),
            dam_batch_start,
            dam_batch_end,
            spp_batch_start,
            spp_batch_end,
            db_name
        )

        try:
            # --- DAM: Split at DAM_ARCHIVE_CUTOFF_DATE ---
            dam_archive_range, dam_regular_range = split_date_range_by_cutoff(
                dam_batch_start, dam_batch_end, DAM_ARCHIVE_CUTOFF_DATE)
            if dam_archive_range:
                dam_archive_product_id = ERCOT_ARCHIVE_PRODUCT_IDS["DAM"].get(
                    "BIDS")
                doc_ids = get_archive_document_ids(
                    dam_archive_product_id, dam_archive_range[0], dam_archive_range[1])
                logger.info(
                    "[Batch %d/%d] Using DAM ARCHIVE API for %s to %s | product_id=%s | doc_ids_found=%d",
                    idx+1,
                    len(batches),
                    dam_archive_range[0],
                    dam_archive_range[1],
                    dam_archive_product_id,
                    len(doc_ids) if doc_ids else 0
                )
                if doc_ids:
                    for i in range(0, len(doc_ids), FILE_LIMITS["DAM"]):
                        batch = doc_ids[i:i + FILE_LIMITS["DAM"]]
                        logger.info(
                            "DAM archive docIds %s-%s (batch %d/%d)",
                            batch[0], batch[-1],
                            i // FILE_LIMITS["DAM"] + 1,
                            (len(doc_ids) +
                             FILE_LIMITS["DAM"] - 1) // FILE_LIMITS["DAM"]
                        )
                        logger.info("product_id=%s", dam_archive_product_id)
                        download_dam_archive_files(
                            dam_archive_product_id, batch, db_name,
                            batch_size=FILE_LIMITS["DAM"])
            if dam_regular_range:
                logger.info(
                    "Using DAM CURRENT API for %s to %s",
                    dam_regular_range[0], dam_regular_range[1])
                fetch_dam_energy_bid_awards(
                    dam_regular_range[0], dam_regular_range[1], db_name=db_name)
                fetch_dam_energy_bids(
                    dam_regular_range[0], dam_regular_range[1], db_name=db_name)
                fetch_dam_energy_only_offer_awards(
                    dam_regular_range[0], dam_regular_range[1], db_name=db_name)
                fetch_dam_energy_only_offers(
                    dam_regular_range[0], dam_regular_range[1], db_name=db_name)

            # --- SPP: Split at SPP_ARCHIVE_CUTOFF_DATE ---
            spp_archive_range, spp_regular_range = split_date_range_by_cutoff(
                spp_batch_start, spp_batch_end, SPP_ARCHIVE_CUTOFF_DATE)
            if spp_archive_range:
                logger.info(
                    "Using SPP ARCHIVE API for %s to %s",
                    spp_archive_range[0], spp_archive_range[1])
                product_id = ERCOT_ARCHIVE_PRODUCT_IDS["SPP"]
                doc_ids = get_archive_document_ids(
                    product_id, spp_archive_range[0], spp_archive_range[1])
                if doc_ids:
                    for i in range(0, len(doc_ids), FILE_LIMITS["SPP"]):
                        batch = doc_ids[i:i + FILE_LIMITS["SPP"]]
                        logger.info(
                            "SPP archive docIds %s-%s (batch %d/%d)",
                            batch[0], batch[-1],
                            i // FILE_LIMITS["SPP"] + 1,
                            (len(doc_ids) +
                             FILE_LIMITS["SPP"] - 1) // FILE_LIMITS["SPP"]
                        )
                        logger.info("product_id=%s", product_id)
                        download_spp_archive_files(
                            product_id, batch, db_name,
                            batch_size=FILE_LIMITS["SPP"])
            if spp_regular_range:
                logger.info(
                    "Using SPP CURRENT API for %s to %s",
                    spp_regular_range[0], spp_regular_range[1])
                fetch_settlement_point_prices(
                    spp_regular_range[0], spp_regular_range[1], db_name=db_name)

            # --- Merge immediately after SPP for this batch ---
            logger.info("Merging data after SPP fetch/store for this batch...")
            merge_data(db_name)

            checkpoint = {
                "stage": "dam_spp_download",
                "details": {
                    "batch_idx": idx + 1,
                    "dam_batch_start": dam_batch_start,
                    "dam_batch_end": dam_batch_end,
                    "spp_batch_start": spp_batch_start,
                    "spp_batch_end": spp_batch_end,
                }
            }
            save_checkpoint_atomic(checkpoint)
            logger.info("Checkpoint saved after batch %d", idx+1)
        except Exception as e:  # TODO: Narrow exception type for better error
            logger.error(
                "Error in batch %d: %s",
                idx+1,
                e
            )
            save_checkpoint_atomic({
                "stage": "dam_spp_download",
                "details": {
                    "batch_idx": idx,
                    "dam_batch_start": dam_batch_start,
                    "dam_batch_end": dam_batch_end,
                    "spp_batch_start": spp_batch_start,
                    "spp_batch_end": spp_batch_end,
                    "error": str(e)
                }
            })
            raise

    clear_checkpoint()
    logger.info("All batches complete. Data merged and checkpoint cleared.")


def store_offers_to_db(*args, **kwargs):
    pass


def store_prices_to_db(*args, **kwargs):
    pass

# --- END OF FILE ---
