"""
Database storage utilities for ERCOT data pipeline.

This module provides functions to store, validate, and aggregate ERCOT market data
(such as settlement point prices, bids, offers, and awards) into a SQLite database.
It also includes logging utilities and helpers for data normalization and filtering.
"""

from typing import Optional, Set, Any
import logging
from datetime import datetime
import sqlite3
import pandas as pd

from ercot_scraping.database.data_models import (
    SettlementPointPrice,
    Offer,
    Bid,
    OfferAward,
    BidAward,
)
from ercot_scraping.config.config import (
    SETTLEMENT_POINT_PRICES_INSERT_QUERY,
    BID_AWARDS_INSERT_QUERY,
    BIDS_INSERT_QUERY,
    OFFERS_INSERT_QUERY,
    OFFER_AWARDS_INSERT_QUERY,
    ERCOT_DB_NAME,
    SETTLEMENT_POINT_PRICES_TABLE_CREATION_QUERY
)
from ercot_scraping.utils.filters import (
    get_active_settlement_points,
    filter_by_settlement_points,
    filter_by_qse_names
)
from ercot_scraping.utils.utils import normalize_data
from ercot_scraping.utils.logging_utils import setup_module_logging


# Configure logging
logger = logging.getLogger(__name__)
per_run_handler = setup_module_logging(__name__)


def dump_logs():
    """
    Dump log messages to separate files based on their severity levels
    and to an aggregated log file.

    This function iterates over a predefined set of logging levels (DEBUG, INFO, WARNING, ERROR).
    For each level, it:
        - Retrieves the log messages for that level using per_run_handler.get_logs_by_level().
        - Writes these messages to a file named "logs_<LEVEL>.txt", where <LEVEL> is the name of the level.

    After processing individual levels, it:
        - Retrieves all log messages using per_run_handler.get_all_logs().
        - Writes them to a file named "logs_ALL.txt".

    Raises:
            Any exceptions that occur during file operations, such as IOError.

    Note:
            This function assumes that 'per_run_handler' is a pre-configured logging handler with the methods:
                - get_logs_by_level(level)
                - get_all_logs()
    """
    levels = [logging.DEBUG, logging.INFO, logging.WARNING, logging.ERROR]
    for lvl in levels:
        logs = per_run_handler.get_logs_by_level(lvl)
        file_name = 'logs_' + logging.getLevelName(lvl) + '.txt'
        with open(file_name, 'w', encoding='utf-8') as f:
            for line in logs:
                f.write(line + '\n')
    with open('logs_ALL.txt', 'w', encoding='utf-8') as f:
        for line in per_run_handler.get_all_logs():
            f.write(line + '\n')


def is_data_empty(data: dict) -> bool:
    """Utility to check if data dict is empty or has no records."""
    return not data or "data" not in data or not data["data"]


def normalize_date_string(date_str):
    """
    Normalize a date string to YYYY-MM-DD format if possible.
    Accepts formats like MM/DD/YYYY, YYYY-MM-DD, etc.
    Returns the normalized string or the original if it can't be parsed.
    """
    if not date_str or not isinstance(date_str, str):
        return date_str
    # Try YYYY-MM-DD first
    try:
        return datetime.strptime(date_str, "%Y-%m-%d").strftime("%Y-%m-%d")
    except ValueError:
        pass
    # Try MM/DD/YYYY
    try:
        return datetime.strptime(date_str, "%m/%d/%Y").strftime("%Y-%m-%d")
    except ValueError:
        pass
    # Try DD/MM/YYYY (rare, but possible)
    try:
        return datetime.strptime(date_str, "%d/%m/%Y").strftime("%Y-%m-%d")
    except ValueError:
        pass
    # Try YYYY/MM/DD
    try:
        return datetime.strptime(date_str, "%Y/%m/%d").strftime("%Y-%m-%d")
    except ValueError:
        pass
    # If all fail, return as is
    return date_str


def store_data_to_db(
    data: dict[str, Any],
    db_name: str,
    table_name: str,
    insert_query: str,
    model_class: type,
    qse_filter: Optional[Set[str]] = None,
    normalize: bool = True,
) -> None:
    """
    Stores data into the specified SQLite database and table.
    This function connects to the provided SQLite database, checks if the target table exists,
    and initializes it if missing. It then iterates over the records in the provided data dictionary,
    instantiates objects of the given model_class using record parameters, and executes the insert query
    to save the data. Finally, the changes are committed and the connection is closed.

    Parameters:
        data (dict[str, any]): A dictionary containing the data to be stored. It is expected to have a key "data"
                               which maps to an iterable of record dictionaries.
        db_name (str): The name (or path) of the SQLite database file.
        table_name (str): The name of the table where data should be stored.
        insert_query (str): The SQL INSERT query to add data into the table.
        model_class (type): The class used to instantiate each record. The class must support initialization with
                            the record's dictionary keys and must provide an as_tuple() method to return the data
                            in tuple format compatible with the insert query.
        qse_filter (Optional[Set[str]]): Set of QSE names to filter by.
        normalize (bool): If True, normalize the data before storing.

    Raises:
        ValueError: If the data provided cannot be used to instantiate an instance of model_class
          due to a TypeError, indicating invalid or missing data fields.
    """
    if normalize:
        data = normalize_data(data, table_name=table_name.lower())

    if qse_filter is not None:
        data = filter_by_qse_names(data, qse_filter)

    if is_data_empty(data):
        logger.warning(
            "No data to store in %s, skipping DB insert.",
            table_name)
        return

    # Log unique dates in the data
    if hasattr(model_class, "deliveryDate"):
        unique_dates = {record.get("deliveryDate", "unknown")
                        for record in data["data"]}
        logger.info(
            "Storing %s data for dates: %s",
            table_name, sorted(unique_dates))

    try:
        conn = sqlite3.connect(db_name)
        cursor = conn.cursor()
        # Check if table exists, create if not
        cursor.execute(
            f"SELECT name FROM sqlite_master WHERE type='table' AND name=?", (table_name,))
        if not cursor.fetchone():
            logger.info(
                "Table %s does not exist yet, creating table.",
                table_name)
            # Use a mapping or convention to get the creation query
            creation_query = None
            if table_name.upper() == "OFFERS":
                from ercot_scraping.config.config import OFFERS_TABLE_CREATION_QUERY
                creation_query = OFFERS_TABLE_CREATION_QUERY
            elif table_name.upper() == "BIDS":
                from ercot_scraping.config.config import BIDS_TABLE_CREATION_QUERY
                creation_query = BIDS_TABLE_CREATION_QUERY
            elif table_name.upper() == "BID_AWARDS":
                from ercot_scraping.config.config import BID_AWARDS_TABLE_CREATION_QUERY
                creation_query = BID_AWARDS_TABLE_CREATION_QUERY
            elif table_name.upper() == "OFFER_AWARDS":
                from ercot_scraping.config.config import OFFER_AWARDS_TABLE_CREATION_QUERY
                creation_query = OFFER_AWARDS_TABLE_CREATION_QUERY
            elif table_name.upper() == "SETTLEMENT_POINT_PRICES":
                from ercot_scraping.config.config import SETTLEMENT_POINT_PRICES_TABLE_CREATION_QUERY
                creation_query = SETTLEMENT_POINT_PRICES_TABLE_CREATION_QUERY
            if creation_query:
                cursor.execute(creation_query)
                conn.commit()
                logger.info("Created table %s successfully.", table_name)
            else:
                logger.error(
                    "No creation query found for table %s.",
                    table_name)
                raise ValueError(
                    f"No creation query found for table {table_name}")

        # Filter out records with dates that already exist
        filtered_rows = []
        # Normalize deliveryDate fields in all records to YYYY-MM-DD
        existing_dates = set()
        if "data" in data and isinstance(data["data"], list):
            # Get existing deliveryDate values from the table if the column
            # exists
            try:
                cursor.execute(f"PRAGMA table_info({table_name})")
                columns = [row[1] for row in cursor.fetchall()]
                if "DeliveryDate" in columns:
                    cursor.execute(
                        f"SELECT DISTINCT DeliveryDate FROM {table_name}")
                    existing_dates = {row[0] for row in cursor.fetchall()}
            except sqlite3.Error as e:
                logger.warning(
                    "Could not fetch existing dates for %s: %s", table_name, e)
            for record in data["data"]:
                if "deliveryDate" in record:
                    record["deliveryDate"] = normalize_date_string(
                        record["deliveryDate"])
        for record in data["data"]:
            try:
                if "deliveryDate" in record and record["deliveryDate"] in existing_dates:
                    continue
                obj = model_class(**record)
                filtered_rows.append(obj.as_tuple())
            except (TypeError, ValueError) as e:
                logger.warning("Skipping record due to error: %s", e)

        logger.info("Found %d new records to insert", len(filtered_rows))

        if filtered_rows:
            try:
                cursor.executemany(insert_query, filtered_rows)
                conn.commit()
                logger.info(
                    "Inserted %d records into %s table.",
                    len(filtered_rows), table_name)
            except sqlite3.OperationalError as e:
                if ("no such table" in str(e)
                        and table_name == "SETTLEMENT_POINT_PRICES"):
                    logger.info(
                        "Table %s does not exist yet: %s", table_name, e)
                    cursor.execute(
                        SETTLEMENT_POINT_PRICES_TABLE_CREATION_QUERY)
                    conn.commit()
                    logger.info(
                        "Created table %s. Retrying insert.", table_name)
                    cursor.executemany(insert_query, filtered_rows)
                    conn.commit()
                    logger.info(
                        "Inserted %d records into %s after creating table.",
                        len(filtered_rows),
                        table_name)
                else:
                    logger.error("Error in store_data_to_db: %s", e)
                    raise
    except sqlite3.Error as e:
        logger.error("SQLite error in store_data_to_db: %s", e)
        raise
    finally:
        try:
            conn.close()
        except Exception:
            pass


def validate_spp_data(data: dict) -> None:
    """Validate settlement point price data structure."""
    required_fields = {
        "deliveryDate",
        "deliveryHour",
        "deliveryInterval",
        "settlementPointName",
        "settlementPointType",
        "settlementPointPrice",
        "dstFlag"
    }

    if not data or "data" not in data or not data["data"]:
        raise ValueError("Invalid or empty data structure")

    if not isinstance(data["data"], list):
        raise ValueError("Data must be a list of records")

    first_record = data["data"][0]
    if isinstance(first_record, list):
        logger.warning(
            "First SPP record is a list, not a dict. Field mapping will be applied or validation skipped.")
        # Optionally, try to map to dict if you know the field order, or just
        # skip validation
        return
    if not isinstance(first_record, dict):
        raise ValueError("Invalid data record format for SPP data")

    if missing_fields := required_fields - set(first_record.keys()):
        raise ValueError(f"Missing required fields: {missing_fields}")


def aggregate_spp_data(data: dict) -> dict:
    """
    Aggregate settlement point price data by delivery date and hour.

    Args:
        data (dict): Dictionary containing settlement point price data

    Returns:
        dict: Aggregated data dictionary
    """
    if not data or "data" not in data:
        return data

    # If first record is a list, map all records to dicts using
    # SettlementPointPrice fields
    if isinstance(
            data["data"],
            list) and data["data"] and isinstance(
            data["data"][0],
            list):
        logger.warning(
            "SPP data records are lists in aggregate_spp_data, mapping to dicts using SettlementPointPrice fields.")
        field_names = [
            'deliveryDate',
            'deliveryHour',
            'deliveryInterval',
            'settlementPointName',
            'settlementPointType',
            'settlementPointPrice',
            'dstFlag'
        ]
        data["data"] = [dict(zip(field_names, row)) for row in data["data"]]

    validate_spp_data(data)  # Add validation before processing

    df = pd.DataFrame(data["data"])

    # Use lowercase column names to match the model field names
    groupby_cols = ["deliveryDate", "deliveryHour"]

    grouped_df = df.groupby(
        groupby_cols,
        as_index=False,
        dropna=False
    ).agg({
        "deliveryInterval": "first",
        "settlementPointName": "first",
        "settlementPointType": "first",
        "settlementPointPrice": "mean",
        "dstFlag": "first"
    })

    return {"data": grouped_df.to_dict("records")}


# Delegation functions for different models using local constants:
def store_prices_to_db(data: dict[str, Any],
                       db_name: str = ERCOT_DB_NAME,
                       filter_by_awards: bool = True) -> None:
    """
    Stores settlement point prices data into the database. If data is empty, does nothing.

    Args:
        data (dict[str, any]): Settlement point price data
        db_name (str): Database name, defaults to ERCOT_DB_NAME
        filter_by_awards (bool): If True, only store prices for settlement points
                               that appear in bid/offer awards. If award tables don't
                               exist, stores all prices.

    Raises:
        ValueError: If the data structure is invalid or missing required fields
    """
    if is_data_empty(data):
        logger.warning(
            "No settlement point prices to store, skipping DB insert.")
        return
    # If first record is a list, map all records to dicts using
    # SettlementPointPrice fields
    if isinstance(
            data["data"],
            list) and data["data"] and isinstance(
            data["data"][0],
            list):
        logger.warning(
            "SPP data records are lists, mapping to dicts using SettlementPointPrice fields.")
        field_names = [
            'deliveryDate',
            'deliveryHour',
            'deliveryInterval',
            'settlementPointName',
            'settlementPointType',
            'settlementPointPrice',
            'dstFlag'
        ]
        data["data"] = [dict(zip(field_names, row)) for row in data["data"]]
    try:
        validate_spp_data(data)  # Validate before any processing
    except ValueError as e:
        logger.error("Invalid settlement point price data: %s", e)
        raise

    if filter_by_awards:
        if active_points := get_active_settlement_points(db_name):
            data = filter_by_settlement_points(data, active_points)

    # Aggregate the data before storing
    data = aggregate_spp_data(data)

    store_data_to_db(
        data,
        db_name,
        "SETTLEMENT_POINT_PRICES",
        SETTLEMENT_POINT_PRICES_INSERT_QUERY,
        SettlementPointPrice,
    )


def validate_model_data(
        data: dict,
        required_fields: set,
        model_name: str) -> None:
    """Validate data structure against required fields."""
    if not data or "data" not in data or not data["data"]:
        raise ValueError(f"Invalid or empty data structure for {model_name}")

    if not isinstance(data["data"], list):
        raise ValueError(f"Data must be a list of records for {model_name}")

    if not data["data"]:
        raise ValueError(f"Empty data list for {model_name}")

    first_record = data["data"][0]
    if not isinstance(first_record, dict):
        raise ValueError(f"Invalid data record format for {model_name}")

    if missing_fields := required_fields - set(first_record.keys()):
        raise ValueError(
            f"Missing required fields for {model_name}: {missing_fields}")


def store_bid_awards_to_db(
    data: dict[str, Any],
    db_name: str = ERCOT_DB_NAME,
    qse_filter: Optional[Set[str]] = None,  # pylint: disable=unused-argument
) -> None:
    """
    Stores bid award records into an SQLite database.
    This function takes bid award data in the form of a dictionary, validates that the required
    fields are present, and then inserts each record into the BID_AWARDS table of the specified database.
    It performs logging at various steps to trace data validation, insertion, and any errors encountered.
    Parameters:
        data (dict[str, Any]): A dictionary expected to contain a key "data", which should be a list of
                               dictionaries where each dictionary represents a bid award record.
        db_name (str): The name (or file path) of the SQLite database where data should be stored.
                       Defaults to ERCOT_DB_NAME.
        qse_filter (Optional[Set[str]]): An optional set of QSE names to filter the records (currently unused).
                                         Defaults to None.
    Raises:
        Exception: Propagates any exceptions encountered during database operations after logging the error.
    """
    # Log the type and length of data['data']
    if not data or "data" not in data:
        logger.warning("No 'data' key found in bid_awards input.")
        return

    logger.debug(
        "store_bid_awards_to_db: type(data['data'])=%s, len=%d",
        type(data['data']), len(data['data']))
    if len(data['data']) > 0:
        logger.debug(
            "store_bid_awards_to_db: First record: %s",
            data['data'][0])
        logger.debug(
            "store_bid_awards_to_db: All keys in first record: %s",
            list(data['data'][0].keys()) if isinstance(data['data'][0], dict)
            else 'Not a dict')
    else:
        logger.debug("store_bid_awards_to_db: data['data'] is empty.")

    if not data["data"]:
        logger.warning("No bid awards to store, skipping DB insert.")
        return
    required_fields = {
        "deliveryDate",
        "hourEnding",
        "settlementPointName",
        "qseName",
        "energyOnlyBidAwardInMW",
        "settlementPointPrice",
        "bidId"
    }
    validate_model_data(data, required_fields, "BidAward")

    # Synchronous DB insert
    try:
        conn = sqlite3.connect(db_name)
        cursor = conn.cursor()
        # Create table if not exists (optional, for robustness)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS BID_AWARDS (
                DeliveryDate TEXT,
                HourEnding INTEGER,
                SettlementPoint TEXT,
                QSEName TEXT,
                EnergyOnlyBidAwardMW REAL,
                SettlementPointPrice REAL,
                BidId TEXT
            )
        """)
        inserted = 0
        for record in data["data"]:
            try:
                instance = BidAward(**record)
                cursor.execute(BID_AWARDS_INSERT_QUERY, instance.as_tuple())
                inserted += 1
            except (TypeError, ValueError) as e:
                logger.error("Error inserting record: %s", e)
                continue
        conn.commit()
        logger.info(
            "Inserted %d bid award records into BID_AWARDS table.",
            inserted)
    except Exception as e:
        logger.error("Error in store_bid_awards_to_db: %s", e)
        raise
    finally:
        if 'conn' in locals():
            conn.close()


def store_bids_to_db(
    data: dict[str, Any],
    db_name: Optional[str] = ERCOT_DB_NAME,
    qse_filter: Optional[Set[str]] = None,  # pylint: disable=unused-argument
) -> None:
    """
    Stores bid records from the provided data dictionary into the SQLite database.

    This function validates that the given data dictionary contains a non-empty 'data' key
    with bid records. It uses a specified set of required fields ("deliveryDate", "hourEnding",
    "settlementPointName", "qseName") to validate each record using the validate_model_data function.
    After validation, it connects to the SQLite database (default name provided by ERCOT_DB_NAME),
    ensures the BIDS table exists, and iterates over each record, attempting to instantiate a Bid
    object and insert its data into the database using the BIDS_INSERT_QUERY. The function logs any
    errors that occur during record insertion or if the bid data is missing/empty, and commits the changes
    to the database before closing the connection.

    Parameters:
        data (dict[str, Any]): A dictionary expected to contain a key "data" with a list of bid records.
        db_name (Optional[str], optional): The name of the SQLite database file. Defaults to ERCOT_DB_NAME.
        qse_filter (Optional[Set[str]], optional): An optional set of QSE names to filter the data. Currently unused.

    Raises:
        Exception: Propagates any exceptions raised during database operations after logging the error.

    Returns:
        None
    """

    if not data or "data" not in data or not data["data"]:
        logger.warning("No bids to store, skipping DB insert.")
        return
    required_fields = {
        "deliveryDate",
        "hourEnding",
        "settlementPointName",
        "qseName"
    }
    validate_model_data(data, required_fields, "Bid")
    try:
        conn = sqlite3.connect(db_name)
        cursor = conn.cursor()
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS BIDS (
                DeliveryDate TEXT,
                HourEnding INTEGER,
                SettlementPoint TEXT,
                QSEName TEXT
            )
        """)
        inserted = 0
        for record in data["data"]:
            try:
                instance = Bid(**record)
                cursor.execute(BIDS_INSERT_QUERY, instance.as_tuple())
                inserted += 1
            except (TypeError, ValueError) as e:
                logger.error("Error inserting bid record: %s", e)
                continue
        conn.commit()
        logger.info(
            "Inserted %d bid records into BIDS table.", inserted)
    except Exception as e:
        logger.error("Error in store_bids_to_db: %s", e)
        raise
    finally:
        if 'conn' in locals():
            conn.close()


def store_offers_to_db(
    data: dict[str, Any],
    db_name: str = ERCOT_DB_NAME,
    qse_filter: Optional[Set[str]] = None,  # pylint: disable=unused-argument
) -> None:
    """
    Stores offer records into the SQLite database.

    This function processes a given data dictionary expected to contain offer records and
    inserts them into the OFFERS table in a SQLite database. It validates that the data
    contains the required fields, creates the OFFERS table if it does not exist, and then
    iterates over each record, converting it into an Offer instance before inserting it
    into the database. Records that fail conversion or insertion are logged and skipped.

    Parameters:
        data (dict[str, Any]): A dictionary containing offer records under the key "data".
                               Each element of data["data"] should be a mapping with at least
                               the following keys: "deliveryDate", "hourEnding",
                               "settlementPointName", and "qseName".
        db_name (str, optional): The name of the SQLite database file. Defaults to ERCOT_DB_NAME.
        qse_filter (Optional[Set[str]]): An optional filter for QSE values. This parameter is
                                         currently unused.

    Raises:
        Exception: Propagates exceptions that occur during database operations after logging the error.

    Returns:
        None
    """

    # logger = logging.getLogger(__name__)  # removed duplicate
    if not data or "data" not in data or not data["data"]:
        logger.warning("No offers to store, skipping DB insert.")
        return
    required_fields = {
        "deliveryDate",
        "hourEnding",
        "settlementPointName",
        "qseName"
    }
    validate_model_data(data, required_fields, "Offer")
    try:
        conn = sqlite3.connect(db_name)
        cursor = conn.cursor()
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS OFFERS (
                DeliveryDate TEXT,
                HourEnding INTEGER,
                SettlementPoint TEXT,
                QSEName TEXT
            )
        """)
        inserted = 0
        for record in data["data"]:
            try:
                instance = Offer(**record)
                cursor.execute(OFFERS_INSERT_QUERY, instance.as_tuple())
                inserted += 1
            except (TypeError, ValueError) as e:
                logger.error("Error inserting offer record: %s", e)
                continue
        conn.commit()
        logger.info(
            "Inserted %d offer records into OFFERS table.", inserted)
    except Exception as e:
        logger.error("Error in store_offers_to_db: %s", e)
        raise
    finally:
        if 'conn' in locals():
            conn.close()


def store_offer_awards_to_db(
    data: dict[str, Any],
    db_name: str = ERCOT_DB_NAME,
    qse_filter: Optional[Set[str]] = None,  # pylint: disable=unused-argument
) -> None:
    """
    Stores offer award records into an SQLite database.

    This function inserts records from the provided data dictionary into the OFFER_AWARDS table of the specified
    SQLite database. If the table does not exist, it is created. Each record within data["data"] must
    contain the following required fields: 'deliveryDate', 'hourEnding', 'settlementPointName', 'qseName',
    'energyOnlyOfferAwardInMW', 'settlementPointPrice', and 'offerId'.
    The data is validated against these fields using the OfferAward model.

    Parameters:
        data (dict[str, Any]): A dictionary containing the offer award data under the key "data".
        db_name (str): The SQLite database name or path. Defaults to ERCOT_DB_NAME.
        qse_filter (Optional[Set[str]]): An optional set of QSE names for filtering
            (note: not directly used in this function).

    Raises:
        Exception: Propagates any exception encountered during the database operations after logging
        the error.

    Notes:
        - The function logs a warning and returns early if there is no valid data to insert.
        - Each record is attempted individually; errors with individual records are logged and
            skipped.
        - The database connection is properly closed in the finally block.
    """

    # logger = logging.getLogger(__name__)  # removed duplicate
    if not data or "data" not in data or not data["data"]:
        logger.warning("No offer awards to store, skipping DB insert.")
        return
    required_fields = {
        "deliveryDate",
        "hourEnding",
        "settlementPointName",
        "qseName",
        "energyOnlyOfferAwardInMW",
        "settlementPointPrice",
        "offerId"
    }
    validate_model_data(data, required_fields, "OfferAward")
    try:
        conn = sqlite3.connect(db_name)
        cursor = conn.cursor()
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS OFFER_AWARDS (
                DeliveryDate TEXT,
                HourEnding INTEGER,
                SettlementPoint TEXT,
                QSEName TEXT,
                EnergyOnlyOfferAwardMW REAL,
                SettlementPointPrice REAL,
                OfferID TEXT
            )
        """)
        inserted = 0
        for record in data["data"]:
            try:
                instance = OfferAward(**record)
                cursor.execute(OFFER_AWARDS_INSERT_QUERY, instance.as_tuple())
                inserted += 1
            except (TypeError, ValueError) as e:
                logger.error("Error inserting offer award record: %s", e)
                continue
        conn.commit()
        logger.info(
            "Inserted %d offer award records into OFFER_AWARDS table.",
            inserted)

    except Exception as e:
        logger.error("Error in store_offer_awards_to_db: %s", e)
        raise
    finally:
        if 'conn' in locals():
            conn.close()
