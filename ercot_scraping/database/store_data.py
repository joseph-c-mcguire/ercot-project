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
    SETTLEMENT_POINT_PRICES_TABLE_CREATION_QUERY,
)
from ercot_scraping.utils.filters import (
    get_active_settlement_points,
    filter_by_settlement_points,
    filter_by_qse_names
)
from ercot_scraping.utils.utils import normalize_data
from ercot_scraping.utils.logging_utils import setup_module_logging
from ercot_scraping.database.create_ercot_tables import create_ercot_tables


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
    # NEW: control number of records per insert
    batch_size: Optional[int] = 10_000,
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
            "SELECT name FROM sqlite_master WHERE type='table' AND name=?", (table_name,))
        if not cursor.fetchone():
            # Instead of creating the table inline, call the central table
            # creation function
            logger.info(
                "Table %s does not exist yet, calling create_ercot_tables().",
                table_name)
            create_ercot_tables(db_name)
            # Re-check if the table now exists
            cursor.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name=?", (table_name,))
            if not cursor.fetchone():
                logger.error(
                    "Table %s still does not exist after create_ercot_tables().",
                    table_name)
                raise ValueError(
                    f"Table {table_name} could not be created in {db_name}")
            else:
                logger.info("Table %s created successfully.", table_name)

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
                # If record is a list, map it to a dict using model field names
                if isinstance(record, list):
                    if model_class is SettlementPointPrice:
                        field_names = [
                            'deliveryDate',
                            'deliveryHour',
                            'deliveryInterval',
                            'settlementPointName',
                            'settlementPointType',
                            'settlementPointPrice',
                            'dstFlag']
                    elif model_class is Offer:
                        field_names = [
                            'deliveryDate',
                            'hourEnding',
                            'settlementPointName',
                            'qseName',
                            'energyOnlyOfferMW1',
                            'energyOnlyOfferPrice1',
                            'energyOnlyOfferMW2',
                            'energyOnlyOfferPrice2',
                            'energyOnlyOfferMW3',
                            'energyOnlyOfferPrice3',
                            'energyOnlyOfferMW4',
                            'energyOnlyOfferPrice4',
                            'energyOnlyOfferMW5',
                            'energyOnlyOfferPrice5',
                            'energyOnlyOfferMW6',
                            'energyOnlyOfferPrice6',
                            'energyOnlyOfferMW7',
                            'energyOnlyOfferPrice7',
                            'energyOnlyOfferMW8',
                            'energyOnlyOfferPrice8',
                            'energyOnlyOfferMW9',
                            'energyOnlyOfferPrice9',
                            'energyOnlyOfferMW10',
                            'energyOnlyOfferPrice10',
                            'offerId',
                            'multiHourBlock',
                            'blockCurve']
                    elif model_class is Bid:
                        field_names = [
                            'deliveryDate',
                            'hourEnding',
                            'settlementPointName',
                            'qseName',
                            'energyOnlyBidMw1',
                            'energyOnlyBidPrice1',
                            'energyOnlyBidMw2',
                            'energyOnlyBidPrice2',
                            'energyOnlyBidMw3',
                            'energyOnlyBidPrice3',
                            'energyOnlyBidMw4',
                            'energyOnlyBidPrice4',
                            'energyOnlyBidMw5',
                            'energyOnlyBidPrice5',
                            'energyOnlyBidMw6',
                            'energyOnlyBidPrice6',
                            'energyOnlyBidMw7',
                            'energyOnlyBidPrice7',
                            'energyOnlyBidMw8',
                            'energyOnlyBidPrice8',
                            'energyOnlyBidMw9',
                            'energyOnlyBidPrice9',
                            'energyOnlyBidMw10',
                            'energyOnlyBidPrice10',
                            'bidId',
                            'multiHourBlock',
                            'blockCurve']
                    elif model_class is OfferAward:
                        field_names = [
                            'deliveryDate',
                            'hourEnding',
                            'settlementPointName',
                            'qseName',
                            'energyOnlyOfferAwardInMW',
                            'settlementPointPrice',
                            'offerId']
                    elif model_class is BidAward:
                        field_names = [
                            'deliveryDate',
                            'hourEnding',
                            'settlementPointName',
                            'qseName',
                            'energyOnlyBidAwardInMW',
                            'settlementPointPrice',
                            'bidId']
                    else:
                        logger.warning(
                            "Unknown model_class for list record mapping: %s", model_class)
                        continue
                    record = dict(zip(field_names, record))
                obj = model_class(**record)
                filtered_rows.append(obj.as_tuple())
            except (TypeError, ValueError) as e:
                logger.warning("Skipping record due to error: %s", e)

        logger.info("Found %d new records to insert", len(filtered_rows))

        if filtered_rows:
            if batch_size is not None and batch_size > 0:
                for i in range(0, len(filtered_rows), batch_size):
                    chunk = filtered_rows[i:i + batch_size]
                    cursor.executemany(insert_query, chunk)
                    # Insert a row into INSERTED_AT for this batch
                    cursor.execute(
                        "INSERT INTO INSERTED_AT (table_name, inserted_at) VALUES (?, CURRENT_TIMESTAMP)",
                        (table_name,)
                    )
                    conn.commit()
                    logger.info(
                        "Inserted batch of %d records into %s table.",
                        len(chunk), table_name)
            else:
                cursor.executemany(insert_query, filtered_rows)
                cursor.execute(
                    "INSERT INTO INSERTED_AT (table_name, inserted_at) VALUES (?, CURRENT_TIMESTAMP)",
                    (table_name,)
                )
                conn.commit()
                logger.info(
                    "Inserted %d records into %s table.",
                    len(filtered_rows), table_name)
    except sqlite3.Error as e:
        logger.error("SQLite error in store_data_to_db: %s", e)
        raise
    finally:
        if 'conn' in locals() and conn:
            conn.close()


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
            "First SPP record is a list, not a dict. Field mapping will be "
            "applied or validation skipped.")
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
            "SPP data records are lists in aggregate_spp_data, mapping to "
            "dicts using SettlementPointPrice fields.")
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
                       filter_by_awards: bool = True,
                       batch_size: int = 500) -> None:
    """
    Stores settlement point prices data into the database. If data is empty, does nothing.

    Args:
        data (dict[str, any]): Settlement point price data
        db_name (str): Database name, defaults to ERCOT_DB_NAME
        filter_by_awards (bool): If True, only store prices for settlement points
                               that appear in bid/offer awards. If award tables don't
                               exist, stores all prices.
        batch_size (int): Number of records to insert per batch (default 500)

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
        batch_size=batch_size
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
    batch_size: int = 500
) -> None:
    """
    Batch version: Stores bid award records in batches.
    """
    if is_data_empty(data):
        logger.warning("No bid awards to store, skipping DB insert.")
        return
    store_data_to_db(
        data,
        db_name,
        "BID_AWARDS",
        BID_AWARDS_INSERT_QUERY,
        BidAward,
        batch_size=batch_size
    )


def store_bids_to_db(
    data: dict[str, Any],
    db_name: Optional[str] = ERCOT_DB_NAME,
    qse_filter: Optional[Set[str]] = None,  # pylint: disable=unused-argument
    batch_size: int = 500
) -> None:
    """
    Batch version: Stores bid records in batches.
    """
    if is_data_empty(data):
        logger.warning("No bids to store, skipping DB insert.")
        return
    store_data_to_db(
        data,
        db_name,
        "BIDS",
        BIDS_INSERT_QUERY,
        Bid,
        batch_size=batch_size
    )


def store_offers_to_db(
    data: dict[str, Any],
    db_name: str = ERCOT_DB_NAME,
    qse_filter: Optional[Set[str]] = None,  # pylint: disable=unused-argument
    batch_size: int = 500
) -> None:
    """
    Batch version: Stores offer records in batches.
    """
    if is_data_empty(data):
        logger.warning("No offers to store, skipping DB insert.")
        return
    store_data_to_db(
        data,
        db_name,
        "OFFERS",
        OFFERS_INSERT_QUERY,
        Offer,
        batch_size=batch_size
    )


def store_offer_awards_to_db(
    data: dict[str, Any],
    db_name: str = ERCOT_DB_NAME,
    qse_filter: Optional[Set[str]] = None,  # pylint: disable=unused-argument
    batch_size: int = 500
) -> None:
    """
    Batch version: Stores offer award records in batches.
    """
    if is_data_empty(data):
        logger.warning("No offer awards to store, skipping DB insert.")
        return
    store_data_to_db(
        data,
        db_name,
        "OFFER_AWARDS",
        OFFER_AWARDS_INSERT_QUERY,
        OfferAward,
        batch_size=batch_size
    )


def store_bid_record_to_db(
    record: dict,
    db_name: str = ERCOT_DB_NAME,
) -> None:
    """
    Stores a single bid record into the SQLite database.
    Ensures the BIDS table exists using the canonical schema before inserting.
    Uses the Bid dataclass for validation and conversion.
    """
    from ercot_scraping.database.data_models import Bid
    try:
        instance = Bid(**record)
    except Exception as e:
        logger.error(f"Invalid Bid record: {record} | Error: {e}")
        raise ValueError(f"Invalid data record for Bid: {e}")
    try:
        import sqlite3
        conn = sqlite3.connect(db_name)
        cursor = conn.cursor()
        cursor.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name=?", ("BIDS",))
        if not cursor.fetchone():
            logger.info(
                "Table BIDS does not exist, calling create_ercot_tables().")
            create_ercot_tables(db_name)
            cursor.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name=?", ("BIDS",))
            if not cursor.fetchone():
                logger.error(
                    "Table BIDS still does not exist after create_ercot_tables().")
                raise ValueError(
                    f"Table BIDS could not be created in {db_name}")
        cursor.execute(BIDS_INSERT_QUERY, instance.as_tuple())
        conn.commit()
        logger.info(
            "Inserted bid record for %s %s.",
            record.get("deliveryDate"),
            record.get("qseName"))
    except Exception as e:
        logger.error("Error inserting bid record: %s", e)
        raise
    finally:
        if 'conn' in locals():
            conn.close()


def store_bid_award_record_to_db(
    record: dict,
    db_name: str = ERCOT_DB_NAME,
) -> None:
    """
    Stores a single bid award record into the SQLite database.
    Ensures the BID_AWARDS table exists using the canonical schema before inserting.
    Uses the BidAward dataclass for validation and conversion.
    """
    try:
        instance = BidAward(**record)
    except Exception as e:
        logger.error(f"Invalid BidAward record: {record} | Error: {e}")
        raise ValueError(f"Invalid data record for BidAward: {e}")
    try:
        conn = sqlite3.connect(db_name)
        cursor = conn.cursor()
        cursor.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name=?", ("BID_AWARDS",))
        if not cursor.fetchone():
            logger.info(
                "Table BID_AWARDS does not exist, calling create_ercot_tables().")
            create_ercot_tables(db_name)
            cursor.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name=?", ("BID_AWARDS",))
            if not cursor.fetchone():
                logger.error(
                    "Table BID_AWARDS still does not exist after create_ercot_tables().")
                raise ValueError(
                    f"Table BID_AWARDS could not be created in {db_name}")
        cursor.execute(BID_AWARDS_INSERT_QUERY, instance.as_tuple())
        conn.commit()
        logger.info(
            "Inserted bid award record for %s %s.",
            record.get("deliveryDate"),
            record.get("qseName"))
    except Exception as e:
        logger.error("Error inserting bid award record: %s", e)
        raise
    finally:
        if 'conn' in locals():
            conn.close()


def store_offer_record_to_db(
    record: dict,
    db_name: str = ERCOT_DB_NAME,
) -> None:
    """
    Stores a single offer record into the SQLite database.
    Ensures the OFFERS table exists using the canonical schema before inserting.
    Uses the Offer dataclass for validation and conversion.
    """
    try:
        instance = Offer(**record)
    except Exception as e:
        logger.error(f"Invalid Offer record: {record} | Error: {e}")
        raise ValueError(f"Invalid data record for Offer: {e}")
    try:
        import sqlite3
        conn = sqlite3.connect(db_name)
        cursor = conn.cursor()
        cursor.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name=?", ("OFFERS",))
        if not cursor.fetchone():
            logger.info(
                "Table OFFERS does not exist, calling create_ercot_tables().")
            create_ercot_tables(db_name)
            cursor.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name=?", ("OFFERS",))
            if not cursor.fetchone():
                logger.error(
                    "Table OFFERS still does not exist after create_ercot_tables().")
                raise ValueError(
                    f"Table OFFERS could not be created in {db_name}")
        cursor.execute(OFFERS_INSERT_QUERY, instance.as_tuple())
        conn.commit()
        logger.info(
            "Inserted offer record for %s %s.",
            record.get("deliveryDate"),
            record.get("qseName"))
    except Exception as e:
        logger.error("Error inserting offer record: %s", e)
        raise
    finally:
        if 'conn' in locals():
            conn.close()


def store_offer_award_record_to_db(
    record: dict,
    db_name: str = ERCOT_DB_NAME,
) -> None:
    """
    Stores a single offer award record into the SQLite database.
    Ensures the OFFER_AWARDS table exists using the canonical schema before inserting.
    Uses the OfferAward dataclass for validation and conversion.
    """
    from ercot_scraping.database.data_models import OfferAward
    try:
        instance = OfferAward(**record)
    except Exception as e:
        logger.error(f"Invalid OfferAward record: {record} | Error: {e}")
        raise ValueError(f"Invalid data record for OfferAward: {e}")
    try:
        import sqlite3
        conn = sqlite3.connect(db_name)
        cursor = conn.cursor()
        cursor.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
            ("OFFER_AWARDS",
             ))
        if not cursor.fetchone():
            logger.info(
                "Table OFFER_AWARDS does not exist, calling create_ercot_tables().")
            create_ercot_tables(db_name)
            cursor.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
                ("OFFER_AWARDS",
                 ))
            if not cursor.fetchone():
                logger.error(
                    "Table OFFER_AWARDS still does not exist after create_ercot_tables().")
                raise ValueError(
                    f"Table OFFER_AWARDS could not be created in {db_name}")
        cursor.execute(OFFER_AWARDS_INSERT_QUERY, instance.as_tuple())
        conn.commit()
        logger.info(
            "Inserted offer award record for %s %s.",
            record.get("deliveryDate"),
            record.get("qseName"))
    except Exception as e:
        logger.error("Error inserting offer award record: %s", e)
        raise
    finally:
        if 'conn' in locals():
            conn.close()


def store_settlement_point_price_record_to_db(
    record: dict,
    db_name: str = ERCOT_DB_NAME,
    status_bar: dict = None,
    total: int = None,
    log_every: int = 1000,
) -> None:
    """
    Stores a single settlement point price record into the SQLite database.
    Ensures the SETTLEMENT_POINT_PRICES table exists using the canonical schema before inserting.
    Accepts both dict and list record formats (auto-maps list to dict if needed).
    Uses the SettlementPointPrice dataclass for validation and conversion.
    Optionally updates a status bar dict for macro progress logging.
    Logs progress every `log_every` records.

    DEPRECATED: Use store_prices_to_db with batch_size for batch inserts.
    """
    # DEPRECATED: Use store_prices_to_db with batch_size for batch inserts.
    logger.warning(
        "store_settlement_point_price_record_to_db is deprecated. Use store_prices_to_db with batch_size for batch inserts.")
    if isinstance(record, list):
        field_names = [
            'deliveryDate',
            'deliveryHour',
            'deliveryInterval',
            'settlementPointName',
            'settlementPointType',
            'settlementPointPrice',
            'dstFlag'
        ]
        record = dict(zip(field_names, record))
    if 'dstFlag' in record and isinstance(record['dstFlag'], bool):
        record['dstFlag'] = 'Y' if record['dstFlag'] else 'N'
    try:
        instance = SettlementPointPrice(**record)
    except Exception as e:
        logger.error(
            f"Invalid SettlementPointPrice record: {record} | Error: {e}")
        raise ValueError(f"Invalid data record for SettlementPointPrice: {e}")
    try:
        conn = sqlite3.connect(db_name)
        cursor = conn.cursor()
        cursor.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
            ("SETTLEMENT_POINT_PRICES",))
        if not cursor.fetchone():
            logger.info(
                f"Table SETTLEMENT_POINT_PRICES does not exist in {db_name}, calling create_ercot_tables().")
            create_ercot_tables(db_name)
            cursor.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
                ("SETTLEMENT_POINT_PRICES",))
            if not cursor.fetchone():
                logger.error(
                    f"Table SETTLEMENT_POINT_PRICES still does not exist after create_ercot_tables() in {db_name}.")
                raise ValueError(
                    f"Table SETTLEMENT_POINT_PRICES could not be created in {db_name}")
        cursor.execute(
            SETTLEMENT_POINT_PRICES_INSERT_QUERY,
            instance.as_tuple())
        conn.commit()
        # Macro status bar logging
        if status_bar is not None:
            status_bar['count'] += 1
            count = status_bar['count']
            if count % log_every == 0 or (total and count == total):
                if total:
                    logger.info(
                        f"[SPP] Progress: {count}/{total} records inserted into {db_name}...")
                else:
                    logger.info(
                        f"[SPP] Progress: {count} records inserted into {db_name}...")
    except Exception as e:
        logger.error(
            f"Error inserting settlement point price record into {db_name}: %s", e)
        raise
    finally:
        if 'conn' in locals():
            conn.close()
