import aiosqlite
from typing import Optional, Set
import json
import logging
import pandas as pd
import re
from datetime import datetime

from ercot_scraping.database.data_models import (
    SettlementPointPrice,
    Bid,
    BidAward,
    Offer,
    OfferAward,
)
from ercot_scraping.config.config import (
    SETTLEMENT_POINT_PRICES_INSERT_QUERY,
    BID_AWARDS_INSERT_QUERY,
    BIDS_INSERT_QUERY,
    OFFERS_INSERT_QUERY,
    OFFER_AWARDS_INSERT_QUERY,
    ERCOT_DB_NAME
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
    levels = [logging.DEBUG, logging.INFO, logging.WARNING, logging.ERROR]
    for lvl in levels:
        logs = per_run_handler.get_logs_by_level(lvl)
        with open(f'logs_{logging.getLevelName(lvl)}.txt', 'w') as f:
            for line in logs:
                f.write(line + '\n')
    with open('logs_ALL.txt', 'w') as f:
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
    except Exception:
        pass
    # Try MM/DD/YYYY
    try:
        return datetime.strptime(date_str, "%m/%d/%Y").strftime("%Y-%m-%d")
    except Exception:
        pass
    # Try DD/MM/YYYY (rare, but possible)
    try:
        return datetime.strptime(date_str, "%d/%m/%Y").strftime("%Y-%m-%d")
    except Exception:
        pass
    # Try YYYY/MM/DD
    try:
        return datetime.strptime(date_str, "%Y/%m/%d").strftime("%Y-%m-%d")
    except Exception:
        pass
    # If all fail, return as is
    return date_str


def store_data_to_db(
    data: dict[str, any],
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
        ValueError: If the data provided cannot be used to instantiate an instance of model_class due to a TypeError,
                    indicating invalid or missing data fields.
    """
    if normalize:
        data = normalize_data(data, table_name=table_name.lower())

    if qse_filter is not None:
        data = filter_by_qse_names(data, qse_filter)

    if is_data_empty(data):
        logger.warning(
            f"No data to store in {table_name}, skipping DB insert.")
        return

    # Log unique dates in the data
    if hasattr(model_class, "deliveryDate"):
        unique_dates = {record.get("deliveryDate", "unknown")
                        for record in data["data"]}
        logger.info(
            f"Storing {table_name} data for dates: {sorted(unique_dates)}")

    import sqlite3
    try:
        conn = sqlite3.connect(db_name)
        cursor = conn.cursor()
        # Check existing dates in the table
        try:
            cursor.execute(f"SELECT DISTINCT DeliveryDate FROM {table_name}")
            rows = cursor.fetchall()
            existing_dates = {row[0] for row in rows}
            logger.info(
                f"Existing dates in {table_name}: {sorted(existing_dates)}")
        except Exception:
            logger.info(f"Table {table_name} does not exist yet")
            existing_dates = set()

        # Filter out records with dates that already exist
        filtered_rows = []
        # Normalize deliveryDate fields in all records to YYYY-MM-DD
        if "data" in data and isinstance(data["data"], list):
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
                logger.warning(f"Skipping record due to error: {e}")

        logger.info(f"Found {len(filtered_rows)} new records to insert")

        if filtered_rows:
            cursor.executemany(insert_query, filtered_rows)
            conn.commit()
            logger.info(
                f"Inserted {len(filtered_rows)} records into {table_name} table.")
    except Exception as e:
        logger.error(f"Error in store_data_to_db: {e}")
        raise
    finally:
        if 'conn' in locals():
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

    import pandas as pd
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
def store_prices_to_db(data: dict[str,
                                  any],
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
        logger.error(f"Invalid settlement point price data: {e}")
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
    data: dict[str, any],
    db_name: str = ERCOT_DB_NAME,
    qse_filter: Optional[Set[str]] = None,
) -> None:
    """Store bid award data into the specified database. If data is empty, does nothing."""
    import sqlite3
    import logging
    logger = logging.getLogger(__name__)

    # Log the type and length of data['data']
    if not data or "data" not in data:
        logger.warning("No 'data' key found in bid_awards input.")
        return

    logger.debug(
        f"store_bid_awards_to_db: type(data['data'])={type(data['data'])}, len={len(data['data'])}")
    if len(data['data']) > 0:
        logger.debug(
            f"store_bid_awards_to_db: First record: {data['data'][0]}")
        logger.debug(
            f"store_bid_awards_to_db: All keys in first record: {list(data['data'][0].keys()) if isinstance(data['data'][0], dict) else 'Not a dict'}")
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
    from ercot_scraping.database.data_models import BidAward
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
                logger.error(f"Error inserting record: {e}")
                continue
        conn.commit()
        logger.info(
            f"Inserted {inserted} bid award records into BID_AWARDS table.")
    except Exception as e:
        logger.error(f"Error in store_bid_awards_to_db: {e}")
        raise
    finally:
        if 'conn' in locals():
            conn.close()


def store_bids_to_db(
    data: dict[str, any],
    db_name: Optional[str] = ERCOT_DB_NAME,
    qse_filter: Optional[Set[str]] = None,
) -> None:
    """Stores bid data into the database. If data is empty, does nothing."""
    import sqlite3
    import logging
    logger = logging.getLogger(__name__)
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
    from ercot_scraping.database.data_models import Bid
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
                logger.error(f"Error inserting bid record: {e}")
                continue
        conn.commit()
        logger.info(f"Inserted {inserted} bid records into BIDS table.")
    except Exception as e:
        logger.error(f"Error in store_bids_to_db: {e}")
        raise
    finally:
        if 'conn' in locals():
            conn.close()


def store_offers_to_db(
    data: dict[str, any],
    db_name: str = ERCOT_DB_NAME,
    qse_filter: Optional[Set[str]] = None,
) -> None:
    """Stores offer data into the database. If data is empty, does nothing."""
    import sqlite3
    logger = logging.getLogger(__name__)
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
    from ercot_scraping.database.data_models import Offer
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
                logger.error(f"Error inserting offer record: {e}")
                continue
        conn.commit()
        logger.info(f"Inserted {inserted} offer records into OFFERS table.")
    except Exception as e:
        logger.error(f"Error in store_offers_to_db: {e}")
        raise
    finally:
        if 'conn' in locals():
            conn.close()


def store_offer_awards_to_db(
    data: dict[str, any],
    db_name: str = ERCOT_DB_NAME,
    qse_filter: Optional[Set[str]] = None,
) -> None:
    """Stores offer awards data to the specified database. If data is empty, does nothing."""
    import sqlite3
    logger = logging.getLogger(__name__)
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
    from ercot_scraping.database.data_models import OfferAward
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
                logger.error(f"Error inserting offer award record: {e}")
                continue
        conn.commit()
        logger.info(
            f"Inserted {inserted} offer award records into OFFER_AWARDS table.")
    except Exception as e:
        logger.error(f"Error in store_offer_awards_to_db: {e}")
        raise
    finally:
        if 'conn' in locals():
            conn.close()
