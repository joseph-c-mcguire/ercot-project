import sqlite3
from typing import Optional, Set, Any
import logging
import pandas as pd

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
    ERCOT_DB_NAME,
)
from ercot_scraping.utils.filters import (
    get_active_settlement_points,
    filter_by_settlement_points,
    filter_by_qse_names,
)
from ercot_scraping.utils.utils import normalize_data


# Configure logging
logger = logging.getLogger(__name__)


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
        ValueError: If the data provided cannot be used to instantiate an instance of model_class due to a TypeError,
                    indicating invalid or missing data fields.
    """
    if normalize:
        data = normalize_data(data, table_name=table_name.lower())

    if qse_filter is not None:
        data = filter_by_qse_names(data, qse_filter)

    if not data or "data" not in data:
        logger.warning(f"No data to store in {table_name}")
        return

    # Log unique dates in the data
    if hasattr(model_class, "deliveryDate"):
        unique_dates = {
            record.get("deliveryDate", "unknown") for record in data["data"]
        }
        logger.info(
            f"Storing {table_name} data for dates: {sorted(unique_dates)}")

    conn = sqlite3.connect(db_name)
    cursor = conn.cursor()

    # Check existing dates in the table
    try:
        cursor.execute(f"SELECT DISTINCT DeliveryDate FROM {table_name}")
        existing_dates = {row[0] for row in cursor.fetchall()}
        logger.info(
            f"Existing dates in {table_name}: {sorted(existing_dates)}")
    except sqlite3.OperationalError:
        logger.info(f"Table {table_name} does not exist yet")
        existing_dates = set()

    # If all dates in the data are already in the database, skip processing
    if hasattr(model_class, "deliveryDate"):
        unique_dates = {
            record.get("deliveryDate", "unknown") for record in data["data"]
        }
        if unique_dates.issubset(existing_dates):
            logger.info(
                f"All dates in the data are already in {table_name}, skipping insertion.")
            conn.close()
            return

    # Filter out records with dates that already exist
    filtered_rows = []
    for record in data["data"]:
        try:
            if isinstance(record, dict):
                record_dict = record
            elif "fields" in data:
                fields = [field["name"] for field in data["fields"]]
                record_dict = dict(zip(fields, record))
            else:
                continue

            if delivery_date := record_dict.get("deliveryDate"):
                # Normalize the date format to YYYY-MM-DD
                try:
                    if "/" in delivery_date:
                        # Convert MM/DD/YYYY to YYYY-MM-DD
                        mm, dd, yyyy = delivery_date.split("/")
                        delivery_date = f"{yyyy}-{mm:0>2}-{dd:0>2}"

                    filtered_rows.append(record_dict)
                except ValueError as e:
                    logger.error(f"Error parsing date {delivery_date}: {e}")
                    continue

        except (TypeError, ValueError) as e:
            logger.error(f"Error processing record: {e}")
            continue

    logger.info(f"Found {len(filtered_rows)} new records to insert")

    if filtered_rows:
        for record in filtered_rows:
            try:
                instance = model_class(**record)
                cursor.execute(insert_query, instance.as_tuple())
            except (TypeError, ValueError) as e:
                logger.error(f"Error inserting record: {e}")
                continue

        conn.commit()

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
        "dstFlag",
    }

    if not data or "data" not in data or not data["data"]:
        raise ValueError("Invalid or empty data structure")

    if not isinstance(data["data"], list):
        raise ValueError("Data must be a list of records")

    first_record = data["data"][0]
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

    validate_spp_data(data)  # Add validation before processing

    df = pd.DataFrame(data["data"])

    # Use lowercase column names to match the model field names
    groupby_cols = ["deliveryDate", "deliveryHour"]

    grouped_df = df.groupby(groupby_cols, as_index=False, dropna=False).agg(
        {
            "deliveryInterval": "first",
            "settlementPointName": "first",
            "settlementPointType": "first",
            "settlementPointPrice": "mean",
            "dstFlag": "first",
        }
    )

    return {"data": grouped_df.to_dict("records")}


# Delegation functions for different models using local constants:
def store_prices_to_db(data: dict[str,
                                  Any],
                       db_name: str = ERCOT_DB_NAME,
                       filter_by_awards: bool = True) -> None:
    """
    Stores settlement point prices data into the database.

    Args:
        data (dict[str, any]): Settlement point price data
        db_name (str): Database name, defaults to ERCOT_DB_NAME
        filter_by_awards (bool): If True, only store prices for settlement points
                               that appear in bid/offer awards. If award tables don't
                               exist, stores all prices.

    Raises:
        ValueError: If the data structure is invalid or missing required fields
    """
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
    data: dict[str, Any],
    db_name: str = ERCOT_DB_NAME,
    qse_filter: Optional[Set[str]] = None,
) -> None:
    """Store bid award data into the specified database."""
    required_fields = {
        "deliveryDate",
        "hourEnding",
        "settlementPointName",
        "qseName",
        "energyOnlyBidAwardInMW",
        "settlementPointPrice",
        "bidId",
    }
    validate_model_data(data, required_fields, "BidAward")
    store_data_to_db(
        data,
        db_name,
        "BID_AWARDS",
        BID_AWARDS_INSERT_QUERY,
        BidAward,
        qse_filter)


def store_bids_to_db(
    data: dict[str, Any],
    db_name: Optional[str] = ERCOT_DB_NAME,
    qse_filter: Optional[Set[str]] = None,
) -> None:
    """Stores bid data into the database."""
    required_fields = {"deliveryDate", "hourEnding",
                       "settlementPointName", "qseName"}
    validate_model_data(data, required_fields, "Bid")
    store_data_to_db(data, db_name, "BIDS", BIDS_INSERT_QUERY, Bid, qse_filter)


def store_offers_to_db(
    data: dict[str, Any],
    db_name: str = ERCOT_DB_NAME,
    qse_filter: Optional[Set[str]] = None,
) -> None:
    """Stores offer data into the database."""
    required_fields = {"deliveryDate", "hourEnding",
                       "settlementPointName", "qseName"}
    validate_model_data(data, required_fields, "Offer")
    store_data_to_db(data, db_name, "OFFERS",
                     OFFERS_INSERT_QUERY, Offer, qse_filter)


def store_offer_awards_to_db(
    data: dict[str, Any],
    db_name: str = ERCOT_DB_NAME,
    qse_filter: Optional[Set[str]] = None,
) -> None:
    """Stores offer awards data to the specified database."""
    required_fields = {
        "deliveryDate",
        "hourEnding",
        "settlementPointName",
        "qseName",
        "energyOnlyOfferAwardInMW",
        "settlementPointPrice",
        "offerId",
    }
    validate_model_data(data, required_fields, "OfferAward")
    store_data_to_db(
        data,
        db_name,
        "OFFER_AWARDS",
        OFFER_AWARDS_INSERT_QUERY,
        OfferAward,
        qse_filter)
