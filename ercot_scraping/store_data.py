import sqlite3
from typing import Optional, Set
import json
import logging

from ercot_scraping.create_ercot_tables import create_ercot_tables
from ercot_scraping.data_models import (
    SettlementPointPrice,
    Bid,
    BidAward,
    Offer,
    OfferAward,
)
from ercot_scraping.config import (
    SETTLEMENT_POINT_PRICES_INSERT_QUERY,
    BID_AWARDS_INSERT_QUERY,
    BIDS_INSERT_QUERY,
    OFFERS_INSERT_QUERY,
    OFFER_AWARDS_INSERT_QUERY,
    ERCOT_DB_NAME
)
from ercot_scraping.filters import (
    get_active_settlement_points,
    filter_by_settlement_points,
    filter_by_qse_names
)


# Configure logging
logger = logging.getLogger(__name__)


def store_data_to_db(
    data: dict[str, any],
    db_name: str,
    table_name: str,
    insert_query: str,
    model_class: type,
    qse_filter: Optional[Set[str]] = None,
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

    Raises:
        ValueError: If the data provided cannot be used to instantiate an instance of model_class due to a TypeError,
                    indicating invalid or missing data fields.
    """
    if qse_filter is not None:
        data = filter_by_qse_names(data, qse_filter)

    if not data or "data" not in data:
        logger.warning(f"No data to store in {table_name}")
        return

    # Log unique dates in the data
    if hasattr(model_class, "deliveryDate"):
        unique_dates = {record.get("deliveryDate", "unknown")
                        for record in data["data"]}
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

    # Check if the table exists; initialize if not
    cursor.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name=?", (
            table_name,)
    )
    if not cursor.fetchone():
        create_ercot_tables(db_name)

    for record in data["data"]:
        try:
            # If the record is already a dict, use it directly
            if isinstance(record, dict):
                record_dict = record
            # If we have fields defined, use them to create the dict
            elif "fields" in data:
                fields = [field["name"] for field in data["fields"]]
                record_dict = dict(zip(fields, record))
            else:
                raise ValueError(
                    f"Invalid data for {model_class.__name__}: Record must be a dictionary or have fields defined")

            instance = model_class(**record_dict)
            cursor.execute(insert_query, instance.as_tuple())
        except TypeError as e:
            missing_fields = [
                field for field in model_class.__annotations__ if field not in record_dict
            ]
            logger.error(
                f"Invalid data for {model_class.__name__}: {e}. Missing fields: {missing_fields}"
            )
    conn.commit()
    conn.close()


# Delegation functions for different models using local constants:
def store_prices_to_db(
    data: dict[str, any], db_name: str = ERCOT_DB_NAME, filter_by_awards: bool = True
) -> None:
    """
    Stores settlement point prices data into the database.

    Args:
        data (dict[str, any]): Settlement point price data
        db_name (str): Database name, defaults to ERCOT_DB_NAME
        filter_by_awards (bool): If True, only store prices for settlement points
                               that appear in bid/offer awards. If award tables don't
                               exist, stores all prices.
    """
    if filter_by_awards:
        active_points = get_active_settlement_points(db_name)
        if active_points:  # Only filter if we found active points
            data = filter_by_settlement_points(data, active_points)

    store_data_to_db(
        data,
        db_name,
        "SETTLEMENT_POINT_PRICES",
        SETTLEMENT_POINT_PRICES_INSERT_QUERY,
        SettlementPointPrice,
    )


def store_bid_awards_to_db(
    data: dict[str, any],
    db_name: str = ERCOT_DB_NAME,
    qse_filter: Optional[Set[str]] = None,
) -> None:
    """
    Store bid award data into the specified database.

    This function acts as a thin wrapper around the lower-level function `store_data_to_db`
    to persist bid awards data into a database table named "BID_AWARDS". The function uses a
    predefined SQL insert query `BID_AWARDS_INSERT_QUERY` and a data model `BidAward` to perform
    the insertion.

    Parameters:
        data (dict[str, any]): A dictionary containing the bid awards data to be stored.
        db_name (str, optional): Name of the database file. Defaults to ERCOT_DB_NAME.
        qse_filter (Optional[Set[str]]): Set of QSE names to filter by.

    Returns:
        None
    """
    store_data_to_db(
        data, db_name, "BID_AWARDS", BID_AWARDS_INSERT_QUERY, BidAward, qse_filter
    )


def store_bids_to_db(
    data: dict[str, any],
    db_name: Optional[str] = ERCOT_DB_NAME,
    qse_filter: Optional[Set[str]] = None,
) -> None:
    """
    Stores bid data into the database by delegating to the generic data storage function.

    This function takes a dictionary containing bid data and inserts it into the "BIDS"
    table in the specified SQLite database. It wraps the call to a lower-level function
    that performs the actual database insertion logic based on the provided SQL query
    (BIDS_INSERT_QUERY) and the Bid model.

    Parameters:
        data (dict[str, any]): A dictionary containing the bid data to be stored.
        db_name (str, optional): The name of the SQLite database file. Defaults to ERCOT_DB_NAME.
        qse_filter (Optional[Set[str]]): Set of QSE names to filter by.

    Returns:
        None
    """
    logger.info("Starting to store bids to database")
    if not data or "data" not in data:
        logger.warning("No bid data to store")
        return

    unique_dates = {record.get("deliveryDate", "unknown")
                    for record in data["data"]}
    logger.info(f"Preparing to store bids for dates: {sorted(unique_dates)}")

    store_data_to_db(data, db_name, "BIDS", BIDS_INSERT_QUERY, Bid, qse_filter)

    logger.info("Finished storing bids to database")


def store_offers_to_db(
    data: dict[str, any],
    db_name: str = ERCOT_DB_NAME,
    qse_filter: Optional[Set[str]] = None,
) -> None:
    """
    Stores offer data into the database.

    This function abstracts the process of inserting offer-related data into the specified
    database by calling the underlying helper function 'store_data_to_db'. The data is stored
    in the 'OFFERS' table using a pre-defined SQL insert query and the corresponding ORM model.

    Parameters:
        data (dict[str, any]): A dictionary containing the offer data to be stored. The structure
                               of the dictionary should align with the expected schema for
                               the 'OFFERS' table.
        db_name (str, optional): The name of the SQLite database file. Defaults to ERCOT_DB_NAME.
        qse_filter (Optional[Set[str]]): Set of QSE names to filter by.

    Returns:
        None: This function does not return a value.
    """
    store_data_to_db(data, db_name, "OFFERS",
                     OFFERS_INSERT_QUERY, Offer, qse_filter)


def store_offer_awards_to_db(
    data: dict[str, any],
    db_name: str = ERCOT_DB_NAME,
    qse_filter: Optional[Set[str]] = None,
) -> None:
    """
    Stores offer awards data to the specified database by calling the underlying store_data_to_db function.

    This function takes a dictionary containing offer awards information and saves it into the "OFFER_AWARDS" table of the provided database.
    It utilizes the pre-defined OFFER_AWARDS_INSERT_QUERY and the OfferAward model to structure and insert the data.

    Parameters:
        data (dict[str, any]): A dictionary with keys as strings and values of any type, containing the offer awards data.
        db_name (str, optional): The name of the SQLite database file. Defaults to ERCOT_DB_NAME.
        qse_filter (Optional[Set[str]]): Set of QSE names to filter by.

    Returns:
        None
    """
    store_data_to_db(
        data, db_name, "OFFER_AWARDS", OFFER_AWARDS_INSERT_QUERY, OfferAward, qse_filter
    )
