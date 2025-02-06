import sqlite3
from ercot_scraping.create_ercot_tables import create_ercot_tables
from ercot_scraping.data_models import (
    SettlementPointPrice,
    Bid,
    BidAward,
    Offer,
    OfferAward,
)

# New: Move INSERT query constants here
SETTLEMENT_POINT_PRICES_INSERT_QUERY = """
    INSERT INTO SETTLEMENT_POINT_PRICES (DeliveryDate, DeliveryHour, DeliveryInterval,
                                         SettlementPointName, SettlementPointType,
                                         SettlementPointPrice, DSTFlag)
    VALUES (?, ?, ?, ?, ?, ?, ?)
"""
BID_AWARDS_INSERT_QUERY = """
    INSERT INTO BID_AWARDS (DeliveryDate, HourEnding, SettlementPoint, QSEName, 
                            EnergyOnlyBidAwardMW, SettlementPointPrice, BidID)
    VALUES (?, ?, ?, ?, ?, ?, ?)
"""
BIDS_INSERT_QUERY = """
    INSERT INTO BIDS (DeliveryDate, HourEnding, SettlementPoint, QSEName,
                      EnergyOnlyBidMW1, EnergyOnlyBidPrice1, EnergyOnlyBidMW2, EnergyOnlyBidPrice2,
                      EnergyOnlyBidMW3, EnergyOnlyBidPrice3, EnergyOnlyBidMW4, EnergyOnlyBidPrice4,
                      EnergyOnlyBidMW5, EnergyOnlyBidPrice5, EnergyOnlyBidMW6, EnergyOnlyBidPrice6,
                      EnergyOnlyBidMW7, EnergyOnlyBidPrice7, EnergyOnlyBidMW8, EnergyOnlyBidPrice8,
                      EnergyOnlyBidMW9, EnergyOnlyBidPrice9, EnergyOnlyBidMW10, EnergyOnlyBidPrice10,
                      EnergyOnlyBidID, MultiHourBlockIndicator, BlockCurveIndicator)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
"""
OFFERS_INSERT_QUERY = """
    INSERT INTO OFFERS (DeliveryDate, HourEnding, SettlementPoint, QSEName,
                         EnergyOnlyOfferMW1, EnergyOnlyOfferPrice1, EnergyOnlyOfferMW2, EnergyOnlyOfferPrice2,
                         EnergyOnlyOfferMW3, EnergyOnlyOfferPrice3, EnergyOnlyOfferMW4, EnergyOnlyOfferPrice4,
                         EnergyOnlyOfferMW5, EnergyOnlyOfferPrice5, EnergyOnlyOfferMW6, EnergyOnlyOfferPrice6,
                         EnergyOnlyOfferMW7, EnergyOnlyOfferPrice7, EnergyOnlyOfferMW8, EnergyOnlyOfferPrice8,
                         EnergyOnlyOfferMW9, EnergyOnlyOfferPrice9, EnergyOnlyOfferMW10, EnergyOnlyOfferPrice10,
                         EnergyOnlyOfferID, MultiHourBlockIndicator, BlockCurveIndicator)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
"""
OFFER_AWARDS_INSERT_QUERY = """
    INSERT INTO OFFER_AWARDS (DeliveryDate, HourEnding, SettlementPoint, QSEName, 
                              EnergyOnlyOfferAwardMW, SettlementPointPrice, OfferID)
    VALUES (?, ?, ?, ?, ?, ?, ?)
"""


def store_data_to_db(
    data: dict[str, any],
    db_name: str,
    table_name: str,
    insert_query: str,
    model_class: type,
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

    Raises:
        ValueError: If the data provided cannot be used to instantiate an instance of model_class due to a TypeError,
                    indicating invalid or missing data fields.
    """
    conn = sqlite3.connect(db_name)
    cursor = conn.cursor()
    # Check if the table exists; initialize if not
    cursor.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name=?", (table_name,)
    )
    if not cursor.fetchone():
        create_ercot_tables(db_name)
    for record in data["data"]:
        try:
            instance = model_class(**record)
        except TypeError as e:
            raise ValueError(f"Invalid data for {model_class.__name__}: {e}")
        cursor.execute(insert_query, instance.as_tuple())
    conn.commit()
    conn.close()


# Delegation functions for different models using local constants:
def store_prices_to_db(data: dict[str, any], db_name: str = "ercot.db") -> None:
    """
    Stores settlement point prices data into the database by utilizing the generic
    store_data_to_db function.

    This function wraps the store_data_to_db call, targeting the "SETTLEMENT_POINT_PRICES"
    table with the pre-defined insert query and model. It accepts the data to be stored,
    along with an optional database name.

    Parameters:
        data (dict[str, any]): A dictionary containing the settlement point price data.
        db_name (str, optional): The name of the database file. Defaults to "ercot.db".

    Returns:
        None
    """
    store_data_to_db(
        data,
        db_name,
        "SETTLEMENT_POINT_PRICES",
        SETTLEMENT_POINT_PRICES_INSERT_QUERY,
        SettlementPointPrice,
    )


def store_bid_awards_to_db(data: dict[str, any], db_name: str = "ercot.db") -> None:
    """
    Store bid award data into the specified database.

    This function acts as a thin wrapper around the lower-level function `store_data_to_db`
    to persist bid awards data into a database table named "BID_AWARDS". The function uses a
    predefined SQL insert query `BID_AWARDS_INSERT_QUERY` and a data model `BidAward` to perform
    the insertion.

    Parameters:
        data (dict[str, any]): A dictionary containing the bid awards data to be stored.
        db_name (str, optional): Name of the database file. Defaults to "ercot.db".

    Returns:
        None
    """
    store_data_to_db(
        data,
        db_name,
        "BID_AWARDS",
        BID_AWARDS_INSERT_QUERY,
        BidAward,
    )


def store_bids_to_db(data: dict[str, any], db_name: str = "ercot.db") -> None:
    """
    Stores bid data into the database by delegating to the generic data storage function.

    This function takes a dictionary containing bid data and inserts it into the "BIDS"
    table in the specified SQLite database. It wraps the call to a lower-level function
    that performs the actual database insertion logic based on the provided SQL query
    (BIDS_INSERT_QUERY) and the Bid model.

    Parameters:
        data (dict[str, any]): A dictionary containing the bid data to be stored.
        db_name (str, optional): The name of the SQLite database file. Defaults to "ercot.db".

    Returns:
        None
    """
    store_data_to_db(
        data,
        db_name,
        "BIDS",
        BIDS_INSERT_QUERY,
        Bid,
    )


def store_offers_to_db(data: dict[str, any], db_name: str = "ercot.db") -> None:
    """
    Stores offer data into the database.

    This function abstracts the process of inserting offer-related data into the specified
    database by calling the underlying helper function 'store_data_to_db'. The data is stored
    in the 'OFFERS' table using a pre-defined SQL insert query and the corresponding ORM model.

    Parameters:
        data (dict[str, any]): A dictionary containing the offer data to be stored. The structure
                               of the dictionary should align with the expected schema for
                               the 'OFFERS' table.
        db_name (str, optional): The name of the SQLite database file. Defaults to "ercot.db".

    Returns:
        None: This function does not return a value.
    """
    store_data_to_db(
        data,
        db_name,
        "OFFERS",
        OFFERS_INSERT_QUERY,
        Offer,
    )


def store_offer_awards_to_db(data: dict[str, any], db_name: str = "ercot.db") -> None:
    """
    Stores offer awards data to the specified database by calling the underlying store_data_to_db function.

    This function takes a dictionary containing offer awards information and saves it into the "OFFER_AWARDS" table of the provided database.
    It utilizes the pre-defined OFFER_AWARDS_INSERT_QUERY and the OfferAward model to structure and insert the data.

    Parameters:
        data (dict[str, any]): A dictionary with keys as strings and values of any type, containing the offer awards data.
        db_name (str, optional): The name of the SQLite database file. Defaults to "ercot.db".

    Returns:
        None
    """
    store_data_to_db(
        data,
        db_name,
        "OFFER_AWARDS",
        OFFER_AWARDS_INSERT_QUERY,
        OfferAward,
    )
