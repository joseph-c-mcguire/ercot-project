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
    store_data_to_db(
        data,
        db_name,
        "SETTLEMENT_POINT_PRICES",
        SETTLEMENT_POINT_PRICES_INSERT_QUERY,
        SettlementPointPrice,
    )


def store_bid_awards_to_db(data: dict[str, any], db_name: str = "ercot.db") -> None:
    store_data_to_db(
        data,
        db_name,
        "BID_AWARDS",
        BID_AWARDS_INSERT_QUERY,
        BidAward,
    )


def store_bids_to_db(data: dict[str, any], db_name: str = "ercot.db") -> None:
    store_data_to_db(
        data,
        db_name,
        "BIDS",
        BIDS_INSERT_QUERY,
        Bid,
    )


def store_offers_to_db(data: dict[str, any], db_name: str = "ercot.db") -> None:
    store_data_to_db(
        data,
        db_name,
        "OFFERS",
        OFFERS_INSERT_QUERY,
        Offer,
    )


def store_offer_awards_to_db(data: dict[str, any], db_name: str = "ercot.db") -> None:
    store_data_to_db(
        data,
        db_name,
        "OFFER_AWARDS",
        OFFER_AWARDS_INSERT_QUERY,
        OfferAward,
    )


# ...existing code...
