"""
Create tables in the SQLite database for ERCOT project.
This function connects to an SQLite database (or creates it if it doesn't exist)
and creates the following tables if they do not already exist:
- SETTLEMENT_POINT_PRICES: Stores settlement point prices with columns for delivery date,
  delivery hour, delivery interval, settlement point name, settlement point type,
  settlement point price, and DST flag.
- BIDS: Stores bid data with columns for delivery date, hour ending, settlement point,
  QSE name, energy-only bid MW and price for up to 10 bids, bid ID, multi-hour block
  indicator, and block curve indicator.
- BID_AWARDS: Stores bid awards with columns for delivery date, hour ending, settlement
  point, QSE name, energy-only bid award MW, settlement point price, and bid ID.
- OFFERS: Stores offer data with columns for delivery date, hour ending, settlement point,
  QSE name, energy-only offer MW and price for up to 10 offers, offer ID, multi-hour block
  indicator, and block curve indicator.
- OFFER_AWARDS: Stores offer awards with columns for delivery date, hour ending, settlement
  point, QSE name, energy-only offer award MW, settlement point price, and offer ID.
The function commits the changes and closes the database connection.
"""

import sqlite3

SETTLEMENT_POINT_PRICES_TABLE_CREATION_QUERY = """
CREATE TABLE IF NOT EXISTS SETTLEMENT_POINT_PRICES (
    DeliveryDate TEXT,
    DeliveryHour INTEGER,
    DeliveryInterval INTEGER,
    SettlementPointName TEXT,
    SettlementPointType TEXT,
    SettlementPointPrice REAL,
    DSTFlag TEXT
)
"""

BIDS_TABLE_CREATION_QUERY = """
CREATE TABLE IF NOT EXISTS BIDS (
    DeliveryDate TEXT,
    HourEnding INTEGER,
    SettlementPoint TEXT,
    QSEName TEXT,
    EnergyOnlyBidMW1 REAL,
    EnergyOnlyBidPrice1 REAL,
    EnergyOnlyBidMW2 REAL,
    EnergyOnlyBidPrice2 REAL,
    EnergyOnlyBidMW3 REAL,
    EnergyOnlyBidPrice3 REAL,
    EnergyOnlyBidMW4 REAL,
    EnergyOnlyBidPrice4 REAL,
    EnergyOnlyBidMW5 REAL,
    EnergyOnlyBidPrice5 REAL,
    EnergyOnlyBidMW6 REAL,
    EnergyOnlyBidPrice6 REAL,
    EnergyOnlyBidMW7 REAL,
    EnergyOnlyBidPrice7 REAL,
    EnergyOnlyBidMW8 REAL,
    EnergyOnlyBidPrice8 REAL,
    EnergyOnlyBidMW9 REAL,
    EnergyOnlyBidPrice9 REAL,
    EnergyOnlyBidMW10 REAL,
    EnergyOnlyBidPrice10 REAL,
    EnergyOnlyBidID TEXT,
    MultiHourBlockIndicator TEXT,
    BlockCurveIndicator TEXT
)
"""

BID_AWARDS_TABLE_CREATION_QUERY = """
CREATE TABLE IF NOT EXISTS BID_AWARDS (
    DeliveryDate TEXT,
    HourEnding INTEGER,
    SettlementPoint TEXT,
    QSEName TEXT,
    EnergyOnlyBidAwardMW REAL,
    SettlementPointPrice REAL,
    BidID TEXT
)
"""

OFFERS_TABLE_CREATION_QUERY = """
CREATE TABLE IF NOT EXISTS OFFERS (
    DeliveryDate TEXT,
    HourEnding INTEGER,
    SettlementPoint TEXT,
    QSEName TEXT,
    EnergyOnlyOfferMW1 REAL,
    EnergyOnlyOfferPrice1 REAL,
    EnergyOnlyOfferMW2 REAL,
    EnergyOnlyOfferPrice2 REAL,
    EnergyOnlyOfferMW3 REAL,
    EnergyOnlyOfferPrice3 REAL,
    EnergyOnlyOfferMW4 REAL,
    EnergyOnlyOfferPrice4 REAL,
    EnergyOnlyOfferMW5 REAL,
    EnergyOnlyOfferPrice5 REAL,
    EnergyOnlyOfferMW6 REAL,
    EnergyOnlyOfferPrice6 REAL,
    EnergyOnlyOfferMW7 REAL,
    EnergyOnlyOfferPrice7 REAL,
    EnergyOnlyOfferMW8 REAL,
    EnergyOnlyOfferPrice8 REAL,
    EnergyOnlyOfferMW9 REAL,
    EnergyOnlyOfferPrice9 REAL,
    EnergyOnlyOfferMW10 REAL,
    EnergyOnlyOfferPrice10 REAL,
    EnergyOnlyOfferID TEXT,
    MultiHourBlockIndicator TEXT,
    BlockCurveIndicator TEXT
)
"""

OFFER_AWARDS_TABLE_CREATION_QUERY = """
CREATE TABLE IF NOT EXISTS OFFER_AWARDS (
    DeliveryDate TEXT,
    HourEnding INTEGER,
    SettlementPoint TEXT,
    QSEName TEXT,
    EnergyOnlyOfferAwardMW REAL,
    SettlementPointPrice REAL,
    OfferID TEXT
)
"""


def create_ercot_tables(save_path: str = "ercot_data.db") -> None:
    """
    Create tables in the SQLite database for ERCOT project.
    This function connects to an SQLite database (or creates it if it doesn't exist)
    and creates the following tables if they do not already exist:
    - SETTLEMENT_POINT_PRICES: Stores settlement point prices with columns for delivery date,
      delivery hour, delivery interval, settlement point name, settlement point type,
      settlement point price, and DST flag.
    - BIDS: Stores bid data with columns for delivery date, hour ending, settlement point,
      QSE name, energy-only bid MW and price for up to 10 bids, bid ID, multi-hour block
      indicator, and block curve indicator.
    - BID_AWARDS: Stores bid awards with columns for delivery date, hour ending, settlement
      point, QSE name, energy-only bid award MW, settlement point price, and bid ID.
    - OFFERS: Stores offer data with columns for delivery date, hour ending, settlement point,
      QSE name, energy-only offer MW and price for up to 10 offers, offer ID, multi-hour block
      indicator, and block curve indicator.
    - OFFER_AWARDS: Stores offer awards with columns for delivery date, hour ending, settlement
      point, QSE name, energy-only offer award MW, settlement point price, and offer ID.
    The function commits the changes and closes the database connection.
    """
    # Connect to SQLite database (or create it if it doesn't exist)
    conn = sqlite3.connect(save_path)
    cursor = conn.cursor()

    # Create tables
    cursor.execute(SETTLEMENT_POINT_PRICES_TABLE_CREATION_QUERY)
    cursor.execute(BIDS_TABLE_CREATION_QUERY)
    cursor.execute(BID_AWARDS_TABLE_CREATION_QUERY)
    cursor.execute(OFFERS_TABLE_CREATION_QUERY)
    cursor.execute(OFFER_AWARDS_TABLE_CREATION_QUERY)

    # Commit changes and close the connection
    conn.commit()
    conn.close()
