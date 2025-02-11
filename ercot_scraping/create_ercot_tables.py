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
from ercot_scraping.config import (
    SETTLEMENT_POINT_PRICES_TABLE_CREATION_QUERY,
    BIDS_TABLE_CREATION_QUERY,
    BID_AWARDS_TABLE_CREATION_QUERY,
    OFFERS_TABLE_CREATION_QUERY,
    OFFER_AWARDS_TABLE_CREATION_QUERY
)


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
