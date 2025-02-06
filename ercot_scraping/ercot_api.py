"""This module provides functionality to fetch settlement point prices from the ERCOT API
and store them into a SQLite database.
Functions:
    fetch_settlement_point_prices(start_date=None, end_date=None):
        Fetches settlement point prices from the ERCOT API and returns the JSON response.
    store_prices_to_db(data, db_name='ercot.db'):
        Stores settlement point prices data into a SQLite database.
Environment Variables:
    ERCOT_API_URL: The URL of the ERCOT API.
    ERCOT_API_SUBSCRIPTION_KEY: The primary subscription key for accessing the ERCOT API.
    REQUEST_HEADERS: The headers to be used in the API request.
"""

from typing import Optional
import requests
import sqlite3
from dotenv import load_dotenv
import os

from ercot_scraping.initialize_database_tables import initialize_database_tables
from ercot_scraping.data_models import (
    SettlementPointPrice,
)  # New import for data validation

load_dotenv()

ERCOT_API_BASE_URL = os.getenv("ERCOT_API_BASE_URL")
ERCOT_API_SUBSCRIPTION_KEY = os.getenv("ERCOT_API_SUBSCRIPTION_KEY")
ERCOT_API_REQUEST_HEADERS = {"Ocp-Apim-Subscription-Key": ERCOT_API_SUBSCRIPTION_KEY}

SETTLEMENT_POINT_PRICES_INSERT_QUERY = """
    INSERT INTO SETTLEMENT_POINT_PRICES (DeliveryDate, DeliveryHour, DeliveryInterval,
                                         SettlementPointName, SettlementPointType,
                                         SettlementPointPrice, DSTFlag)
    VALUES (?, ?, ?, ?, ?, ?, ?)
"""
QUERY_CHECK_TABLE_EXISTS = "SELECT name FROM sqlite_master WHERE type='table' AND name='SETTLEMENT_POINT_PRICES'"


def fetch_settlement_point_prices(
    ercot_api_url: Optional[str] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    header: Optional[dict[str, any]] = None,
):
    """
    Fetches settlement point prices from the ERCOT API.

    This function sends a GET request to the ERCOT API using the provided
    subscription key and returns the JSON response containing the settlement
    point prices.

    Args:
        ercot_api_url (str): The URL of the ERCOT API.
        start_date (str): The start date for fetching data in YYYY-MM-DD format.
        end_date (str): The end date for fetching data in YYYY-MM-DD format.
        header (dict): The headers to be used in the API request.
    Returns:
        dict: A dictionary containing the settlement point prices.

    Raises:
        requests.exceptions.HTTPError: If the HTTP request returned an unsuccessful status code.
    """
    if header is None:
        header = ERCOT_API_REQUEST_HEADERS
    if ercot_api_url is None:
        ercot_api_url = ERCOT_API_BASE_URL
    if not ERCOT_API_SUBSCRIPTION_KEY:
        raise ValueError(
            "ERCOT_API_SUBSCRIPTION_KEY is not set. Please set it in the .env file."
        )
    params = {}
    if start_date:
        params["deliveryDateFrom"] = start_date
    if end_date:
        params["deliveryDateTo"] = end_date

    response = requests.get(url=ercot_api_url, headers=header, params=params)
    if response.status_code == 401:
        raise requests.exceptions.HTTPError(
            "Unauthorized: Check your ERCOT_API_SUBSCRIPTION_KEY."
        )
    response.raise_for_status()
    return response.json()


def validate_sql_query(query: str) -> bool:
    """
    Validates an SQL query by attempting to execute it in a transaction and rolling back.
    Bypasses validation for INSERT statements.
    """
    if query.lstrip().upper().startswith("INSERT"):
        return True
    try:
        conn = sqlite3.connect(":memory:")
        cursor = conn.cursor()
        cursor.execute("BEGIN TRANSACTION;")
        cursor.execute(query, (None,) * query.count("?"))
        cursor.execute("ROLLBACK;")
        conn.close()
        return True
    except sqlite3.Error:
        return False


def store_prices_to_db(data: dict[str, any], db_name: str = "ercot.db"):
    """
    Stores settlement point prices data into a SQLite database.

    This function connects to a SQLite database and creates a table
    named 'settlement_point_prices' if it does not already exist. It then inserts
    records from the provided data into this table.

    Args:
        data (dict): A dictionary containing the settlement point prices data. The dictionary
                     should have a key "data" which maps to a list of records. Each record should
                     be a dictionary with keys "DeliveryDate", "DeliveryHour", "DeliveryInterval",
                     "SettlementPointName", "SettlementPointType", "SettlementPointPrice", and "DSTFlag".
        db_name (str): The name of the SQLite database file.

    Example:
        data = {
            "data": [
                {"DeliveryDate": "2023-10-01", "DeliveryHour": 1, "DeliveryInterval": 15,
                 "SettlementPointName": "ABC", "SettlementPointType": "Type1",
                 "SettlementPointPrice": 25.5, "DSTFlag": "N"},
                {"DeliveryDate": "2023-10-01", "DeliveryHour": 2, "DeliveryInterval": 30,
                 "SettlementPointName": "XYZ", "SettlementPointType": "Type2",
                 "SettlementPointPrice": 30.0, "DSTFlag": "N"}
            ]
        }
        store_prices_to_db(data)
    """
    conn = sqlite3.connect(db_name)
    cursor = conn.cursor()

    # Check if the table exists
    cursor.execute(QUERY_CHECK_TABLE_EXISTS)
    table_exists = cursor.fetchone()

    if not table_exists:
        initialize_database_tables(db_name)

    for record in data["data"]:
        # Validate and convert record using the dataclass
        try:
            spp_record = SettlementPointPrice(**record)
        except TypeError as e:
            raise ValueError(f"Invalid data for SettlementPointPrice: {e}")

        if validate_sql_query(SETTLEMENT_POINT_PRICES_INSERT_QUERY):
            cursor.execute(SETTLEMENT_POINT_PRICES_INSERT_QUERY, spp_record.as_tuple())
        else:
            raise ValueError("Invalid SQL query")

    conn.commit()
    conn.close()


def fetch_data_from_endpoint(
    endpoint: str,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    header: Optional[dict[str, any]] = None,
):
    if header is None:
        header = ERCOT_API_REQUEST_HEADERS
    url = f"{ERCOT_API_BASE_URL}/{endpoint}"
    params = {}
    if start_date:
        params["deliveryDateFrom"] = start_date
    if end_date:
        params["deliveryDateTo"] = end_date

    response = requests.get(url=url, headers=header, params=params)
    response.raise_for_status()
    return response.json()


def fetch_dam_energy_bid_awards(
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    header: Optional[dict[str, any]] = None,
):
    return fetch_data_from_endpoint(
        "60_dam_energy_bid_awards", start_date, end_date, header
    )


def fetch_dam_energy_bids(
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    header: Optional[dict[str, any]] = None,
):
    return fetch_data_from_endpoint("60_dam_energy_bids", start_date, end_date, header)


def fetch_dam_energy_only_offer_awards(
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    header: Optional[dict[str, any]] = None,
):
    return fetch_data_from_endpoint(
        "60_dam_energy_only_offer_awards", start_date, end_date, header
    )


def fetch_dam_energy_only_offers(
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    header: Optional[dict[str, any]] = None,
):
    return fetch_data_from_endpoint(
        "60_dam_energy_only_offers", start_date, end_date, header
    )
