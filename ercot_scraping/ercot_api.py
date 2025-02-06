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

load_dotenv()

ERCOT_API_BASE_URL = os.getenv("ERCOT_API_BASE_URL")
ERCOT_API_SUBSCRIPTION_KEY = os.getenv("ERCOT_API_SUBSCRIPTION_KEY")
ERCOT_API_REQUEST_HEADERS = {"Ocp-Apim-Subscription-Key": ERCOT_API_SUBSCRIPTION_KEY}

# Removed all SQL INSERT query constants from this file.

# Removed store_* functions and store_data_to_db as insertion logic is now in store_data.py


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


def fetch_data_from_endpoint(
    endpoint: str,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    header: Optional[dict[str, any]] = None,
) -> dict[str, any]:
    """
    Fetch data from a specified API endpoint with optional date filtering.
    Constructs the URL using the given endpoint and sends an HTTP GET request to the API.
    If custom headers are not provided, the default ERCOT_API_REQUEST_HEADERS are used.
    Optional start and end dates can be specified to filter the API results by delivery dates.
    Parameters:
        endpoint (str): The API endpoint to request data from, appended to the base URL.
        start_date (Optional[str]): The start date (inclusive) for filtering the data. Defaults to None.
        end_date (Optional[str]): The end date (inclusive) for filtering the data. Defaults to None.
        header (Optional[dict[str, any]]): A custom dictionary of HTTP headers for the request. Defaults to ERCOT_API_REQUEST_HEADERS if None.
    Returns:
        dict[str, any]: The parsed JSON response from the API.
    Raises:
        HTTPError: If an error occurs during the HTTP request (non-successful status code).
    """
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


def fetch_settlement_point_prices(
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    header: Optional[dict[str, any]] = None,
) -> dict[str, any]:
    """
    Fetches settlement point prices from the ERCOT API by using a generic endpoint.
    """
    return fetch_data_from_endpoint("spp_node_zone_hub", start_date, end_date, header)
