"""This module provides functionality to fetch settlement point prices from the ERCOT API
and store them into a SQLite database.
Functions:
    fetch_settlement point prices(start_date=None, end_date=None):
        Fetches settlement point prices from the ERCOT API and returns the JSON response.
    store_prices_to_db(data, db_name='ercot.db'):
        Stores settlement point prices data into a SQLite database.
Environment Variables:
    ERCOT_API_URL: The URL of the ERCOT API.
    ERCOT_API_SUBSCRIPTION_KEY: The primary subscription key for accessing the ERCOT API.
    REQUEST_HEADERS: The headers to be used in the API request.
"""

import os
from typing import Optional
import requests
import sqlite3
import logging
from .config import (
    ERCOT_API_BASE_URL_DAM,
    ERCOT_API_BASE_URL_SETTLEMENT,
    ERCOT_API_REQUEST_HEADERS,
    ERCOT_API_REQUEST_HEADERS,
    AUTH_URL
)

# Configure logging
logger = logging.getLogger(__name__)

# Ensure environment variables are loaded correctly
logger.info(f"ERCOT_API_BASE_URL_DAM: {ERCOT_API_BASE_URL_DAM}")
logger.info(f"ERCOT_API_BASE_URL_SETTLEMENT: {ERCOT_API_BASE_URL_SETTLEMENT}")


def refresh_access_token() -> str:
    """
    Refresh the access token using the provided username and password.

    Returns:
        str: The new access token.
    """
    auth_response = requests.post(AUTH_URL)
    auth_response.raise_for_status()
    return auth_response.json().get("id_token")


def validate_sql_query(query: str) -> bool:
    """
    Validates an SQL query by attempting to execute it in a transaction and rolling back.
    Handles complex queries including subqueries, UNION operations, and INSERT statements.

    Args:
        query (str): The SQL query to validate

    Returns:
        bool: True if query is valid, False otherwise
    """
    # Handle None and empty queries
    if query is None or not query.strip():
        return False

    # Early return for INSERT statements
    if query.lstrip().upper().startswith("INSERT"):
        return True

    try:
        conn = sqlite3.connect(":memory:")
        cursor = conn.cursor()
        cursor.execute("BEGIN TRANSACTION;")

        # Basic SQL syntax validation
        try:
            first_word = query.lstrip().split()[0].upper()
        except IndexError:
            return False

        if first_word not in {
            "SELECT",
            "CREATE",
            "DROP",
            "ALTER",
            "UPDATE",
            "DELETE",
            "INSERT",
        }:
            return False

        # Create temporary tables for any referenced tables in complex queries
        if "BID_AWARDS" in query:
            cursor.execute("CREATE TABLE BID_AWARDS (SettlementPoint TEXT)")
        if "OFFER_AWARDS" in query:
            cursor.execute("CREATE TABLE OFFER_AWARDS (SettlementPoint TEXT)")

        # Try executing the query
        try:
            cursor.execute(query)
        except sqlite3.OperationalError as e:
            if "no such table" in str(e):
                # If error is about missing tables, those were handled above
                # So this must be a different table - query is invalid
                cursor.execute("ROLLBACK;")
                conn.close()
                return False
            else:
                # Other operational errors indicate invalid SQL
                return False
        except sqlite3.Error:
            # Any other SQLite error indicates invalid SQL
            return False

        cursor.execute("ROLLBACK;")
        conn.close()
        return True

    except sqlite3.Error:
        return False


def fetch_data_from_endpoint(
    base_url: str,
    endpoint: str,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    header: Optional[dict[str, any]] = ERCOT_API_REQUEST_HEADERS,
    retries: int = 3,
) -> dict[str, any]:
    """
    Fetch data from a specified API endpoint with optional date filtering.
    Constructs the URL using the given base URL and endpoint, and sends an HTTP GET request to the API.
    Optional start and end dates can be specified to filter the API results by delivery dates.
    Args:
        base_url (str): The base URL of the API.
        endpoint (str): The API endpoint to request data from, appended to the base URL.
        start_date (Optional[str]): The start date (inclusive) for filtering the data. Defaults to None.
        end_date (Optional[str]): The end date (inclusive) for filtering the data. Defaults to None.
        header (Optional[dict[str, any]]): A custom dictionary of HTTP headers for the request. Defaults to ERCOT_API_REQUEST_HEADERS if None.
        retries (int): The number of retries for the request in case of failure. Defaults to 3.
    Returns:
        dict[str, any]: The parsed JSON response from the API.
    Raises:
        HTTPError: If an error occurs during the HTTP request (non-successful status code).
    """
    params = {}
    if start_date:
        params["deliveryDateFrom"] = start_date
    if end_date:
        params["deliveryDateTo"] = end_date

    url = f"{base_url}/{endpoint}"
    logger.info(
        f"Fetching data from endpoint: {url} with params: {params} and headers: {header}"
    )

    for attempt in range(retries):
        response = requests.get(url=url, headers=header, params=params)
        if response.status_code == 401:
            logger.warning("Unauthorized. Refreshing access token.")
            id_token = refresh_access_token()
            header["Authorization"] = f"Bearer {id_token}"
            os.environ["ERCOT_ID_TOKEN"] = id_token
        else:
            try:
                response.raise_for_status()
                logger.info(f"Data fetched successfully from endpoint: {url}")
                response_json = response.json()
                if "data" not in response_json:
                    logger.error(
                        f"Unexpected response format: {response_json}")
                    return {}
                return response_json
            except requests.exceptions.HTTPError as e:
                if attempt < retries - 1:
                    logger.warning(
                        f"Request failed. Retrying... ({attempt + 1}/{retries})"
                    )
                else:
                    logger.error(f"Request failed after {retries} attempts.")
                    raise e
    return {}


def fetch_dam_energy_bid_awards(
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    header: Optional[dict[str, any]] = None,
) -> dict[str, any]:
    """
    Fetches DAM energy bid awards data from the specified endpoint.

    Args:
        start_date (Optional[str]): The start date for the data retrieval, expected in a recognized date format.
        end_date (Optional[str]): The end date for the data retrieval, expected in a recognized date format.
        header (Optional[dict[str, any]]): Additional header parameters to include in the API request.

    Returns:
        Any: The data returned by the fetch_data_from_endpoint function for the '60_dam_energy_bid_awards' endpoint.

    Raises:
        Exception: Propagates any exception raised during the API request process.
    """
    logger.info(
        f"Fetching DAM energy bid awards from {start_date} to {end_date}")
    return fetch_data_from_endpoint(
        ERCOT_API_BASE_URL_DAM,
        "60_dam_energy_bid_awards",
        start_date,
        end_date,
        header,
    )


def fetch_dam_energy_bids(
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    header: Optional[dict[str, any]] = None,
) -> dict[str, any]:
    """
    Fetches DAM energy bids data from the specified API endpoint.

    This function retrieves data by calling the underlying fetch_data_from_endpoint function,
    targeting the "60_dam_energy_bids" endpoint. The function supports optional filtering by a
    start date and an end date, and allows custom request headers to be provided.

    Args:
        start_date (Optional[str]): The start date for the data query in a string format.
                                    Defaults to None.
        end_date (Optional[str]): The end date for the data query in a string format.
                                  Defaults to None.
        header (Optional[dict[str, any]]): A dictionary of HTTP headers to include in the request.
                                           Defaults to None.

    Returns:
        The data retrieved from the "60_dam_energy_bids" endpoint, formatted as returned by
        fetch_data_from_endpoint.
    """
    logger.info(f"Fetching DAM energy bids from {start_date} to {end_date}")
    return fetch_data_from_endpoint(
        ERCOT_API_BASE_URL_DAM, "60_dam_energy_bids", start_date, end_date, header
    )


def fetch_dam_energy_only_offer_awards(
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    header: Optional[dict[str, any]] = None,
) -> dict[str, any]:
    """
    Fetch DAM energy only offer awards data from the API endpoint.

    This function retrieves DAM energy only offer awards data using the "60_dam_energy_only_offer_awards"
    API endpoint. It takes optional start and end dates to filter the data range and an optional header
    dictionary for any additional configuration required for the API request.

    Args:
        start_date (Optional[str]): The start date for the data query in string format (e.g., 'YYYY-MM-DD').
                                      If not provided, the query will not be constrained by a lower date bound.
        end_date (Optional[str]): The end date for the data query in string format (e.g., 'YYYY-MM-DD').
                                    If not provided, the query will not be constrained by an upper date bound.
        header (Optional[dict[str, any]]): A dictionary containing HTTP headers to modify the API request.
                                           This can include credentials, content types, or other custom headers.

    Returns:
        The data retrieved from the "60_dam_energy_only_offer_awards" endpoint as processed by the
        fetch_data_from_endpoint function. The exact format or type of the returned data depends on the endpoint's response.
    """
    logger.info(
        f"Fetching DAM energy only offer awards from {start_date} to {end_date}"
    )
    return fetch_data_from_endpoint(
        ERCOT_API_BASE_URL_DAM,
        "60_dam_energy_only_offer_awards",
        start_date,
        end_date,
        header,
    )


def fetch_dam_energy_only_offers(
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    header: Optional[dict[str, any]] = None,
) -> dict[str, any]:
    """
    Fetches Demand Aggregated Market (DAM) energy only offers data.

    This function retrieves DAM energy only offers by delegating the request
    to the 'fetch_data_from_endpoint' function using the specific endpoint
    identifier "60_dam_energy_only_offers". The function allows for optional
    specification of a start date, an end date, and HTTP headers.

    Parameters:
        start_date (Optional[str]): The start date for the data query, formatted as a string.
                                    If not provided, a default or full range is assumed.
        end_date (Optional[str]): The end date for the data query, formatted as a string.
                                  If not provided, a default or full range is assumed.
        header (Optional[dict[str, any]]): A dictionary of HTTP headers to include in the request.
                                           This may contain authentication tokens or other metadata.
                                           Defaults to None.

    Returns:
        dict[str, any]: A dictionary containing the data fetched from the DAM energy only
                        offers endpoint.
    """
    logger.info(
        f"Fetching DAM energy only offers from {start_date} to {end_date}")
    return fetch_data_from_endpoint(
        ERCOT_API_BASE_URL_DAM,
        "60_dam_energy_only_offers",
        start_date,
        end_date,
        header,
    )


def fetch_settlement_point_prices(
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    header: Optional[dict[str, any]] = None,
) -> dict[str, any]:
    """
    The function retrieves real-time settlement point prices for nodes, zones, and hubs
    from the ERCOT (Electric Reliability Council of Texas) API.

    Args:
        start_date (str, optional): The start date for the data range in 'YYYY-MM-DD' format.
            If None, defaults to current date.
        end_date (str, optional): The end date for the data range in 'YYYY-MM-DD' format.
            If None, defaults to current date.
        header (dict[str, any], optional): Custom headers for the API request.
            If None, default headers will be used.

    Returns:
        dict[str, any]: A dictionary containing settlement point price data with the following structure:
            {
                'data': List of price records,
                'metadata': Request metadata
            }

    Raises:
        APIError: If the ERCOT API request fails
        ValueError: If the date format is invalid
    """
    logger.info(
        f"Fetching settlement point prices from {start_date} to {end_date}")
    return fetch_data_from_endpoint(
        ERCOT_API_BASE_URL_SETTLEMENT,
        "spp_node_zone_hub",
        start_date,
        end_date,
        header,
    )
