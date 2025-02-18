"""This module provides functionality to fetch settlement point prices from the ERCOT API
and store them into a SQLite database.
Functions:
    fetch_settlement point prices(start_date=None, end_date=None):
        Fetches settlement point prices from the ERCOT API and returns the JSON response.
    store_prices_to_db(data, db_name=ERCOT_DB_NAME):
        Stores settlement point prices data into a SQLite database.
Environment Variables:
    ERCOT_API_URL: The URL of the ERCOT API.
    ERCOT_API_SUBSCRIPTION_KEY: The primary subscription key for accessing the ERCOT API.
    REQUEST_HEADERS: The headers to be used in the API request.
"""

import os
from typing import Optional
import requests


from ercot_scraping.config import (
    ERCOT_API_BASE_URL_DAM,
    ERCOT_API_BASE_URL_SETTLEMENT,
    ERCOT_API_REQUEST_HEADERS,
    ERCOT_API_REQUEST_HEADERS,
    QSE_FILTER_CSV,
    ERCOT_ARCHIVE_PRODUCT_IDS,
    LOGGER,
    ERCOT_DB_NAME
)
from ercot_scraping.filters import load_qse_shortnames
from ercot_scraping.batched_api import fetch_in_batches, rate_limited_request
from ercot_scraping.utils import refresh_access_token, should_use_archive_api
from ercot_scraping.archive_api import get_archive_document_ids, download_spp_archive_files


def fetch_data_from_endpoint(
    base_url: str,
    endpoint: str,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    header: Optional[dict[str, any]] = ERCOT_API_REQUEST_HEADERS,
    retries: int = 3,
    qse_name: Optional[str] = None,  # Changed from qse_names to qse_name
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
    if qse_name:  # Changed from qse_names to qse_name
        # No need for formatting, just use the single name
        params["qseName"] = qse_name

    url = f"{base_url}/{endpoint}"
    LOGGER.debug(
        f"Fetching data from endpoint: {url} with params: {params} and headers: {header}"
    )

    for attempt in range(retries):
        response = rate_limited_request(
            "GET",
            url=url,
            headers=header,
            params=params
        )
        if response.status_code == 401:
            LOGGER.warning("Unauthorized. Refreshing access token.")
            id_token = refresh_access_token()
            header["Authorization"] = f"Bearer {id_token}"
            os.environ["ERCOT_ID_TOKEN"] = id_token
        else:
            try:
                response.raise_for_status()
                LOGGER.info(f"Data fetched successfully from endpoint: {url}")
                response_json = response.json()
                # Add this line
                LOGGER.debug(f"Response data: {response_json}")
                if "data" not in response_json:
                    LOGGER.error(
                        f"Unexpected response format: {response_json}")
                    return {}
                return response_json
            except requests.exceptions.HTTPError as e:
                if attempt < retries - 1:
                    LOGGER.warning(
                        f"Request failed. Retrying... ({attempt + 1}/{retries})"
                    )
                else:
                    LOGGER.error(f"Request failed after {retries} attempts.")
                    raise e
    return {}


def fetch_dam_energy_bid_awards(
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    header: Optional[dict[str, any]] = ERCOT_API_REQUEST_HEADERS,
    tracking_list_path: Optional[str] = QSE_FILTER_CSV,
    batch_days: int = 30
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
    return fetch_in_batches(
        lambda s, e, **kw: fetch_data_from_endpoint(
            ERCOT_API_BASE_URL_DAM,
            "60_dam_energy_bid_awards",
            s,
            e,
            header
        ),
        start_date,
        end_date,
        batch_days
    )


def fetch_dam_energy_bids(
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    header: Optional[dict[str, any]] = ERCOT_API_REQUEST_HEADERS,
    tracking_list_path: Optional[str] = QSE_FILTER_CSV,
    batch_days: int = 30
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
    return fetch_in_batches(
        lambda s, e, **kw: fetch_data_from_endpoint(
            ERCOT_API_BASE_URL_DAM,
            "60_dam_energy_bids",
            s,
            e,
            header
        ),
        start_date,
        end_date,
        batch_days
    )


def fetch_dam_energy_only_offer_awards(
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    header: Optional[dict[str, any]] = ERCOT_API_REQUEST_HEADERS,
    tracking_list_path: Optional[str] = QSE_FILTER_CSV,
    batch_days: int = 30
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
    return fetch_in_batches(
        lambda s, e, **kw: fetch_data_from_endpoint(
            ERCOT_API_BASE_URL_DAM,
            "60_dam_energy_only_offer_awards",
            s,
            e,
            header
        ),
        start_date,
        end_date,
        batch_days
    )


def fetch_dam_energy_only_offers(
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    header: Optional[dict[str, any]] = ERCOT_API_REQUEST_HEADERS,
    tracking_list_path: Optional[str] = QSE_FILTER_CSV,
    batch_days: int = 30
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
    return fetch_in_batches(
        lambda s, e, **kw: fetch_data_from_endpoint(
            ERCOT_API_BASE_URL_DAM,
            "60_dam_energy_only_offers",
            s,
            e,
            header
        ),
        start_date,
        end_date,
        batch_days
    )


def fetch_settlement_point_prices(
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    header: Optional[dict[str, any]] = ERCOT_API_REQUEST_HEADERS,
    tracking_list_path: Optional[str] = QSE_FILTER_CSV,
    batch_days: int = 30
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
    # Load QSE names from tracking list
    LOGGER.info(
        f"Fetching settlement point prices from {start_date} to {end_date}")
    return fetch_in_batches(
        lambda s, e, **kw: fetch_data_from_endpoint(
            ERCOT_API_BASE_URL_SETTLEMENT,
            "spp_node_zone_hub",
            s, e,
            header,
        ),
        start_date,
        end_date,
        batch_days,
    )
