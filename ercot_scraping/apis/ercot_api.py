"""This module provides functionality to fetch settlement point prices from the ERCOT API
and store them into a SQLite database.
Functions:
    fetch_settlement point_prices(start_date=None, end_date=None):
        Fetches settlement point prices from the ERCOT API and returns the JSON response.
    store_prices_to_db(data, db_name=ERCOT_DB_NAME):
        Stores settlement point prices data into a SQLite database.
Environment Variables:
    ERCOT_API_URL: The URL of the ERCOT API.
    ERCOT_API_SUBSCRIPTION_KEY: The primary subscription key for accessing the ERCOT API.
    REQUEST_HEADERS: The headers to be used in the API request.
"""

from typing import Optional
import requests
from ercot_scraping.apis.batched_api import fetch_in_batches, rate_limited_request
from ercot_scraping.utils.utils import refresh_access_token
from ercot_scraping.utils.logging_utils import setup_module_logging
import logging
import os

from ercot_scraping.config.config import (
    ERCOT_API_BASE_URL_DAM,
    ERCOT_API_BASE_URL_SETTLEMENT,
    ERCOT_API_REQUEST_HEADERS,
    QSE_FILTER_CSV,
    LOGGER,
    DEFAULT_BATCH_DAYS,
)

# Configure logging
logger = logging.getLogger(__name__)
per_run_handler = setup_module_logging(__name__)


def fetch_data_from_endpoint(
    base_url: str,
    endpoint: str,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    header: Optional[dict[str, any]] = ERCOT_API_REQUEST_HEADERS,
    retries: int = 3,
    qse_name: Optional[str] = None,  # Changed from qse_names to qse_name
    page: Optional[int] = None,  # Add page parameter
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
    if page is not None:  # Add page to params if provided
        params["page"] = page

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
                LOGGER.debug(f"Response data: {response_json}")
                if "data" not in response_json:
                    LOGGER.error(
                        f"Unexpected response format: {response_json}")
                    return {}
                return response_json
            except Exception as e:
                if attempt < retries - 1:
                    LOGGER.warning(
                        f"Request failed. Retrying... ({attempt + 1}/{retries})"
                    )
                else:
                    LOGGER.error(
                        f"Request failed after {retries} attempts: {e}")
                    raise e
    return {}


def fetch_dam_energy_bid_awards(
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    header: Optional[dict[str, any]] = ERCOT_API_REQUEST_HEADERS,
    tracking_list_path: Optional[str] = QSE_FILTER_CSV,
    batch_days: int = DEFAULT_BATCH_DAYS,
    qse_names: Optional[set[str]] = None,
    max_concurrent: int = 1,
    **kwargs
) -> dict[str, any]:
    def fetch_func(s, e, **kw):
        return fetch_data_from_endpoint(
            ERCOT_API_BASE_URL_DAM,
            "60_dam_energy_bid_awards",
            s,
            e,
            header=header,
            qse_name=kw.get('qse_name'),
            page=kw.get('page')
        )
    return fetch_in_batches(
        fetch_func,
        start_date,
        end_date,
        batch_days,
        qse_names=qse_names,
        max_concurrent=max_concurrent
    )


def fetch_dam_energy_bids(
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    header: Optional[dict[str, any]] = ERCOT_API_REQUEST_HEADERS,
    tracking_list_path: Optional[str] = QSE_FILTER_CSV,
    batch_days: int = DEFAULT_BATCH_DAYS,
    qse_names: Optional[set[str]] = None
) -> dict[str, any]:
    def fetch_func(s, e, **kw):
        return fetch_data_from_endpoint(
            ERCOT_API_BASE_URL_DAM,
            "60_dam_energy_bids",
            s,
            e,
            header=header,
            qse_name=kw.get('qse_name'),
            page=kw.get('page')
        )
    return fetch_in_batches(
        fetch_func,
        start_date,
        end_date,
        batch_days,
        qse_names=qse_names
    )


def fetch_dam_energy_only_offer_awards(
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    header: Optional[dict[str, any]] = ERCOT_API_REQUEST_HEADERS,
    tracking_list_path: Optional[str] = QSE_FILTER_CSV,
    batch_days: int = DEFAULT_BATCH_DAYS,
    qse_names: Optional[set[str]] = None
) -> dict[str, any]:
    def fetch_func(s, e, **kw):
        return fetch_data_from_endpoint(
            ERCOT_API_BASE_URL_DAM,
            "60_dam_energy_only_offer_awards",
            s,
            e,
            header=header,
            qse_name=kw.get('qse_name'),
            page=kw.get('page')
        )
    return fetch_in_batches(
        fetch_func,
        start_date,
        end_date,
        batch_days,
        qse_names=qse_names
    )


def fetch_dam_energy_only_offers(
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    header: Optional[dict[str, any]] = ERCOT_API_REQUEST_HEADERS,
    tracking_list_path: Optional[str] = QSE_FILTER_CSV,
    batch_days: int = DEFAULT_BATCH_DAYS,
    qse_names: Optional[set[str]] = None
) -> dict[str, any]:
    def fetch_func(s, e, **kw):
        return fetch_data_from_endpoint(
            ERCOT_API_BASE_URL_DAM,
            "60_dam_energy_only_offers",
            s,
            e,
            header=header,
            qse_name=kw.get('qse_name'),
            page=kw.get('page')
        )
    return fetch_in_batches(
        fetch_func,
        start_date,
        end_date,
        batch_days,
        qse_names=qse_names
    )


def fetch_settlement_point_prices(
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    header: Optional[dict[str, any]] = ERCOT_API_REQUEST_HEADERS,
    tracking_list_path: Optional[str] = QSE_FILTER_CSV,
    batch_days: int = DEFAULT_BATCH_DAYS
) -> dict[str, any]:
    def fetch_func(s, e, **kw):
        return fetch_data_from_endpoint(
            ERCOT_API_BASE_URL_SETTLEMENT,
            "spp_node_zone_hub",
            s, e,
            header=header,
            page=kw.get('page')
        )
    return fetch_in_batches(
        fetch_func,
        start_date,
        end_date,
        batch_days,
    )
