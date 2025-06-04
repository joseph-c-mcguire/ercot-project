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
    Fetch data from a specified API endpoint with optional date filtering and pagination.
    Loops through all pages and aggregates the data.
    """
    params = {}
    if start_date:
        params["deliveryDateFrom"] = start_date
    if end_date:
        params["deliveryDateTo"] = end_date
    if qse_name:
        params["qseName"] = qse_name
    url = f"{base_url}/{endpoint}"
    all_data = []
    total_pages = 1
    current_page = 1
    first_response_json = None
    while current_page <= total_pages:
        params["page"] = current_page
        LOGGER.debug(
            "Fetching data from endpoint: %s with params: %s and headers: %s (page %d)",
            url,
            params,
            header,
            current_page)
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
                continue
            try:
                response.raise_for_status()
                LOGGER.info(
                    "Data fetched successfully from endpoint: %s (page %d)",
                    url,
                    current_page)
                response_json = response.json()
                # Log _meta and fields for traceability
                meta = response_json.get("_meta")
                if meta:
                    LOGGER.debug(
                        "_meta field for page %d: %s", current_page, meta)
                fields = response_json.get("fields")
                if fields:
                    LOGGER.debug(
                        "fields field for page %d: %s",
                        current_page,
                        fields)
                if first_response_json is None:
                    first_response_json = response_json
                if "data" not in response_json:
                    LOGGER.error(
                        "Unexpected response format: %s", response_json)
                    return {}
                all_data.extend(response_json["data"])
                # Pagination logic
                if meta:
                    total_pages = meta.get("totalPages", 1)
                break  # Success, break retry loop
            except requests.HTTPError as e:
                if attempt < retries - 1:
                    LOGGER.warning(
                        "Request failed. Retrying... (%d/%d)",
                        attempt + 1,
                        retries)
                else:
                    LOGGER.error(
                        "Request failed after %d attempts: %s", retries, e)
                    raise
            except requests.RequestException as e:
                LOGGER.error("Request exception: %s", e)
                raise
        current_page += 1
    # Return the first response structure but with all data aggregated
    if first_response_json is not None:
        first_response_json["data"] = all_data
        return first_response_json
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
        # Do NOT pass qse_name to the API
        return fetch_data_from_endpoint(
            ERCOT_API_BASE_URL_DAM,
            "60_dam_energy_bid_awards",
            s,
            e,
            header=header
        )
    result = fetch_in_batches(
        fetch_func,
        start_date,
        end_date,
        batch_days,
        qse_filter=qse_names,
        max_concurrent=max_concurrent
    )
    return result


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
            header=header
        )
    result = fetch_in_batches(
        fetch_func,
        start_date,
        end_date,
        batch_days,
        qse_filter=qse_names
    )
    return result


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
            header=header
        )
    result = fetch_in_batches(
        fetch_func,
        start_date,
        end_date,
        batch_days,
        qse_filter=qse_names
    )
    return result


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
            header=header
        )
    result = fetch_in_batches(
        fetch_func,
        start_date,
        end_date,
        batch_days,
        qse_filter=qse_names
    )
    return result


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
