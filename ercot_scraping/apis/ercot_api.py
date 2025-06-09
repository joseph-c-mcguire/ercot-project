"""This module provides functionality to fetch settlement point prices from the ERCOT API
and store them into a SQLite database.
Functions:
    fetch_settlement_point_prices(start_date=None, end_date=None):
        Fetches settlement point prices from the ERCOT API and returns the JSON response.
    store_prices_to_db(data, db_name=ERCOT_DB_NAME):
        Stores settlement point prices data into a SQLite database.
Environment Variables:
    ERCOT_API_URL: The URL of the ERCOT API.
    ERCOT_API_SUBSCRIPTION_KEY: The primary subscription key for accessing the ERCOT API.
    REQUEST_HEADERS: The headers to be used in the API request.
"""

from ercot_scraping.config.config import (
    ERCOT_API_BASE_URL_DAM,
    ERCOT_API_BASE_URL_SETTLEMENT,
    ERCOT_API_REQUEST_HEADERS,
    LOGGER,
    DEFAULT_BATCH_DAYS,
    ERCOT_DB_NAME,
)
import os
import logging
from ercot_scraping.utils.logging_utils import setup_module_logging
from ercot_scraping.utils.utils import refresh_access_token
from typing import Optional
import requests
from ercot_scraping.apis.batched_api import (
    fetch_in_batches,
    rate_limited_request
)


# Configure logging
logger = logging.getLogger(__name__)
per_run_handler = setup_module_logging(__name__)

try:
    # from tqdm import tqdm
    TQDM_AVAILABLE = False
except ImportError:
    TQDM_AVAILABLE = False


def fetch_data_from_endpoint(
    base_url: str,
    endpoint: str,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    header: Optional[dict[str, any]] = None,
    retries: int = 3,
    qse_name: Optional[str] = None,  # Changed from qse_names to qse_name
    page: Optional[int] = None,  # Add page parameter
    store_func: Optional[callable] = None,
    db_name: Optional[str] = None,
    # NEW: callback for checkpointing
    checkpoint_func: Optional[callable] = None,
    batch_info: Optional[dict] = None,           # NEW: info for checkpointing
) -> dict[str, any]:
    if header is None:
        header = ERCOT_API_REQUEST_HEADERS
    if db_name is None:
        db_name = ERCOT_DB_NAME
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
    # progress_bar = None
    # use_progress = TQDM_AVAILABLE
    while current_page <= total_pages:
        # if use_progress and progress_bar is None and total_pages > 1:
        #     progress_bar = tqdm(total=total_pages, desc="API Pages")
        params["page"] = current_page
        LOGGER.info(
            "Fetching page %d/%d from endpoint: %s with params: %s",
            current_page, total_pages, url, params
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
                continue
            try:
                response.raise_for_status()
                response_json = response.json()
                # --- PATCH START ---
                # If the response is a list, wrap it in a dict
                if isinstance(response_json, list):
                    LOGGER.warning(
                        "API returned a list instead of dict; "
                        "wrapping in {'data': ...}"
                    )
                    response_json = {"data": response_json}
                # If the response is a dict but doesn't have 'data',
                # treat the whole dict as data
                elif (
                    isinstance(response_json, dict)
                    and "data" not in response_json
                ):
                    LOGGER.warning(
                        "API response dict missing 'data' key; "
                        "wrapping whole dict as data"
                    )
                    response_json = {"data": [response_json]}
                # --- PATCH END ---
                meta = response_json.get("_meta")
                if meta:
                    total_pages = meta.get("totalPages", 1)
                    current_page_num = meta.get("currentPage", current_page)
                    # if use_progress and progress_bar:
                    #     progress_bar.n = current_page_num
                    #     progress_bar.total = total_pages
                    #     progress_bar.refresh()
                    # else:
                    LOGGER.info(
                        "Fetched page %d of %d (records this page: %d)",
                        current_page_num,
                        total_pages,
                        len(response_json.get('data', []))
                    )
                else:
                    LOGGER.info(
                        "Fetched page %d (records this page: %d)",
                        current_page,
                        len(response_json.get('data', []))
                    )
                if first_response_json is None:
                    first_response_json = response_json
                if "data" not in response_json:
                    LOGGER.error(
                        "Unexpected response format: %s", response_json)
                    # Checkpoint on error
                    if checkpoint_func:
                        checkpoint_func({
                            "stage": "api_fetch_error",
                            "details": {
                                **(batch_info or {}),
                                "page": current_page,
                                "endpoint": endpoint,
                                "error": "missing_data_key"
                            }
                        })
                    return {}
                # STREAMING STORAGE: Store each record as soon as it's fetched
                if store_func is not None:
                    for record in response_json["data"]:
                        store_func(record, db_name)
                all_data.extend(response_json["data"])
                # Checkpoint after each page
                if checkpoint_func:
                    checkpoint_func({
                        "stage": "api_fetch",
                        "details": {
                            **(batch_info or {}),
                            "page": current_page,
                            "endpoint": endpoint
                        }
                    })
                # Pagination logic
                if meta:
                    total_pages = meta.get("totalPages", 1)
                break  # Success, break retry loop
            except requests.HTTPError as e:
                if checkpoint_func:
                    checkpoint_func({
                        "stage": "api_fetch_error",
                        "details": {
                            **(batch_info or {}),
                            "page": current_page,
                            "endpoint": endpoint,
                            "error": str(e)
                        }
                    })
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
                if checkpoint_func:
                    checkpoint_func({
                        "stage": "api_fetch_error",
                        "details": {
                            **(batch_info or {}),
                            "page": current_page,
                            "endpoint": endpoint,
                            "error": str(e)
                        }
                    })
                raise
        current_page += 1
    # if progress_bar:
    #     progress_bar.n = total_pages
    #     progress_bar.refresh()
    #     progress_bar.close()
    if first_response_json is not None:
        first_response_json["data"] = all_data
        return first_response_json
    return {}


def fetch_dam_energy_bids(
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    header: Optional[dict[str, any]] = None,
    tracking_list_path: Optional[str] = None,
    batch_days: int = DEFAULT_BATCH_DAYS,
    qse_names: Optional[set[str]] = None,
    db_name: Optional[str] = None,
    log_every: int = 100,
    batch_size: int = 500,
    checkpoint_func: Optional[callable] = None,  # NEW
    batch_info: Optional[dict] = None,          # NEW
) -> None:
    if header is None:
        header = ERCOT_API_REQUEST_HEADERS
    if db_name is None:
        db_name = ERCOT_DB_NAME

    def fetch_func(s, e, **kw):
        response_json = fetch_data_from_endpoint(
            ERCOT_API_BASE_URL_DAM,
            "60_dam_energy_bids",
            s,
            e,
            header=header,
            qse_name=None,  # Not used in this context
            checkpoint_func=checkpoint_func,
            batch_info=batch_info
        )
        if response_json and "data" in response_json:
            from ercot_scraping.database.store_data import store_bids_to_db
            store_bids_to_db(
                {"data": response_json["data"]},
                db_name=db_name,
                batch_size=batch_size
            )
            count = len(response_json["data"])
            logger.info(
                "[BIDS] Progress: Inserted %d records into %s for %s to %s.",
                count, db_name, s, e
            )

    fetch_in_batches(
        fetch_func,
        start_date,
        end_date,
        batch_days,
        qse_filter=qse_names,
        checkpoint_func=checkpoint_func
    )


def fetch_dam_energy_bid_awards(
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    header: Optional[dict[str, any]] = None,
    tracking_list_path: Optional[str] = None,
    batch_days: int = DEFAULT_BATCH_DAYS,
    qse_names: Optional[set[str]] = None,
    db_name: Optional[str] = None,
    log_every: int = 100,
    batch_size: int = 500,
    checkpoint_func: Optional[callable] = None,  # NEW
    batch_info: Optional[dict] = None,          # NEW
) -> None:
    if header is None:
        header = ERCOT_API_REQUEST_HEADERS
    if db_name is None:
        db_name = ERCOT_DB_NAME

    def fetch_func(s, e, **kw):
        response_json = fetch_data_from_endpoint(
            ERCOT_API_BASE_URL_DAM,
            "60_dam_energy_bid_awards",
            s,
            e,
            header=header,
            qse_name=None,  # Not Used in this context
            checkpoint_func=checkpoint_func,
            batch_info=batch_info
        )
        if response_json and "data" in response_json:
            from ercot_scraping.database.store_data import \
                store_bid_awards_to_db
            store_bid_awards_to_db(
                {"data": response_json["data"]},
                db_name=db_name,
                batch_size=batch_size
            )
            count = len(response_json["data"])
            logger.info(
                "[BID_AWARDS] Progress: Inserted %d records into %s for %s to %s.",
                count, db_name, s, e
            )

    fetch_in_batches(
        fetch_func,
        start_date,
        end_date,
        batch_days,
        qse_filter=qse_names,
        checkpoint_func=checkpoint_func
    )


def fetch_dam_energy_only_offer_awards(
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    header: Optional[dict[str, any]] = None,
    tracking_list_path: Optional[str] = None,
    batch_days: int = DEFAULT_BATCH_DAYS,
    qse_names: Optional[set[str]] = None,
    db_name: Optional[str] = None,
    log_every: int = 100,
    batch_size: int = 500,
    checkpoint_func: Optional[callable] = None,  # NEW
    batch_info: Optional[dict] = None,          # NEW
) -> None:
    if header is None:
        header = ERCOT_API_REQUEST_HEADERS
    if db_name is None:
        db_name = ERCOT_DB_NAME

    def fetch_func(s, e, **kw):
        response_json = fetch_data_from_endpoint(
            ERCOT_API_BASE_URL_DAM,
            "60_dam_energy_only_offer_awards",
            s,
            e,
            header=header,
            qse_name=None,  # Not used in this context
            checkpoint_func=checkpoint_func,
            batch_info=batch_info
        )
        if response_json and "data" in response_json:
            from ercot_scraping.database.store_data import store_offer_awards_to_db
            store_offer_awards_to_db(
                {"data": response_json["data"]},
                db_name=db_name,
                batch_size=batch_size
            )
            count = len(response_json["data"])
            logger.info(
                "[OFFER_AWARDS] Progress: Inserted %d records into %s for %s to %s.",
                count, db_name, s, e
            )

    fetch_in_batches(
        fetch_func,
        start_date,
        end_date,
        batch_days,
        qse_filter=qse_names,
        checkpoint_func=checkpoint_func
    )


def fetch_dam_energy_only_offers(
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    header: Optional[dict[str, any]] = None,
    tracking_list_path: Optional[str] = None,
    batch_days: int = DEFAULT_BATCH_DAYS,
    qse_names: Optional[set[str]] = None,
    db_name: Optional[str] = None,
    log_every: int = 100,
    batch_size: int = 500,
    checkpoint_func: Optional[callable] = None,  # NEW
    batch_info: Optional[dict] = None,          # NEW
) -> None:
    if header is None:
        header = ERCOT_API_REQUEST_HEADERS
    if db_name is None:
        db_name = ERCOT_DB_NAME

    def fetch_func(s, e, **kw):
        response_json = fetch_data_from_endpoint(
            ERCOT_API_BASE_URL_DAM,
            "60_dam_energy_only_offers",
            s,
            e,
            header=header,
            qse_name=None,  # Not used in this context
            checkpoint_func=checkpoint_func,
            batch_info=batch_info
        )
        if response_json and "data" in response_json:
            from ercot_scraping.database.store_data import store_offers_to_db
            store_offers_to_db(
                {"data": response_json["data"]},
                db_name=db_name,
                batch_size=batch_size
            )
            count = len(response_json["data"])
            logger.info(
                "[OFFERS] Progress: Inserted %d records into %s for %s to %s.",
                count, db_name, s, e
            )

    fetch_in_batches(
        fetch_func,
        start_date,
        end_date,
        batch_days,
        qse_filter=qse_names,
        checkpoint_func=checkpoint_func
    )


def fetch_settlement_point_prices(
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    header: Optional[dict[str, any]] = None,
    batch_days: int = DEFAULT_BATCH_DAYS,
    db_name: Optional[str] = None,
    log_every: int = 100,
    batch_size: int = 500,
    checkpoint_func: Optional[callable] = None,  # NEW
    batch_info: Optional[dict] = None,          # NEW
) -> None:
    if header is None:
        header = ERCOT_API_REQUEST_HEADERS
    if db_name is None:
        db_name = ERCOT_DB_NAME

    def fetch_func(s, e, **kw):
        response_json = fetch_data_from_endpoint(
            ERCOT_API_BASE_URL_SETTLEMENT,
            "spp_node_zone_hub",
            s,
            e,
            header=header,
            checkpoint_func=checkpoint_func,
            batch_info=batch_info
        )
        # Batch insert for each page
        if response_json and "data" in response_json:
            from ercot_scraping.database.store_data import store_prices_to_db
            store_prices_to_db(
                {"data": response_json["data"]},
                db_name=db_name,
                batch_size=batch_size
            )
            count = len(response_json["data"])
            logger.info(
                "[SPP] Progress: Inserted %d records into %s for %s to %s.",
                count, db_name, s, e
            )

    fetch_in_batches(
        fetch_func,
        start_date,
        end_date,
        batch_days,
        checkpoint_func=checkpoint_func
    )

    # Example usage for any direct requests (if present):
    # LOGGER.info(
    #     "Requesting: %s %s | params=%s | headers=%s",
    #     "GET", url, params, mask_headers(header)
    # )
    # response = requests.get(url, headers=header, params=params)
    # LOGGER.info(
    #     "Response: status=%d, content_length=%d",
    #     response.status_code, len(response.content)
    # )
    # try:
    #     LOGGER.debug("Response preview: %s", response.text[:200])
    # except Exception:
    #     pass
