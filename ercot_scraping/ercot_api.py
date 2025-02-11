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
from typing import Optional, Iterator, List
import requests
import sqlite3
import logging
from datetime import datetime, timedelta
import csv
import io
from ratelimit import limits, sleep_and_retry
import chardet
import zipfile
from io import BytesIO

from ercot_scraping.config import (
    ERCOT_API_BASE_URL_DAM,
    ERCOT_API_BASE_URL_SETTLEMENT,
    ERCOT_API_REQUEST_HEADERS,
    ERCOT_API_REQUEST_HEADERS,
    AUTH_URL,
    QSE_FILTER_CSV,
    ERCOT_ARCHIVE_API_BASE_URL,
    ERCOT_ARCHIVE_PRODUCT_IDS,
    API_RATE_LIMIT_REQUESTS,
    API_RATE_LIMIT_INTERVAL,
    API_MAX_ARCHIVE_FILES,
    API_CUTOFF_DATE,
)
from ercot_scraping.filters import load_qse_shortnames, format_qse_filter_param


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
    logger.debug(
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
                # Add this line
                logger.debug(f"Response data: {response_json}")
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
    if should_use_archive_api(start_date, end_date):
        logger.info("Using archive API for historical DAM bid awards data")
        doc_ids = get_archive_document_ids(
            ERCOT_ARCHIVE_PRODUCT_IDS["DAM_BID_AWARDS"],
            start_date,
            end_date
        )
        data = list(download_archive_files(
            ERCOT_ARCHIVE_PRODUCT_IDS["DAM_BID_AWARDS"], doc_ids))
        return {"data": data}

    return fetch_in_batches(
        lambda s, e, qse_name, **kw: fetch_data_from_endpoint(
            ERCOT_API_BASE_URL_DAM,
            "60_dam_energy_bid_awards",
            s, e,
            header,
            qse_name=qse_name
        ),
        start_date,
        end_date,
        batch_days,
        qse_names=load_qse_shortnames(tracking_list_path)
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
    if should_use_archive_api(start_date, end_date):
        logger.info("Using archive API for historical DAM bids data")
        doc_ids = get_archive_document_ids(
            ERCOT_ARCHIVE_PRODUCT_IDS["DAM_BIDS"],
            start_date,
            end_date
        )
        data = list(download_archive_files(
            ERCOT_ARCHIVE_PRODUCT_IDS["DAM_BIDS"], doc_ids))
        return {"data": data}

    return fetch_in_batches(
        lambda s, e, qse_name, **kw: fetch_data_from_endpoint(
            ERCOT_API_BASE_URL_DAM,
            "60_dam_energy_bids",
            s, e,
            header,
            qse_name=qse_name
        ),
        start_date,
        end_date,
        batch_days,
        qse_names=load_qse_shortnames(tracking_list_path)
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
    if should_use_archive_api(start_date, end_date):
        logger.info("Using archive API for historical DAM offer awards data")
        doc_ids = get_archive_document_ids(
            ERCOT_ARCHIVE_PRODUCT_IDS["DAM_OFFER_AWARDS"],
            start_date,
            end_date
        )
        data = list(download_archive_files(
            ERCOT_ARCHIVE_PRODUCT_IDS["DAM_OFFER_AWARDS"], doc_ids))
        return {"data": data}

    return fetch_in_batches(
        lambda s, e, qse_name, **kw: fetch_data_from_endpoint(
            ERCOT_API_BASE_URL_DAM,
            "60_dam_energy_only_offer_awards",
            s, e,
            header,
            qse_name=qse_name
        ),
        start_date,
        end_date,
        batch_days,
        qse_names=load_qse_shortnames(tracking_list_path)
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
    if should_use_archive_api(start_date, end_date):
        logger.info("Using archive API for historical DAM offers data")
        doc_ids = get_archive_document_ids(
            ERCOT_ARCHIVE_PRODUCT_IDS["DAM_OFFERS"],
            start_date,
            end_date
        )
        data = list(download_archive_files(
            ERCOT_ARCHIVE_PRODUCT_IDS["DAM_OFFERS"], doc_ids))
        return {"data": data}

    return fetch_in_batches(
        lambda s, e, qse_name, **kw: fetch_data_from_endpoint(
            ERCOT_API_BASE_URL_DAM,
            "60_dam_energy_only_offers",
            s, e,
            header,
            qse_name=qse_name
        ),
        start_date,
        end_date,
        batch_days,
        qse_names=load_qse_shortnames(tracking_list_path)
    )


@sleep_and_retry
@limits(calls=API_RATE_LIMIT_REQUESTS, period=API_RATE_LIMIT_INTERVAL)
def rate_limited_request(*args, **kwargs) -> requests.Response:
    """Make a rate-limited request using the requests library."""
    response = requests.request(*args, **kwargs)
    if response.status_code == 429:
        raise Exception("Rate limit exceeded")
    return response


def get_archive_document_ids(
    product_id: str,
    start_date: str,
    end_date: str,
) -> List[int]:
    """
    Get document IDs for the specified date range from the archive API.
    """
    url = f"{ERCOT_ARCHIVE_API_BASE_URL}/{product_id}"
    params = {
        "postDatetimeFrom": f"{start_date}T00:00:00.000",
        "postDatetimeTo": f"{end_date}T23:59:59.999",
    }

    doc_ids = []
    page = 1
    while True:
        params["page"] = page
        response = rate_limited_request(
            "GET", url, headers=ERCOT_API_REQUEST_HEADERS, params=params)
        data = response.json()

        if not data.get("archives"):
            break

        doc_ids.extend(archive["docId"] for archive in data["archives"])

        if page >= data["_meta"]["totalPages"]:
            break

        page += 1

    return doc_ids


def detect_encoding(content: bytes) -> str:
    """
    Detect the encoding of binary content.

    Args:
        content (bytes): Binary content to analyze

    Returns:
        str: Detected encoding, defaults to 'utf-8' if detection fails
    """
    result = chardet.detect(content)
    return result['encoding'] if result['encoding'] else 'utf-8'


def download_archive_files(product_id: str, doc_ids: List[int]) -> Iterator[dict]:
    """
    Download archive files in batches and yield parsed data.
    """
    if not doc_ids:
        logger.warning(f"No document IDs found for product {product_id}")
        return []

    url = f"{ERCOT_ARCHIVE_API_BASE_URL}/{product_id}/download"
    logger.info(f"Downloading {len(doc_ids)} documents from archive API")

    for i in range(0, len(doc_ids), API_MAX_ARCHIVE_FILES):
        batch = doc_ids[i:i + API_MAX_ARCHIVE_FILES]
        payload = {"docIds": batch}

        logger.debug(
            f"Requesting batch {i//API_MAX_ARCHIVE_FILES + 1} with {len(batch)} documents")
        logger.debug(f"Request URL: {url}")
        logger.debug(f"Request payload: {payload}")

        try:
            response = rate_limited_request(
                "POST",
                url,
                headers=ERCOT_API_REQUEST_HEADERS,
                json=payload,
                stream=True
            )

            if not response.ok:
                logger.error(
                    f"Failed to download batch. Status code: {response.status_code}")
                continue

            content = response.content
            if not content:
                logger.error("Empty response content")
                continue

            # Try to open as zip file
            try:
                with zipfile.ZipFile(BytesIO(content)) as zip_file:
                    logger.debug(f"Zip file contents: {zip_file.namelist()}")

                    # Process each file in the zip
                    for filename in zip_file.namelist():
                        logger.debug(f"Processing file: {filename}")

                        # Skip directories or non-CSV files
                        if filename.endswith('/'):
                            continue

                        try:
                            # Read CSV from zip
                            with zip_file.open(filename) as csv_file:
                                # Try multiple encodings
                                for encoding in ['utf-8', 'latin1', 'cp1252', 'iso-8859-1']:
                                    try:
                                        csv_content = csv_file.read().decode(encoding)
                                        break
                                    except UnicodeDecodeError:
                                        continue
                                else:
                                    logger.error(
                                        f"Could not decode {filename} with any encoding")
                                    continue

                                # Process CSV content
                                try:
                                    reader = csv.DictReader(
                                        io.StringIO(csv_content.replace(
                                            '\r\n', '\n').replace('\r', '\n'))
                                    )

                                    if not reader.fieldnames:
                                        logger.warning(
                                            f"No headers found in {filename}")
                                        continue

                                    logger.debug(
                                        f"CSV headers: {reader.fieldnames}")

                                    # Normalize field names
                                    field_mapping = {
                                        'delivery_date': 'deliveryDate',
                                        'delivery_hour': 'deliveryHour',
                                        'delivery_interval': 'deliveryInterval',
                                        'settlement_point_name': 'settlementPointName',
                                        'settlement_point_type': 'settlementPointType',
                                        'settlement_point_price': 'settlementPointPrice',
                                        'dst_flag': 'dstFlag'
                                    }

                                    for row in reader:
                                        normalized_row = {}
                                        for key, value in row.items():
                                            if not key:
                                                continue
                                            # Normalize field name
                                            norm_key = key.lower().strip().replace(' ', '_')
                                            final_key = field_mapping.get(
                                                norm_key, norm_key)
                                            # Clean and convert value
                                            if value:
                                                value = value.strip()
                                                # Convert types for known fields
                                                if final_key in ['deliveryHour', 'deliveryInterval']:
                                                    value = int(value)
                                                elif final_key == 'settlementPointPrice':
                                                    value = float(value)
                                            normalized_row[final_key] = value
                                        yield normalized_row

                                except csv.Error as e:
                                    logger.error(
                                        f"CSV parsing error in {filename}: {e}")
                                    continue

                        except Exception as e:
                            logger.error(f"Error processing {filename}: {e}")
                            continue

            except zipfile.BadZipFile:
                logger.error("Response is not a valid zip file")
                logger.debug(f"First 100 bytes of content: {content[:100]}")
                continue

        except Exception as e:
            logger.error(f"Error downloading batch: {str(e)}")
            logger.debug("Exception details:", exc_info=True)
            continue


def should_use_archive_api(start_date: str, end_date: str) -> bool:
    """
    Determine if the archive API should be used based on date range.
    """
    cutoff = datetime.strptime(API_CUTOFF_DATE, "%Y-%m-%d")
    start = datetime.strptime(start_date, "%Y-%m-%d")
    end = datetime.strptime(end_date, "%Y-%m-%d")

    return start < cutoff or end < cutoff


def split_date_range(start_date: str, end_date: str, batch_days: int = 30) -> List[tuple[str, str]]:
    """
    Split a date range into smaller batches.

    Args:
        start_date (str): Start date in YYYY-MM-DD format
        end_date (str): End date in YYYY-MM-DD format
        batch_days (int): Number of days per batch

    Returns:
        List[tuple[str, str]]: List of (batch_start, batch_end) date tuples
    """
    start = datetime.strptime(start_date, "%Y-%m-%d")
    end = datetime.strptime(end_date, "%Y-%m-%d")

    batches = []
    batch_start = start
    while batch_start <= end:
        batch_end = min(batch_start + timedelta(days=batch_days-1), end)
        batches.append((
            batch_start.strftime("%Y-%m-%d"),
            batch_end.strftime("%Y-%m-%d")
        ))
        batch_start = batch_end + timedelta(days=1)

    return batches


def fetch_in_batches(
    fetch_func: callable,
    start_date: str,
    end_date: str,
    batch_days: int = 30,
    qse_names: Optional[set[str]] = None,  # Add qse_names parameter
    **kwargs
) -> dict[str, any]:
    """
    Execute a fetch function in batches over a date range.

    Args:
        fetch_func (callable): Function to fetch data
        start_date (str): Overall start date
        end_date (str): Overall end date
        batch_days (int): Days per batch
        **kwargs: Additional arguments to pass to fetch_func

    Returns:
        dict[str, any]: Combined results from all batches
    """
    batches = split_date_range(start_date, end_date, batch_days)
    total_batches = len(batches)
    total_qses = len(qse_names) if qse_names else 1

    logger.info(
        f"Processing {total_batches} batches for {total_qses} QSEs"
    )

    combined_data = []
    empty_batches = []
    failed_batches = []

    # If we have QSE names, iterate through each one
    if qse_names:
        for qse_name in sorted(qse_names):
            logger.info(f"Processing QSE: {qse_name}")

            for i, (batch_start, batch_end) in enumerate(batches, 1):
                logger.info(
                    f"Fetching batch {i}/{total_batches} for QSE {qse_name}: {batch_start} to {batch_end}"
                )

                try:
                    # Pass single QSE name to fetch function
                    batch_data = fetch_func(
                        batch_start, batch_end, qse_name=qse_name, **kwargs)

                    # ... rest of batch processing logic ...
                    if not batch_data:
                        logger.warning(
                            f"Batch {i}/{total_batches} for QSE {qse_name} returned None")
                        failed_batches.append(
                            (qse_name, batch_start, batch_end))
                        continue

                    if "data" not in batch_data:
                        logger.warning(
                            f"Batch {i}/{total_batches} for QSE {qse_name} missing 'data' key")
                        logger.debug(f"Batch response: {batch_data}")
                        failed_batches.append(
                            (qse_name, batch_start, batch_end))
                        continue

                    records = batch_data["data"]
                    if not records:
                        logger.warning(
                            f"No data found for QSE {qse_name} period {batch_start} to {batch_end}")
                        empty_batches.append(
                            (qse_name, batch_start, batch_end))
                        continue

                    combined_data.extend(records)
                    logger.info(
                        f"Batch {i}/{total_batches} for QSE {qse_name} successful - got {len(records)} records"
                    )

                except Exception as e:
                    logger.error(
                        f"Error in batch {i}/{total_batches} for QSE {qse_name}: {str(e)}")
                    failed_batches.append((qse_name, batch_start, batch_end))
                    continue
    else:
        # Original batch processing without QSE filtering
        for i, (batch_start, batch_end) in enumerate(batches, 1):
            logger.info(
                f"Fetching batch {i}/{total_batches}: {batch_start} to {batch_end}")

            try:
                batch_data = fetch_func(batch_start, batch_end, **kwargs)

                if not batch_data:
                    logger.warning(f"Batch {i}/{total_batches} returned None")
                    failed_batches.append((batch_start, batch_end))
                    continue

                if "data" not in batch_data:
                    logger.warning(
                        f"Batch {i}/{total_batches} missing 'data' key")
                    logger.debug(f"Batch response: {batch_data}")
                    failed_batches.append((batch_start, batch_end))
                    continue

                records = batch_data["data"]
                if not records:
                    logger.warning(
                        f"No data found for period {batch_start} to {batch_end}")
                    empty_batches.append((batch_start, batch_end))
                    continue

                combined_data.extend(records)
                logger.info(
                    f"Batch {i}/{total_batches} successful - got {len(records)} records"
                )

            except Exception as e:
                logger.error(f"Error in batch {i}/{total_batches}: {str(e)}")
                failed_batches.append((batch_start, batch_end))
                continue

    # Enhanced summary logging
    if empty_batches:
        logger.warning(f"Empty batches: {empty_batches}")
    if failed_batches:
        logger.error(f"Failed batches: {failed_batches}")

    total_records = len(combined_data)
    successful_batches = total_batches * total_qses - len(failed_batches)

    logger.info(f"Total records retrieved: {total_records}")
    logger.info(
        f"Average records per successful batch: {total_records / successful_batches if successful_batches > 0 else 0:.2f}"
    )

    return {"data": combined_data}


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

    if should_use_archive_api(start_date, end_date):
        doc_ids = get_archive_document_ids(
            ERCOT_ARCHIVE_PRODUCT_IDS["SPP"],
            start_date,
            end_date
        )
        data = list(download_archive_files(
            ERCOT_ARCHIVE_PRODUCT_IDS["SPP"], doc_ids))
        return {"data": data}

    # Load QSE names from tracking list
    qse_names = load_qse_shortnames(tracking_list_path)
    logger.info(f"Filtering by QSE names: {sorted(qse_names)}")
    logger.info(
        f"Fetching settlement point prices from {start_date} to {end_date}")
    return fetch_in_batches(
        lambda s, e, qse_name, **kw: fetch_data_from_endpoint(
            ERCOT_API_BASE_URL_SETTLEMENT,
            "spp_node_zone_hub",
            s, e,
            header,
            qse_name=qse_name
        ),
        start_date,
        end_date,
        batch_days,
        qse_names=qse_names
    )
