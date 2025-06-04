"""
batched_api.py

Batching and rate-limited API utilities for ERCOT data ingestion.

This module provides functions to efficiently fetch large amounts of ERCOT market data
by splitting date ranges into manageable batches and handling Qualified
Scheduling Entities (QSEs) in a single API call per batch. It also implements
thread-safe, rate-limited HTTP requests with logging and error handling,
to comply with ERCOT API rate limits and ensure robust data collection.

Key features:
- fetch_in_batches: Batches API calls over date ranges and QSE lists,
    combining results.
- rate_limited_request: Thread-safe, rate-limited HTTP requests with logging
    and header masking.
- Utilities for logging, batching, and concurrency (future-proofed for
    async/multi-threaded use).

Intended for use in the ERCOT data pipeline to optimize API usage and database
ingestion.
"""

from typing import Optional
import time
from datetime import datetime, timedelta
import logging
import traceback
import threading

import requests
from ratelimit import limits, sleep_and_retry

from ercot_scraping.config.config import (
    API_RATE_LIMIT_REQUESTS,
    API_RATE_LIMIT_INTERVAL,
    LOGGER,
    DEFAULT_BATCH_DAYS,
)
from ercot_scraping.utils.logging_utils import setup_module_logging


# Configure logging
logger = logging.getLogger(__name__)
per_run_handler = setup_module_logging(__name__)


LOGGER = logging.getLogger(__name__)


# Calculate the minimum interval between requests (in seconds)
_MIN_REQUEST_INTERVAL = API_RATE_LIMIT_INTERVAL / \
    API_RATE_LIMIT_REQUESTS  # e.g. 6.0 for 10/min

# Global lock and timestamp for sync rate limiting
_sync_rate_limit_lock = threading.Lock()


def fetch_in_batches(
    fetch_func: callable,
    start_date: str,
    end_date: str,
    batch_days: int = DEFAULT_BATCH_DAYS,
    qse_filter: Optional[set[str]] = None,
    max_concurrent: int = 1,  # ignored, for compatibility
    **kwargs
) -> dict[str, any]:
    """
    Fetch data from an API in batches over a date range.
    This function divides the period between start_date and end_date
    into smaller batches using the given batch_days. It then calls the
    provided fetch_func for each batch. If a set of qse_filter is provided,
    all QSEs are queried in a single API call for each batch. Additional
    keyword arguments (kwargs) are passed to the fetch_func.
    Parameters:
        fetch_func (callable): The function to be used for fetching data from the API.
                                It should accept parameters for start date,
                                end date, qse_name, page, and any additional kwargs.
        start_date (str): The start date for fetching data in the format
                            "%Y-%m-%d".
        end_date (str): The end date for fetching data in the format "%Y-%m-%d".
        batch_days (int, optional): The maximal number of days to include
                                    in each batch. Defaults to
                                    DEFAULT_BATCH_DAYS.
        qse_filter (Optional[set[str]], optional): An optional set of QSE names.
                                                    If provided, the names are
                                                    joined into a
                                                    comma-separated string
                                                    and passed to fetch_func.
        max_concurrent (int, optional): Parameter for
            compatibility with other interfaces; currently ignored.
        **kwargs: Additional keyword arguments to pass to fetch_func.
    Returns:
        dict[str, any]: A dictionary with the following structure:
            "data" (list): Combined list of records fetched from all batches.
            "fields" (list): A list of field names (empty list if not set).
    """

    LOGGER.info(
        "[CALL] fetch_in_batches(\n"
        "    %s,\n"
        "    %s,\n"
        "    %s,\n"
        "    batch_days=%s,\n"
        "    qse_filter=%s,\n"
        "    max_concurrent=%s\n"
        ") called from:\n"
        "    %s",
        fetch_func,
        start_date,
        end_date,
        batch_days,
        qse_filter,
        max_concurrent,
        traceback.format_stack(limit=3))
    fmt = "%Y-%m-%d"
    dt_start = datetime.strptime(start_date, fmt)
    dt_end = datetime.strptime(end_date, fmt)
    total_days = (dt_end - dt_start).days + 1
    if total_days - 1 <= batch_days:
        batches = [(start_date, end_date)]
    else:
        batches = []
        # First batch covers only the start_date.
        batches.append((start_date, start_date))
        remaining_days = total_days - 1
        next_day = dt_start + timedelta(days=1)
        while remaining_days > 0:
            current_batch_days = min(batch_days, remaining_days)
            batch_start = next_day.strftime(fmt)
            batch_end = (
                next_day +
                timedelta(
                    days=current_batch_days -
                    1)).strftime(fmt)
            batches.append((batch_start, batch_end))
            next_day += timedelta(days=current_batch_days)
            remaining_days -= current_batch_days
    total_batches = len(batches)
    LOGGER.info(
        "Processing %d batches (sync)", total_batches)
    combined_data = []
    fields = None
    for batch_start, batch_end in batches:
        batch_data = fetch_func(batch_start, batch_end)
        if not isinstance(batch_data, dict) or "data" not in batch_data:
            continue
        records = batch_data["data"]
        # Filter here if qse_filter is provided
        if qse_filter:
            records = [row for row in records if row.get(
                "qseName") in qse_filter]
        combined_data.extend(records)
        # Log _meta and fields for traceability if present
        meta = batch_data.get("_meta")
        if meta:
            LOGGER.debug(
                "_meta field for batch %s-%s: %s",
                batch_start,
                batch_end,
                meta)
        fields_val = batch_data.get("fields")
        if fields_val:
            LOGGER.debug("fields field for batch %s-%s: %s",
                         batch_start, batch_end, fields_val)
    return {
        "data": combined_data,
        "fields": fields if fields is not None else []
    }


@sleep_and_retry
@limits(calls=API_RATE_LIMIT_REQUESTS, period=API_RATE_LIMIT_INTERVAL)
def rate_limited_request(*args, **kwargs) -> requests.Response:
    """
    Sends an HTTP request with enforced rate limiting.

    This function wraps the standard requests.request method to ensure that successive
    HTTP requests are made with a minimum time interval specified by _MIN_REQUEST_INTERVAL.
    It achieves this by acquiring a synchronization lock, checking the time elapsed since
    the previous request, and sleeping if necessary to maintain the rate limit.

    Details:
        - Thread Safety: Uses _sync_rate_limit_lock to ensure that concurrent requests are properly
          rate-limited.
        - Timing: Compares the current time with the timestamp of the last request and sleeps if
          the elapsed time is less than the configured minimum interval.
        - Logging: Logs the request parameters but masks sensitive headers ("Authorization" and
          "Ocp-Apim-Subscription-Key") for security.
        - Timeout Management: Calls requests.request with a fixed timeout of 30 seconds.

    Parameters:
        *args: Positional arguments passed directly to requests.request,
            typically including the HTTP method and URL.
        **kwargs: Keyword arguments passed directly to requests.request,
            such as headers, data, params, etc.

    Returns:
        requests.Response: The HTTP response returned by the requests.request call.
    """
    with _sync_rate_limit_lock:
        now = time.time()
        last_time = getattr(rate_limited_request, '_last_sync_request_time', 0)
        elapsed = now - last_time
        if elapsed < _MIN_REQUEST_INTERVAL:
            time.sleep(_MIN_REQUEST_INTERVAL - elapsed)
        rate_limited_request._last_sync_request_time = time.time()
    # Mask sensitive headers for logging
    log_kwargs = dict(kwargs)
    headers = log_kwargs.get('headers', {})
    if headers:
        headers = dict(headers)  # copy
        if 'Authorization' in headers:
            headers['Authorization'] = '***MASKED***'
        if 'Ocp-Apim-Subscription-Key' in headers:
            headers['Ocp-Apim-Subscription-Key'] = '***MASKED***'
        log_kwargs['headers'] = headers
    LOGGER.info(
        "[CALL] rate_limited_request(args=%s, kwargs=%s)", args, log_kwargs)
    response = requests.request(*args, timeout=30, **kwargs)
    # Removed extra sleep; now strictly rate-limited by _MIN_REQUEST_INTERVAL
    return response
