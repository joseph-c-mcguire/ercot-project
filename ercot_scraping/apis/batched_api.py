from typing import Callable, Any
from typing import Optional
import requests
import time
from datetime import datetime, timedelta
import logging
import traceback
import threading

from ratelimit import limits, sleep_and_retry

from ercot_scraping.config.config import (
    API_RATE_LIMIT_REQUESTS,
    API_RATE_LIMIT_INTERVAL,
    LOGGER,
    DEFAULT_BATCH_DAYS,
    MAX_DATE_RANGE,
    DISABLE_RATE_LIMIT_SLEEP  # import new flag
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
_last_sync_request_time = 0


def fetch_in_batches(
    fetch_func: callable,
    start_date: str,
    end_date: str,
    batch_days: int = DEFAULT_BATCH_DAYS,
    qse_names: Optional[set[str]] = None,
    max_concurrent: int = 1,  # ignored, for compatibility
    **kwargs
) -> dict[str, any]:
    LOGGER.info(
        f"[CALL] fetch_in_batches({fetch_func}, {start_date}, {end_date}, batch_days={batch_days}, qse_names={qse_names}, max_concurrent={max_concurrent}) called from: {traceback.format_stack(limit=3)}")
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
    total_qses = len(qse_names) if qse_names else 1
    LOGGER.info(
        f"Processing {total_batches} batches for {total_qses} QSEs (sync)")
    combined_data = []
    fields = None

    def fetch_one_batch(batch_start, batch_end, qse_name=None):
        result = fetch_func(
            batch_start,
            batch_end,
            qse_name=qse_name,
            page=1,
            **kwargs)
        if not isinstance(result, dict):
            return []
        first_page_data = result
        if not first_page_data or "data" not in first_page_data:
            return []
        batch_records = list(first_page_data["data"])
        return batch_records

    if qse_names:
        for qse_name in sorted(qse_names):
            for batch_start, batch_end in batches:
                combined_data.extend(
                    fetch_one_batch(
                        batch_start,
                        batch_end,
                        qse_name=qse_name))
    else:
        for batch_start, batch_end in batches:
            combined_data.extend(fetch_one_batch(batch_start, batch_end))
    return {
        "data": combined_data,
        "fields": fields if fields is not None else []
    }


@sleep_and_retry
@limits(calls=API_RATE_LIMIT_REQUESTS, period=API_RATE_LIMIT_INTERVAL)
def rate_limited_request(*args, **kwargs) -> requests.Response:
    global _last_sync_request_time
    with _sync_rate_limit_lock:
        now = time.time()
        elapsed = now - _last_sync_request_time
        if elapsed < _MIN_REQUEST_INTERVAL:
            time.sleep(_MIN_REQUEST_INTERVAL - elapsed)
        _last_sync_request_time = time.time()
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
        f"[CALL] rate_limited_request(args={args}, kwargs={log_kwargs})")
    response = requests.request(*args, **kwargs)
    # Removed extra sleep; now strictly rate-limited by _MIN_REQUEST_INTERVAL
    return response
