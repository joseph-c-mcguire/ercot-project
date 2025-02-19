from typing import Optional
import requests
import time
from datetime import datetime, timedelta

from ratelimit import limits, sleep_and_retry

from ercot_scraping.config import (
    API_RATE_LIMIT_REQUESTS,
    API_RATE_LIMIT_INTERVAL,
    LOGGER,
    DEFAULT_BATCH_DAYS,
    MAX_DATE_RANGE,
    DISABLE_RATE_LIMIT_SLEEP  # import new flag
)
from ercot_scraping.utils import split_date_range


def fetch_in_batches(
    fetch_func: callable,
    start_date: str,
    end_date: str,
    batch_days: int = DEFAULT_BATCH_DAYS,
    qse_names: Optional[set[str]] = None,
    **kwargs
) -> dict[str, any]:
    """
    Execute a fetch function in batches over a date range.

    Args:
        fetch_func (callable): Function to fetch data
        start_date (str): Overall start date
        end_date (str): Overall end date
        batch_days (int): Days per batch, defaults to 1 to handle API limits
        qse_names (Optional[set[str]]): Set of QSE names to filter by
        **kwargs: Additional arguments to pass to fetch_func

    Returns:
        dict[str, any]: Combined results from all batches
    """
    # Validate batch_days
    batch_days = min(batch_days, MAX_DATE_RANGE)

    # --- Begin custom date-splitting logic ---
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
                next_day + timedelta(days=current_batch_days - 1)).strftime(fmt)
            batches.append((batch_start, batch_end))
            next_day += timedelta(days=current_batch_days)
            remaining_days -= current_batch_days
    # --- End custom date-splitting logic ---

    total_batches = len(batches)
    total_qses = len(qse_names) if qse_names else 1

    LOGGER.info(f"Processing {total_batches} batches for {total_qses} QSEs")

    combined_data = []
    fields = None  # Initialize as None to detect if fields have been set
    empty_batches = []
    failed_batches = []

    if qse_names:
        for qse_name in sorted(qse_names):
            LOGGER.info(f"Processing QSE: {qse_name}")
            for i, (batch_start, batch_end) in enumerate(batches, 1):
                try:
                    batch_data = fetch_func(
                        batch_start, batch_end,
                        page=1,  # Start with first page
                        qse_name=qse_name,
                        **kwargs
                    )

                    # Handle fields first, before any data processing
                    if fields is None and batch_data and "fields" in batch_data:
                        fields = batch_data["fields"]

                    # Process data if available
                    if batch_data and "data" in batch_data:
                        combined_data.extend(batch_data["data"])
                    else:
                        empty_batches.append((batch_start, batch_end))

                except Exception as e:
                    LOGGER.error(
                        f"Error in batch {i}/{total_batches}: {str(e)}")
                    failed_batches.append((batch_start, batch_end))
                    continue

    else:
        # Handle non-QSE batches
        for i, (batch_start, batch_end) in enumerate(batches, 1):
            try:
                batch_data = fetch_func(
                    batch_start, batch_end,
                    page=1,  # Start with first page
                    **kwargs
                )

                # Handle fields first, before any data processing
                if fields is None and batch_data and "fields" in batch_data:
                    fields = batch_data["fields"]

                # Process data if available
                if batch_data and "data" in batch_data:
                    combined_data.extend(batch_data["data"])
                else:
                    empty_batches.append((batch_start, batch_end))

            except Exception as e:
                LOGGER.error(f"Error in batch {i}/{total_batches}: {str(e)}")
                failed_batches.append((batch_start, batch_end))
                continue

    # Enhanced summary logging
    if empty_batches:
        LOGGER.warning(f"Empty batches: {empty_batches}")
    if failed_batches:
        LOGGER.error(f"Failed batches: {failed_batches}")

    total_records = len(combined_data)
    successful_batches = total_batches - len(failed_batches)

    LOGGER.info(f"Total records retrieved: {total_records}")
    LOGGER.info(
        f"Average records per successful batch: {total_records / successful_batches if successful_batches > 0 else 0:.2f}"
    )

    return {
        "data": combined_data,
        # Return empty list if no fields found
        "fields": fields if fields is not None else []
    }


@sleep_and_retry
@limits(calls=API_RATE_LIMIT_REQUESTS, period=API_RATE_LIMIT_INTERVAL)
def rate_limited_request(*args, **kwargs) -> requests.Response:
    response = requests.request(*args, **kwargs)
    # Conditionally sleep based on configuration flag
    if not DISABLE_RATE_LIMIT_SLEEP:
        time.sleep(3)
    return response
