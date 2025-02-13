from typing import Optional

from ratelimit import limits, sleep_and_retry
import requests
import time

from ercot_scraping.config import API_RATE_LIMIT_REQUESTS, API_RATE_LIMIT_INTERVAL, LOGGER
from ercot_scraping.utils import split_date_range


def fetch_in_batches(
    fetch_func: callable,
    start_date: str,
    end_date: str,
    batch_days: int = 30,
    qse_names: Optional[set[str]] = None,
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

    LOGGER.info(f"Processing {total_batches} batches for {total_qses} QSEs")

    combined_data = []
    empty_batches = []
    failed_batches = []

    if qse_names:
        for qse_name in sorted(qse_names):
            LOGGER.info(f"Processing QSE: {qse_name}")
            for i, (batch_start, batch_end) in enumerate(batches, 1):
                LOGGER.info(
                    f"Fetching batch {i}/{total_batches} for QSE {qse_name}: {batch_start} to {batch_end}"
                )
                try:
                    batch_data = fetch_func(
                        batch_start, batch_end, qse_name=qse_name, **kwargs)

                    if not batch_data:
                        LOGGER.warning(
                            f"Batch {i}/{total_batches} returned None")
                        failed_batches.append((batch_start, batch_end))
                        continue

                    if "data" not in batch_data:
                        LOGGER.warning(
                            f"Batch {i}/{total_batches} missing 'data' key")
                        LOGGER.debug(f"Batch response: {batch_data}")
                        failed_batches.append((batch_start, batch_end))
                        continue

                    records = batch_data["data"]
                    if not records:
                        LOGGER.warning(
                            f"No data found for period {batch_start} to {batch_end}")
                        empty_batches.append((batch_start, batch_end))
                        continue

                    combined_data.extend(records)
                    LOGGER.info(
                        f"Batch {i}/{total_batches} successful - got {len(records)} records"
                    )

                except Exception as e:
                    LOGGER.error(
                        f"Error in batch {i}/{total_batches}: {str(e)}")
                    failed_batches.append((batch_start, batch_end))
                    continue

    else:
        # Original batch processing without QSE filtering
        for i, (batch_start, batch_end) in enumerate(batches, 1):
            LOGGER.info(
                f"Fetching batch {i}/{total_batches}: {batch_start} to {batch_end}")

            try:
                batch_data = fetch_func(batch_start, batch_end, **kwargs)

                if not batch_data:
                    LOGGER.warning(f"Batch {i}/{total_batches} returned None")
                    failed_batches.append((batch_start, batch_end))
                    continue

                if "data" not in batch_data:
                    LOGGER.warning(
                        f"Batch {i}/{total_batches} missing 'data' key")
                    LOGGER.debug(f"Batch response: {batch_data}")
                    failed_batches.append((batch_start, batch_end))
                    continue

                records = batch_data["data"]
                if not records:
                    LOGGER.warning(
                        f"No data found for period {batch_start} to {batch_end}")
                    empty_batches.append((batch_start, batch_end))
                    continue

                combined_data.extend(records)
                LOGGER.info(
                    f"Batch {i}/{total_batches} successful - got {len(records)} records"
                )

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

    return {"data": combined_data}


@sleep_and_retry
@limits(calls=API_RATE_LIMIT_REQUESTS, period=API_RATE_LIMIT_INTERVAL)
def rate_limited_request(*args, **kwargs) -> requests.Response:
    response = requests.request(*args, **kwargs)
    time.sleep(3)
    return response
