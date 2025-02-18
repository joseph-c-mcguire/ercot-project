from typing import Optional

from ratelimit import limits, sleep_and_retry
import requests
import time

from ercot_scraping.config import (
    API_RATE_LIMIT_REQUESTS,
    API_RATE_LIMIT_INTERVAL,
    LOGGER,
    DEFAULT_BATCH_DAYS,
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
    batches = split_date_range(start_date, end_date, batch_days)
    total_batches = len(batches)
    total_qses = len(qse_names) if qse_names else 1

    LOGGER.info(f"Processing {total_batches} batches for {total_qses} QSEs")

    combined_data = []
    fields = []
    empty_batches = []
    failed_batches = []

    if qse_names:
        for qse_name in sorted(qse_names):
            LOGGER.info(f"Processing QSE: {qse_name}")
            for i, (batch_start, batch_end) in enumerate(batches, 1):
                try:
                    # Initialize for pagination
                    current_page = 1
                    total_pages = 1
                    batch_records = []
                    
                    while current_page <= total_pages:
                        LOGGER.info(
                            f"Fetching batch {i}/{total_batches} for QSE {qse_name}, page {current_page}/{total_pages}: {batch_start} to {batch_end}"
                        )
                        
                        batch_data = fetch_func(
                            batch_start, batch_end, page=current_page, qse_name=qse_name, **kwargs)

                        if not batch_data or "data" not in batch_data:
                            LOGGER.warning(f"Invalid response for page {current_page}/{total_pages}")
                            break

                        # Update pagination info
                        if "_meta" in batch_data:
                            total_pages = batch_data["_meta"].get("totalPages", 1)
                            current_page = batch_data["_meta"].get("currentPage", 1) + 1
                            LOGGER.info(f"Found {total_pages} total pages of data for this batch")

                        records = batch_data["data"]
                        if records:
                            batch_records.extend(records)
                            LOGGER.info(f"Got {len(records)} records from page {current_page-1}")
                        
                        # Grab fields from first page if not already set
                        if not fields and "fields" in batch_data:
                            fields = batch_data["fields"]

                    # After all pages, process the batch results
                    if batch_records:
                        combined_data.extend(batch_records)
                        LOGGER.info(
                            f"Batch {i}/{total_batches} complete - got {len(batch_records)} total records across {total_pages} pages"
                        )
                    else:
                        LOGGER.warning(f"No data found for batch {i}")
                        empty_batches.append((batch_start, batch_end))

                except Exception as e:
                    LOGGER.error(f"Error in batch {i}/{total_batches}: {str(e)}")
                    failed_batches.append((batch_start, batch_end))
                    continue

    else:
        # Handle non-QSE batches
        for i, (batch_start, batch_end) in enumerate(batches, 1):
            try:
                # Initialize for pagination
                current_page = 1
                total_pages = 1
                batch_records = []
                
                while current_page <= total_pages:
                    LOGGER.info(
                        f"Fetching batch {i}/{total_batches}, page {current_page}/{total_pages}: {batch_start} to {batch_end}"
                    )
                    
                    batch_data = fetch_func(batch_start, batch_end, page=current_page, **kwargs)

                    if not batch_data or "data" not in batch_data:
                        LOGGER.warning(f"Invalid response for page {current_page}/{total_pages}")
                        break

                    # Update pagination info
                    if "_meta" in batch_data:
                        total_pages = batch_data["_meta"].get("totalPages", 1)
                        current_page = batch_data["_meta"].get("currentPage", 1) + 1
                        LOGGER.info(f"Found {total_pages} total pages of data for this batch")

                    records = batch_data["data"]
                    if records:
                        batch_records.extend(records)
                        LOGGER.info(f"Got {len(records)} records from page {current_page-1}")
                    
                    # Grab fields from first page if not already set
                    if not fields and "fields" in batch_data:
                        fields = batch_data["fields"]

                # After all pages, process the batch results
                if batch_records:
                    combined_data.extend(batch_records)
                    LOGGER.info(
                        f"Batch {i}/{total_batches} complete - got {len(batch_records)} total records across {total_pages} pages"
                    )
                else:
                    LOGGER.warning(f"No data found for batch {i}")
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

    return {"data": combined_data, "fields": fields}


@sleep_and_retry
@limits(calls=API_RATE_LIMIT_REQUESTS, period=API_RATE_LIMIT_INTERVAL)
def rate_limited_request(*args, **kwargs) -> requests.Response:
    response = requests.request(*args, **kwargs)
    time.sleep(3)
    return response
