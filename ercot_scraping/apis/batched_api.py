from typing import Optional
import requests
import time
from datetime import datetime, timedelta

from ratelimit import limits, sleep_and_retry

from ercot_scraping.config.config import (
    API_RATE_LIMIT_REQUESTS,
    API_RATE_LIMIT_INTERVAL,
    LOGGER,
    DEFAULT_BATCH_DAYS,
    MAX_DATE_RANGE,
    DISABLE_RATE_LIMIT_SLEEP  # import new flag
)

from ercot_scraping.database.store_data import store_data_to_db


def fetch_in_batches(
    fetch_func: callable,
    start_date: str,
    end_date: str,
    batch_days: int = DEFAULT_BATCH_DAYS,
    qse_names: Optional[set[str]] = None,
    db_name: Optional[str] = None,
    table_name: Optional[str] = None,
    model_class: Optional[object] = None,
    insert_query: Optional[str] = None,
    **kwargs
) -> dict[str, any]:
    """
    Execute a fetch function in batches over a date range.
    For each API pull, data is immediately stored to the DB.

    Args:
        fetch_func (callable): Function to fetch data
        start_date (str): Overall start date
        end_date (str): Overall end date
        batch_days (int): Days per batch, defaults to 1 to handle API limits
        qse_names (Optional[set[str]]): Set of QSE names to filter by
        db_name (Optional[str]): Database name
        table_name (Optional[str]): Table name
        model_class (Optional[object]): Model class
        insert_query (Optional[str]): Insert query
        **kwargs: Additional arguments to pass to fetch_func

    Returns:
        dict[str, any]: Combined results from all batches
    """
    LOGGER.debug(
        f"Starting fetch_in_batches with start_date={start_date} and end_date={end_date}")
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
        batches = [(start_date, start_date)]
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
    fields = None  # Initialize fields as None
    empty_batches = []
    failed_batches = []

    if qse_names:
        for qse_name in sorted(qse_names):
            LOGGER.info(f"Processing QSE: {qse_name}")
            for i, (batch_start, batch_end) in enumerate(batches, 1):
                LOGGER.debug(
                    f"Creating batch {i}/{total_batches} from {batch_start} to {batch_end}")
                # If desired, compare batch_start/batch_end with earliest DB date:
                # if batch_end < earliest_db_date:
                #     LOGGER.debug(f"Skipping batch {batch_start}-{batch_end}: before earliest DB date {earliest_db_date}")
                #     continue
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
                            batch_start, batch_end,
                            page=current_page,  # Pass page parameter
                            qse_name=qse_name,
                            **kwargs
                        )

                        # Update fields if they're in the response and not set yet
                        if fields is None and "fields" in batch_data:
                            fields = batch_data["fields"]

                        if not batch_data:
                            break

                        if "data" in batch_data and batch_data["data"]:
                            batch_records.extend(batch_data["data"])
                            LOGGER.info(
                                f"Got {len(batch_data['data'])} records from page {current_page}"
                            )
                            if db_name and table_name:
                                store_data_to_db(
                                    data={"data": batch_data["data"]},
                                    db_name=db_name,
                                    table_name=table_name,
                                    model_class=model_class,
                                    insert_query=insert_query
                                )

                        # Update pagination info
                        if "_meta" in batch_data:
                            total_pages = batch_data["_meta"].get(
                                "totalPages", 1)
                            current_page = batch_data["_meta"].get(
                                "currentPage", 1) + 1
                        else:
                            # If no _meta info, increment page and assume we're done after first page
                            total_pages = 1
                            current_page += 1

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
                    LOGGER.error(
                        f"Error in batch {i}/{total_batches}: {str(e)}")
                    failed_batches.append((batch_start, batch_end))
                    continue

    else:
        # Handle non-QSE batches
        for i, (batch_start, batch_end) in enumerate(batches, 1):
            LOGGER.debug(
                f"Creating batch {i}/{total_batches} from {batch_start} to {batch_end}")
            # If desired, compare batch_start/batch_end with earliest DB date:
            # if batch_end < earliest_db_date:
            #     LOGGER.debug(f"Skipping batch {batch_start}-{batch_end}: before earliest DB date {earliest_db_date}")
            #     continue
            try:
                # Initialize for pagination
                current_page = 1
                total_pages = 1
                batch_records = []

                while current_page <= total_pages:
                    LOGGER.info(
                        f"Fetching batch {i}/{total_batches}, page {current_page}/{total_pages}: {batch_start} to {batch_end}"
                    )

                    batch_data = fetch_func(
                        batch_start, batch_end,
                        page=current_page,  # Pass page parameter
                        **kwargs
                    )

                    # Update fields if they're in the response and not set yet
                    if fields is None and "fields" in batch_data:
                        fields = batch_data["fields"]

                    if not batch_data:
                        break

                    if "data" in batch_data and batch_data["data"]:
                        batch_records.extend(batch_data["data"])
                        LOGGER.info(
                            f"Got {len(batch_data['data'])} records from page {current_page}"
                        )
                        if db_name and table_name:
                            store_data_to_db(
                                data={"data": batch_data["data"]},
                                db_name=db_name,
                                table_name=table_name,
                                model_class=model_class,
                                insert_query=insert_query
                            )

                    # Update pagination info
                    if "_meta" in batch_data:
                        total_pages = batch_data["_meta"].get("totalPages", 1)
                        current_page = batch_data["_meta"].get(
                            "currentPage", 1) + 1
                        LOGGER.info(
                            f"Found {total_pages} total pages of data for this batch")

                    if records := batch_data["data"]:
                        batch_records.extend(records)
                        LOGGER.info(
                            f"Got {len(records)} records from page {current_page-1}")

                    # Update fields only if not set yet
                    if fields is None and "fields" in batch_data:
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

    return {
        "data": combined_data,
        # Return fields if set, empty list otherwise
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
