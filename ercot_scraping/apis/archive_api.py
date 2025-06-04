"""
archive_api.py

This module provides functions to download, extract, and process ERCOT archive files (SPP and DAM),
including storing the processed data into a database. It handles batching, progress reporting, and
normalization of CSV data for ingestion.
"""

from typing import List
import csv
import io
import zipfile
from io import BytesIO
import requests
import traceback
import logging

from ercot_scraping.config.config import (
    ERCOT_API_REQUEST_HEADERS,
    ERCOT_ARCHIVE_API_BASE_URL,
    API_MAX_ARCHIVE_FILES,
    LOGGER,
    DAM_FILENAMES,
    DAM_TABLE_DATA_MAPPING,
    COLUMN_MAPPINGS)
from ercot_scraping.apis.batched_api import rate_limited_request
from ercot_scraping.utils.utils import get_table_name
from ercot_scraping.database.store_data import store_data_to_db
from ercot_scraping.utils.logging_utils import setup_module_logging

# Configure logging
logger = logging.getLogger(__name__)
per_run_handler = setup_module_logging(__name__)

try:
    from tqdm import tqdm
    TQDM_AVAILABLE = True
except ImportError:
    TQDM_AVAILABLE = False

# NOTE: All API calls are rate-limited to 1 request every 2 seconds via batched_api.py's rate_limited_request and async_rate_limited_request.
# Do not bypass these wrappers for direct API calls.


def download_spp_archive_files(
    product_id: str,
    doc_ids: list[int],
    db_name: str
) -> None:
    """
    Downloads and processes SPP archive files from the ERCOT archive API.
    Args:
        product_id (str): The product ID for the SPP documents to be downloaded.
        doc_ids (list[int]): A list of document IDs to be downloaded.
        db_name (str): The name of the database where the processed files will be stored.
    Returns:
        None
    """
    LOGGER.info(
        f"[CALL] download_spp_archive_files({product_id}, {doc_ids}, {db_name}) called from: {traceback.format_stack(limit=3)}")

    if not doc_ids:
        LOGGER.warning("No document IDs found for SPP product %s", product_id)
        return

    url = f"{ERCOT_ARCHIVE_API_BASE_URL}/{product_id}/download"
    LOGGER.info("Downloading %d SPP documents from archive API", len(doc_ids))

    for i in range(0, len(doc_ids), API_MAX_ARCHIVE_FILES):
        batch = doc_ids[i:i + API_MAX_ARCHIVE_FILES]
        payload = {"docIds": batch}
        try:
            LOGGER.info(
                "Posting to SPP archive API: url=%s, payload=%s",
                url,
                payload)
            response = requests.post(
                url, headers=ERCOT_API_REQUEST_HEADERS, json=payload)
            LOGGER.info(
                "Received response with status: %s",
                response.status_code)
            if response.status_code != 200:
                LOGGER.error(
                    "Failed to download SPP batch. Status: %s",
                    response.status_code)
                continue
            content = response.content
            LOGGER.info("Read %d bytes from response", len(content))
            with zipfile.ZipFile(BytesIO(content)) as zip_folder:
                for filename in zip_folder.namelist():
                    LOGGER.info("Extracting nested zip: %s", filename)
                    with zip_folder.open(filename) as nested_zip_file:
                        nested_content = nested_zip_file.read()
                        LOGGER.info(
                            "Read %d bytes from nested zip %s",
                            len(nested_content),
                            filename)
                        with zipfile.ZipFile(BytesIO(nested_content)) as nested_zip:
                            for nested_filename in nested_zip.namelist():
                                LOGGER.info(
                                    "Found file in nested zip: %s", nested_filename)
                                if not nested_filename.endswith('.csv'):
                                    LOGGER.info(
                                        "Skipping non-CSV file: %s", nested_filename)
                                    continue
                                LOGGER.info(
                                    "Processing SPP file: %s", nested_filename)
                                try:
                                    process_spp_file(
                                        nested_zip, nested_filename, "SETTLEMENT_POINT_PRICES", db_name)
                                except Exception as file_exc:
                                    LOGGER.error(
                                        "Exception processing file %s: %s\n%s",
                                        nested_filename,
                                        file_exc,
                                        traceback.format_exc())
        except Exception as e:
            LOGGER.error(
                "Exception in SPP batch download: %s\n%s",
                e,
                traceback.format_exc())


def process_spp_file(
        zip_folder: zipfile.ZipFile,
        filename: str,
        table_name: str,
        db_name: str) -> None:
    """
    Processes a CSV file from a zip folder, normalizes its content, and stores it in a database.
    Args:
        zip_folder (zipfile.ZipFile): The zip folder containing the CSV file.
        filename (str): The name of the CSV file to process.
        table_name (str): The name of the database table to store the data.
        db_name (str): The name of the database.
    Returns:
        None
    Raises:
        None
    Logs:
        - A warning if no headers are found in the CSV file.
        - An info message indicating the number of rows stored in the database.
        - A warning if no data mapping is found for the specified table.
    """
    with zip_folder.open(filename) as csv_file:
        csv_content = csv_file.read().decode('utf-8')
        csv_buffer = io.StringIO(csv_content)
        # Check if the file is empty or has no valid headers
        first_line = csv_buffer.readline().strip()
        if not first_line or ',' not in first_line:
            LOGGER.warning("No headers found in %s", filename)
            return

        # Reset buffer position and create reader
        csv_buffer.seek(0)
        reader = csv.DictReader(csv_buffer)

        if not reader.fieldnames:
            LOGGER.warning("No headers found in %s", filename)
            return

        mapping = COLUMN_MAPPINGS.get(table_name.lower(), {})
        rows: List[dict] = []

        for row in reader:
            normalized_row: dict = {}
            for key, value in row.items():
                if not key:
                    continue
                norm_key = key.lower().strip().replace(' ', '_')
                final_key = mapping.get(norm_key, norm_key)
                normalized_row[final_key] = value.strip() if value else None
            rows.append(normalized_row)

        if rows:
            LOGGER.info("Storing %d rows to %s", len(rows), table_name)
            if table_name in DAM_TABLE_DATA_MAPPING:
                model_class = DAM_TABLE_DATA_MAPPING[table_name]["model_class"]
                insert_query = DAM_TABLE_DATA_MAPPING[table_name]["insert_query"]
                store_data_to_db(
                    data={
                        "data": rows},
                    db_name=db_name,
                    table_name=table_name,
                    model_class=model_class,
                    insert_query=insert_query)
            else:
                LOGGER.warning(
                    "No data mapping found for table: %s", table_name)


def download_dam_archive_files(
    product_id: str,
    doc_ids: list[int],
    db_name: str,
    show_progress: bool = True
) -> None:
    LOGGER.info(
        f"[CALL] download_dam_archive_files({product_id}, {doc_ids}, {db_name}, show_progress={show_progress}) called from: {traceback.format_stack(limit=3)}")
    """
    Downloads and processes DAM (Day-Ahead Market) archive files from the ERCOT API.
    This function retrieves batches of document IDs, downloads the corresponding
    zip files from the ERCOT archive API, and processes the nested zip files
    containing CSV files. The CSV files are then processed and stored in the
    specified database.
    """
    if not doc_ids:
        LOGGER.warning("No document IDs found for DAM product %s", product_id)
        return

    batch_size = 25
    url = f"{ERCOT_ARCHIVE_API_BASE_URL}/{product_id}/download"
    LOGGER.info("Downloading %d DAM documents from archive API", len(doc_ids))
    LOGGER.debug("Document IDs being sent: %s", doc_ids)

    total_batches = (len(doc_ids) + batch_size - 1) // batch_size
    batch_indices = list(range(0, len(doc_ids), batch_size))
    use_progress = show_progress and TQDM_AVAILABLE and total_batches > 1

    for idx, i in enumerate(batch_indices):
        batch = doc_ids[i:i + batch_size]
        payload = {"docIds": batch}
        try:
            response = requests.post(
                url, headers=ERCOT_API_REQUEST_HEADERS, json=payload)
            if response.status_code != 200:
                err_msg = (
                    f"Failed to download DAM batch {idx+1}/{total_batches}. "
                    f"Status: {response.status_code}.")
                if use_progress:
                    tqdm.write(err_msg)
                else:
                    LOGGER.error(err_msg)
                continue
            content = response.content
            files_in_batch = 0
            with zipfile.ZipFile(BytesIO(content)) as zip_folder:
                for filename in zip_folder.namelist():
                    file_msg = (
                        f"Batch {idx+1}/{total_batches}: Extracting nested zip "
                        f"{filename}")
                    if use_progress:
                        tqdm.write(file_msg)
                    else:
                        LOGGER.info(file_msg)
                    with zip_folder.open(filename) as nested_zip_file:
                        nested_content = nested_zip_file.read()
                        with zipfile.ZipFile(BytesIO(nested_content)) as nested_zip:
                            for nested_filename in nested_zip.namelist():
                                table_name = get_table_name(nested_filename)
                                if not any(
                                    nested_filename.startswith(prefix)
                                    for prefix in DAM_FILENAMES
                                ):
                                    continue
                                if not table_name:
                                    warn_msg = (
                                        f"Unrecognized DAM file type: "
                                        f"{nested_filename}"
                                    )
                                    if use_progress:
                                        tqdm.write(warn_msg)
                                    else:
                                        LOGGER.warning(warn_msg)
                                    continue
                                if not nested_filename.endswith('.csv'):
                                    continue
                                proc_msg = (
                                    f"Batch {idx+1}/{total_batches}: Processing DAM file: "
                                    f"{nested_filename}")
                                if use_progress:
                                    tqdm.write(proc_msg)
                                else:
                                    LOGGER.info(proc_msg)
                                process_dam_file(
                                    nested_zip, nested_filename, table_name, db_name)
                                files_in_batch += 1
            done_msg = (
                f"Completed batch {idx+1}/{total_batches}, processed {files_in_batch} files."
            )
            if use_progress:
                tqdm.write(done_msg)
            else:
                LOGGER.info(done_msg)
        except Exception as e:
            LOGGER.error(f"Exception in DAM batch download: {e}")


def get_archive_document_ids(
    product_id: str,
    start_date: str,
    end_date: str,
) -> list[int]:
    LOGGER.info(
        f"[CALL] get_archive_document_ids({product_id}, {start_date}, {end_date}) called from: {traceback.format_stack(limit=3)}")
    """
    Retrieves a list of document IDs for a given product from the ERCOT archive API.
    The function queries the API with specified start and end dates, handling
    pagination to retrieve all available IDs.
    Args:
        product_id (str): The ID of the product for which to retrieve document IDs.
        start_date (str): The start date for the document search (YYYY-MM-DD format).
        end_date (str): The end date for the document search (YYYY-MM-DD format).
    Returns:
        List[int]: A list of document IDs.
    Raises:
        None
    Logs:
        None
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
        # Log _meta and fields for traceability
        meta = data.get("_meta")
        if meta:
            LOGGER.debug("_meta field for archive doc page %d: %s", page, meta)
        fields = data.get("fields")
        if fields:
            LOGGER.debug(
                "fields field for archive doc page %d: %s",
                page,
                fields)

        if not data.get("archives"):
            break

        doc_ids.extend(archive["docId"] for archive in data["archives"])

        if page >= data["_meta"]["totalPages"]:
            break

        page += 1

    return doc_ids


def process_dam_file(
    zip_folder: zipfile.ZipFile,
    filename: str,
    table_name: str,
    db_name: str
) -> None:
    """
    Processes a single DAM file from a zip folder,
    normalizes its content, and stores it in a database.
    Args:
        zip_folder (zipfile.ZipFile): The zip folder containing the DAM file.
        filename (str): The name of the DAM file to process.
        table_name (str): The name of the database table to store the data.
        db_name (str): The name of the database.
    Returns:
        None
    Raises:
        None
    Logs:
        - A warning if no headers are found in the CSV file.
        - An info message indicating the number of rows stored in the database.
        - A warning if no data mapping is found for the specified table.
    """
    with zip_folder.open(filename) as csv_file:
        csv_content = csv_file.read().decode('utf-8')
        csv_buffer = io.StringIO(csv_content)

        # Read and check the first line
        first_line = csv_buffer.readline().strip()
        if not first_line or ',' not in first_line:
            LOGGER.warning("No headers found in %s", filename)
            return

        # Reset and create reader
        csv_buffer.seek(0)
        reader = csv.DictReader(csv_buffer)

        if not reader.fieldnames:
            LOGGER.warning("No headers found in %s", filename)
            return

        # Read all data rows
        rows = list(reader)
        # If no data rows and header does not start with 'col', then warn.
        if not rows and not first_line.lower().startswith("col"):
            LOGGER.warning("No headers found in %s", filename)
            return

        mapping = COLUMN_MAPPINGS.get(table_name.lower(), {})
        normalized_rows = []
        for row in rows:
            normalized_row = {}
            for key, value in row.items():
                if not key:
                    continue
                norm_key = key.lower().strip().replace(' ', '_')
                final_key = mapping.get(norm_key, norm_key)
                normalized_row[final_key] = value.strip() if value else None
            normalized_rows.append(normalized_row)

        if normalized_rows:
            LOGGER.info(
                "Storing %d rows to %s",
                len(normalized_rows),
                table_name)
            if table_name in DAM_TABLE_DATA_MAPPING:
                model_class = DAM_TABLE_DATA_MAPPING[table_name]["model_class"]
                insert_query = DAM_TABLE_DATA_MAPPING[table_name]["insert_query"]
                store_data_to_db(
                    data={"data": normalized_rows},
                    db_name=db_name,
                    table_name=table_name,
                    model_class=model_class,
                    insert_query=insert_query)
            else:
                LOGGER.warning(
                    "No data mapping found for table: %s", table_name)
