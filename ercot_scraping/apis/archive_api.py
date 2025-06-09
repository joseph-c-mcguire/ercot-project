"""
archive_api.py

This module provides functions to download, extract, and process ERCOT archive
files (SPP and DAM), including storing the processed data into a database. It
handles batching, progress reporting, and normalization of CSV data for
ingestion.
"""

from typing import List
import zipfile
from io import BytesIO
import traceback
import logging
import io  # Move import to top
import csv  # Move import to top

from ercot_scraping.config.config import (
    ERCOT_API_REQUEST_HEADERS,
    ERCOT_ARCHIVE_API_BASE_URL,
    LOGGER)
from ercot_scraping.apis.batched_api import rate_limited_request
from ercot_scraping.utils.utils import get_table_name
from ercot_scraping.database.store_data import store_data_to_db
from ercot_scraping.utils.logging_utils import setup_module_logging

# Configure logging
logger = logging.getLogger(__name__)
per_run_handler = setup_module_logging(__name__)

try:
    # from tqdm import tqdm
    TQDM_AVAILABLE = False
except ImportError:
    TQDM_AVAILABLE = False

# NOTE: All API calls are rate-limited to 1 request every 2 seconds via
# batched_api.py's rate_limited_request and async_rate_limited_request.
# Do not bypass these wrappers for direct API calls.


def download_spp_archive_files(
    product_id: str,
    doc_ids: list[int],
    db_name: str
) -> None:
    """
    Downloads and processes SPP archive files from the ERCOT archive API.
    Args:
        product_id (str): The product ID for the SPP documents to be
            downloaded.
        doc_ids (list[int]): A list of document IDs to be downloaded.
        db_name (str): The name of the database where the processed files
            will be stored.
    Returns:
        None
    """
    LOGGER.info(
        "[CALL] download_spp_archive_files(%s, %s, %s) called from: %s",
        product_id, doc_ids, db_name, traceback.format_stack(limit=3))
    if not doc_ids:
        LOGGER.warning("No document IDs found for SPP product %s", product_id)
        return
    url = f"{ERCOT_ARCHIVE_API_BASE_URL}/{product_id}/download"
    LOGGER.info("Downloading %d SPP documents from archive API", len(doc_ids))
    batch_size = 25
    total_batches = (len(doc_ids) + batch_size - 1) // batch_size
    batch_indices = list(range(0, len(doc_ids), batch_size))
    # use_progress = TQDM_AVAILABLE and total_batches > 1
    # pbar = tqdm(
    #     total=total_batches,
    #     desc="SPP Archive Batches",
    #     disable=not use_progress
    # )
    for idx, i in enumerate(batch_indices):
        batch = doc_ids[i:i + batch_size]
        LOGGER.info(
            "[SPP Batch %d/%d] batch for product_id=%s | doc_ids=%s",
            idx+1, total_batches, product_id, batch
        )
        payload = {"docIds": batch}
        try:
            response = rate_limited_request(
                "POST", url, headers=ERCOT_API_REQUEST_HEADERS, json=payload)
            if response.status_code != 200:
                LOGGER.error(
                    "Failed to download SPP archive batch: %s",
                    response.status_code)
                continue
            content = response.content
            files_in_batch = 0
            with zipfile.ZipFile(BytesIO(content)) as zip_folder:
                for filename in zip_folder.namelist():
                    LOGGER.info(
                        "[SPP Batch %d/%d] Processing file: %s",
                        idx+1, total_batches, filename
                    )
                    process_spp_file(zip_folder, filename,
                                     get_table_name(filename), db_name)
                    files_in_batch += 1
            LOGGER.info(
                "[SPP Batch %d/%d] Completed, processed %d files.",
                idx+1, total_batches, files_in_batch
            )
        except zipfile.BadZipFile as e:
            LOGGER.error("Bad zip file in SPP batch download: %s", e)
        except (OSError, ValueError) as e:
            LOGGER.error("File error in SPP batch download: %s", e)
        except Exception as e:
            LOGGER.error("Exception in SPP batch download: %s", e)
        # if pbar:
        #     pbar.update(1)
    # if pbar:
    #     pbar.close()
    LOGGER.info(
        "Completed download_spp_archive_files for product_id=%s",
        product_id
    )


def _decode_csv_content(raw_bytes, filename):
    """
    Attempt to decode CSV bytes using UTF-8, then fall back to latin-1 and
    windows-1252. Logs a warning if a fallback is used, and an error if all
    fail.
    Returns (decoded_str, encoding) or (None, None) if all fail.
    """
    encodings = ['utf-8', 'latin-1', 'windows-1252']
    for enc in encodings:
        try:
            return raw_bytes.decode(enc), enc
        except UnicodeDecodeError:
            continue
    LOGGER.error(
        "Skipping file %s: could not decode with known encodings.",
        filename
    )
    return None, None


def process_spp_file(
        zip_folder: zipfile.ZipFile,
        filename: str,
        table_name: str,
        db_name: str) -> None:
    """
    Processes a CSV file from a zip folder, normalizes its content, and stores
    it in a database.
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
    LOGGER.info(
        "[PROCESS] Starting file: %s for table: %s", filename, table_name
    )
    with zip_folder.open(filename) as csv_file:
        raw_bytes = csv_file.read()
        csv_content, encoding = _decode_csv_content(raw_bytes, filename)
        if csv_content is None:
            LOGGER.warning(
                "Skipping file %s: could not decode with known encodings.",
                filename
            )
            return
        if encoding != 'utf-8':
            LOGGER.warning(
                "File %s decoded with fallback encoding: %s",
                filename, encoding)
        csv_buffer = io.StringIO(csv_content)
        first_line = csv_buffer.readline().strip()
        if not first_line or ',' not in first_line:
            LOGGER.warning("No headers found in file: %s", filename)
            return
        csv_buffer.seek(0)
        reader = csv.DictReader(csv_buffer)
        if not reader.fieldnames:
            LOGGER.warning("No fieldnames found in file: %s", filename)
            return
        LOGGER.info(
            "[PROCESS] Normalizing rows for file: %s", filename
        )
        rows: List[dict] = []
        for row in reader:
            rows.append(row)
        LOGGER.info(
            "[PROCESS] Normalized %d rows for file: %s",
            len(rows), filename
        )
        if rows:
            store_data_to_db({"data": rows}, db_name, table_name, None, None)
        else:
            LOGGER.warning("No data rows found in file: %s", filename)


def download_dam_archive_files(
    product_id: str,
    doc_ids: list[int],
    db_name: str,
    show_progress: bool = True
) -> None:
    LOGGER.info(
        "[CALL] download_dam_archive_files(%s, %s, %s, show_progress=%s) "
        "called from: %s",
        product_id, doc_ids, db_name, show_progress,
        traceback.format_stack(limit=3))
    """
    Downloads and processes DAM (Day-Ahead Market) archive files from the
    ERCOT API. This function retrieves batches of document IDs, downloads the
    corresponding zip files from the ERCOT archive API, and processes the
    nested zip files containing CSV files. The CSV files are then processed
    and stored in the specified database.
    """
    LOGGER.debug(
        "Entering download_dam_archive_files: product_id=%s, num_doc_ids=%d, "
        "db_name=%s, show_progress=%s",
        product_id, len(doc_ids), db_name, show_progress
    )
    LOGGER.info(
        "Starting download_dam_archive_files for product_id=%s, "
        "doc_ids=%s, db_name=%s",
        product_id, doc_ids, db_name
    )
    if not doc_ids:
        LOGGER.warning("No document IDs found for DAM product %s", product_id)
        return

    batch_size = 25
    url = f"{ERCOT_ARCHIVE_API_BASE_URL}/{product_id}/download"
    LOGGER.info("Downloading %d DAM documents from archive API", len(doc_ids))
    LOGGER.debug("Document IDs being sent: %s", doc_ids)

    total_batches = (len(doc_ids) + batch_size - 1) // batch_size
    batch_indices = list(range(0, len(doc_ids), batch_size))
    # use_progress = show_progress and TQDM_AVAILABLE and total_batches > 1
    # pbar = tqdm(
    #     total=total_batches,
    #     desc="DAM Archive Batches",
    #     disable=not use_progress
    # )
    for idx, i in enumerate(batch_indices):
        LOGGER.info(
            "[DAM Batch %d/%d] Downloading batch for product_id=%s | "
            "doc_ids=%s",
            idx+1, total_batches, product_id, doc_ids[i:i+batch_size]
        )
        batch = doc_ids[i:i + batch_size]
        payload = {"docIds": batch}
        try:
            response = rate_limited_request(
                "POST", url, headers=ERCOT_API_REQUEST_HEADERS, json=payload)
            if response.status_code != 200:
                LOGGER.error(
                    "Failed to download DAM archive batch: %s",
                    response.status_code
                )
                continue
            content = response.content
            files_in_batch = 0
            LOGGER.debug("Opening outer zip for batch %d", idx+1)
            with zipfile.ZipFile(BytesIO(content)) as zip_folder:
                for filename in zip_folder.namelist():
                    LOGGER.info(
                        "[DAM Batch %d/%d] Processing file: %s",
                        idx+1, total_batches, filename
                    )
                    process_dam_file(
                        zip_folder, filename,
                        get_table_name(filename), db_name
                    )
                    files_in_batch += 1
            LOGGER.info(
                "[DAM Batch %d/%d] Completed, processed %d files.",
                idx+1, total_batches, files_in_batch
            )
        except zipfile.BadZipFile as e:
            LOGGER.error("Bad zip file in DAM batch download: %s", e)
        except (OSError, ValueError) as e:
            LOGGER.error("File error in DAM batch download: %s", e)
        except Exception as e:
            LOGGER.error("Exception in DAM batch download: %s", e)
        # if pbar:
        #     pbar.update(1)
    # if pbar:
    #     pbar.close()
    LOGGER.info(
        "Completed download_dam_archive_files for product_id=%s",
        product_id
    )


def get_archive_document_ids(
    product_id: str,
    start_date: str,
    end_date: str,
) -> list[int]:
    LOGGER.info(
        "[CALL] get_archive_document_ids(%s, %s, %s) called from: %s",
        product_id, start_date, end_date, traceback.format_stack(limit=3))
    """
    Retrieves a list of document IDs for a given product from the ERCOT archive
    API. The function queries the API with specified start and end dates,
    handling pagination to retrieve all available IDs.
    Args:
        product_id (str): The ID of the product for which to retrieve document
            IDs.
        start_date (str): The start date for the document search (YYYY-MM-DD
            format).
        end_date (str): The end date for the document search (YYYY-MM-DD
            format).
    Returns:
        List[int]: A list of document IDs.
    Raises:
        None
    Logs:
        None
    """
    LOGGER.debug(
        "Entering get_archive_document_ids: product_id=%s, start_date=%s, "
        "end_date=%s",
        product_id, start_date, end_date
    )
    url = f"{ERCOT_ARCHIVE_API_BASE_URL}/{product_id}"
    params = {
        "postDatetimeFrom": f"{start_date}T00:00:00.000",
        "postDatetimeTo": f"{end_date}T23:59:59.999",
    }

    doc_ids = []
    page = 1
    while True:
        LOGGER.debug("Requesting archive doc page %d", page)
        params["page"] = page
        response = rate_limited_request(
            "GET", url, headers=ERCOT_API_REQUEST_HEADERS, params=params)
        LOGGER.debug("Received response for page %d", page)
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
            LOGGER.debug("No archives found on page %d", page)
            break

        doc_ids.extend(archive["docId"] for archive in data["archives"])

        if page >= data["_meta"]["totalPages"]:
            break

        page += 1
    LOGGER.debug(
        "Returning %d doc_ids from get_archive_document_ids", len(doc_ids))
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
        - A warning if a fallback encoding is used.
        - An error if the file cannot be decoded.
    """
    LOGGER.info(
        "[PROCESS] Starting file: %s for table: %s", filename, table_name
    )
    with zip_folder.open(filename) as csv_file:
        raw_bytes = csv_file.read()
        csv_content, encoding = _decode_csv_content(raw_bytes, filename)
        if csv_content is None:
            LOGGER.warning(
                "Skipping file %s: could not decode with known encodings.",
                filename
            )
            return
        if encoding != 'utf-8':
            LOGGER.warning(
                "File %s decoded with fallback encoding: %s",
                filename, encoding)
        csv_buffer = io.StringIO(csv_content)
        first_line = csv_buffer.readline().strip()
        if not first_line or ',' not in first_line:
            LOGGER.warning("No headers found in file: %s", filename)
            return
        csv_buffer.seek(0)
        reader = csv.DictReader(csv_buffer)
        if not reader.fieldnames:
            LOGGER.warning("No fieldnames found in file: %s", filename)
            return
        LOGGER.info(
            "[PROCESS] Normalizing rows for file: %s", filename
        )
        rows: List[dict] = []
        for row in reader:
            rows.append(row)
        LOGGER.info(
            "[PROCESS] Normalized %d rows for file: %s",
            len(rows), filename
        )
        if rows:
            store_data_to_db({"data": rows}, db_name, table_name, None, None)
        else:
            LOGGER.warning("No data rows found in file: %s", filename)
