"""
archive_api.py

This module provides functions to download, extract, and process ERCOT archive
files (SPP and DAM), including storing the processed data into a database. It
handles batching, progress reporting, and normalization of CSV data for
ingestion.
"""

from io import BytesIO
import csv
import io
import zipfile
import traceback
import logging

from ercot_scraping.config.config import (
    ERCOT_API_REQUEST_HEADERS,
    ERCOT_ARCHIVE_API_BASE_URL,
    DAM_FILENAMES,
    DAM_TABLE_DATA_MAPPING,
    FILE_LIMITS,
)
from ercot_scraping.apis.batched_api import rate_limited_request
from ercot_scraping.utils.utils import get_table_name
from ercot_scraping.database.store_data import store_data_to_db
from ercot_scraping.utils.logging_utils import setup_module_logging
from ercot_scraping.config.column_mappings import COLUMN_MAPPINGS

# Configure logging
logger = logging.getLogger(__name__)
per_run_handler = setup_module_logging(__name__)

try:
    TQDM_AVAILABLE = False
except ImportError:
    TQDM_AVAILABLE = False


def process_spp_file_to_rows(
    zip_folder: zipfile.ZipFile,
    filename: str,
    table_name: str
) -> list[dict]:
    """
    Processes a CSV file from a zip folder, normalizes its content,
    and returns rows as a list of dicts.
    """

    rows = []
    # Always try to get mapping, fallback to empty dict if not found
    try:
        mapping = COLUMN_MAPPINGS.get(table_name.lower(), {})
    except Exception:
        mapping = {}

    try:
        with zip_folder.open(filename) as csv_file:
            print(f"[TRACE] Opened file {filename} from zip_folder")
            try:
                csv_content = csv_file.read().decode('utf-8')
            except UnicodeDecodeError as e:
                print(f"[ERROR] Could not decode {filename} as UTF-8: {e}")
                return []
            csv_buffer = io.StringIO(csv_content)
            first_line = csv_buffer.readline().strip()
            print(f"[TRACE] First line of {filename}: {first_line}")
            if not first_line or ',' not in first_line:
                print(f"[WARN] No headers found in {filename}")
                return []
            csv_buffer.seek(0)
            reader = csv.DictReader(csv_buffer)
            print(
                f"[TRACE] CSV fieldnames for {filename}: {reader.fieldnames}")
            if not reader.fieldnames:
                print(f"[WARN] No headers found in {filename}")
                return []
            # Normalize headers
            normalized_headers = []
            for h in reader.fieldnames:
                norm = mapping.get(h.strip().lower())
                if norm:
                    normalized_headers.append(norm)
                else:
                    normalized_headers.append(
                        h.strip().lower().replace(" ", "_"))
            for row_num, row in enumerate(reader):
                norm_row = {}
                for idx, key in enumerate(normalized_headers):
                    orig_key = reader.fieldnames[idx]
                    val = row.get(orig_key)
                    # Split long line for PEP8
                    if isinstance(val, str) and val.strip() != "":
                        norm_row[key] = val.strip()
                    else:
                        norm_row[key] = None
                if norm_row:
                    rows.append(norm_row)
                if row_num < 3:
                    print(f"[TRACE] Row {row_num} in {filename}: {norm_row}")
        print(f"[TRACE] Returning {len(rows)} rows from {filename}")
        return rows
    except Exception as e:
        print(f"[ERROR] Exception processing {filename}: {e}")
        return []


def download_spp_archive_files(
    product_id: str,
    doc_ids: list[int],
    db_name: str,
    batch_size: int = FILE_LIMITS["SPP"]
) -> None:
    """
    Downloads and processes SPP (Settlement Point Price) archive files from
    the ERCOT archive API in batches.

    For each batch of document ID's, this function:
    - Sends a POST request to the ERCOT archive API to download a ZIP archive
    containing nested ZIP files.
    - Extracts each nested ZIP file and processes CSV files named
    'SETTLEMENT_POINT_PRICES'.
    - Converts the CSV data into rows and stores them in the specified
    database table using the provided data mapping.

    Args:
        product_id (str): The ERCOT product ID for which to download archive
        files.
        doc_ids (list[int]): List of document IDs to download.
        db_name (str): Name of the database where data will be stored.
        batch_size (int, optional): Number of document IDs to process per
        batch. Defaults to FILE_LIMITS["SPP"].

    Returns:
        None

    Notes:
        - Skips non-CSV files found in the nested ZIP archives.
        - Handles and logs errors related to HTTP requests, ZIP extraction,
        and data processing.
        - Assumes existence of supporting functions and constants such as
        `rate_limited_request`, `process_spp_file_to_rows`, `store_data_to_db`,
        `DAM_TABLE_DATA_MAPPING`,
        `ERCOT_ARCHIVE_API_BASE_URL`, and `ERCOT_API_REQUEST_HEADERS`.
    """

    print(
        "[CALL] download_spp_archive_files(" +
        f"{product_id}, {doc_ids}, {db_name}) called"
    )
    print(f"[TRACE] batch_size set to {batch_size}")
    if not doc_ids:
        print(f"[WARN] No document IDs found for SPP product {product_id}")
        return
    url = f"{ERCOT_ARCHIVE_API_BASE_URL}/{product_id}/download"
    print(f"Downloading {len(doc_ids)} SPP documents from archive API")
    for i in range(0, len(doc_ids), batch_size):
        batch = doc_ids[i:i + batch_size]
        print(
            "[TRACE] Processing SPP batch "
            f"{i//batch_size+1}: docIds={batch}"
        )
        payload = {"docIds": batch}
        all_rows = []
        try:
            print(f"Posting to SPP archive API: url={url}, payload={payload}")
            response = rate_limited_request(
                "POST", url, headers=ERCOT_API_REQUEST_HEADERS, json=payload)
            print(f"Received response with status: {response.status_code}")
            if response.status_code != 200:
                try:
                    error = response.json()
                    print(
                        "[ERROR] Failed to download SPP batch. Status: "
                        f"{response.status_code}. Error: "
                        f"{error.get('message', 'Unknown error')}"
                    )
                except ValueError:
                    print(
                        "[ERROR] Failed to download SPP batch. Status: "
                        f"{response.status_code}. Response: {response.text}"
                    )
                continue
            content = response.content
            print(f"Read {len(content)} bytes from response")
            with zipfile.ZipFile(BytesIO(content)) as zip_folder:
                for filename in zip_folder.namelist():
                    print(f"Extracting nested zip: {filename}")
                    with zip_folder.open(filename) as nested_zip_file:
                        nested_content = nested_zip_file.read()
                        print(
                            "Read " + str(len(nested_content)) +
                            f" bytes from nested zip {filename}"
                        )
                        with zipfile.ZipFile(
                            BytesIO(nested_content)
                        ) as nested_zip:
                            for nested_filename in nested_zip.namelist():
                                print(
                                    "Found file in nested zip: "
                                    f"{nested_filename}"
                                )
                                if not nested_filename.endswith('.csv'):
                                    print(
                                        "Skipping non-CSV file: "
                                        f"{nested_filename}"
                                    )
                                    continue
                                print(
                                    f"Processing SPP file: {nested_filename}"
                                )
                                rows = process_spp_file_to_rows(
                                    nested_zip,
                                    nested_filename,
                                    "SETTLEMENT_POINT_PRICES"
                                )
                                print(
                                    f"[TRACE] Got {len(rows)} rows from "
                                    f"{nested_filename}"
                                )
                                all_rows.extend(rows)
            # Store all rows for this batch at once
            if all_rows:
                print(
                    "Storing " + str(len(all_rows)) +
                    " rows to SETTLEMENT_POINT_PRICES (batched)"
                )
                if "SETTLEMENT_POINT_PRICES" in DAM_TABLE_DATA_MAPPING:
                    model_class = (
                        DAM_TABLE_DATA_MAPPING[
                            "SETTLEMENT_POINT_PRICES"
                        ]["model_class"]
                    )
                    insert_query = (
                        DAM_TABLE_DATA_MAPPING[
                            "SETTLEMENT_POINT_PRICES"
                        ]["insert_query"]
                    )
                    print(
                        "[TRACE] Calling store_data_to_db for"
                        "SETTLEMENT_POINT_PRICES")
                    store_data_to_db(
                        data={"data": all_rows},
                        db_name=db_name,
                        table_name="SETTLEMENT_POINT_PRICES",
                        model_class=model_class,
                        insert_query=insert_query)
                else:
                    print(
                        "[WARN] No data mapping found for table: "
                        "SETTLEMENT_POINT_PRICES"
                    )
            else:
                print("[TRACE] No rows to store for this SPP batch.")
        except (zipfile.BadZipFile, ValueError, KeyError) as e:
            print(
                "[ERROR] Exception in SPP batch download: "
                f"{e}\n{traceback.format_exc()}"
            )
    print(
        "Completed SPP archive download. Total docIds processed: "
        f"{len(doc_ids)}"
    )


def download_dam_archive_files(
    product_id: str,
    doc_ids: list[int],
    db_name: str,
    show_progress: bool = True,
    batch_size: int = FILE_LIMITS["DAM"]
) -> None:
    """
    Download and process DAM archive files from ERCOT API, handling errors and
    storing CSV data to the database. Handles batching, zip extraction, and
    skips non-CSV and unmapped files. Prints trace and error messages for all
    error and skip cases.
    """
    print(
        "[CALL] download_dam_archive_files(" +
        f"{product_id}, {doc_ids}, {db_name}, show_progress={show_progress}) "
    )
    print(f"[TRACE] batch_size set to {batch_size}")
    if not isinstance(doc_ids, list) or not doc_ids:
        print(f"No document IDs found for DAM product {product_id}")
        print("Completed DAM archive download. Total docIds processed: 0")
        return
    print(f"Downloading {len(doc_ids)} DAM documents from archive API")
    total_batches = (len(doc_ids) + batch_size - 1) // batch_size
    batch_indices = list(range(0, len(doc_ids), batch_size))
    print(f"[TRACE] total_batches: {total_batches}")
    print(f"[TRACE] batch_indices: {batch_indices}")
    use_progress = show_progress and total_batches > 1
    print(f"[TRACE] use_progress: {use_progress}")

    for idx, i in enumerate(batch_indices):
        batch = doc_ids[i:i + batch_size]
        print(
            "[TRACE] Processing DAM batch "
            f"{idx+1}/{total_batches}: docIds={batch}"
        )
        try:
            response = rate_limited_request(
                "POST",
                f"{ERCOT_ARCHIVE_API_BASE_URL}/{product_id}/download",
                headers=ERCOT_API_REQUEST_HEADERS,
                json={"docIds": batch}
            )
            print(
                f"[TRACE] Received response with status: "
                f"{response.status_code}"
            )
            if response.status_code != 200:
                try:
                    error = response.json()
                except ValueError:
                    error = response.text
                print(
                    f"Failed to download DAM batch {idx+1}: {error}"
                )
                continue
            content = response.content
            print(
                f"[TRACE] Read {len(content)} bytes from DAM response "
                f"for batch {idx+1}"
            )
            process_dam_outer_zip(content, db_name)
        except Exception as e:
            print(f"Exception in DAM batch download: {e}")
    print(
        "Completed DAM archive download. Total docIds processed: "
        f"{len(doc_ids)}"
    )


def process_dam_outer_zip(content: bytes, db_name: str) -> None:
    """
    Process the outer zip file containing nested zip files for DAM data.
    """
    try:
        with zipfile.ZipFile(BytesIO(content)) as zip_folder:
            print("[TRACE] Opened DAM batch zip")
            for name in zip_folder.namelist():
                if not name.lower().endswith(".zip"):
                    print(f"Skipping {name} (not a nested zip)")
                    continue
                try:
                    with zip_folder.open(name) as nested_zip_file:
                        nested_zip_bytes = nested_zip_file.read()
                        process_dam_nested_zip(nested_zip_bytes, db_name)
                except (IOError, zipfile.BadZipFile, ValueError) as e:
                    print(
                        f"Exception in DAM batch download (open nested zip): "
                        f"{e}"
                    )
                    continue
    except zipfile.BadZipFile as e:
        print(
            f"Exception in DAM batch download (outer zip): {e}"
        )
    except (IOError, ValueError) as e:
        print(
            f"Exception in DAM batch download (outer zip): {e}"
        )


def process_dam_nested_zip(nested_zip_bytes: bytes, db_name: str) -> None:
    """
    Process a nested zip file containing CSV files for DAM data.
    """
    try:
        with zipfile.ZipFile(BytesIO(nested_zip_bytes)) as inner_zip:
            namelist = inner_zip.namelist()
            print(f"[DEBUG] inner_zip.namelist(): {namelist}", flush=True)
            for fname in namelist:
                # Always print for non-CSV before any file operations
                if not fname.lower().endswith(".csv"):
                    print(f"Skipping {fname} (not a CSV)", flush=True)
                    continue
                if not any(
                    fname.startswith(prefix) for prefix in DAM_FILENAMES
                ):
                    print(
                        f"Skipping {fname} (does not match DAM_FILENAMES)",
                        flush=True
                    )
                    continue
                table_name = get_table_name(fname)
                if table_name not in DAM_TABLE_DATA_MAPPING:
                    msg = (
                        f"Skipping {fname} (no mapping for table "
                        f"{table_name})"
                    )
                    print(msg, flush=True)
                    continue
                model_class = DAM_TABLE_DATA_MAPPING[table_name]["model_class"]
                insert_query = DAM_TABLE_DATA_MAPPING[table_name][
                    "insert_query"
                ]
                try:
                    with inner_zip.open(fname) as csv_file:
                        process_dam_csv_file(
                            csv_file, fname, table_name, db_name,
                            model_class, insert_query
                        )
                except (IOError, ValueError) as e:
                    print(
                        f"Exception in DAM batch download (CSV): {e}",
                        flush=True
                    )
                    continue
    except zipfile.BadZipFile as e:
        print(
            f"Exception in DAM batch download (nested zip): {e}", flush=True
        )
    except (IOError, ValueError) as e:
        print(
            f"Exception in DAM batch download (nested zip): {e}", flush=True
        )


def process_dam_csv_file(
    csv_file,
    fname: str,
    table_name: str,
    db_name: str,
    model_class,
    insert_query
) -> None:
    """
    Process a single DAM CSV file and store its data in the database.
    """
    try:
        csv_text = csv_file.read().decode("utf-8")
        csv_reader = csv.DictReader(io.StringIO(csv_text))
        rows = list(csv_reader)
    except (csv.Error, UnicodeDecodeError, ValueError) as e:
        print(f"[TRACE] Error reading CSV {fname}: {e}")
        return
    if not rows:
        print(
            f"[TRACE] No data rows found in {fname}"
        )
        return
    print(
        f"[TRACE] Storing {len(rows)} rows from {fname} to {table_name}"
    )
    try:
        store_data_to_db(
            data={"data": rows},
            db_name=db_name,
            table_name=table_name,
            model_class=model_class,
            insert_query=insert_query
        )
    except (ValueError, TypeError) as e:
        print(f"[TRACE] Error storing data for {fname}: {e}")


def get_archive_document_ids(
    product_id: str,
    start_date: str,
    end_date: str,
) -> list[int]:
    """
    Retrieve a list of document IDs from the ERCOT archive API for a given
    product and date range.

    Args:
        product_id (str): The identifier of the ERCOT product to query.
        start_date (str): The start date (inclusive) in 'YYYY-MM-DD' format.
        end_date (str): The end date (inclusive) in 'YYYY-MM-DD' format.

    Returns:
        list[int]: A list of document IDs matching the specified product and
        date range.

    Raises:
        Any exceptions raised by the underlying HTTP request or JSON parsing.

    Note:
        This function paginates through all available results and collects
        document IDs from each page.
        It also prints detailed trace information for debugging purposes.
    """
    print(
        "[CALL] get_archive_document_ids(" +
        f"{product_id}, {start_date}, {end_date}) called from: " +
        f"{traceback.format_stack(limit=3)}"
    )
    url = f"{ERCOT_ARCHIVE_API_BASE_URL}/{product_id}"
    print(f"[TRACE] Archive doc URL: {url}")
    params = {
        "postDatetimeFrom": f"{start_date}T00:00:00.000",
        "postDatetimeTo": f"{end_date}T23:59:59.999",
    }
    print(f"[TRACE] Params: {params}")
    doc_ids = []
    page = 1
    while True:
        params["page"] = page
        print(
            f"[TRACE] Requesting archive doc page {page} with params: {params}"
        )
        response = rate_limited_request(
            "GET", url, headers=ERCOT_API_REQUEST_HEADERS, params=params)
        print(
            "[TRACE] Received response for page "
            f"{page} with status: {response.status_code}"
        )
        data = response.json()
        meta = data.get("_meta")
        if meta:
            print(f"_meta field for archive doc page {page}: {meta}")
        fields = data.get("fields")
        if fields:
            print(f"fields field for archive doc page {page}: {fields}")
        if not data.get("archives"):
            print(f"[TRACE] No archives found on page {page}")
            break
        doc_ids.extend(archive["docId"] for archive in data["archives"])
        print(
            "[TRACE] Collected " + str(len(data['archives'])) +
            f" docIds from page {page}"
        )
        # Safely handle missing _meta or totalPages
        if not meta or "totalPages" not in meta:
            print(f"[TRACE] Reached last page: {page} (no _meta/totalPages)")
            break
        if page >= meta["totalPages"]:
            print(f"[TRACE] Reached last page: {page}")
            break
        page += 1
    print(
        "[TRACE] Returning " + str(len(doc_ids)) +
        " docIds from get_archive_document_ids"
    )
    return doc_ids

# --- END OF FILE ---
