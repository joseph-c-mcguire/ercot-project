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
    COLUMN_MAPPINGS,
    FILE_LIMITS,
)
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


def process_spp_file_to_rows(
    zip_folder: zipfile.ZipFile,
    filename: str,
    table_name: str
) -> list[dict]:
    """
    Processes a CSV file from a zip folder, normalizes its content,
    and returns rows as a list of dicts.
    """
    print(
        "[TRACE] Entered process_spp_file_to_rows with filename="
        f"{filename}, table_name={table_name}"
    )
    rows = []
    with zip_folder.open(filename) as csv_file:
        print(f"[TRACE] Opened file {filename} from zip_folder")
        csv_content = csv_file.read().decode('utf-8')
        print(f"[TRACE] Read {len(csv_content)} bytes from {filename}")
        csv_buffer = io.StringIO(csv_content)
        first_line = csv_buffer.readline().strip()
        print(f"[TRACE] First line of {filename}: {first_line}")
        if not first_line or ',' not in first_line:
            print(f"[WARN] No headers found in {filename}")
            return []
        csv_buffer.seek(0)
        reader = csv.DictReader(csv_buffer)
        print(f"[TRACE] CSV fieldnames for {filename}: {reader.fieldnames}")
        if not reader.fieldnames:
            print(f"[WARN] No headers found in {filename}")
            return []
        mapping = COLUMN_MAPPINGS.get(table_name.lower(), {})
        for row_num, row in enumerate(reader):
            normalized_row = {}
            for key, value in row.items():
                if not key:
                    continue
                norm_key = key.lower().strip().replace(' ', '_')
                final_key = mapping.get(norm_key, norm_key)
                normalized_row[final_key] = value.strip() if value else None
            rows.append(normalized_row)
            if row_num < 3:
                print(
                    f"[TRACE] Row {row_num} in {filename}: {normalized_row}"
                )
    print(f"[TRACE] Returning {len(rows)} rows from {filename}")
    return rows


def download_spp_archive_files(
    product_id: str,
    doc_ids: list[int],
    db_name: str,
    batch_size: int = FILE_LIMITS["SPP"]
) -> None:
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
                except Exception:
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
                        "[TRACE] Calling store_data_to_db for SETTLEMENT_POINT_PRICES")
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
        except Exception as e:
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
    print(
        "[CALL] download_dam_archive_files(" +
        f"{product_id}, {doc_ids}, {db_name}, show_progress={show_progress}) "
    )
    print(f"[TRACE] batch_size set to {batch_size}")
    if not doc_ids:
        print(f"[WARN] No document IDs found for DAM product {product_id}")
        return
    url = f"{ERCOT_ARCHIVE_API_BASE_URL}/{product_id}/download"
    print(f"Downloading {len(doc_ids)} DAM documents from archive API")
    total_batches = (len(doc_ids) + batch_size - 1) // batch_size
    print(f"[TRACE] total_batches: {total_batches}")
    batch_indices = list(range(0, len(doc_ids), batch_size))
    print(f"[TRACE] batch_indices: {batch_indices}")
    use_progress = show_progress and TQDM_AVAILABLE and total_batches > 1
    print(f"[TRACE] use_progress: {use_progress}")
    for idx, i in enumerate(batch_indices):
        batch = doc_ids[i:i + batch_size]
        print(
            "[TRACE] Processing DAM batch "
            f"{idx+1}/{total_batches}: docIds={batch}"
        )
        payload = {"docIds": batch}
        try:
            print(f"[TRACE] Sending POST to {url} with payload: {payload}")
            response = rate_limited_request(
                "POST", url, headers=ERCOT_API_REQUEST_HEADERS, json=payload)
            print(
                "[TRACE] Received response with status: "
                f"{response.status_code}"
            )
            if response.status_code != 200:
                try:
                    error = response.json()
                    err_msg = (
                        "[ERROR] Failed to download DAM batch "
                        f"{idx+1}/{total_batches}. Status: "
                        f"{response.status_code}. Error: "
                        f"{error.get('message', 'Unknown error')}"
                    )
                except Exception:
                    err_msg = (
                        "[ERROR] Failed to download DAM batch "
                        f"{idx+1}/{total_batches}. Status: "
                        f"{response.status_code}. Response: {response.text}"
                    )
                print(f"[TRACE] {err_msg}")
                continue
            content = response.content
            print(
                "[TRACE] Read " + str(len(content)) +
                f" bytes from DAM response for batch {idx+1}"
            )
            with zipfile.ZipFile(BytesIO(content)) as zip_folder:
                print(f"[TRACE] Opened DAM batch zip for batch {idx+1}")
                for filename in zip_folder.namelist():
                    print(
                        f"[TRACE] Found nested zip: {filename} in DAM batch "
                        f"{idx+1}"
                    )
                    with zip_folder.open(filename) as nested_zip_file:
                        nested_content = nested_zip_file.read()
                        print(
                            "[TRACE] Read " + str(len(nested_content)) +
                            f" bytes from nested zip {filename}"
                        )
                        with zipfile.ZipFile(
                            BytesIO(nested_content)
                        ) as nested_zip:
                            print(
                                f"[TRACE] Opened nested zip {filename} in DAM "
                                f"batch {idx+1}"
                            )
                            for nested_filename in nested_zip.namelist():
                                print(
                                    f"[TRACE] Found file in nested zip: "
                                    f"{nested_filename}"
                                )
                                table_name = get_table_name(nested_filename)
                                print(
                                    "[TRACE] table_name for "
                                    f"{nested_filename}: {table_name}"
                                )
                                if not any(
                                    nested_filename.startswith(prefix)
                                    for prefix in DAM_FILENAMES
                                ):
                                    print(
                                        f"[TRACE] Skipping {nested_filename} "
                                        f"(prefix not in DAM_FILENAMES)"
                                    )
                                    continue
                                if not table_name:
                                    print(
                                        f"[TRACE] Skipping {nested_filename} "
                                        f"(no table_name)"
                                    )
                                    continue
                                if not nested_filename.endswith('.csv'):
                                    print(
                                        f"[TRACE] Skipping {nested_filename} "
                                        f"(not a CSV)"
                                    )
                                    continue
                                print(
                                    f"[TRACE] Processing DAM file: "
                                    f"{nested_filename} for table {table_name}"
                                )
                                process_dam_file(
                                    nested_zip,
                                    nested_filename,
                                    table_name,
                                    db_name)
        except Exception as e:
            print(
                "[ERROR] Exception in DAM batch download: "
                f"{e}\n{traceback.format_exc()}"
            )
    print(
        "Completed DAM archive download. Total docIds processed: "
        f"{len(doc_ids)}"
    )


def get_archive_document_ids(
    product_id: str,
    start_date: str,
    end_date: str,
) -> list[int]:
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
        if page >= data["_meta"]["totalPages"]:
            print(f"[TRACE] Reached last page: {page}")
            break
        page += 1
    print(
        "[TRACE] Returning " + str(len(doc_ids)) +
        " docIds from get_archive_document_ids"
    )
    return doc_ids


def process_dam_file(
    zip_folder: zipfile.ZipFile,
    filename: str,
    table_name: str,
    db_name: str
) -> None:
    print(
        "[TRACE] Entered process_dam_file with filename="
        f"{filename}, table_name={table_name}, db_name={db_name}"
    )
    with zip_folder.open(filename) as csv_file:
        print(f"[TRACE] Opened file {filename} from zip_folder")
        csv_content = csv_file.read().decode('utf-8')
        print(f"[TRACE] Read {len(csv_content)} bytes from {filename}")
        csv_buffer = io.StringIO(csv_content)
        first_line = csv_buffer.readline().strip()
        print(f"[TRACE] First line of {filename}: {first_line}")
        if not first_line or ',' not in first_line:
            print(f"[WARN] No headers found in {filename}")
            return
        csv_buffer.seek(0)
        reader = csv.DictReader(csv_buffer)
        print(f"[TRACE] CSV fieldnames for {filename}: {reader.fieldnames}")
        if not reader.fieldnames:
            print(f"[WARN] No headers found in {filename}")
            return
        rows = list(reader)
        print(f"[TRACE] Read {len(rows)} rows from {filename}")
        if not rows and not first_line.lower().startswith("col"):
            print(f"[WARN] No headers found in {filename}")
            return
        mapping = COLUMN_MAPPINGS.get(table_name.lower(), {})
        normalized_rows = []
        for row_num, row in enumerate(rows):
            normalized_row = {}
            for key, value in row.items():
                if not key:
                    continue
                norm_key = key.lower().strip().replace(' ', '_')
                final_key = mapping.get(norm_key, norm_key)
                normalized_row[final_key] = value.strip() if value else None
            normalized_rows.append(normalized_row)
            if row_num < 3:
                print(
                    f"[TRACE] Row {row_num} in {filename}: {normalized_row}"
                )
        if normalized_rows:
            print(f"Storing {len(normalized_rows)} rows to {table_name}")
            if table_name in DAM_TABLE_DATA_MAPPING:
                model_class = DAM_TABLE_DATA_MAPPING[table_name]["model_class"]
                insert_query = DAM_TABLE_DATA_MAPPING[table_name][
                    "insert_query"]
                print(f"[TRACE] Calling store_data_to_db for {table_name}")
                store_data_to_db(
                    data={"data": normalized_rows},
                    db_name=db_name,
                    table_name=table_name,
                    model_class=model_class,
                    insert_query=insert_query
                )
            else:
                print(f"[WARN] No data mapping found for table: {table_name}")
        else:
            print(f"[TRACE] No rows to store for {filename}")
