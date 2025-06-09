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
    import io
    import csv
    rows = []
    with zip_folder.open(filename) as csv_file:
        csv_content = csv_file.read().decode('utf-8')
        csv_buffer = io.StringIO(csv_content)
        first_line = csv_buffer.readline().strip()
        if not first_line or ',' not in first_line:
            print(f"[WARN] No headers found in {filename}")
            return []
        csv_buffer.seek(0)
        reader = csv.DictReader(csv_buffer)
        if not reader.fieldnames:
            print(f"[WARN] No headers found in {filename}")
            return []
        mapping = COLUMN_MAPPINGS.get(table_name.lower(), {})
        for row in reader:
            normalized_row = {}
            for key, value in row.items():
                if not key:
                    continue
                norm_key = key.lower().strip().replace(' ', '_')
                final_key = mapping.get(norm_key, norm_key)
                normalized_row[final_key] = value.strip() if value else None
            rows.append(normalized_row)
    return rows


def download_spp_archive_files(
    product_id: str,
    doc_ids: list[int],
    db_name: str,
    # Use config SPP file limit for batch size
    batch_size: int = FILE_LIMITS["SPP"]
) -> None:
    """
    Downloads and processes SPP archive files from the ERCOT archive API in
    batches. Accumulates rows for SETTLEMENT_POINT_PRICES and calls
    store_data_to_db once per batch of files.
    """
    print(
        f"[CALL] download_spp_archive_files({product_id}, {doc_ids}, {db_name}) called")

    if not doc_ids:
        print(f"[WARN] No document IDs found for SPP product {product_id}")
        return

    url = f"{ERCOT_ARCHIVE_API_BASE_URL}/{product_id}/download"
    print(f"Downloading {len(doc_ids)} SPP documents from archive API")

    for i in range(0, len(doc_ids), batch_size):
        batch = doc_ids[i:i + batch_size]
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
                        f"[ERROR] Failed to download SPP batch. Status: {response.status_code}. Error: {error.get('message', 'Unknown error')}")
                except Exception:
                    print(
                        f"[ERROR] Failed to download SPP batch. Status: {response.status_code}. Response: {response.text}")
                continue
            content = response.content
            print(f"Read {len(content)} bytes from response")
            with zipfile.ZipFile(BytesIO(content)) as zip_folder:
                for filename in zip_folder.namelist():
                    print(f"Extracting nested zip: {filename}")
                    with zip_folder.open(filename) as nested_zip_file:
                        nested_content = nested_zip_file.read()
                        print(
                            f"Read {len(nested_content)} bytes from nested zip {filename}")
                        with zipfile.ZipFile(
                            BytesIO(nested_content)
                        ) as nested_zip:
                            for nested_filename in nested_zip.namelist():
                                print(
                                    f"Found file in nested zip: {nested_filename}")
                                if not nested_filename.endswith('.csv'):
                                    print(
                                        f"Skipping non-CSV file: {nested_filename}")
                                    continue
                                print(
                                    f"Processing SPP file: {nested_filename}")
                                rows = process_spp_file_to_rows(
                                    nested_zip,
                                    nested_filename,
                                    "SETTLEMENT_POINT_PRICES"
                                )
                                all_rows.extend(rows)
            # Store all rows for this batch at once
            if all_rows:
                print(
                    f"Storing {len(all_rows)} rows to SETTLEMENT_POINT_PRICES (batched)")
                if "SETTLEMENT_POINT_PRICES" in DAM_TABLE_DATA_MAPPING:
                    model_class = (
                        DAM_TABLE_DATA_MAPPING["SETTLEMENT_POINT_PRICES"]["model_class"]
                    )
                    insert_query = (
                        DAM_TABLE_DATA_MAPPING["SETTLEMENT_POINT_PRICES"]["insert_query"]
                    )
                    store_data_to_db(
                        data={"data": all_rows},
                        db_name=db_name,
                        table_name="SETTLEMENT_POINT_PRICES",
                        model_class=model_class,
                        insert_query=insert_query)
                else:
                    print(
                        "[WARN] No data mapping found for table: SETTLEMENT_POINT_PRICES")
        except Exception as e:
            print(
                f"[ERROR] Exception in SPP batch download: {e}\n{traceback.format_exc()}")
    print(
        f"Completed SPP archive download. Total docIds processed: {len(doc_ids)}")


def download_dam_archive_files(
    product_id: str,
    doc_ids: list[int],
    db_name: str,
    show_progress: bool = True,
    # Use config DAM file limit for batch size
    batch_size: int = FILE_LIMITS["DAM"]
) -> None:
    print(
        f"[CALL] download_dam_archive_files({product_id}, {doc_ids}, {db_name}, show_progress={show_progress}) ")
    if not doc_ids:
        print(f"[WARN] No document IDs found for DAM product {product_id}")
        return

    url = f"{ERCOT_ARCHIVE_API_BASE_URL}/{product_id}/download"
    print(f"Downloading {len(doc_ids)} DAM documents from archive API")
    total_batches = (len(doc_ids) + batch_size - 1) // batch_size
    batch_indices = list(range(0, len(doc_ids), batch_size))
    use_progress = show_progress and TQDM_AVAILABLE and total_batches > 1

    for idx, i in enumerate(batch_indices):
        batch = doc_ids[i:i + batch_size]
        payload = {"docIds": batch}
        try:
            response = rate_limited_request(
                "POST", url, headers=ERCOT_API_REQUEST_HEADERS, json=payload)
            if response.status_code != 200:
                try:
                    error = response.json()
                    err_msg = (
                        f"[ERROR] Failed to download DAM batch {idx+1}/{total_batches}. Status: {response.status_code}. Error: {error.get('message', 'Unknown error')}")
                except Exception:
                    err_msg = (
                        f"[ERROR] Failed to download DAM batch {idx+1}/{total_batches}. Status: {response.status_code}. Response: {response.text}")
                if use_progress:
                    print(err_msg)
                else:
                    print(err_msg)
                continue
            content = response.content
            with zipfile.ZipFile(BytesIO(content)) as zip_folder:
                for filename in zip_folder.namelist():
                    with zip_folder.open(filename) as nested_zip_file:
                        nested_content = nested_zip_file.read()
                        with zipfile.ZipFile(
                            BytesIO(nested_content)
                        ) as nested_zip:
                            for nested_filename in nested_zip.namelist():
                                table_name = get_table_name(nested_filename)
                                if not any(
                                    nested_filename.startswith(prefix)
                                    for prefix in DAM_FILENAMES
                                ):
                                    continue
                                if not table_name:
                                    continue
                                if not nested_filename.endswith('.csv'):
                                    continue
                                process_dam_file(
                                    nested_zip,
                                    nested_filename,
                                    table_name,
                                    db_name)
        except Exception as e:
            print(
                f"[ERROR] Exception in DAM batch download: {e}\n{traceback.format_exc()}")
    print(
        f"Completed DAM archive download. Total docIds processed: {len(doc_ids)}")


def get_archive_document_ids(
    product_id: str,
    start_date: str,
    end_date: str,
) -> list[int]:
    print(
        f"[CALL] get_archive_document_ids({product_id}, {start_date}, {end_date}) called from: {traceback.format_stack(limit=3)}")
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
        meta = data.get("_meta")
        if meta:
            print(f"_meta field for archive doc page {page}: {meta}")
        fields = data.get("fields")
        if fields:
            print(f"fields field for archive doc page {page}: {fields}")
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
    """
    with zip_folder.open(filename) as csv_file:
        csv_content = csv_file.read().decode('utf-8')
        csv_buffer = io.StringIO(csv_content)

        first_line = csv_buffer.readline().strip()
        if not first_line or ',' not in first_line:
            print(f"[WARN] No headers found in {filename}")
            return

        csv_buffer.seek(0)
        reader = csv.DictReader(csv_buffer)

        if not reader.fieldnames:
            print(f"[WARN] No headers found in {filename}")
            return

        rows = list(reader)
        if not rows and not first_line.lower().startswith("col"):
            print(f"[WARN] No headers found in {filename}")
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
            print(f"Storing {len(normalized_rows)} rows to {table_name}")
            if table_name in DAM_TABLE_DATA_MAPPING:
                model_class = DAM_TABLE_DATA_MAPPING[table_name]["model_class"]
                insert_query = DAM_TABLE_DATA_MAPPING[table_name][
                    "insert_query"]
                store_data_to_db(
                    data={"data": normalized_rows},
                    db_name=db_name,
                    table_name=table_name,
                    model_class=model_class,
                    insert_query=insert_query
                )
            else:
                print(f"[WARN] No data mapping found for table: {table_name}")
