from typing import List
import csv
import io
import zipfile
from io import BytesIO

from ercot_scraping.config import ERCOT_API_REQUEST_HEADERS, ERCOT_ARCHIVE_API_BASE_URL, API_MAX_ARCHIVE_FILES, LOGGER, DAM_FILENAMES, DAM_TABLE_DATA_MAPPING, COLUMN_MAPPINGS
from ercot_scraping.batched_api import rate_limited_request
from ercot_scraping.utils import get_table_name
from ercot_scraping.store_data import store_data_to_db


def download_spp_archive_files(
    product_id: str,
    doc_ids: list[int],
    db_name: str
) -> None:
    """
    Download and process SPP archive files, storing each type in its respective table.
    """
    if not doc_ids:
        LOGGER.warning(f"No document IDs found for SPP product {product_id}")
        return

    url = f"{ERCOT_ARCHIVE_API_BASE_URL}/{product_id}/download"
    LOGGER.info(f"Downloading {len(doc_ids)} SPP documents from archive API")

    for i in range(0, len(doc_ids), API_MAX_ARCHIVE_FILES):
        batch = doc_ids[i:i + API_MAX_ARCHIVE_FILES]
        payload = {"docIds": batch}

        response = rate_limited_request(
            "POST",
            url,
            headers=ERCOT_API_REQUEST_HEADERS,
            json=payload,
            stream=True
        )

        if not response.ok:
            LOGGER.error(
                f"Failed to download SPP batch. Status: {response.status_code}")
            continue

        content = response.content
        with zipfile.ZipFile(BytesIO(content)) as zip_folder:
            for filename in zip_folder.namelist():

                with zip_folder.open(filename) as nested_zip_file:
                    nested_content = nested_zip_file.read()
                    with zipfile.ZipFile(BytesIO(nested_content)) as nested_zip:
                        for nested_filename in nested_zip.namelist():

                            if not nested_filename.endswith('.csv'):
                                continue
                            LOGGER.info(
                                f"Processing SPP file: {nested_filename}")
                            process_spp_file(
                                nested_zip, nested_filename, "SETTLEMENT_POINT_PRICES", db_name)


def process_spp_file(zip_folder, filename, table_name, db_name):
    """
    Process a single SPP file and store its data in the database.
    """
    with zip_folder.open(filename) as csv_file:
        csv_content = csv_file.read().decode('utf-8')
        reader = csv.DictReader(io.StringIO(csv_content))

        if not reader.fieldnames:
            LOGGER.warning(f"No headers found in {filename}")
            return

        mapping = COLUMN_MAPPINGS.get(table_name.lower(), {})
        rows = []

        for row in reader:
            normalized_row = {}
            for key, value in row.items():
                if not key:
                    continue
                norm_key = key.lower().strip().replace(' ', '_')
                final_key = mapping.get(norm_key, norm_key)
                normalized_row[final_key] = value.strip() if value else None
            rows.append(normalized_row)

        if rows:
            LOGGER.info(f"Storing {len(rows)} rows to {table_name}")
            if table_name in DAM_TABLE_DATA_MAPPING:
                model_class = DAM_TABLE_DATA_MAPPING[table_name]["model_class"]
                insert_query = DAM_TABLE_DATA_MAPPING[table_name]["insert_query"]
                store_data_to_db(
                    data={"data": rows}, db_name=db_name, table_name=table_name, model_class=model_class, insert_query=insert_query)
            else:
                LOGGER.warning(
                    f"No data mapping found for table: {table_name}")


def download_dam_archive_files(
    product_id: str,
    doc_ids: list[int],
    db_name: str
) -> None:
    """
    Download and process DAM archive files, storing each type in its respective table.
    This function handles the specific DAM file types:
    - 60d_DAM_EnergyBidAwards-*.csv -> BID_AWARDS
    - 60d_DAM_EnergyBids-*.csv -> BIDS
    - 60d_DAM_EnergyOnlyOfferAwards-*.csv -> OFFER_AWARDS
    - 60d_DAM_EnergyOnlyOffers-*.csv -> OFFERS
    """
    if not doc_ids:
        LOGGER.warning(f"No document IDs found for DAM product {product_id}")
        return

    url = f"{ERCOT_ARCHIVE_API_BASE_URL}/{product_id}/download"
    LOGGER.info(f"Downloading {len(doc_ids)} DAM documents from archive API")

    for i in range(0, len(doc_ids), API_MAX_ARCHIVE_FILES):
        batch = doc_ids[i:i + API_MAX_ARCHIVE_FILES]
        payload = {"docIds": batch}

        response = rate_limited_request(
            "POST",
            url,
            headers=ERCOT_API_REQUEST_HEADERS,
            json=payload,
            stream=True
        )

        if not response.ok:
            LOGGER.error(
                f"Failed to download DAM batch. Status: {response.status_code}")
            continue

        content = response.content
        with zipfile.ZipFile(BytesIO(content)) as zip_folder:
            for filename in zip_folder.namelist():

                with zip_folder.open(filename) as nested_zip_file:
                    nested_content = nested_zip_file.read()
                    with zipfile.ZipFile(BytesIO(nested_content)) as nested_zip:
                        for nested_filename in nested_zip.namelist():

                            table_name = get_table_name(nested_filename)
                            if not any(nested_filename.startswith(prefix) for prefix in DAM_FILENAMES):
                                continue
                            if not table_name:
                                LOGGER.warning(
                                    f"Unrecognized DAM file type: {nested_filename}")
                                continue
                            if not nested_filename.endswith('.csv'):
                                continue
                            LOGGER.info(
                                f"Processing DAM file: {nested_filename}")
                            process_dam_file(
                                nested_zip, nested_filename, table_name, db_name)


def process_dam_file(zip_folder, filename, table_name, db_name):
    """
    Process a single DAM file and store its data in the database.
    """
    with zip_folder.open(filename) as csv_file:
        csv_content = csv_file.read().decode('utf-8')
        reader = csv.DictReader(io.StringIO(csv_content))

        if not reader.fieldnames:
            LOGGER.warning(f"No headers found in {filename}")
            return

        mapping = COLUMN_MAPPINGS.get(table_name.lower(), {})
        rows = []

        for row in reader:
            normalized_row = {}
            for key, value in row.items():
                if not key:
                    continue
                norm_key = key.lower().strip().replace(' ', '_')
                final_key = mapping.get(norm_key, norm_key)
                normalized_row[final_key] = value.strip() if value else None
            rows.append(normalized_row)

        if rows:
            LOGGER.info(f"Storing {len(rows)} rows to {table_name}")
            if table_name in DAM_TABLE_DATA_MAPPING:
                model_class = DAM_TABLE_DATA_MAPPING[table_name]["model_class"]
                insert_query = DAM_TABLE_DATA_MAPPING[table_name]["insert_query"]
                store_data_to_db(
                    data={"data": rows}, db_name=db_name, table_name=table_name, model_class=model_class, insert_query=insert_query)
            else:
                LOGGER.warning(
                    f"No data mapping found for table: {table_name}")


def get_archive_document_ids(
    product_id: str,
    start_date: str,
    end_date: str,
) -> List[int]:
    """
    Get document IDs for the specified date range from the archive API.
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

        if not data.get("archives"):
            break

        doc_ids.extend(archive["docId"] for archive in data["archives"])

        if page >= data["_meta"]["totalPages"]:
            break

        page += 1

    return doc_ids
