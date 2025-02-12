from typing import Iterator, List
import csv
import io
import zipfile
from io import BytesIO

from ercot_scraping.config import ERCOT_API_REQUEST_HEADERS, ERCOT_ARCHIVE_API_BASE_URL, API_MAX_ARCHIVE_FILES, LOGGER, DAM_FILENAMES, DAM_TABLE_DATA_MAPPING
from ercot_scraping.batched_api import rate_limited_request
from ercot_scraping.utils import get_table_name, get_column_mapping
from ercot_scraping.store_data import store_data_to_db


def download_archive_files(product_id: str, doc_ids: list[int]) -> Iterator[dict]:
    """
    Download archive files in batches and yield parsed data.
    """
    if not doc_ids:
        LOGGER.warning(f"No document IDs found for product {product_id}")
        return []

    url = f"{ERCOT_ARCHIVE_API_BASE_URL}/{product_id}/download"
    LOGGER.info(f"Downloading {len(doc_ids)} documents from archive API")

    for i in range(0, len(doc_ids), API_MAX_ARCHIVE_FILES):
        batch = doc_ids[i:i + API_MAX_ARCHIVE_FILES]
        payload = {"docIds": batch}

        LOGGER.debug(
            f"Requesting batch {i//API_MAX_ARCHIVE_FILES + 1} with {len(batch)} documents")
        LOGGER.debug(f"Request URL: {url}")
        LOGGER.debug(f"Request payload: {payload}")

        try:
            response = rate_limited_request(
                "POST",
                url,
                headers=ERCOT_API_REQUEST_HEADERS,
                json=payload,
                stream=True
            )

            if not response.ok:
                LOGGER.error(
                    f"Failed to download batch. Status code: {response.status_code}")
                continue

            content = response.content
            if not content:
                LOGGER.error("Empty response content")
                continue

            # Try to open as zip file
            try:
                with zipfile.ZipFile(BytesIO(content)) as zip_file:
                    LOGGER.debug(f"Zip file contents: {zip_file.namelist()}")

                    # Process each file in the zip
                    for filename in zip_file.namelist():
                        LOGGER.debug(f"Processing file: {filename}")

                        # Skip directories or non-CSV files
                        if filename.endswith('/'):
                            continue

                        try:
                            # Read CSV from zip
                            with zip_file.open(filename) as csv_file:
                                # Try multiple encodings
                                for encoding in ['utf-8', 'latin1', 'cp1252', 'iso-8859-1']:
                                    try:
                                        csv_content = csv_file.read().decode(encoding)
                                        break
                                    except UnicodeDecodeError:
                                        continue
                                else:
                                    LOGGER.error(
                                        f"Could not decode {filename} with any encoding")
                                    continue

                                # Process CSV content
                                try:
                                    reader = csv.DictReader(
                                        io.StringIO(csv_content.replace(
                                            '\r\n', '\n').replace('\r', '\n'))
                                    )

                                    if not reader.fieldnames:
                                        LOGGER.warning(
                                            f"No headers found in {filename}, attempting fallback parsing")
                                        fallback_io = io.StringIO(csv_content)
                                        first_line = fallback_io.readline().strip()
                                        if first_line:
                                            field_candidates = first_line.split(
                                                ',')
                                            fallback_io.seek(0)
                                            reader = csv.DictReader(
                                                fallback_io, fieldnames=field_candidates)
                                            # Skip the line that is now fieldnames
                                            next(reader, None)

                                            # If we still have no valid fields, skip
                                            if not any(field_candidates):
                                                LOGGER.warning(
                                                    f"Fallback also failed in {filename}, skipping file")
                                                continue
                                        else:
                                            LOGGER.warning(
                                                f"File {filename} appears empty, skipping")
                                            continue

                                    # Normalize field names
                                    field_mapping = {
                                        'delivery_date': 'deliveryDate',
                                        'delivery_hour': 'deliveryHour',
                                        'delivery_interval': 'deliveryInterval',
                                        'settlement_point_name': 'settlementPointName',
                                        'settlement_point_type': 'settlementPointType',
                                        'settlement_point_price': 'settlementPointPrice',
                                        'dst_flag': 'dstFlag'
                                    }

                                    for row in reader:
                                        normalized_row = {}
                                        for key, value in row.items():
                                            if not key:
                                                continue
                                            # Normalize field name
                                            norm_key = key.lower().strip().replace(' ', '_')
                                            final_key = field_mapping.get(
                                                norm_key, norm_key)
                                            # Clean and convert value
                                            if value:
                                                value = value.strip()
                                                # Convert types for known fields
                                                if final_key in ['deliveryHour', 'deliveryInterval']:
                                                    value = int(value)
                                                elif final_key == 'settlementPointPrice':
                                                    value = float(value)
                                            normalized_row[final_key] = value
                                        yield normalized_row

                                except csv.Error as e:
                                    LOGGER.error(
                                        f"CSV parsing error in {filename}: {e}")
                                    continue

                        except Exception as e:
                            LOGGER.error(f"Error processing {filename}: {e}")
                            continue

            except zipfile.BadZipFile:
                LOGGER.error("Response is not a valid zip file")
                LOGGER.debug(f"First 100 bytes of content: {content[:100]}")
                continue

        except Exception as e:
            LOGGER.error(f"Error downloading batch: {str(e)}")
            LOGGER.debug("Exception details:", exc_info=True)
            continue


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

        mapping = get_column_mapping(reader.fieldnames)
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
