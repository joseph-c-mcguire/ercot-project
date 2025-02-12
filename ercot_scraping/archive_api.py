from typing import Iterator, List
import csv
import io
import zipfile
from io import BytesIO

from ercot_scraping.config import ERCOT_API_REQUEST_HEADERS, ERCOT_ARCHIVE_API_BASE_URL, API_MAX_ARCHIVE_FILES, LOGGER
from ercot_scraping.batched_api import rate_limited_request
from ercot_scraping.utils import get_column_mapping


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
            LOGGER.debug(
                f"Downloaded {len(content)} bytes; expecting ZIP format.")
            LOGGER.info(
                "Verifying downloaded content is a ZIP file (not a CSV).")
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
                            with zip_file.open(filename) as possible_zip_file:
                                file_content = possible_zip_file.read()
                                try:
                                    # Attempt to open as nested zip
                                    with zipfile.ZipFile(BytesIO(file_content)) as nested_zip:
                                        LOGGER.debug(
                                            f"Nested zip contents: {nested_zip.namelist()}")
                                        for nested_filename in nested_zip.namelist():
                                            LOGGER.debug(
                                                f"Processing nested file: {nested_filename}")
                                            if nested_filename.endswith('/'):
                                                continue
                                            try:
                                                with nested_zip.open(nested_filename) as csv_file:
                                                    # Try multiple encodings
                                                    for encoding in ['utf-8', 'latin1', 'cp1252', 'iso-8859-1']:
                                                        try:
                                                            csv_content = csv_file.read().decode(encoding)
                                                            break
                                                        except UnicodeDecodeError:
                                                            continue
                                                    else:
                                                        LOGGER.error(
                                                            f"Could not decode {nested_filename} with any encoding")
                                                        continue

                                                    # Process CSV content
                                                    try:
                                                        reader = csv.DictReader(
                                                            io.StringIO(csv_content.replace(
                                                                '\r\n', '\n').replace('\r', '\n'))
                                                        )

                                                        if not reader.fieldnames:
                                                            LOGGER.warning(
                                                                f"No headers found in {nested_filename}, attempting fallback parsing")
                                                            fallback_io = io.StringIO(
                                                                csv_content)
                                                            first_line = fallback_io.readline().strip()
                                                            if first_line:
                                                                field_candidates = first_line.split(
                                                                    ',')
                                                                fallback_io.seek(
                                                                    0)
                                                                reader = csv.DictReader(
                                                                    fallback_io, fieldnames=field_candidates)
                                                                # Skip the line that is now fieldnames
                                                                next(
                                                                    reader, None)

                                                                # If we still have no valid fields, skip
                                                                if not any(field_candidates):
                                                                    LOGGER.warning(
                                                                        f"Fallback also failed in {nested_filename}, skipping file")
                                                                    continue
                                                            else:
                                                                LOGGER.warning(
                                                                    f"File {nested_filename} appears empty, skipping")
                                                                continue

                                                        # Detect the appropriate column mapping
                                                        chosen_mapping = get_column_mapping(
                                                            reader.fieldnames)

                                                        for row in reader:
                                                            normalized_row = {}
                                                            for key, value in row.items():
                                                                if not key:
                                                                    continue
                                                                # Normalize field name
                                                                norm_key = key.lower().strip().replace(' ', '_')
                                                                final_key = chosen_mapping.get(
                                                                    norm_key, norm_key)
                                                                # Clean and convert value
                                                                if value:
                                                                    value = value.strip()
                                                                    # Convert types for known fields
                                                                    if final_key in ['deliveryHour', 'deliveryInterval']:
                                                                        value = int(
                                                                            value)
                                                                    elif final_key == 'settlementPointPrice':
                                                                        value = float(
                                                                            value)
                                                                normalized_row[final_key] = value
                                                            yield normalized_row

                                                    except csv.Error as e:
                                                        LOGGER.error(
                                                            f"CSV parsing error in {nested_filename}: {e}")
                                                        continue

                                            except Exception as e:
                                                LOGGER.error(
                                                    f"Error processing {nested_filename}: {e}")
                                                continue

                                except zipfile.BadZipFile:
                                    # Fallback: treat file_content as an actual CSV
                                    csv_content = file_content.decode('utf-8')
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

                                    # Detect the appropriate column mapping
                                    chosen_mapping = get_column_mapping(
                                        reader.fieldnames)

                                    for row in reader:
                                        normalized_row = {}
                                        for key, value in row.items():
                                            if not key:
                                                continue
                                            # Normalize field name
                                            norm_key = key.lower().strip().replace(' ', '_')
                                            final_key = chosen_mapping.get(
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

                        except Exception as e:
                            LOGGER.error(f"Error processing {filename}: {e}")
                            continue

            except zipfile.BadZipFile:
                LOGGER.error(
                    "Response is not a valid ZIP file from the archive API.")
                LOGGER.error(
                    "Expected a ZIP file but got an invalid format instead.")
                LOGGER.debug(f"First 100 bytes of content: {content[:100]}")
                continue

        except Exception as e:
            LOGGER.error(f"Error downloading batch: {str(e)}")
            LOGGER.debug("Exception details:", exc_info=True)
            continue


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
