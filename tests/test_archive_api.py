import pytest
from unittest import mock
from ercot_scraping.archive_api import download_spp_archive_files
from ercot_scraping.config import LOGGER, ERCOT_API_REQUEST_HEADERS, ERCOT_ARCHIVE_API_BASE_URL, API_MAX_ARCHIVE_FILES
from ercot_scraping.batched_api import rate_limited_request
import zipfile
import io
import csv
from ercot_scraping.archive_api import process_spp_file
from ercot_scraping.config import LOGGER, COLUMN_MAPPINGS, DAM_TABLE_DATA_MAPPING
from ercot_scraping.store_data import store_data_to_db


@mock.patch('ercot_scraping.archive_api.rate_limited_request')
@mock.patch('ercot_scraping.archive_api.zipfile.ZipFile')
@mock.patch('ercot_scraping.archive_api.LOGGER')
def test_download_spp_archive_files_no_doc_ids(mock_logger, mock_zipfile, mock_rate_limited_request):
    download_spp_archive_files('product_id', [], 'db_name')
    mock_logger.warning.assert_called_once_with(
        "No document IDs found for SPP product product_id")


@mock.patch('ercot_scraping.archive_api.rate_limited_request')
@mock.patch('ercot_scraping.archive_api.zipfile.ZipFile')
@mock.patch('ercot_scraping.archive_api.BytesIO')
@mock.patch('ercot_scraping.archive_api.process_spp_file')
@mock.patch('ercot_scraping.archive_api.LOGGER')
def test_download_spp_archive_files_success(mock_logger, mock_process_spp, mock_bytesio, mock_zipfile, mock_rate_limited_request):
    # Setup API response mock
    mock_response = mock.Mock()
    mock_response.ok = True
    mock_response.content = b'zip_content'
    mock_rate_limited_request.return_value = mock_response

    # Setup BytesIO mocks
    mock_bytes_outer = mock.MagicMock()
    mock_bytes_inner = mock.MagicMock()
    mock_bytesio.side_effect = [mock_bytes_outer, mock_bytes_inner]

    # Setup inner zip file structure
    mock_inner_zip = mock.MagicMock()
    mock_inner_zip.namelist.return_value = ['file1.csv']
    mock_inner_zip.__enter__.return_value = mock_inner_zip

    # Setup outer zip file structure
    mock_outer_zip = mock.MagicMock()
    mock_outer_zip.namelist.return_value = ['file1.zip']
    mock_outer_zip.__enter__.return_value = mock_outer_zip

    # Setup nested zip content
    mock_nested_zip_content = mock.MagicMock()
    mock_nested_zip_content.read.return_value = b'nested_zip_content'
    mock_outer_zip.open.return_value.__enter__.return_value = mock_nested_zip_content

    # Configure ZipFile mock to return appropriate zip objects
    mock_zipfile.side_effect = [mock_outer_zip, mock_inner_zip]

    # Run the function
    download_spp_archive_files('product_id', [1, 2, 3], 'db_name')

    # Verify the entire chain of calls
    assert mock_bytesio.call_count == 2
    assert mock_zipfile.call_count == 2
    assert mock_process_spp.call_count == 1

    # Verify logging calls in order
    mock_logger.info.assert_has_calls([
        mock.call("Downloading 3 SPP documents from archive API"),
        mock.call("Processing SPP file: file1.csv")
    ])

    # Verify process_spp_file was called correctly
    mock_process_spp.assert_called_once_with(
        mock_inner_zip,
        'file1.csv',
        'SETTLEMENT_POINT_PRICES',
        'db_name'
    )


@mock.patch('ercot_scraping.archive_api.rate_limited_request')
@mock.patch('ercot_scraping.archive_api.zipfile.ZipFile')
@mock.patch('ercot_scraping.archive_api.LOGGER')
def test_download_spp_archive_files_failed_download(mock_logger, mock_zipfile, mock_rate_limited_request):
    mock_response = mock.Mock()
    mock_response.ok = False
    mock_response.status_code = 500
    mock_rate_limited_request.return_value = mock_response

    download_spp_archive_files('product_id', [1, 2, 3], 'db_name')

    mock_logger.error.assert_called_once_with(
        "Failed to download SPP batch. Status: 500")


@mock.patch('ercot_scraping.archive_api.store_data_to_db')
@mock.patch('ercot_scraping.archive_api.LOGGER')
def test_process_spp_file_no_headers(mock_logger, mock_store_data):
    # Setup mock file with binary content - empty CSV file
    mock_zipfile = mock.MagicMock()
    mock_csv_file = mock.MagicMock()
    mock_csv_file.read.return_value = b"\n"  # Empty CSV file
    mock_zipfile.open.return_value.__enter__.return_value = mock_csv_file

    process_spp_file(mock_zipfile, 'test.csv', 'test_table', 'test_db')

    mock_logger.warning.assert_called_once_with("No headers found in test.csv")
    mock_store_data.assert_not_called()


@mock.patch('ercot_scraping.archive_api.store_data_to_db')
@mock.patch('ercot_scraping.archive_api.LOGGER')
def test_process_spp_file_with_data(mock_logger, mock_store_data):
    # Setup mock file with binary content
    mock_zipfile = mock.MagicMock()
    mock_csv_file = mock.MagicMock()
    mock_csv_file.read.return_value = b"header1,header2\nvalue1,value2\nvalue3,value4"
    mock_zipfile.open.return_value.__enter__.return_value = mock_csv_file

    COLUMN_MAPPINGS['test_table'] = {
        'header1': 'mapped_header1', 'header2': 'mapped_header2'}
    DAM_TABLE_DATA_MAPPING['test_table'] = {
        "model_class": mock.Mock(),
        "insert_query": "INSERT INTO test_table (mapped_header1, mapped_header2) VALUES (:mapped_header1, :mapped_header2)"
    }

    process_spp_file(mock_zipfile, 'test.csv', 'test_table', 'test_db')

    mock_logger.info.assert_called_once_with(
        "Storing 2 rows to test_table")
    mock_store_data.assert_called_once_with(
        data={"data": [
            {'mapped_header1': 'value1', 'mapped_header2': 'value2'},
            {'mapped_header1': 'value3', 'mapped_header2': 'value4'}
        ]},
        db_name='test_db',
        table_name='test_table',
        model_class=DAM_TABLE_DATA_MAPPING['test_table']["model_class"],
        insert_query=DAM_TABLE_DATA_MAPPING['test_table']["insert_query"]
    )


@mock.patch('ercot_scraping.archive_api.store_data_to_db')
@mock.patch('ercot_scraping.archive_api.LOGGER')
def test_process_spp_file_no_data_mapping(mock_logger, mock_store_data):
    # Setup mock file with binary content
    mock_zipfile = mock.MagicMock()
    mock_csv_file = mock.MagicMock()
    mock_csv_file.read.return_value = b"header1,header2\nvalue1,value2\nvalue3,value4"
    mock_zipfile.open.return_value.__enter__.return_value = mock_csv_file

    process_spp_file(mock_zipfile, 'test.csv',
                     'unknown_table', 'test_db')

    mock_logger.warning.assert_called_once_with(
        "No data mapping found for table: unknown_table")
    mock_store_data.assert_not_called()
