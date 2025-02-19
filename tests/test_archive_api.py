from unittest import mock
from unittest.mock import patch, MagicMock
import zipfile
from io import BytesIO

import pytest

from ercot_scraping.config.column_mappings import COLUMN_MAPPINGS
from ercot_scraping.config.config import DAM_TABLE_DATA_MAPPING
from ercot_scraping.apis.archive_api import (
    download_spp_archive_files,
    download_dam_archive_files,
    process_spp_file,
    process_dam_file,
    get_archive_document_ids
)


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


@mock.patch('ercot_scraping.archive_api.rate_limited_request')
@mock.patch('ercot_scraping.archive_api.zipfile.ZipFile')
@mock.patch('ercot_scraping.archive_api.LOGGER')
def test_download_dam_archive_files_no_doc_ids(mock_logger, mock_zipfile, mock_rate_limited_request):
    download_dam_archive_files('product_id', [], 'db_name')
    mock_logger.warning.assert_called_once_with(
        "No document IDs found for DAM product product_id")


@mock.patch('ercot_scraping.archive_api.rate_limited_request')
@mock.patch('ercot_scraping.archive_api.zipfile.ZipFile')
@mock.patch('ercot_scraping.archive_api.BytesIO')
@mock.patch('ercot_scraping.archive_api.process_dam_file')
@mock.patch('ercot_scraping.archive_api.get_table_name')
@mock.patch('ercot_scraping.archive_api.DAM_FILENAMES', ['60d_DAM_'])
@mock.patch('ercot_scraping.archive_api.LOGGER')
def test_download_dam_archive_files_success(
    mock_logger, mock_get_table_name, mock_process_dam, mock_bytesio, mock_zipfile,
    mock_rate_limited_request
):
    # Setup API response mock
    mock_response = mock.Mock()
    mock_response.ok = True
    mock_response.content = b'zip_content'
    mock_rate_limited_request.return_value = mock_response

    # Setup BytesIO mocks
    mock_bytes_outer = mock.MagicMock()
    mock_bytes_inner = mock.MagicMock()
    mock_bytesio.side_effect = [mock_bytes_outer, mock_bytes_inner]

    # Setup inner zip file structure with valid DAM filename
    mock_inner_zip = mock.MagicMock()
    mock_inner_zip.namelist.return_value = ['60d_DAM_file1.csv']
    mock_inner_zip.__enter__.return_value = mock_inner_zip

    # Setup outer zip structure
    mock_outer_zip = mock.MagicMock()
    mock_outer_zip.namelist.return_value = ['file1.zip']
    mock_outer_zip.__enter__.return_value = mock_outer_zip

    # Setup nested zip content
    mock_nested_zip_content = mock.MagicMock()
    mock_nested_zip_content.read.return_value = b'nested_zip_content'
    mock_outer_zip.open.return_value.__enter__.return_value = mock_nested_zip_content

    # Configure ZipFile mock
    mock_zipfile.side_effect = [mock_outer_zip, mock_inner_zip]

    # Setup table name mock
    mock_get_table_name.return_value = 'test_table'

    # Run the function
    download_dam_archive_files('product_id', [1, 2, 3], 'db_name')

    # Verify calls
    assert mock_bytesio.call_count == 2
    assert mock_zipfile.call_count == 2
    assert mock_process_dam.call_count == 1

    # Verify logging calls in order
    mock_logger.info.assert_has_calls([
        mock.call("Downloading 3 DAM documents from archive API"),
        mock.call("Processing DAM file: 60d_DAM_file1.csv")
    ])

    # Verify process_dam_file was called correctly
    mock_process_dam.assert_called_once_with(
        mock_inner_zip,
        '60d_DAM_file1.csv',
        'test_table',
        'db_name'
    )


@mock.patch('ercot_scraping.archive_api.rate_limited_request')
@mock.patch('ercot_scraping.archive_api.zipfile.ZipFile')
@mock.patch('ercot_scraping.archive_api.LOGGER')
def test_download_dam_archive_files_failed_download(mock_logger, mock_zipfile, mock_rate_limited_request):
    mock_response = mock.Mock()
    mock_response.ok = False
    mock_response.status_code = 500
    mock_rate_limited_request.return_value = mock_response

    download_dam_archive_files('product_id', [1, 2, 3], 'db_name')

    mock_logger.error.assert_called_once_with(
        "Failed to download DAM batch. Status: 500")


@mock.patch('ercot_scraping.archive_api.rate_limited_request')
@mock.patch('ercot_scraping.archive_api.zipfile.ZipFile')
@mock.patch('ercot_scraping.archive_api.BytesIO')
@mock.patch('ercot_scraping.archive_api.get_table_name')
@mock.patch('ercot_scraping.archive_api.DAM_FILENAMES', ['60d_DAM_'])
@mock.patch('ercot_scraping.archive_api.LOGGER')
def test_download_dam_archive_files_unrecognized_file_type(
    mock_logger, mock_get_table_name, mock_bytesio, mock_zipfile, mock_rate_limited_request
):
    # Setup API response mock
    mock_response = mock.Mock()
    mock_response.ok = True
    mock_response.content = b'zip_content'
    mock_rate_limited_request.return_value = mock_response

    # Setup BytesIO mocks
    mock_bytes_outer = mock.MagicMock()
    mock_bytes_inner = mock.MagicMock()
    mock_bytesio.side_effect = [mock_bytes_outer, mock_bytes_inner]

    # Setup inner zip file structure with valid DAM prefix but unrecognized type
    mock_inner_zip = mock.MagicMock()
    mock_inner_zip.namelist.return_value = ['60d_DAM_unknown.csv']
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

    # Setup table name mock to return None for unrecognized file
    mock_get_table_name.return_value = None

    # Run the function
    download_dam_archive_files('product_id', [1, 2, 3], 'db_name')

    # Verify the warning was logged
    mock_logger.warning.assert_called_once_with(
        "Unrecognized DAM file type: 60d_DAM_unknown.csv")


@pytest.mark.parametrize(
    "test_id, csv_content, table_name, expected_rows, column_mappings",
    [
        (
            "happy_path_simple",
            "col1,col2\n1,2",
            "test_table",
            [{"col1": "1", "col2": "2"}],
            {},
        ),
        (
            "happy_path_with_mapping",
            "Col 1,Col 2\n1,2",
            "test_table",
            [{"col_1": "1", "col_2": "2"}],
            {"col_1": "col_1", "col_2": "col_2"},

        ),
        (
            "happy_path_with_empty_values",
            "col1,col2\n,2",
            "test_table",
            [{"col1": None, "col2": "2"}],
            {},
        ),
        (
            "happy_path_with_spaces",
            "col 1, col 2\n 1 ,2 ",
            "test_table",
            [{"col_1": "1", "col_2": "2"}],
            {},
        ),
    ],
)
@patch("ercot_scraping.archive_api.store_data_to_db")
def test_process_dam_file_happy_path(
    mock_store_data, test_id, csv_content, table_name, expected_rows, column_mappings
):
    # Arrange
    zip_buffer = BytesIO()
    with zipfile.ZipFile(zip_buffer, "w") as zip_file:
        zip_file.writestr("data.csv", csv_content)
    zip_buffer.seek(0)
    zip_folder = zipfile.ZipFile(zip_buffer)

    db_name = "test_db"

    with patch.dict(COLUMN_MAPPINGS, {table_name.lower(): column_mappings}):

        # Act
        process_dam_file(zip_folder, "data.csv", table_name, db_name)

        # Assert
        if table_name in DAM_TABLE_DATA_MAPPING:
            mock_store_data.assert_called_once_with(
                data={"data": expected_rows},
                db_name=db_name,
                table_name=table_name,
                model_class=DAM_TABLE_DATA_MAPPING[table_name]["model_class"],
                insert_query=DAM_TABLE_DATA_MAPPING[table_name]["insert_query"],
            )


@pytest.mark.parametrize(
    "test_id, csv_content, table_name, expected_warning",
    [
        ("empty_file", "", "test_table", "No headers found in data.csv"),
        ("no_headers", "val1,val2", "test_table", "No headers found in data.csv"),
        ("empty_rows", "col1,col2\n", "test_table", None),
    ],
)
@patch("ercot_scraping.archive_api.store_data_to_db")
@patch("ercot_scraping.archive_api.LOGGER.warning")
@patch("ercot_scraping.archive_api.DAM_TABLE_DATA_MAPPING", {
    "test_table": {
        "model_class": mock.Mock(),
        "insert_query": "INSERT INTO test_table (col1, col2) VALUES (:col1, :col2)"
    }
})
def test_process_dam_file_edge_cases(
    mock_logger_warning, mock_store_data, test_id, csv_content, table_name, expected_warning
):
    # Arrange
    zip_buffer = BytesIO()
    with zipfile.ZipFile(zip_buffer, "w", zipfile.ZIP_DEFLATED) as zip_file:
        zip_file.writestr("data.csv", csv_content)
    zip_buffer.seek(0)
    zip_folder = zipfile.ZipFile(zip_buffer, 'r')

    db_name = "test_db"

    # Act
    process_dam_file(zip_folder, "data.csv", table_name, db_name)

    # Assert
    mock_store_data.assert_not_called()

    if expected_warning:
        mock_logger_warning.assert_called_once_with(expected_warning)
    else:
        mock_logger_warning.assert_not_called()

    zip_folder.close()


@patch("ercot_scraping.archive_api.DAM_TABLE_DATA_MAPPING", {})
@patch("ercot_scraping.archive_api.store_data_to_db")
@patch("ercot_scraping.archive_api.LOGGER.warning")
def test_process_dam_file_no_table_mapping(mock_logger_warning, mock_store_data):
    # Arrange
    csv_content = "col1,col2\n1,2"
    table_name = "test_table"
    zip_buffer = BytesIO()
    with zipfile.ZipFile(zip_buffer, "w") as zip_file:
        zip_file.writestr("data.csv", csv_content)
    zip_buffer.seek(0)
    zip_folder = zipfile.ZipFile(zip_buffer)
    db_name = "test_db"

    # Act
    process_dam_file(zip_folder, "data.csv", table_name, db_name)

    # Assert
    mock_logger_warning.assert_called_once()
    mock_store_data.assert_not_called()


@pytest.mark.parametrize(
    "test_id, product_id, start_date, end_date, mock_responses, expected_doc_ids",
    [
        (
            "happy_path_single_page",
            "1",
            "2024-07-01",
            "2024-07-01",
            [{"archives": [{"docId": 1}, {"docId": 2}], "_meta": {"totalPages": 1}}],
            [1, 2],
        ),
        (
            "happy_path_multiple_pages",
            "1",
            "2024-07-01",
            "2024-07-02",
            [
                {"archives": [{"docId": 1}, {"docId": 2}],
                    "_meta": {"totalPages": 2}},
                {"archives": [{"docId": 3}, {"docId": 4}],
                    "_meta": {"totalPages": 2}},
            ],
            [1, 2, 3, 4],
        ),
        (
            "edge_case_no_archives",
            "1",
            "2024-07-01",
            "2024-07-01",
            [{"archives": [], "_meta": {"totalPages": 1}}],
            [],
        ),

    ],
)
def test_get_archive_document_ids(
    test_id, product_id, start_date, end_date, mock_responses, expected_doc_ids
):
    # Arrange
    mock_response = MagicMock()
    mock_response.json.side_effect = mock_responses
    with patch("ercot_scraping.archive_api.rate_limited_request", return_value=mock_response) as mock_request:

        # Act
        doc_ids = get_archive_document_ids(product_id, start_date, end_date)

        # Assert
        assert doc_ids == expected_doc_ids
