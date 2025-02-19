import pytest
from unittest import mock
from ercot_scraping.archive_api import download_spp_archive_files
from ercot_scraping.config import LOGGER, ERCOT_API_REQUEST_HEADERS, ERCOT_ARCHIVE_API_BASE_URL, API_MAX_ARCHIVE_FILES
from ercot_scraping.batched_api import rate_limited_request


@mock.patch('ercot_scraping.archive_api.rate_limited_request')
@mock.patch('ercot_scraping.archive_api.zipfile.ZipFile')
@mock.patch('ercot_scraping.archive_api.LOGGER')
def test_download_spp_archive_files_no_doc_ids(mock_logger, mock_zipfile, mock_rate_limited_request):
    download_spp_archive_files('product_id', [], 'db_name')
    mock_logger.warning.assert_called_once_with(
        "No document IDs found for SPP product product_id")


@mock.patch('ercot_scraping.archive_api.rate_limited_request')
@mock.patch('ercot_scraping.archive_api.zipfile.ZipFile')
@mock.patch('ercot_scraping.archive_api.LOGGER')
def test_download_spp_archive_files_success(mock_logger, mock_zipfile, mock_rate_limited_request):
    mock_response = mock.Mock()
    mock_response.ok = True
    mock_response.content = b'zip_content'
    mock_rate_limited_request.return_value = mock_response

    mock_zip = mock.Mock()
    mock_zip.namelist.return_value = ['file1.zip']
    mock_zipfile.return_value.__enter__.return_value = mock_zip

    nested_zip = mock.Mock()
    nested_zip.namelist.return_value = ['file1.csv']
    mock_zip.open.return_value.__enter__.return_value.read.return_value = b'nested_zip_content'
    mock_zipfile.side_effect = [mock_zip, nested_zip]

    download_spp_archive_files('product_id', [1, 2, 3], 'db_name')

    mock_logger.info.assert_any_call(
        "Downloading 3 SPP documents from archive API")
    mock_logger.info.assert_any_call("Processing SPP file: file1.csv")


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
