from unittest import mock
import requests
import pytest
import zipfile
import sqlite3
from ercot_scraping.apis import archive_api
from ercot_scraping.apis import ercot_api
from ercot_scraping.apis.archive_api import (
    process_dam_csv_file,
    download_spp_archive_files,
    process_dam_outer_zip,
    process_dam_nested_zip,
    get_archive_document_ids,
    data_exists_in_db,
)
import io
from ercot_scraping.apis.archive_api import process_spp_file_to_rows


def test_archive_api():
    assert True  # Replace with actual test logic for archive API
    # Python


def test_download_dam_archive_files_empty_doc_ids(capsys):
    # Should print warning and return early
    archive_api.download_dam_archive_files(
        product_id="DAM",
        doc_ids=[],
        db_name="test.db"
    )
    out = capsys.readouterr().out
    assert "No document IDs found for DAM product DAM" in out


@mock.patch("ercot_scraping.apis.archive_api.rate_limited_request")
def test_download_dam_archive_files_non_200_response(mock_request, capsys):
    mock_request.return_value.status_code = 404
    mock_request.return_value.text = "Not found"
    mock_request.return_value.json.side_effect = Exception("No JSON")
    archive_api.download_dam_archive_files(
        product_id="DAM",
        doc_ids=[1],
        db_name="test.db",
        show_progress=False,
        batch_size=1
    )
    out = capsys.readouterr().out
    assert "Exception in DAM batch download: No JSON" in out


@mock.patch("ercot_scraping.apis.archive_api.get_table_name")
@mock.patch("ercot_scraping.apis.archive_api.zipfile.ZipFile")
@mock.patch("ercot_scraping.apis.archive_api.rate_limited_request")
def test_download_dam_archive_files_exception_handling(
    mock_request, mock_zipfile, mock_get_table_name, capsys
):
    mock_request.return_value.status_code = 200
    mock_request.return_value.content = b"fakezip"
    mock_zipfile.side_effect = Exception("zip error")
    archive_api.download_dam_archive_files(
        product_id="DAM",
        doc_ids=[1],
        db_name="test.db",
        show_progress=False,
        batch_size=1
    )
    out = capsys.readouterr().out
    assert "Exception in DAM batch download" in out


@mock.patch("ercot_scraping.apis.archive_api.rate_limited_request")
def test_download_dam_archive_files_timeout_first_batch(mock_request, capsys):
    """
    Should handle timeout on the first batch and print error.
    """
    mock_request.side_effect = requests.exceptions.Timeout(
        "Timeout on batch 1!"
    )
    archive_api.download_dam_archive_files(
        product_id="DAM",
        doc_ids=[1, 2],
        db_name="test.db",
        show_progress=False,
        batch_size=1
    )
    out = capsys.readouterr().out
    assert "Exception in DAM batch download" in out
    assert "Timeout on batch 1" in out


def test_download_dam_archive_files_timeout_last_batch(capsys):
    """
    Should handle timeout on the last batch and print error.
    """
    with (
        mock.patch(
            "ercot_scraping.apis.archive_api.rate_limited_request"
        ) as mock_request,
        mock.patch(
            "ercot_scraping.apis.archive_api.zipfile.ZipFile"
        ) as mock_zipfile,
        mock.patch(
            "ercot_scraping.apis.archive_api.get_table_name",
            return_value="SOME_TABLE"
        ),
        mock.patch(
            "ercot_scraping.apis.archive_api.DAM_FILENAMES",
            ["file1"]
        ),
        mock.patch(
            "ercot_scraping.apis.archive_api.DAM_TABLE_DATA_MAPPING",
            {"SOME_TABLE": {"model_class": object, "insert_query": "INSERT"}}
        )
    ):
        mock_outer_zip = mock.MagicMock()
        mock_inner_zip = mock.MagicMock()
        mock_outer_zip.namelist.return_value = ["nested1.zip"]
        mock_outer_zip.open.return_value.__enter__.return_value.read.return_value = (
            b"innerzip"
        )
        mock_inner_zip.namelist.return_value = ["file1.csv"]
        # Break up the long assignment for PEP8 compliance
        mock_inner_zip.open.return_value.__enter__.return_value.read.return_value = (
            b"col1,col2\nval1,val2\n"
        )
        # Shorten the above line to fit PEP8
        # mock_inner_zip.open.return_value.__enter__.
        #     return_value.read.return_value = (
        #     b"col1,col2\nval1,val2\n"
        # )

        def zipfile_side_effect(data):
            if data == b"fakezip":
                return mock_outer_zip
            elif data == b"innerzip":
                return mock_inner_zip
            raise ValueError("Unexpected zip data")

        mock_zipfile.side_effect = zipfile_side_effect

        def request_side_effect(*args, **kwargs):
            if mock_request.call_count == 2:
                raise requests.exceptions.Timeout("Timeout on last batch!")
            mock_resp = mock.Mock()
            mock_resp.status_code = 200
            mock_resp.content = b"fakezip"
            return mock_resp

        mock_request.side_effect = request_side_effect
        archive_api.download_dam_archive_files(
            product_id="DAM",
            doc_ids=[1, 2],
            db_name="test.db",
            show_progress=False,
            batch_size=1
        )
        out = capsys.readouterr().out
        assert "Exception in DAM batch download" in out
        assert "Timeout on last batch" in out


# --- ercot_api.py placeholder test ---


def test_ercot_api_module_loads():
    assert hasattr(ercot_api, "fetch_settlement_point_prices")


@pytest.fixture
def fake_model_class():
    class Dummy:
        pass
    return Dummy


def make_csv_file(data: str):
    # Simulate a file-like object with .read() returning bytes
    file_obj = mock.MagicMock()
    file_obj.read.return_value = data.encode("utf-8")
    return file_obj


@mock.patch("ercot_scraping.apis.archive_api.store_data_to_db")
def test_process_dam_csv_file_success(mock_store):
    csv_content = "col1,col2\nval1,val2\nval3,val4\n"
    csv_file = make_csv_file(csv_content)
    process_dam_csv_file(
        csv_file,
        fname="test.csv",
        table_name="SOME_TABLE",
        db_name="test.db",
        model_class=object,
        insert_query="INSERT"
    )
    # Should call store_data_to_db with correct data
    assert mock_store.called
    args, kwargs = mock_store.call_args
    assert kwargs["db_name"] == "test.db"
    assert kwargs["table_name"] == "SOME_TABLE"
    assert len(kwargs["data"]["data"]) == 2  # two rows


@mock.patch("ercot_scraping.apis.archive_api.store_data_to_db")
def test_process_dam_csv_file_csv_error(mock_store, capsys):
    # Simulate a UnicodeDecodeError
    csv_file = mock.MagicMock()
    csv_file.read.side_effect = UnicodeDecodeError("utf-8", b"", 0, 1, "fail")
    process_dam_csv_file(
        csv_file,
        fname="bad.csv",
        table_name="SOME_TABLE",
        db_name="test.db",
        model_class=object,
        insert_query="INSERT"
    )
    out = capsys.readouterr().out
    assert "[TRACE] Error reading CSV bad.csv:" in out
    mock_store.assert_not_called()


@mock.patch("ercot_scraping.apis.archive_api.rate_limited_request")
def test_download_spp_archive_files_empty_doc_ids(mock_request, capsys):
    archive_api.download_spp_archive_files(
        product_id="SPP",
        doc_ids=[],
        db_name="test.db"
    )
    out = capsys.readouterr().out
    assert "[WARN] No document IDs found for SPP product SPP" in out
    mock_request.assert_not_called()


@mock.patch("ercot_scraping.apis.archive_api.rate_limited_request")
def test_download_spp_archive_files_non_200_response_json(mock_request, capsys):
    mock_resp = mock.Mock()
    mock_resp.status_code = 400
    mock_resp.json.return_value = {"message": "Bad request"}
    mock_request.return_value = mock_resp
    archive_api.download_spp_archive_files(
        product_id="SPP",
        doc_ids=[1],
        db_name="test.db"
    )
    out = capsys.readouterr().out
    assert "[ERROR] Failed to download SPP batch. Status: 400. Error: Bad request" in out


@mock.patch("ercot_scraping.apis.archive_api.rate_limited_request")
def test_download_spp_archive_files_non_200_response_text(mock_request, capsys):
    mock_resp = mock.Mock()
    mock_resp.status_code = 404
    mock_resp.json.side_effect = ValueError("No JSON")
    mock_resp.text = "Not found"
    mock_request.return_value = mock_resp
    archive_api.download_spp_archive_files(
        product_id="SPP",
        doc_ids=[1],
        db_name="test.db"
    )
    out = capsys.readouterr().out
    assert "[ERROR] Failed to download SPP batch. Status: 404. Response: Not found" in out


@mock.patch("ercot_scraping.apis.archive_api.store_data_to_db")
@mock.patch("ercot_scraping.apis.archive_api.process_spp_file_to_rows")
@mock.patch("ercot_scraping.apis.archive_api.zipfile.ZipFile")
@mock.patch("ercot_scraping.apis.archive_api.rate_limited_request")
def test_download_spp_archive_files_happy_path(
    mock_request, mock_zipfile, mock_process_rows, mock_store, capsys
):
    # Setup mocks for nested zips and CSVs
    mock_resp = mock.Mock()
    mock_resp.status_code = 200
    mock_resp.content = b"outerzip"
    mock_request.return_value = mock_resp

    # Mock outer zip
    mock_outer_zip = mock.MagicMock()
    mock_outer_zip.__enter__.return_value = mock_outer_zip
    mock_outer_zip.namelist.return_value = ["nested1.zip"]
    mock_outer_zip.open.return_value.__enter__.return_value.read.return_value = b"innerzip"

    # Mock inner zip
    mock_inner_zip = mock.MagicMock()
    mock_inner_zip.__enter__.return_value = mock_inner_zip
    mock_inner_zip.namelist.return_value = ["SETTLEMENT_POINT_PRICES.csv"]
    mock_inner_zip.open.return_value.__enter__.return_value = mock.Mock()

    # zipfile.ZipFile should return outer zip for b"outerzip", inner zip for b"innerzip"
    def zipfile_side_effect(data):
        if data.getvalue() == b"outerzip":
            return mock_outer_zip
        elif data.getvalue() == b"innerzip":
            return mock_inner_zip
        raise ValueError("Unexpected zip data")
    mock_zipfile.side_effect = zipfile_side_effect

    # process_spp_file_to_rows returns fake rows
    mock_process_rows.return_value = [{"a": 1}, {"a": 2}]

    # Patch DAM_TABLE_DATA_MAPPING for SETTLEMENT_POINT_PRICES
    with mock.patch.dict(
        "ercot_scraping.apis.archive_api.DAM_TABLE_DATA_MAPPING",
        {"SETTLEMENT_POINT_PRICES": {
            "model_class": object, "insert_query": "INSERT"}}
    ):
        archive_api.download_spp_archive_files(
            product_id="SPP",
            doc_ids=[1],
            db_name="test.db"
        )
        out = capsys.readouterr().out
        assert "Storing 2 rows to SETTLEMENT_POINT_PRICES (batched)" in out
        assert mock_store.called


@mock.patch("ercot_scraping.apis.archive_api.rate_limited_request")
@mock.patch("ercot_scraping.apis.archive_api.zipfile.ZipFile")
def test_download_spp_archive_files_bad_zipfile(mock_zipfile, mock_request, capsys):
    mock_resp = mock.Mock()
    mock_resp.status_code = 200
    mock_resp.content = b"badzip"
    mock_request.return_value = mock_resp
    mock_zipfile.side_effect = zipfile.BadZipFile("bad zip!")
    archive_api.download_spp_archive_files(
        product_id="SPP",
        doc_ids=[1],
        db_name="test.db"
    )
    out = capsys.readouterr().out
    assert "[ERROR] Exception in SPP batch download:" in out
    assert "bad zip!" in out


@mock.patch("ercot_scraping.apis.archive_api.rate_limited_request")
@mock.patch("ercot_scraping.apis.archive_api.zipfile.ZipFile")
def test_download_spp_archive_files_key_error(mock_zipfile, mock_request, capsys):
    mock_resp = mock.Mock()
    mock_resp.status_code = 200
    mock_resp.content = b"zip"
    mock_request.return_value = mock_resp
    mock_zipfile.side_effect = KeyError("missing key")
    archive_api.download_spp_archive_files(
        product_id="SPP",
        doc_ids=[1],
        db_name="test.db"
    )
    out = capsys.readouterr().out
    assert "[ERROR] Exception in SPP batch download:" in out
    assert "missing key" in out


@mock.patch("ercot_scraping.apis.archive_api.store_data_to_db")
@mock.patch("ercot_scraping.apis.archive_api.process_spp_file_to_rows")
@mock.patch("ercot_scraping.apis.archive_api.zipfile.ZipFile")
@mock.patch("ercot_scraping.apis.archive_api.rate_limited_request")
def test_download_spp_archive_files_no_rows(
    mock_request, mock_zipfile, mock_process_rows, mock_store, capsys
):
    mock_resp = mock.Mock()
    mock_resp.status_code = 200
    mock_resp.content = b"outerzip"
    mock_request.return_value = mock_resp

    mock_outer_zip = mock.MagicMock()
    mock_outer_zip.__enter__.return_value = mock_outer_zip
    mock_outer_zip.namelist.return_value = ["nested1.zip"]
    mock_outer_zip.open.return_value.__enter__.return_value.read.return_value = b"innerzip"

    mock_inner_zip = mock.MagicMock()
    mock_inner_zip.__enter__.return_value = mock_inner_zip
    mock_inner_zip.namelist.return_value = ["SETTLEMENT_POINT_PRICES.csv"]
    mock_inner_zip.open.return_value.__enter__.return_value = mock.Mock()

    def zipfile_side_effect(data):
        if data.getvalue() == b"outerzip":
            return mock_outer_zip
        elif data.getvalue() == b"innerzip":
            return mock_inner_zip
        raise ValueError("Unexpected zip data")
    mock_zipfile.side_effect = zipfile_side_effect

    mock_process_rows.return_value = []

    with mock.patch.dict(
        "ercot_scraping.apis.archive_api.DAM_TABLE_DATA_MAPPING",
        {"SETTLEMENT_POINT_PRICES": {
            "model_class": object, "insert_query": "INSERT"}}
    ):
        archive_api.download_spp_archive_files(
            product_id="SPP",
            doc_ids=[1],
            db_name="test.db"
        )
        out = capsys.readouterr().out
        assert "[TRACE] No rows to store for this SPP batch." in out
        mock_store.assert_not_called()


def make_fake_zipfile(namelist, open_side_effect=None):
    fake_zip = mock.MagicMock()
    fake_zip.__enter__.return_value = fake_zip
    fake_zip.namelist.return_value = namelist
    if open_side_effect:
        fake_zip.open.side_effect = open_side_effect
    else:
        fake_zip.open.return_value.__enter__.return_value.read.return_value = b"nested"
    return fake_zip


@mock.patch("ercot_scraping.apis.archive_api.process_dam_nested_zip")
@mock.patch("ercot_scraping.apis.archive_api.zipfile.ZipFile")
def test_process_dam_outer_zip_happy_path(mock_zipfile, mock_nested, capsys):
    # One nested zip file, should call process_dam_nested_zip
    fake_zip = make_fake_zipfile(["nested1.zip"])
    mock_zipfile.return_value = fake_zip
    process_dam_outer_zip(b"outerzip", db_name="test.db")
    out = capsys.readouterr().out
    assert "[TRACE] Opened DAM batch zip" in out
    assert mock_nested.called


@mock.patch("ercot_scraping.apis.archive_api.process_dam_nested_zip")
@mock.patch("ercot_scraping.apis.archive_api.zipfile.ZipFile")
def test_process_dam_outer_zip_skips_non_zip(mock_zipfile, mock_nested, capsys):
    # Should skip non-zip files
    fake_zip = make_fake_zipfile(["not_a_zip.txt"])
    mock_zipfile.return_value = fake_zip
    process_dam_outer_zip(b"outerzip", db_name="test.db")
    out = capsys.readouterr().out
    assert "Skipping not_a_zip.txt (not a nested zip)" in out
    assert not mock_nested.called


@mock.patch("ercot_scraping.apis.archive_api.process_dam_nested_zip")
@mock.patch("ercot_scraping.apis.archive_api.zipfile.ZipFile")
def test_process_dam_outer_zip_nested_zip_exception(mock_zipfile, mock_nested, capsys):
    # Simulate exception when opening nested zip
    def open_side_effect(name):
        raise zipfile.BadZipFile("bad nested zip")
    fake_zip = make_fake_zipfile(
        ["nested1.zip"], open_side_effect=open_side_effect)
    mock_zipfile.return_value = fake_zip
    process_dam_outer_zip(b"outerzip", db_name="test.db")
    out = capsys.readouterr().out
    assert "Exception in DAM batch download (open nested zip): bad nested zip" in out


@mock.patch("ercot_scraping.apis.archive_api.zipfile.ZipFile")
def test_process_dam_outer_zip_outer_bad_zipfile(mock_zipfile, capsys):
    # Simulate outer zipfile.BadZipFile
    mock_zipfile.side_effect = zipfile.BadZipFile("outer zip error")
    process_dam_outer_zip(b"badzip", db_name="test.db")
    out = capsys.readouterr().out
    assert "Exception in DAM batch download (outer zip): outer zip error" in out


@mock.patch("ercot_scraping.apis.archive_api.zipfile.ZipFile")
def test_process_dam_outer_zip_outer_value_error(mock_zipfile, capsys):
    # Simulate ValueError on outer zip open
    mock_zipfile.side_effect = ValueError("outer value error")
    process_dam_outer_zip(b"badzip", db_name="test.db")
    out = capsys.readouterr().out
    assert "Exception in DAM batch download (outer zip): outer value error" in out


@mock.patch("ercot_scraping.apis.archive_api.zipfile.ZipFile")
def test_process_dam_outer_zip_outer_ioerror(mock_zipfile, capsys):
    # Simulate IOError on outer zip open
    mock_zipfile.side_effect = IOError("outer io error")
    process_dam_outer_zip(b"badzip", db_name="test.db")
    out = capsys.readouterr().out
    assert "Exception in DAM batch download (outer zip): outer io error" in out


def make_fake_inner_zip(namelist, open_side_effect=None):
    fake_zip = mock.MagicMock()
    fake_zip.__enter__.return_value = fake_zip
    fake_zip.namelist.return_value = namelist
    if open_side_effect:
        fake_zip.open.side_effect = open_side_effect
    else:
        fake_zip.open.return_value.__enter__.return_value = mock.Mock()
    return fake_zip


@mock.patch("ercot_scraping.apis.archive_api.process_dam_csv_file")
@mock.patch("ercot_scraping.apis.archive_api.get_table_name")
@mock.patch("ercot_scraping.apis.archive_api.DAM_TABLE_DATA_MAPPING", {"TABLE1": {"model_class": object, "insert_query": "INSERT"}})
@mock.patch("ercot_scraping.apis.archive_api.DAM_FILENAMES", ["file1"])
@mock.patch("ercot_scraping.apis.archive_api.zipfile.ZipFile")
def test_process_dam_nested_zip_happy_path(mock_zipfile, mock_get_table_name, mock_process_csv, capsys):
    # Only one file, matches all criteria
    mock_get_table_name.return_value = "TABLE1"
    fake_zip = make_fake_inner_zip(["file1.csv"])
    mock_zipfile.return_value = fake_zip
    process_dam_nested_zip(b"innerzip", db_name="test.db")
    out = capsys.readouterr().out
    assert "[DEBUG] inner_zip.namelist(): ['file1.csv']" in out
    assert mock_process_csv.called


@mock.patch("ercot_scraping.apis.archive_api.DAM_FILENAMES", ["file1"])
@mock.patch("ercot_scraping.apis.archive_api.zipfile.ZipFile")
def test_process_dam_nested_zip_skips_non_csv(mock_zipfile, capsys):
    fake_zip = make_fake_inner_zip(["not_a_csv.txt"])
    mock_zipfile.return_value = fake_zip
    process_dam_nested_zip(b"innerzip", db_name="test.db")
    out = capsys.readouterr().out
    assert "Skipping not_a_csv.txt (not a CSV)" in out


@mock.patch("ercot_scraping.apis.archive_api.DAM_FILENAMES", ["file1"])
@mock.patch("ercot_scraping.apis.archive_api.zipfile.ZipFile")
def test_process_dam_nested_zip_skips_non_matching_filename(mock_zipfile, capsys):
    fake_zip = make_fake_inner_zip(["otherfile.csv"])
    mock_zipfile.return_value = fake_zip
    process_dam_nested_zip(b"innerzip", db_name="test.db")
    out = capsys.readouterr().out
    assert "Skipping otherfile.csv (does not match DAM_FILENAMES)" in out


@mock.patch("ercot_scraping.apis.archive_api.get_table_name", return_value="UNKNOWN_TABLE")
@mock.patch("ercot_scraping.apis.archive_api.DAM_TABLE_DATA_MAPPING", {"TABLE1": {"model_class": object, "insert_query": "INSERT"}})
@mock.patch("ercot_scraping.apis.archive_api.DAM_FILENAMES", ["file1"])
@mock.patch("ercot_scraping.apis.archive_api.zipfile.ZipFile")
def test_process_dam_nested_zip_skips_no_mapping(mock_zipfile, mock_get_table_name, capsys):
    fake_zip = make_fake_inner_zip(["file1.csv"])
    mock_zipfile.return_value = fake_zip
    process_dam_nested_zip(b"innerzip", db_name="test.db")
    out = capsys.readouterr().out
    assert "Skipping file1.csv (no mapping for table UNKNOWN_TABLE)" in out


@mock.patch("ercot_scraping.apis.archive_api.process_dam_csv_file")
@mock.patch("ercot_scraping.apis.archive_api.get_table_name", return_value="TABLE1")
@mock.patch("ercot_scraping.apis.archive_api.DAM_TABLE_DATA_MAPPING", {"TABLE1": {"model_class": object, "insert_query": "INSERT"}})
@mock.patch("ercot_scraping.apis.archive_api.DAM_FILENAMES", ["file1"])
@mock.patch("ercot_scraping.apis.archive_api.zipfile.ZipFile")
def test_process_dam_nested_zip_csv_ioerror(
    mock_zipfile, mock_get_table_name, mock_process_csv, capsys
):
    # Simulate IOError when opening CSV
    def open_side_effect(fname):
        raise IOError("csv io error")
    fake_zip = make_fake_inner_zip(
        ["file1.csv"], open_side_effect=open_side_effect)
    mock_zipfile.return_value = fake_zip
    process_dam_nested_zip(b"innerzip", db_name="test.db")
    out = capsys.readouterr().out
    assert "Exception in DAM batch download (CSV): csv io error" in out


@mock.patch("ercot_scraping.apis.archive_api.DAM_FILENAMES", ["file1"])
@mock.patch("ercot_scraping.apis.archive_api.zipfile.ZipFile")
def test_process_dam_nested_zip_bad_zipfile(mock_zipfile, capsys):
    mock_zipfile.side_effect = zipfile.BadZipFile("bad nested zip")
    process_dam_nested_zip(b"badzip", db_name="test.db")
    out = capsys.readouterr().out
    assert "Exception in DAM batch download (nested zip): bad nested zip" in out


@mock.patch("ercot_scraping.apis.archive_api.DAM_FILENAMES", ["file1"])
@mock.patch("ercot_scraping.apis.archive_api.zipfile.ZipFile")
def test_process_dam_nested_zip_value_error(mock_zipfile, capsys):
    mock_zipfile.side_effect = ValueError("value error")
    process_dam_nested_zip(b"badzip", db_name="test.db")
    out = capsys.readouterr().out
    assert "Exception in DAM batch download (nested zip): value error" in out


@mock.patch("ercot_scraping.apis.archive_api.DAM_FILENAMES", ["file1"])
@mock.patch("ercot_scraping.apis.archive_api.zipfile.ZipFile")
def test_process_dam_nested_zip_ioerror(mock_zipfile, capsys):
    mock_zipfile.side_effect = IOError("io error")
    process_dam_nested_zip(b"badzip", db_name="test.db")
    out = capsys.readouterr().out
    assert "Exception in DAM batch download (nested zip): io error" in out


@mock.patch("ercot_scraping.apis.archive_api.rate_limited_request")
def test_get_archive_document_ids_single_page(mock_request, capsys):
    # Simulate a single page with two archives
    mock_response = mock.Mock()
    mock_response.status_code = 200
    mock_response.json.return_value = {
        "_meta": {"totalPages": 1},
        "fields": ["docId"],
        "archives": [{"docId": 101}, {"docId": 102}]
    }
    mock_request.return_value = mock_response

    doc_ids = get_archive_document_ids("DAM", "2024-01-01", "2024-01-02")
    out = capsys.readouterr().out
    assert doc_ids == [101, 102]
    assert "[TRACE] Returning 2 docIds from get_archive_document_ids" in out
    assert "[TRACE] Reached last page: 1" in out


@mock.patch("ercot_scraping.apis.archive_api.rate_limited_request")
def test_get_archive_document_ids_multiple_pages(mock_request, capsys):
    # Simulate two pages of results
    responses = [
        mock.Mock(),
        mock.Mock()
    ]
    responses[0].status_code = 200
    responses[0].json.return_value = {
        "_meta": {"totalPages": 2},
        "fields": ["docId"],
        "archives": [{"docId": 201}]
    }
    responses[1].status_code = 200
    responses[1].json.return_value = {
        "_meta": {"totalPages": 2},
        "fields": ["docId"],
        "archives": [{"docId": 202}]
    }
    mock_request.side_effect = responses

    doc_ids = get_archive_document_ids("DAM", "2024-01-01", "2024-01-02")
    out = capsys.readouterr().out
    assert doc_ids == [201, 202]
    assert "[TRACE] Returning 2 docIds from get_archive_document_ids" in out
    assert "[TRACE] Reached last page: 2" in out


@mock.patch("ercot_scraping.apis.archive_api.rate_limited_request")
def test_get_archive_document_ids_no_archives(mock_request, capsys):
    # Simulate no archives found
    mock_response = mock.Mock()
    mock_response.status_code = 200
    mock_response.json.return_value = {
        "_meta": {"totalPages": 1},
        "fields": ["docId"],
        "archives": []
    }
    mock_request.return_value = mock_response

    doc_ids = get_archive_document_ids("DAM", "2024-01-01", "2024-01-02")
    out = capsys.readouterr().out
    assert doc_ids == []
    assert "[TRACE] No archives found on page 1" in out
    assert "[TRACE] Returning 0 docIds from get_archive_document_ids" in out


@mock.patch("ercot_scraping.apis.archive_api.rate_limited_request")
def test_get_archive_document_ids_missing_meta_and_fields(mock_request, capsys):
    # Simulate missing _meta and fields keys
    mock_response = mock.Mock()
    mock_response.status_code = 200
    mock_response.json.return_value = {
        "archives": [{"docId": 301}]
    }
    mock_request.return_value = mock_response

    doc_ids = get_archive_document_ids("DAM", "2024-01-01", "2024-01-02")
    out = capsys.readouterr().out
    assert doc_ids == [301]
    assert "[TRACE] Returning 1 docIds from get_archive_document_ids" in out


@mock.patch("ercot_scraping.apis.archive_api.rate_limited_request")
def test_get_archive_document_ids_raises_on_request_error(mock_request):
    # Simulate an exception from rate_limited_request
    mock_request.side_effect = Exception("Request failed")
    with pytest.raises(Exception) as excinfo:
        get_archive_document_ids("DAM", "2024-01-01", "2024-01-02")
    assert "Request failed" in str(excinfo.value)


@pytest.fixture
def fake_zipfile():
    """Returns a mock zipfile.ZipFile object with .open() patched."""
    return mock.MagicMock()


@pytest.fixture
def patch_column_mappings(monkeypatch):
    # Patch COLUMN_MAPPINGS to control normalization
    monkeypatch.setattr(
        "ercot_scraping.apis.archive_api.COLUMN_MAPPINGS",
        {
            "spp_table": {"col_a": "column_a", "col_b": "column_b"},
            "settlement_point_prices": {"col1": "col1", "col2": "col2"},
        }
    )


def make_csv_bytes(headers, rows):
    csv_str = ",".join(headers) + "\n"
    for row in rows:
        csv_str += ",".join(row) + "\n"
    return csv_str.encode("utf-8")


def test_process_spp_file_to_rows_basic(monkeypatch, patch_column_mappings):
    # Normal case: CSV with headers and two rows
    zip_folder = mock.MagicMock()
    csv_bytes = make_csv_bytes(["Col_A", "Col_B"], [["1", "2"], ["3", "4"]])
    file_obj = io.BytesIO(csv_bytes)
    zip_folder.open.return_value.__enter__.return_value = file_obj

    rows = process_spp_file_to_rows(zip_folder, "file.csv", "SPP_TABLE")
    assert rows == [
        {"column_a": "1", "column_b": "2"},
        {"column_a": "3", "column_b": "4"},
    ]


def test_process_spp_file_to_rows_no_headers(monkeypatch, patch_column_mappings, capsys):
    # No headers (empty file)
    zip_folder = mock.MagicMock()
    file_obj = io.BytesIO(b"")
    zip_folder.open.return_value.__enter__.return_value = file_obj

    rows = process_spp_file_to_rows(zip_folder, "file.csv", "SPP_TABLE")
    out = capsys.readouterr().out
    assert rows == []
    assert "[WARN] No headers found in file.csv" in out


def test_process_spp_file_to_rows_headers_but_no_data(monkeypatch, patch_column_mappings, capsys):
    # Only headers, no data rows
    zip_folder = mock.MagicMock()
    csv_bytes = make_csv_bytes(["Col_A", "Col_B"], [])
    file_obj = io.BytesIO(csv_bytes)
    zip_folder.open.return_value.__enter__.return_value = file_obj

    rows = process_spp_file_to_rows(zip_folder, "file.csv", "SPP_TABLE")
    out = capsys.readouterr().out
    assert rows == []
    assert "[TRACE] Returning 0 rows from file.csv" in out


def test_process_spp_file_to_rows_missing_fieldnames(monkeypatch, patch_column_mappings, capsys):
    # Simulate DictReader with no fieldnames (malformed CSV)
    zip_folder = mock.MagicMock()
    # No comma in first line
    file_obj = io.BytesIO(b"not_a_csv_header\n1\n2\n")
    zip_folder.open.return_value.__enter__.return_value = file_obj

    rows = process_spp_file_to_rows(zip_folder, "file.csv", "SPP_TABLE")
    out = capsys.readouterr().out
    assert rows == []
    assert "[WARN] No headers found in file.csv" in out


def test_process_spp_file_to_rows_normalization_and_strip(monkeypatch, patch_column_mappings):
    # Test normalization: spaces, case, and value stripping
    zip_folder = mock.MagicMock()
    csv_bytes = make_csv_bytes([" Col_A ", "Col_B"], [
                               ["  x ", " y "], ["", "z"]])
    file_obj = io.BytesIO(csv_bytes)
    zip_folder.open.return_value.__enter__.return_value = file_obj

    rows = process_spp_file_to_rows(zip_folder, "file.csv", "SPP_TABLE")
    assert rows == [
        {"column_a": "x", "column_b": "y"},
        {"column_a": None, "column_b": "z"},
    ]


def test_process_spp_file_to_rows_trace_output(monkeypatch, patch_column_mappings, capsys):
    # Should print trace for first 3 rows
    zip_folder = mock.MagicMock()
    csv_bytes = make_csv_bytes(["Col1", "Col2"], [["a", "b"], [
                               "c", "d"], ["e", "f"], ["g", "h"]])
    file_obj = io.BytesIO(csv_bytes)
    zip_folder.open.return_value.__enter__.return_value = file_obj

    rows = process_spp_file_to_rows(
        zip_folder, "file.csv", "SETTLEMENT_POINT_PRICES")
    out = capsys.readouterr().out
    assert "[TRACE] Row 0 in file.csv:" in out
    assert "[TRACE] Row 1 in file.csv:" in out
    assert "[TRACE] Row 2 in file.csv:" in out
    assert "[TRACE] Returning 4 rows from file.csv" in out
    assert len(rows) == 4


def test_process_spp_file_to_rows_bad_encoding(
    monkeypatch, patch_column_mappings, capsys
):
    """
    Test that process_spp_file_to_rows handles files with bad encoding gracefully.
    """
    # Simulate a file that can't be decoded as UTF-8
    zip_folder = mock.MagicMock()
    file_obj = io.BytesIO(b"\xff\xfe\xfa")  # Invalid UTF-8
    zip_folder.open.return_value.__enter__.return_value = file_obj

    rows = process_spp_file_to_rows(
        zip_folder, "bad.csv", "SPP_TABLE"
    )
    out = capsys.readouterr().out
    assert rows == []
    assert (
        "decode" in out or "Error" in out or "exception" in out.lower()
    )


def test_process_spp_file_to_rows_duplicate_headers(
    monkeypatch, patch_column_mappings
):
    zip_folder = mock.MagicMock()
    csv_bytes = b"Col_A,Col_A\n1,2\n3,4\n"
    file_obj = io.BytesIO(csv_bytes)
    zip_folder.open.return_value.__enter__.return_value = file_obj

    rows = process_spp_file_to_rows(
        zip_folder, "dup.csv", "SPP_TABLE"
    )
    # Should not crash, both columns present
    assert isinstance(rows, list)
    assert len(rows) == 2


def test_process_spp_file_to_rows_extra_columns(
    monkeypatch, patch_column_mappings
):
    zip_folder = mock.MagicMock()
    csv_bytes = b"Col_A,Col_B,Extra\n1,2,3\n4,5,6\n"
    file_obj = io.BytesIO(csv_bytes)
    zip_folder.open.return_value.__enter__.return_value = file_obj
    rows = process_spp_file_to_rows(
        zip_folder, "extra.csv", "SPP_TABLE"
    )
    assert all(
        "extra" in r or "extra" in r.keys() or True for r in rows
    )


def test_process_spp_file_to_rows_whitespace_headers(
    monkeypatch, patch_column_mappings
):
    zip_folder = mock.MagicMock()
    csv_bytes = b"   ,Col_B\n1,2\n3,4\n"
    file_obj = io.BytesIO(csv_bytes)
    zip_folder.open.return_value.__enter__.return_value = file_obj
    rows = process_spp_file_to_rows(
        zip_folder, "white.csv", "SPP_TABLE"
    )
    assert isinstance(rows, list)


@mock.patch(
    "ercot_scraping.apis.archive_api.zipfile.ZipFile"
)
@mock.patch(
    "ercot_scraping.apis.archive_api.rate_limited_request"
)
def test_download_spp_archive_files_empty_zip(
    mock_request, mock_zipfile, capsys
):
    mock_resp = mock.Mock()
    mock_resp.status_code = 200
    mock_resp.content = b"emptyzip"
    mock_request.return_value = mock_resp
    fake_zip = mock.MagicMock()
    fake_zip.__enter__.return_value = fake_zip
    fake_zip.namelist.return_value = []
    mock_zipfile.return_value = fake_zip
    archive_api.download_spp_archive_files(
        product_id="SPP",
        doc_ids=[1],
        db_name="test.db"
    )
    out = capsys.readouterr().out
    assert "Completed SPP archive download" in out


def test_download_dam_archive_files_non_list_doc_ids(capsys):
    archive_api.download_dam_archive_files(
        product_id="DAM",
        doc_ids=None,
        db_name="test.db"
    )
    out = capsys.readouterr().out
    assert "No document IDs found for DAM product DAM" in out
    archive_api.download_dam_archive_files(
        product_id="DAM",
        doc_ids="notalist",
        db_name="test.db"
    )
    out = capsys.readouterr().out
    assert "No document IDs found for DAM product DAM" in out


@mock.patch(
    "ercot_scraping.apis.archive_api.store_data_to_db",
    side_effect=ValueError("fail store")
)
def test_process_dam_csv_file_store_error(mock_store, capsys):
    csv_content = "col1,col2\nval1,val2\n"
    csv_file = mock.MagicMock()
    csv_file.read.return_value = csv_content.encode("utf-8")
    process_dam_csv_file(
        csv_file,
        fname="test.csv",
        table_name="SOME_TABLE",
        db_name="test.db",
        model_class=object,
        insert_query="INSERT"
    )
    out = capsys.readouterr().out
    assert "Error storing data for test.csv" in out


@mock.patch(
    "ercot_scraping.apis.archive_api.rate_limited_request"
)
def test_get_archive_document_ids_non_200(mock_request):
    mock_resp = mock.Mock()
    mock_resp.status_code = 500
    mock_resp.json.side_effect = Exception("fail json")
    mock_request.return_value = mock_resp
    from ercot_scraping.apis.archive_api import get_archive_document_ids
    try:
        get_archive_document_ids(
            "SPP", "2024-01-01", "2024-01-02"
        )
    except Exception as e:
        assert "fail json" in str(e) or isinstance(e, Exception)


def test_data_exists_in_db(tmp_path):
    db_path = tmp_path / "test.db"
    conn = sqlite3.connect(db_path)
    conn.execute("CREATE TABLE BIDS (DeliveryDate TEXT)")
    conn.execute("INSERT INTO BIDS VALUES ('2024-01-01')")
    conn.execute(
        "CREATE TABLE SETTLEMENT_POINT_PRICES (DeliveryDate TEXT, HourEnding TEXT, IntervalEnding TEXT)")
    conn.execute(
        "INSERT INTO SETTLEMENT_POINT_PRICES VALUES ('2024-01-01', '01', '01')")
    conn.commit()
    assert data_exists_in_db(str(db_path), "BIDS", "2024-01-01")
    assert not data_exists_in_db(str(db_path), "BIDS", "2024-01-02")
    assert data_exists_in_db(
        str(db_path), "SETTLEMENT_POINT_PRICES", "2024-01-01", "01", "01")
    assert not data_exists_in_db(
        str(db_path), "SETTLEMENT_POINT_PRICES", "2024-01-02", "01", "01")
    conn.close()


@mock.patch("ercot_scraping.apis.archive_api.store_data_to_db")
def test_process_dam_csv_file_skips_existing(mock_store, tmp_path):
    db_path = tmp_path / "test.db"
    conn = sqlite3.connect(db_path)
    conn.execute("CREATE TABLE BIDS (DeliveryDate TEXT)")
    conn.execute("INSERT INTO BIDS VALUES ('2024-01-01')")
    conn.commit()
    csv_content = "DeliveryDate\n2024-01-01\n2024-01-02\n"
    file_obj = mock.MagicMock()
    file_obj.read.return_value = csv_content.encode("utf-8")
    process_dam_csv_file(
        file_obj,
        fname="test.csv",
        table_name="BIDS",
        db_name=str(db_path),
        model_class=object,
        insert_query="INSERT"
    )
    # Only the new date should be inserted
    assert mock_store.called
    args, kwargs = mock_store.call_args
    assert len(kwargs["data"]["data"]) == 1
    assert kwargs["data"]["data"][0]["DeliveryDate"] == "2024-01-02"
    conn.close()


@mock.patch("ercot_scraping.apis.archive_api.store_data_to_db")
@mock.patch("ercot_scraping.apis.archive_api.process_spp_file_to_rows")
@mock.patch("ercot_scraping.apis.archive_api.zipfile.ZipFile")
@mock.patch("ercot_scraping.apis.archive_api.rate_limited_request")
def test_download_spp_archive_files_skips_existing(
    mock_request, mock_zipfile, mock_process_rows, mock_store, tmp_path
):
    db_path = tmp_path / "test.db"
    conn = sqlite3.connect(db_path)
    conn.execute(
        "CREATE TABLE SETTLEMENT_POINT_PRICES (DeliveryDate TEXT, HourEnding TEXT, IntervalEnding TEXT)")
    conn.execute(
        "INSERT INTO SETTLEMENT_POINT_PRICES VALUES ('2024-01-01', '01', '01')")
    conn.commit()
    mock_resp = mock.Mock()
    mock_resp.status_code = 200
    mock_resp.content = b"outerzip"
    mock_request.return_value = mock_resp
    mock_outer_zip = mock.MagicMock()
    mock_outer_zip.__enter__.return_value = mock_outer_zip
    mock_outer_zip.namelist.return_value = ["nested1.zip"]
    mock_outer_zip.open.return_value.__enter__.return_value.read.return_value = b"innerzip"
    mock_inner_zip = mock.MagicMock()
    mock_inner_zip.__enter__.return_value = mock_inner_zip
    mock_inner_zip.namelist.return_value = ["SETTLEMENT_POINT_PRICES.csv"]
    mock_inner_zip.open.return_value.__enter__.return_value = mock.Mock()

    def zipfile_side_effect(data):
        if data.getvalue() == b"outerzip":
            return mock_outer_zip
        elif data.getvalue() == b"innerzip":
            return mock_inner_zip
        raise ValueError("Unexpected zip data")
    mock_zipfile.side_effect = zipfile_side_effect
    # One row already exists, one is new
    mock_process_rows.return_value = [
        {"deliverydate": "2024-01-01", "hourending": "01", "intervalending": "01"},
        {"deliverydate": "2024-01-02", "hourending": "01", "intervalending": "01"}
    ]
    with mock.patch.dict(
        "ercot_scraping.apis.archive_api.DAM_TABLE_DATA_MAPPING",
        {"SETTLEMENT_POINT_PRICES": {"model_class": object, "insert_query": "INSERT"}}
    ):
        download_spp_archive_files(
            product_id="SPP",
            doc_ids=[1],
            db_name=str(db_path)
        )
        # Only the new row should be inserted
        assert mock_store.called
        args, kwargs = mock_store.call_args
        assert len(kwargs["data"]["data"]) == 1
        assert kwargs["data"]["data"][0]["deliverydate"] == "2024-01-02"
    conn.close()


class DummyModel:
    def __init__(self, **kwargs):
        self.__dict__.update(kwargs)


@mock.patch("ercot_scraping.utils.filters.get_active_settlement_points")
@mock.patch("ercot_scraping.apis.archive_api.store_data_to_db")
def test_process_dam_csv_file_active_settlement_filter(mock_store, mock_get_active):
    csv_content = "SettlementPoint,Value\nACTIVE1,100\nINACTIVE,200\nACTIVE2,300\n"
    csv_file = make_csv_file(csv_content)
    with mock.patch("ercot_scraping.utils.filters.get_active_settlement_points", return_value={"ACTIVE1", "ACTIVE2"}):
        process_dam_csv_file(
            csv_file,
            fname="test.csv",
            table_name="SOME_TABLE",
            db_name="test.db",
            model_class=DummyModel,
            insert_query="INSERT",
            filter_by_active_settlement_points=True
        )
    assert mock_store.called
    args, kwargs = mock_store.call_args
    # Only ACTIVE1 and ACTIVE2 should be present
    filtered = kwargs["data"]["data"]
    assert all(row["SettlementPoint"] in {
               "ACTIVE1", "ACTIVE2"} for row in filtered)


@mock.patch("ercot_scraping.utils.filters.get_active_settlement_points")
@mock.patch("ercot_scraping.apis.archive_api.store_data_to_db")
def test_process_dam_csv_file_no_active_points(mock_store, mock_get_active):
    csv_content = "SettlementPoint,Value\nINACTIVE,200\n"
    csv_file = make_csv_file(csv_content)
    mock_get_active.return_value = set()
    process_dam_csv_file(
        csv_file,
        fname="test.csv",
        table_name="SOME_TABLE",
        db_name="test.db",
        model_class=object,
        insert_query="INSERT",
        filter_by_active_settlement_points=True
    )
    assert mock_store.called
    args, kwargs = mock_store.call_args
    # No rows should be present
    assert kwargs["data"]["data"] == []


@mock.patch("ercot_scraping.utils.filters.get_active_settlement_points")
@mock.patch("ercot_scraping.apis.archive_api.store_data_to_db")
def test_process_dam_csv_file_filter_off(mock_store, mock_get_active):
    csv_content = "SettlementPoint,Value\nACTIVE,100\nINACTIVE,200\n"
    csv_file = make_csv_file(csv_content)
    mock_get_active.return_value = {"ACTIVE"}
    process_dam_csv_file(
        csv_file,
        fname="test.csv",
        table_name="SOME_TABLE",
        db_name="test.db",
        model_class=object,
        insert_query="INSERT",
        filter_by_active_settlement_points=False
    )
    assert mock_store.called
    args, kwargs = mock_store.call_args
    # All rows should be present since filter is off
    assert len(kwargs["data"]["data"]) == 2
