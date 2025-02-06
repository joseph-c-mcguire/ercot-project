import csv
from ercot_scraping.filters import load_qse_shortnames, filter_by_qse_names


def write_csv(tmp_path, filename, header, rows):
    file_path = tmp_path / filename
    with file_path.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=header)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)
    return file_path


def test_load_qse_shortnames_valid(tmp_path):
    # Prepare CSV file with valid data.
    header = ["SHORT NAME", "OTHER"]
    rows = [
        {"SHORT NAME": " QSE1 ", "OTHER": "Value1"},
        {"SHORT NAME": "QSE2", "OTHER": "Value2"},
        {"SHORT NAME": "", "OTHER": "Ignore"},  # Should be ignored
        {"SHORT NAME": "QSE3", "OTHER": "Value3"},
    ]
    csv_file = write_csv(tmp_path, "qse_valid.csv", header, rows)

    # Call the function and assert the result.
    shortnames = load_qse_shortnames(str(csv_file))
    assert shortnames == {"QSE1", "QSE2", "QSE3"}


def test_load_qse_shortnames_empty_file(tmp_path):
    # Prepare CSV file with a header but no valid rows.
    header = ["SHORT NAME", "OTHER"]
    rows = []
    csv_file = write_csv(tmp_path, "qse_empty.csv", header, rows)

    shortnames = load_qse_shortnames(str(csv_file))
    assert shortnames == set()


def test_load_qse_shortnames_missing_shortname_column(tmp_path):
    # Prepare CSV file with a different header.
    header = ["NAME", "OTHER"]
    rows = [
        {"NAME": "QSE1", "OTHER": "Value1"},
        {"NAME": "QSE2", "OTHER": "Value2"},
    ]
    csv_file = write_csv(tmp_path, "qse_missing.csv", header, rows)

    shortnames = load_qse_shortnames(str(csv_file))
    # Since the file does not have "SHORT NAME", no entries should be added.
    assert shortnames == set()


def test_filter_by_qse_names_valid():
    data = {
        "data": [
            {"QSEName": "QSE1", "value": 10},
            {"QSEName": "QSE2", "value": 20},
            {"QSEName": "OTHER", "value": 30},
            {"value": 40},  # Record missing QSEName key
        ]
    }
    qse_names = {"QSE1", "QSE2"}
    result = filter_by_qse_names(data, qse_names)
    expected = {
        "data": [
            {"QSEName": "QSE1", "value": 10},
            {"QSEName": "QSE2", "value": 20},
        ]
    }
    assert result == expected


def test_filter_by_qse_names_no_data_key():
    # Test when input data does not have a "data" key.
    data = {"something": []}
    qse_names = {"QSE1", "QSE2"}
    result = filter_by_qse_names(data, qse_names)
    # Function should return the original data unmodified.
    assert result == data


def test_filter_by_qse_names_empty_data_list():
    # Test when data["data"] is an empty list.
    data = {"data": []}
    qse_names = {"QSE1", "QSE2"}
    result = filter_by_qse_names(data, qse_names)
    expected = {"data": []}
    assert result == expected


def test_filter_by_qse_names_empty_qse_names():
    # Test with a valid data dictionary but an empty set of qse_names.
    data = {
        "data": [
            {"QSEName": "QSE1", "value": 10},
            {"QSEName": "QSE2", "value": 20},
            {"QSEName": "OTHER", "value": 30},
        ]
    }
    result = filter_by_qse_names(data, set())
    expected = {"data": []}
    assert result == expected
