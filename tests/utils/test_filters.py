from ercot_scraping.utils.filters import filter_by_qse_names
from ercot_scraping.utils.filters import load_qse_shortnames
from ercot_scraping.utils.filters import format_qse_filter_param
from ercot_scraping.utils.filters import filter_by_settlement_points


def filter_data(data, criteria):
    return [item for item in data if all(item[key] == value for key, value in criteria.items())]


def test_filter_data():
    data = [
        {'name': 'Alice', 'age': 30},
        {'name': 'Bob', 'age': 25},
        {'name': 'Charlie', 'age': 30}
    ]
    criteria = {'age': 30}
    filtered = filter_data(data, criteria)
    assert len(filtered) == 2
    assert filtered[0]['name'] == 'Alice'
    assert filtered[1]['name'] == 'Charlie'


def test_filter_by_qse_names_basic():
    data = {
        "data": [
            {"QSEName": "QABC", "value": 1},
            {"QSEName": "QXYZ", "value": 2},
            {"QSEName": "Q123", "value": 3},
        ]
    }
    qse_names = {"QABC", "Q123"}
    result = filter_by_qse_names(data, qse_names)
    assert result == {
        "data": [
            {"QSEName": "QABC", "value": 1},
            {"QSEName": "Q123", "value": 3},
        ]
    }


def test_filter_by_qse_names_empty_data():
    data = {"data": []}
    qse_names = {"QABC"}
    result = filter_by_qse_names(data, qse_names)
    assert result == {"data": []}


def test_filter_by_qse_names_no_matching_qse():
    data = {
        "data": [
            {"QSEName": "QDEF", "value": 1},
            {"QSEName": "QGHI", "value": 2},
        ]
    }
    qse_names = {"QABC"}
    result = filter_by_qse_names(data, qse_names)
    assert result == {"data": []}


def test_filter_by_qse_names_missing_qsename_field():
    data = {
        "data": [
            {"NotQSEName": "QABC", "value": 1},
            {"QSEName": "QXYZ", "value": 2},
        ]
    }
    qse_names = {"QXYZ"}
    result = filter_by_qse_names(data, qse_names)
    assert result == {"data": [{"QSEName": "QXYZ", "value": 2}]}


def test_filter_by_qse_names_data_is_none():
    data = None
    qse_names = {"QABC"}
    result = filter_by_qse_names(data, qse_names)
    assert result is None


def test_filter_by_qse_names_data_missing_data_key():
    data = {"not_data": []}
    qse_names = {"QABC"}
    result = filter_by_qse_names(data, qse_names)
    assert result == {"not_data": []}


def test_load_qse_shortnames_basic(tmp_path):
    csv_content = "SHORT NAME,Other\nQABC,1\nQXYZ,2\nQ123,3\n"
    csv_file = tmp_path / "qse.csv"
    csv_file.write_text(csv_content, encoding="utf-8")
    result = load_qse_shortnames(str(csv_file))
    assert result == {"QABC", "QXYZ", "Q123"}


def test_load_qse_shortnames_empty_file(tmp_path):
    csv_file = tmp_path / "empty.csv"
    csv_file.write_text("", encoding="utf-8")
    result = load_qse_shortnames(str(csv_file))
    assert result == set()


def test_load_qse_shortnames_missing_short_name_column(tmp_path):
    csv_content = "NOT_SHORT_NAME,Other\nA,1\nB,2\n"
    csv_file = tmp_path / "bad.csv"
    csv_file.write_text(csv_content, encoding="utf-8")
    result = load_qse_shortnames(str(csv_file))
    assert result == set()


def test_load_qse_shortnames_file_not_found():
    result = load_qse_shortnames("nonexistent_file.csv")
    assert result == set()


def test_load_qse_shortnames_ignores_blank_names(tmp_path):
    csv_content = "SHORT NAME,Other\nQABC,1\n,2\nQXYZ,3\n"
    csv_file = tmp_path / "qse.csv"
    csv_file.write_text(csv_content, encoding="utf-8")
    result = load_qse_shortnames(str(csv_file))
    assert result == {"QABC", "QXYZ"}


def test_load_qse_shortnames_strips_whitespace(tmp_path):
    csv_content = "SHORT NAME,Other\n QABC ,1\nQXYZ ,2\n Q123,3\n"
    csv_file = tmp_path / "qse.csv"
    csv_file.write_text(csv_content, encoding="utf-8")
    result = load_qse_shortnames(str(csv_file))
    assert result == {"QABC", "QXYZ", "Q123"}


def test_format_qse_filter_param_basic():
    qse_names = {"QABC", "QXYZ", "Q123"}
    result = format_qse_filter_param(qse_names)
    assert result == "Q123,QABC,QXYZ"


def test_format_qse_filter_param_ignores_non_q_names():
    qse_names = {"QABC", "ABC", "123", "QXYZ"}
    result = format_qse_filter_param(qse_names)
    assert result == "QABC,QXYZ"


def test_format_qse_filter_param_empty_set():
    qse_names = set()
    result = format_qse_filter_param(qse_names)
    assert result == ""


def test_format_qse_filter_param_all_non_q_names():
    qse_names = {"ABC", "123", "XYZ"}
    result = format_qse_filter_param(qse_names)
    assert result == ""


def test_format_qse_filter_param_single_q_name():
    qse_names = {"QONLY"}
    result = format_qse_filter_param(qse_names)
    assert result == "QONLY"


def test_format_qse_filter_param_mixed_case():
    qse_names = {"Qabc", "qXYZ", "Q123"}
    result = format_qse_filter_param(qse_names)
    # Only "Qabc" and "Q123" start with uppercase 'Q'
    assert result == "Q123,Qabc"


def test_filter_by_settlement_points_basic():
    data = {
        "data": [
            {"SettlementPoint": "SP1", "value": 1},
            {"SettlementPoint": "SP2", "value": 2},
            {"SettlementPoint": "SP3", "value": 3},
        ]
    }
    settlement_points = {"SP1", "SP3"}
    result = filter_by_settlement_points(data, settlement_points)
    assert result == {
        "data": [
            {"SettlementPoint": "SP1", "value": 1},
            {"SettlementPoint": "SP3", "value": 3},
        ]
    }


def test_filter_by_settlement_points_empty_data():
    data = {"data": []}
    settlement_points = {"SP1"}
    result = filter_by_settlement_points(data, settlement_points)
    assert result == {"data": []}


def test_filter_by_settlement_points_no_matching_points():
    data = {
        "data": [
            {"SettlementPoint": "SPX", "value": 1},
            {"SettlementPoint": "SPY", "value": 2},
        ]
    }
    settlement_points = {"SP1"}
    result = filter_by_settlement_points(data, settlement_points)
    assert result == {"data": []}


def test_filter_by_settlement_points_missing_field_variations():
    data = {
        "data": [
            {"OtherField": "SP1", "value": 1},
            {"settlementPointName": "SP2", "value": 2},
            {"SettlementPointName": "SP3", "value": 3},
            {"settlementPoint": "SP4", "value": 4},
        ]
    }
    settlement_points = {"SP2", "SP3", "SP4"}
    result = filter_by_settlement_points(data, settlement_points)
    assert result == {
        "data": [
            {"settlementPointName": "SP2", "value": 2},
            {"SettlementPointName": "SP3", "value": 3},
            {"settlementPoint": "SP4", "value": 4},
        ]
    }


def test_filter_by_settlement_points_data_is_none():
    data = None
    settlement_points = {"SP1"}
    result = filter_by_settlement_points(data, settlement_points)
    assert result == {"data": []}


def test_filter_by_settlement_points_data_missing_data_key():
    data = {"not_data": []}
    settlement_points = {"SP1"}
    result = filter_by_settlement_points(data, settlement_points)
    assert result == {"data": []}


def test_filter_by_settlement_points_point_name_is_none():
    data = {
        "data": [
            {"SettlementPoint": None, "value": 1},
            {"settlementPointName": "", "value": 2},
            {"SettlementPointName": "SP1", "value": 3},
        ]
    }
    settlement_points = {"SP1"}
    result = filter_by_settlement_points(data, settlement_points)
    assert result == {
        "data": [
            {"SettlementPointName": "SP1", "value": 3},
        ]
    }


def test_filter_by_settlement_points_multiple_fields_present():
    data = {
        "data": [
            {"SettlementPoint": "SP1", "settlementPointName": "SP2", "value": 1},
            {"SettlementPointName": "SP3", "settlementPoint": "SP4", "value": 2},
        ]
    }
    # Should match on the first field found in field_variations
    settlement_points = {"SP1", "SP3"}
    result = filter_by_settlement_points(data, settlement_points)
    assert result == {
        "data": [
            {"SettlementPoint": "SP1", "settlementPointName": "SP2", "value": 1},
            {"SettlementPointName": "SP3", "settlementPoint": "SP4", "value": 2},
        ]
    }
