import sqlite3
from ercot_scraping.apis.ercot_api import data_exists_in_db, fetch_settlement_point_prices
import pytest
from unittest.mock import MagicMock, patch
from ercot_scraping.apis.ercot_api import fetch_data_from_endpoint


def test_ercot_api():
    assert True  # Replace with actual test logic for ERCOT API


def test_data_exists_in_db(tmp_path):
    db_path = tmp_path / "test.db"
    conn = sqlite3.connect(db_path)
    conn.execute(
        "CREATE TABLE SETTLEMENT_POINT_PRICES (DeliveryDate TEXT, HourEnding TEXT, IntervalEnding TEXT)")
    conn.execute(
        "INSERT INTO SETTLEMENT_POINT_PRICES VALUES ('2024-01-01', '01', '01')")
    conn.commit()
    assert data_exists_in_db(
        str(db_path), "SETTLEMENT_POINT_PRICES", "2024-01-01", "01", "01")
    assert not data_exists_in_db(
        str(db_path), "SETTLEMENT_POINT_PRICES", "2024-01-02", "01", "01")
    conn.close()


def test_fetch_settlement_point_prices_skips_existing(monkeypatch, tmp_path):
    db_path = tmp_path / "test.db"
    conn = sqlite3.connect(db_path)
    conn.execute(
        "CREATE TABLE SETTLEMENT_POINT_PRICES (DeliveryDate TEXT, HourEnding TEXT, IntervalEnding TEXT)")
    conn.execute(
        "INSERT INTO SETTLEMENT_POINT_PRICES VALUES ('2024-01-01', '01', '01')")
    conn.commit()
    called = {}

    def fake_store_prices_to_db(data, db_name, batch_size):
        called['called'] = True
        # Should only be called for new data
        assert all(row['DeliveryDate'] != '2024-01-01' for row in data['data'])
    monkeypatch.setattr(
        "ercot_scraping.database.store_data.store_prices_to_db", fake_store_prices_to_db)

    def fake_fetch_data_from_endpoint(*a, **k):
        return {"data": [
            {"DeliveryDate": "2024-01-01", "HourEnding": "01", "IntervalEnding": "01"},
            {"DeliveryDate": "2024-01-02", "HourEnding": "01", "IntervalEnding": "01"}
        ]}
    monkeypatch.setattr(
        "ercot_scraping.apis.ercot_api.fetch_data_from_endpoint", fake_fetch_data_from_endpoint)
    fetch_settlement_point_prices(
        start_date="2024-01-01", end_date="2024-01-02", db_name=str(db_path)
    )
    assert called['called']
    conn.close()


def create_test_db(db_path, table_name="SETTLEMENT_POINT_PRICES"):
    conn = sqlite3.connect(db_path)
    conn.execute(
        f"CREATE TABLE {table_name} (DeliveryDate TEXT, HourEnding TEXT, IntervalEnding TEXT)"
    )
    conn.commit()
    return conn


def test_data_exists_in_db_full_match(tmp_path):
    db_path = tmp_path / "test.db"
    conn = create_test_db(db_path)
    conn.execute(
        "INSERT INTO SETTLEMENT_POINT_PRICES VALUES ('2024-01-01', '01', '01')"
    )
    conn.commit()
    assert data_exists_in_db(
        str(db_path), "SETTLEMENT_POINT_PRICES", "2024-01-01", "01", "01")
    conn.close()


def test_data_exists_in_db_partial_match(tmp_path):
    db_path = tmp_path / "test.db"
    conn = create_test_db(db_path)
    conn.execute(
        "INSERT INTO SETTLEMENT_POINT_PRICES VALUES ('2024-01-01', '01', '01')"
    )
    conn.commit()
    # Should not match if hour or interval is different
    assert not data_exists_in_db(
        str(db_path), "SETTLEMENT_POINT_PRICES", "2024-01-01", "02", "01")
    assert not data_exists_in_db(
        str(db_path), "SETTLEMENT_POINT_PRICES", "2024-01-01", "01", "02")
    conn.close()


def test_data_exists_in_db_other_table_date_only(tmp_path):
    db_path = tmp_path / "test.db"
    table_name = "OTHER_TABLE"
    conn = sqlite3.connect(db_path)
    conn.execute(
        f"CREATE TABLE {table_name} (DeliveryDate TEXT, HourEnding TEXT, IntervalEnding TEXT)"
    )
    conn.execute(
        f"INSERT INTO {table_name} VALUES ('2024-02-01', '01', '01')"
    )
    conn.commit()
    # Should match on date only for non-SPP tables
    assert data_exists_in_db(str(db_path), table_name, "2024-02-01")
    assert not data_exists_in_db(str(db_path), table_name, "2024-02-02")
    conn.close()


def test_data_exists_in_db_no_date(tmp_path):
    db_path = tmp_path / "test.db"
    conn = create_test_db(db_path)
    # No date provided, should return False
    assert not data_exists_in_db(str(db_path), "SETTLEMENT_POINT_PRICES")
    conn.close()


def test_data_exists_in_db_table_missing(tmp_path):
    db_path = tmp_path / "test.db"
    # Table does not exist, should handle exception and return False
    assert not data_exists_in_db(
        str(db_path), "NON_EXISTENT_TABLE", "2024-01-01", "01", "01")


def test_data_exists_in_db_db_missing(tmp_path):
    db_path = tmp_path / "does_not_exist.db"
    # DB file does not exist, should handle exception and return False
    assert not data_exists_in_db(
        str(db_path), "SETTLEMENT_POINT_PRICES", "2024-01-01", "01", "01")


@pytest.fixture
def fake_headers():
    return {"Authorization": "Bearer testtoken"}


@pytest.fixture
def fake_base_url():
    return "https://fake.api"


@pytest.fixture
def fake_endpoint():
    return "test_endpoint"


def make_response(json_data, status_code=200):
    resp = MagicMock()
    resp.status_code = status_code
    resp.json.return_value = json_data
    resp.raise_for_status.side_effect = None if status_code == 200 else Exception(
        "HTTP Error")
    return resp


@patch("ercot_scraping.apis.ercot_api.rate_limited_request")
def test_fetch_data_from_endpoint_dict_with_data(mock_req, fake_base_url, fake_endpoint, fake_headers):
    mock_req.return_value = make_response(
        {"data": [{"foo": "bar"}], "_meta": {"totalPages": 1, "currentPage": 1}})
    result = fetch_data_from_endpoint(
        fake_base_url, fake_endpoint, start_date="2024-01-01", end_date="2024-01-02", header=fake_headers
    )
    assert "data" in result
    assert result["data"] == [{"foo": "bar"}]


@patch("ercot_scraping.apis.ercot_api.rate_limited_request")
def test_fetch_data_from_endpoint_list_response(mock_req, fake_base_url, fake_endpoint, fake_headers):
    mock_req.return_value = make_response([{"foo": "bar"}])
    result = fetch_data_from_endpoint(
        fake_base_url, fake_endpoint, header=fake_headers
    )
    assert "data" in result
    assert result["data"] == [{"foo": "bar"}]


@patch("ercot_scraping.apis.ercot_api.rate_limited_request")
def test_fetch_data_from_endpoint_dict_no_data_key(mock_req, fake_base_url, fake_endpoint, fake_headers):
    mock_req.return_value = make_response({"foo": "bar"})
    result = fetch_data_from_endpoint(
        fake_base_url, fake_endpoint, header=fake_headers
    )
    assert "data" in result
    assert isinstance(result["data"], list)
    assert result["data"][0]["foo"] == "bar"


@patch("ercot_scraping.apis.ercot_api.rate_limited_request")
def test_fetch_data_from_endpoint_multiple_pages(mock_req, fake_base_url, fake_endpoint, fake_headers):
    # Simulate two pages
    page1 = make_response({"data": [{"foo": 1}], "_meta": {
                          "totalPages": 2, "currentPage": 1}})
    page2 = make_response({"data": [{"foo": 2}], "_meta": {
                          "totalPages": 2, "currentPage": 2}})
    mock_req.side_effect = [page1, page2]
    result = fetch_data_from_endpoint(
        fake_base_url, fake_endpoint, header=fake_headers
    )
    assert "data" in result
    assert {"foo": 1} in result["data"]
    assert {"foo": 2} in result["data"]
    assert len(result["data"]) == 2


@patch("ercot_scraping.apis.ercot_api.refresh_access_token")
@patch("ercot_scraping.apis.ercot_api.rate_limited_request")
def test_fetch_data_from_endpoint_401_refresh_token(mock_req, mock_refresh, fake_base_url, fake_endpoint, fake_headers):
    resp_401 = make_response({}, status_code=401)
    resp_ok = make_response({"data": [{"foo": "bar"}]})
    mock_req.side_effect = [resp_401, resp_ok]
    mock_refresh.return_value = "newtoken"
    result = fetch_data_from_endpoint(
        fake_base_url, fake_endpoint, header=fake_headers
    )
    assert "data" in result
    assert result["data"] == [{"foo": "bar"}]
    assert fake_headers["Authorization"].startswith("Bearer")


@patch("ercot_scraping.apis.ercot_api.rate_limited_request")
def test_fetch_data_from_endpoint_store_func_called(mock_req, fake_base_url, fake_endpoint, fake_headers):
    mock_req.return_value = make_response({"data": [{"foo": "bar"}]})
    called = {}

    def store_func(record, db_name):
        called["called"] = True
        assert record == {"foo": "bar"}
        assert db_name == "ercot.db"
    fetch_data_from_endpoint(
        fake_base_url, fake_endpoint, header=fake_headers, store_func=store_func, db_name="ercot.db"
    )
    assert called.get("called")


@patch("ercot_scraping.apis.ercot_api.rate_limited_request")
def test_fetch_data_from_endpoint_checkpoint_func_called(mock_req, fake_base_url, fake_endpoint, fake_headers):
    mock_req.return_value = make_response({"data": [{"foo": "bar"}]})
    called = {}

    def checkpoint_func(info):
        called["called"] = info
    fetch_data_from_endpoint(
        fake_base_url, fake_endpoint, header=fake_headers, checkpoint_func=checkpoint_func, batch_info={
            "batch": 1}
    )
    assert called.get("called")
    assert called["called"]["stage"] == "api_fetch"
    assert called["called"]["details"]["batch"] == 1


@patch("ercot_scraping.apis.ercot_api.rate_limited_request")
def test_fetch_data_from_endpoint_http_error(mock_req, fake_base_url, fake_endpoint, fake_headers):
    resp = make_response({}, status_code=500)
    resp.raise_for_status.side_effect = Exception("HTTP Error")
    mock_req.return_value = resp
    with pytest.raises(Exception):
        fetch_data_from_endpoint(
            fake_base_url, fake_endpoint, header=fake_headers, retries=1
        )


@patch("ercot_scraping.apis.ercot_api.rate_limited_request")
def test_fetch_data_from_endpoint_request_exception(mock_req, fake_base_url, fake_endpoint, fake_headers):
    mock_req.side_effect = Exception("Request Exception")
    with pytest.raises(Exception):
        fetch_data_from_endpoint(
            fake_base_url, fake_endpoint, header=fake_headers, retries=1
        )
