import pytest
import sqlite3
import os
from unittest.mock import patch
from ercot_scraping.ercot_api import fetch_settlement_point_prices, store_prices_to_db

TEST_DB = "test_ercot.db"


@patch("ercot_scraping.ercot_api.requests.get")
def test_fetch_settlement_point_prices(mock_get):
    mock_response = {
        "data": [
            {
                "DeliveryDate": "2023-10-01",
                "DeliveryHour": 1,
                "DeliveryInterval": 15,
                "SettlementPointName": "ABC",
                "SettlementPointType": "Type1",
                "SettlementPointPrice": 25.5,
                "DSTFlag": "N",
            }
        ]
    }
    mock_get.return_value.status_code = 200
    mock_get.return_value.json.return_value = mock_response

    response = fetch_settlement_point_prices(
        start_date="2023-10-01", end_date="2023-10-02"
    )
    assert response is not None
    assert "data" in response


def test_store_prices_to_db():
    data = {
        "data": [
            {
                "DeliveryDate": "2023-10-01",
                "DeliveryHour": 1,
                "DeliveryInterval": 15,
                "SettlementPointName": "ABC",
                "SettlementPointType": "Type1",
                "SettlementPointPrice": 25.5,
                "DSTFlag": "N",
            },
            {
                "DeliveryDate": "2023-10-01",
                "DeliveryHour": 2,
                "DeliveryInterval": 30,
                "SettlementPointName": "DEF",
                "SettlementPointType": "Type2",
                "SettlementPointPrice": 30.0,
                "DSTFlag": "N",
            },
        ]
    }
    store_prices_to_db(data, db_name=TEST_DB)

    conn = sqlite3.connect(TEST_DB)
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM SETTLEMENT_POINT_PRICES")
    rows = cursor.fetchall()
    conn.close()

    assert len(rows) == 2
    assert rows[0][3] == "ABC"
    assert rows[0][5] == 25.5
    assert rows[0][0] == "2023-10-01"
    assert rows[1][3] == "DEF"
    assert rows[1][5] == 30.0
    assert rows[1][0] == "2023-10-01"


@pytest.fixture(autouse=True)
def cleanup():
    yield
    if os.path.exists(TEST_DB):
        os.remove(TEST_DB)
