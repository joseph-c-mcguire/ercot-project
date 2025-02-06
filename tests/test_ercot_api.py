import pytest
import sqlite3
import os
from unittest.mock import patch
from ercot_scraping.ercot_api import (
    fetch_settlement_point_prices,
    store_prices_to_db,
    fetch_dam_energy_bid_awards,
    fetch_dam_energy_bids,
    fetch_dam_energy_only_offer_awards,
    fetch_dam_energy_only_offers,
    validate_sql_query,
    SETTLEMENT_POINT_PRICES_INSERT_QUERY,
    QUERY_CHECK_TABLE_EXISTS,
)
from ercot_scraping.initialize_database_tables import (
    SETTLEMENT_POINT_PRICES_TABLE_CREATION_QUERY,
    BIDS_TABLE_CREATION_QUERY,
    BID_AWARDS_TABLE_CREATION_QUERY,
    OFFERS_TABLE_CREATION_QUERY,
    OFFER_AWARDS_TABLE_CREATION_QUERY,
)

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


@patch("ercot_scraping.ercot_api.requests.get")
def test_fetch_dam_energy_bid_awards(mock_get):
    mock_response = {
        "data": [{"DeliveryDate": "2023-10-01", "SettlementPointName": "ABC"}]
    }
    mock_get.return_value.status_code = 200
    mock_get.return_value.json.return_value = mock_response

    response = fetch_dam_energy_bid_awards(
        start_date="2023-10-01", end_date="2023-10-02"
    )
    assert response is not None
    assert "data" in response


@patch("ercot_scraping.ercot_api.requests.get")
def test_fetch_dam_energy_bids(mock_get):
    mock_response = {
        "data": [{"DeliveryDate": "2023-10-01", "SettlementPointName": "DEF"}]
    }
    mock_get.return_value.status_code = 200
    mock_get.return_value.json.return_value = mock_response

    response = fetch_dam_energy_bids(start_date="2023-10-01", end_date="2023-10-02")
    assert response is not None
    assert "data" in response


@patch("ercot_scraping.ercot_api.requests.get")
def test_fetch_dam_energy_only_offer_awards(mock_get):
    mock_response = {
        "data": [{"DeliveryDate": "2023-10-01", "SettlementPointName": "GHI"}]
    }
    mock_get.return_value.status_code = 200
    mock_get.return_value.json.return_value = mock_response

    response = fetch_dam_energy_only_offer_awards(
        start_date="2023-10-01", end_date="2023-10-02"
    )
    assert response is not None
    assert "data" in response


@patch("ercot_scraping.ercot_api.requests.get")
def test_fetch_dam_energy_only_offers(mock_get):
    mock_response = {
        "data": [{"DeliveryDate": "2023-10-01", "SettlementPointName": "JKL"}]
    }
    mock_get.return_value.status_code = 200
    mock_get.return_value.json.return_value = mock_response

    response = fetch_dam_energy_only_offers(
        start_date="2023-10-01", end_date="2023-10-02"
    )
    assert response is not None
    assert "data" in response


@pytest.mark.parametrize(
    "query",
    [
        SETTLEMENT_POINT_PRICES_INSERT_QUERY,
        SETTLEMENT_POINT_PRICES_TABLE_CREATION_QUERY,
        BIDS_TABLE_CREATION_QUERY,
        BID_AWARDS_TABLE_CREATION_QUERY,
        OFFERS_TABLE_CREATION_QUERY,
        OFFER_AWARDS_TABLE_CREATION_QUERY,
        QUERY_CHECK_TABLE_EXISTS,
    ],
)
def test_validate_sql_query(query):
    assert validate_sql_query(query) is True


@pytest.fixture(autouse=True)
def cleanup():
    yield
    if os.path.exists(TEST_DB):
        os.remove(TEST_DB)
