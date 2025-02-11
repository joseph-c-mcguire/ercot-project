import pytest
import requests
import os
import logging
from unittest.mock import patch, Mock
from requests.exceptions import HTTPError
from ercot_scraping.ercot_api import (
    fetch_settlement_point_prices,
    fetch_dam_energy_bid_awards,
    fetch_dam_energy_bids,
    fetch_dam_energy_only_offer_awards,
    fetch_dam_energy_only_offers,
    fetch_data_from_endpoint,
)
from ercot_scraping.config import (
    ERCOT_API_REQUEST_HEADERS,
    ERCOT_API_BASE_URL_DAM,
    ERCOT_API_BASE_URL_SETTLEMENT,
)

TEST_DB = "test_ercot.db"
LOG_FILE = "test_ercot_api.log"
# Configure logging
logging.basicConfig(level=logging.DEBUG, filename=LOG_FILE, filemode="w")
logger = logging.getLogger(__name__)


@pytest.fixture(autouse=True)
def cleanup():
    yield
    if os.path.exists(TEST_DB):
        os.remove(TEST_DB)


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

    response = fetch_dam_energy_bids(
        start_date="2023-10-01", end_date="2023-10-02")
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


@patch("ercot_scraping.ercot_api.requests.get")
def test_fetch_data_from_endpoint_default_params(mock_get):
    # Setup the mock response
    mock_response = Mock()
    expected_json = {"data": "default"}
    mock_response.status_code = 200
    mock_response.json.return_value = expected_json
    mock_get.return_value = mock_response

    # Call function with only endpoint
    endpoint = "test_endpoint"
    result = fetch_data_from_endpoint(ERCOT_API_BASE_URL_SETTLEMENT, endpoint)

    # Verify URL construction and default header usage
    expected_url = f"{ERCOT_API_BASE_URL_SETTLEMENT}/{endpoint}"
    mock_get.assert_called_with(
        url=expected_url, headers=ERCOT_API_REQUEST_HEADERS, params={}
    )
    assert result == expected_json


@patch("ercot_scraping.ercot_api.requests.get")
def test_fetch_data_from_endpoint_with_dates(mock_get):
    # Setup the mock response
    mock_response = Mock()
    expected_json = {"data": "with_dates"}
    mock_response.status_code = 200
    mock_response.json.return_value = expected_json
    mock_get.return_value = mock_response

    # Call function with start_date and end_date
    endpoint = "test_endpoint"
    start_date = "2023-10-01"
    end_date = "2023-10-02"
    result = fetch_data_from_endpoint(
        ERCOT_API_BASE_URL_SETTLEMENT, endpoint, start_date, end_date
    )

    expected_url = f"{ERCOT_API_BASE_URL_SETTLEMENT}/{endpoint}"
    expected_params = {"deliveryDateFrom": start_date,
                       "deliveryDateTo": end_date}
    mock_get.assert_called_with(
        url=expected_url, headers=ERCOT_API_REQUEST_HEADERS, params=expected_params
    )
    assert result == expected_json


@patch("ercot_scraping.ercot_api.requests.get")
def test_fetch_data_from_endpoint_with_custom_header(mock_get):
    # Setup the mock response
    mock_response = Mock()
    expected_json = {"data": "custom_header"}
    mock_response.status_code = 200
    mock_response.json.return_value = expected_json
    mock_get.return_value = mock_response

    # Call function with a custom header
    endpoint = "test_endpoint"
    custom_header = {"Custom-Header": "Value"}
    result = fetch_data_from_endpoint(
        ERCOT_API_BASE_URL_SETTLEMENT, endpoint, header=custom_header
    )

    expected_url = f"{ERCOT_API_BASE_URL_SETTLEMENT}/{endpoint}"
    mock_get.assert_called_with(
        url=expected_url, headers=custom_header, params={})
    assert result == expected_json


@patch("ercot_scraping.ercot_api.requests.get")
def test_fetch_data_from_endpoint_http_error(mock_get):
    # Setup the mock response to simulate HTTP error
    mock_response = Mock()
    mock_response.status_code = 404
    mock_response.raise_for_status.side_effect = HTTPError("404 Client Error")
    mock_get.return_value = mock_response

    endpoint = "test_endpoint"

    with pytest.raises(HTTPError):
        fetch_data_from_endpoint(ERCOT_API_BASE_URL_SETTLEMENT, endpoint)


def test_dam_base_url():
    response = requests.get(ERCOT_API_BASE_URL_DAM,
                            headers=ERCOT_API_REQUEST_HEADERS)
    logger.info(
        f"Response status code for DAM base URL: {response.status_code}")
    logger.info(f"Response text for DAM base URL: {response.text}")
    assert response.status_code == 200


def test_settlement_base_url():
    response = requests.get(ERCOT_API_BASE_URL_SETTLEMENT,
                            headers=ERCOT_API_REQUEST_HEADERS)
    logger.info(
        f"Response status code for Settlement base URL: {response.status_code}")
    logger.info(f"Response text for Settlement base URL: {response.text}")
    assert response.status_code == 200


def test_subscription_key_validity():
    response = requests.get(
        f"{ERCOT_API_BASE_URL_SETTLEMENT}/spp_node_zone_hub",
        headers=ERCOT_API_REQUEST_HEADERS,
    )
    logger.info(
        f"Response status code for subscription key validity: {response.status_code}"
    )
    logger.info(
        f"Response text for subscription key validity: {response.text}")
    assert response.status_code == 200
