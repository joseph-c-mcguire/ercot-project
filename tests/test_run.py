import pytest
from unittest.mock import patch, Mock
from datetime import datetime, timedelta
from ercot_scraping.run import (
    download_historical_dam_data,
    download_historical_spp_data,
    update_daily_dam_data,
    update_daily_spp_data,
)


@pytest.fixture
def mock_api_responses():
    return {"data": [{"DeliveryDate": "2023-10-01", "SettlementPointName": "TEST"}]}


@patch("ercot_scraping.run.fetch_dam_energy_bid_awards")
@patch("ercot_scraping.run.fetch_dam_energy_bids")
@patch("ercot_scraping.run.fetch_dam_energy_only_offer_awards")
@patch("ercot_scraping.run.fetch_dam_energy_only_offers")
@patch("ercot_scraping.run.store_bid_awards_to_db")
@patch("ercot_scraping.run.store_bids_to_db")
@patch("ercot_scraping.run.store_offer_awards_to_db")
@patch("ercot_scraping.run.store_offers_to_db")
def test_download_historical_dam_data(
    mock_store_offers,
    mock_store_offer_awards,
    mock_store_bids,
    mock_store_bid_awards,
    mock_fetch_offers,
    mock_fetch_offer_awards,
    mock_fetch_bids,
    mock_fetch_bid_awards,
    mock_api_responses,
):
    # Setup all mocks to return the same test data
    for mock in [
        mock_fetch_bid_awards,
        mock_fetch_bids,
        mock_fetch_offer_awards,
        mock_fetch_offers,
    ]:
        mock.return_value = mock_api_responses

    # Test the function
    download_historical_dam_data("2023-10-01", "2023-10-02")

    # Verify all fetch functions were called with correct dates
    for mock in [
        mock_fetch_bid_awards,
        mock_fetch_bids,
        mock_fetch_offer_awards,
        mock_fetch_offers,
    ]:
        mock.assert_called_once_with("2023-10-01", "2023-10-02")

    # Verify all store functions were called with the mock data
    for mock in [
        mock_store_bid_awards,
        mock_store_bids,
        mock_store_offer_awards,
        mock_store_offers,
    ]:
        mock.assert_called_once()


@patch("ercot_scraping.run.fetch_settlement_point_prices")
@patch("ercot_scraping.run.store_prices_to_db")
def test_download_historical_spp_data(
    mock_store_prices, mock_fetch_prices, mock_api_responses
):
    mock_fetch_prices.return_value = mock_api_responses

    download_historical_spp_data("2023-10-01", "2023-10-02")

    mock_fetch_prices.assert_called_once_with("2023-10-01", "2023-10-02")
    mock_store_prices.assert_called_once_with(mock_api_responses, "ercot.db")


@patch("ercot_scraping.run.download_historical_dam_data")
def test_update_daily_dam_data(mock_download):
    update_daily_dam_data()
    yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
    mock_download.assert_called_once_with(yesterday, yesterday, "ercot.db", None)


@patch("ercot_scraping.run.download_historical_spp_data")
def test_update_daily_spp_data(mock_download):
    update_daily_spp_data()
    yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
    mock_download.assert_called_once_with(yesterday, yesterday, "ercot.db")
