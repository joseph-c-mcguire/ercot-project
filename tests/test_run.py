import pytest
from unittest.mock import patch, Mock
from datetime import datetime, timedelta
from pathlib import Path  # Add this import
from ercot_scraping.run import (
    download_historical_dam_data,
    download_historical_spp_data,
    update_daily_dam_data,
    update_daily_spp_data,
    parse_args,
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


@patch("ercot_scraping.run.datetime")
@patch("ercot_scraping.run.fetch_dam_energy_bid_awards")
@patch("ercot_scraping.run.fetch_dam_energy_bids")
@patch("ercot_scraping.run.fetch_dam_energy_only_offer_awards")
@patch("ercot_scraping.run.fetch_dam_energy_only_offers")
@patch("ercot_scraping.run.store_bid_awards_to_db")
@patch("ercot_scraping.run.store_bids_to_db")
@patch("ercot_scraping.run.store_offer_awards_to_db")
@patch("ercot_scraping.run.store_offers_to_db")
def test_download_historical_dam_data_default_end_date(
    mock_store_offers,
    mock_store_offer_awards,
    mock_store_bids,
    mock_store_bid_awards,
    mock_fetch_offers,
    mock_fetch_offer_awards,
    mock_fetch_bids,
    mock_fetch_bid_awards,
    mock_datetime,
):
    # Mock current date
    mock_date = datetime(2023, 10, 1)
    mock_datetime.now.return_value = mock_date
    mock_today = mock_date.strftime("%Y-%m-%d")

    # Setup mock returns
    test_data = {"data": [{"DeliveryDate": "2023-10-01"}]}
    for mock in [
        mock_fetch_bid_awards,
        mock_fetch_bids,
        mock_fetch_offer_awards,
        mock_fetch_offers,
    ]:
        mock.return_value = test_data

    # Test with only start_date
    download_historical_dam_data("2023-09-01")

    # Verify fetch calls used correct dates
    for mock in [
        mock_fetch_bid_awards,
        mock_fetch_bids,
        mock_fetch_offer_awards,
        mock_fetch_offers,
    ]:
        mock.assert_called_once_with("2023-09-01", mock_today)

    # Verify store calls
    mock_store_bid_awards.assert_called_once_with(test_data, "ercot.db", None)
    mock_store_bids.assert_called_once_with(test_data, "ercot.db", None)
    mock_store_offer_awards.assert_called_once_with(test_data, "ercot.db", None)
    mock_store_offers.assert_called_once_with(test_data, "ercot.db", None)


@patch("ercot_scraping.run.fetch_dam_energy_bid_awards")
@patch("ercot_scraping.run.fetch_dam_energy_bids")
@patch("ercot_scraping.run.fetch_dam_energy_only_offer_awards")
@patch("ercot_scraping.run.fetch_dam_energy_only_offers")
@patch("ercot_scraping.run.store_bid_awards_to_db")
@patch("ercot_scraping.run.store_bids_to_db")
@patch("ercot_scraping.run.store_offer_awards_to_db")
@patch("ercot_scraping.run.store_offers_to_db")
def test_download_historical_dam_data_with_qse_filter(
    mock_store_offers,
    mock_store_offer_awards,
    mock_store_bids,
    mock_store_bid_awards,
    mock_fetch_offers,
    mock_fetch_offer_awards,
    mock_fetch_bids,
    mock_fetch_bid_awards,
):
    test_data = {"data": [{"DeliveryDate": "2023-10-01"}]}
    qse_filter = {"QSE1", "QSE2"}

    for mock in [
        mock_fetch_bid_awards,
        mock_fetch_bids,
        mock_fetch_offer_awards,
        mock_fetch_offers,
    ]:
        mock.return_value = test_data

    download_historical_dam_data(
        "2023-10-01", "2023-10-02", db_name="test.db", qse_filter=qse_filter
    )

    # Verify store calls include QSE filter
    mock_store_bid_awards.assert_called_once_with(test_data, "test.db", qse_filter)
    mock_store_bids.assert_called_once_with(test_data, "test.db", qse_filter)
    mock_store_offer_awards.assert_called_once_with(test_data, "test.db", qse_filter)
    mock_store_offers.assert_called_once_with(test_data, "test.db", qse_filter)


@patch("ercot_scraping.run.fetch_dam_energy_bid_awards")
def test_download_historical_dam_data_error_handling(mock_fetch):
    mock_fetch.side_effect = Exception("API Error")

    with pytest.raises(Exception) as exc_info:
        download_historical_dam_data("2023-10-01")

    assert str(exc_info.value) == "API Error"


@patch("ercot_scraping.run.datetime")
@patch("ercot_scraping.run.fetch_settlement_point_prices")
@patch("ercot_scraping.run.store_prices_to_db")
def test_download_historical_spp_data_default_end_date(
    mock_store_prices, mock_fetch_prices, mock_datetime
):
    # Mock current date
    mock_date = datetime(2023, 10, 1)
    mock_datetime.now.return_value = mock_date
    mock_today = mock_date.strftime("%Y-%m-%d")

    test_data = {"data": [{"DeliveryDate": "2023-10-01"}]}
    mock_fetch_prices.return_value = test_data

    # Test with only start_date
    download_historical_spp_data("2023-09-01")

    # Verify fetch prices called with correct dates
    mock_fetch_prices.assert_called_once_with("2023-09-01", mock_today)

    # Verify store prices called with data
    mock_store_prices.assert_called_once_with(test_data, "ercot.db")


@patch("ercot_scraping.run.fetch_settlement_point_prices")
def test_download_historical_spp_data_error_handling(mock_fetch):
    mock_fetch.side_effect = Exception("API Error")

    with pytest.raises(Exception) as exc_info:
        download_historical_spp_data("2023-10-01")

    assert str(exc_info.value) == "API Error"


@patch("ercot_scraping.run.fetch_settlement_point_prices")
@patch("ercot_scraping.run.store_prices_to_db")
def test_download_historical_spp_data_custom_db(mock_store_prices, mock_fetch_prices):
    test_data = {"data": [{"DeliveryDate": "2023-10-01"}]}
    mock_fetch_prices.return_value = test_data

    download_historical_spp_data("2023-10-01", "2023-10-02", db_name="test.db")

    # Verify store prices uses custom db name
    mock_store_prices.assert_called_once_with(test_data, "test.db")


@pytest.fixture
def mock_argv(monkeypatch):
    def _mock_argv(args):
        monkeypatch.setattr("sys.argv", ["ercot_scraping.run"] + args)

    return _mock_argv


def test_parse_args_historical_dam(mock_argv):
    mock_argv(["historical-dam", "--start", "2023-01-01", "--end", "2023-12-31"])
    args = parse_args()
    assert args.command == "historical-dam"
    assert args.start == "2023-01-01"
    assert args.end == "2023-12-31"
    assert args.db == "ercot.db"
    assert args.qse_filter is None


def test_parse_args_historical_dam_with_db(mock_argv):
    mock_argv(["historical-dam", "--start", "2023-01-01", "--db", "test.db"])
    args = parse_args()
    assert args.command == "historical-dam"
    assert args.start == "2023-01-01"
    assert args.end is None
    assert args.db == "test.db"


def test_parse_args_historical_spp(mock_argv):
    mock_argv(["historical-spp", "--start", "2023-01-01"])
    args = parse_args()
    assert args.command == "historical-spp"
    assert args.start == "2023-01-01"
    assert args.end is None
    assert args.db == "ercot.db"


def test_parse_args_update_dam(mock_argv):
    mock_argv(["update-dam"])
    args = parse_args()
    assert args.command == "update-dam"
    assert args.db == "ercot.db"
    assert args.qse_filter is None


def test_parse_args_update_dam_with_qse_filter(mock_argv):
    mock_argv(["update-dam", "--qse-filter", "test.csv"])
    args = parse_args()
    assert args.command == "update-dam"
    assert args.db == "ercot.db"
    assert args.qse_filter == Path("test.csv")


def test_parse_args_update_spp(mock_argv):
    mock_argv(["update-spp"])
    args = parse_args()
    assert args.command == "update-spp"
    assert args.db == "ercot.db"


def test_parse_args_no_command(mock_argv):
    mock_argv([])
    args = parse_args()
    assert args.command is None


def test_parse_args_historical_dam_missing_start(mock_argv):
    with pytest.raises(SystemExit):
        mock_argv(["historical-dam"])
        parse_args()
