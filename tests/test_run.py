from unittest.mock import patch, ANY
from pathlib import Path
import pytest
import sqlite3
from ercot_scraping.run import (
    download_historical_dam_data,
    download_historical_spp_data,
    update_daily_spp_data,
    parse_args,
    main,
)
from ercot_scraping.config import (
    SETTLEMENT_POINT_PRICES_TABLE_CREATION_QUERY,
    BIDS_TABLE_CREATION_QUERY,
    BID_AWARDS_TABLE_CREATION_QUERY,
    OFFERS_TABLE_CREATION_QUERY,
    OFFER_AWARDS_TABLE_CREATION_QUERY,
    ERCOT_DB_NAME,
)


@pytest.fixture
def setup_database(tmp_path):
    """Create a test database with all required tables."""
    db_path = tmp_path / "test.db"
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # Create all tables
    cursor.execute(SETTLEMENT_POINT_PRICES_TABLE_CREATION_QUERY)
    cursor.execute(BIDS_TABLE_CREATION_QUERY)
    cursor.execute(BID_AWARDS_TABLE_CREATION_QUERY)
    cursor.execute(OFFERS_TABLE_CREATION_QUERY)
    cursor.execute(OFFER_AWARDS_TABLE_CREATION_QUERY)

    conn.commit()
    conn.close()

    return str(db_path)

# Single test data fixture for reuse


@pytest.fixture
def mock_data():
    return {
        "api_response": {"data": [{"DeliveryDate": "2023-10-01", "SettlementPointName": "TEST"}]},
        "qse_filter": {"QSE1", "QSE2"},
        "test_date": "2023-10-01"
    }

# Combined test for all DAM data downloads


@patch("ercot_scraping.run.should_use_archive_api", return_value=False)
@patch("ercot_scraping.run.load_qse_shortnames")
@patch("ercot_scraping.run.fetch_dam_energy_bid_awards")
@patch("ercot_scraping.run.fetch_dam_energy_bids")
@patch("ercot_scraping.run.fetch_dam_energy_only_offer_awards")
@patch("ercot_scraping.run.fetch_dam_energy_only_offers")
@patch("ercot_scraping.run.store_bid_awards_to_db")
@patch("ercot_scraping.run.store_bids_to_db")
@patch("ercot_scraping.run.store_offer_awards_to_db")
@patch("ercot_scraping.run.store_offers_to_db")
def test_download_historical_dam_data_all(
    mock_store_offers, mock_store_offer_awards, mock_store_bids, mock_store_bid_awards,
    mock_fetch_offers, mock_fetch_offer_awards, mock_fetch_bids, mock_fetch_bid_awards,
    mock_load_qse, mock_should_use_archive_api,
    mock_data, setup_database
):
    """Test downloading historical DAM data with various scenarios."""
    # Setup mocks
    mock_load_qse.return_value = mock_data["qse_filter"]
    for mock in [mock_fetch_bid_awards, mock_fetch_bids, mock_fetch_offer_awards, mock_fetch_offers]:
        mock.return_value = mock_data["api_response"]

    # Test scenarios
    scenarios = [
        ("2023-10-01", "2023-10-02", setup_database,
         mock_data["qse_filter"]),  # Standard case
        ("2023-10-01", None, setup_database, None),  # Default end date case
        ("2023-10-01", "2023-10-02", setup_database,
         {"QSE1"}),  # Custom QSE filter
    ]

    for start_date, end_date, db_name, qse_filter in scenarios:
        # Reset mocks before each scenario
        mock_fetch_bid_awards.reset_mock()
        mock_fetch_bids.reset_mock()
        mock_fetch_offer_awards.reset_mock()
        mock_fetch_offers.reset_mock()

        download_historical_dam_data(start_date, end_date, db_name, qse_filter)

        # Verify all fetch functions were called correctly
        if qse_filter:
            # Match the actual call signature using positional args
            mock_fetch_bid_awards.assert_called_with(
                start_date,
                end_date or ANY,
                header=ANY,
                qse_names=qse_filter
            )

            mock_fetch_bids.assert_called_with(
                start_date,
                end_date or ANY,
                header=ANY,
                qse_names=qse_filter
            )

            mock_fetch_offer_awards.assert_called_with(
                start_date,
                end_date or ANY,
                header=ANY,
                qse_names=qse_filter
            )

            mock_fetch_offers.assert_called_with(
                start_date,
                end_date or ANY,
                header=ANY,
                qse_names=qse_filter
            )

# Combined test for all SPP data operations


@patch("ercot_scraping.run.should_use_archive_api", return_value=False)
@patch("ercot_scraping.run.fetch_settlement_point_prices")
@patch("ercot_scraping.run.store_prices_to_db")
def test_spp_operations(mock_store_prices, mock_fetch_prices, mock_should_use_archive, mock_data, setup_database):
    """Test both historical and daily SPP data operations."""
    # Setup mock
    mock_fetch_prices.return_value = mock_data["api_response"]

    # Test historical SPP data download
    download_historical_spp_data("2023-10-01", "2023-10-02", setup_database)

    # Verify historical download calls
    mock_fetch_prices.assert_called_with(
        "2023-10-01",
        "2023-10-02",
        header=ANY
    )
    mock_store_prices.assert_called_with(
        mock_data["api_response"],
        db_name=setup_database
    )

    # Reset mocks for daily update test
    mock_fetch_prices.reset_mock()
    mock_store_prices.reset_mock()

    # Test daily SPP update
    update_daily_spp_data(setup_database)

    # Verify daily update calls
    assert mock_fetch_prices.call_count == 1, "fetch_settlement_point_prices should be called once"
    assert mock_store_prices.call_count == 1, "store_prices_to_db should be called once"

# Combined CLI argument tests


@pytest.mark.parametrize("args,expected", [
    (["historical-dam", "--start", "2023-01-01", "--end", "2023-12-31"],
     {"command": "historical-dam", "start": "2023-01-01", "end": "2023-12-31", "db": ERCOT_DB_NAME}),  # Updated to use config value
    (["historical-spp", "--start", "2023-01-01"],
     {"command": "historical-spp", "start": "2023-01-01", "end": None, "db": ERCOT_DB_NAME}),  # Updated to use config value
    (["update-dam", "--qse-filter", "test.csv"],
     {"command": "update-dam", "db": ERCOT_DB_NAME, "qse_filter": Path("test.csv")}),  # Updated to expect Path object
])
def test_parse_args_combined(args, expected, monkeypatch):
    monkeypatch.setattr("sys.argv", ["ercot_scraping.run"] + args)
    args = parse_args()
    for key, value in expected.items():
        assert getattr(args, key) == value

# Combined main function test


@patch("ercot_scraping.run.load_qse_shortnames")
@patch("ercot_scraping.run.download_historical_dam_data")
@patch("ercot_scraping.run.download_historical_spp_data")
@patch("ercot_scraping.run.update_daily_dam_data")
@patch("ercot_scraping.run.update_daily_spp_data")
def test_main_combined(
    mock_update_spp, mock_update_dam, mock_dl_spp, mock_dl_dam, mock_load_qse,
    mock_data, monkeypatch
):
    scenarios = [
        (["historical-dam", "--start", "2023-01-01"],
         lambda: mock_dl_dam.assert_called_once()),
        (["historical-spp", "--start", "2023-01-01"],
         lambda: mock_dl_spp.assert_called_once()),
        (["update-dam"],
         lambda: mock_update_dam.assert_called_once()),
        (["update-spp"],
         lambda: mock_update_spp.assert_called_once()),
    ]

    mock_load_qse.return_value = mock_data["qse_filter"]

    for args, assertion in scenarios:
        monkeypatch.setattr("sys.argv", ["ercot_scraping.run"] + args)
        main()
        assertion()

        # Reset all mocks after each scenario
        mock_dl_dam.reset_mock()
        mock_dl_spp.reset_mock()
        mock_update_dam.reset_mock()
        mock_update_spp.reset_mock()

# Error handling test


def test_error_handling(monkeypatch):
    monkeypatch.setattr("sys.argv", ["ercot_scraping.run"])
    with patch("ercot_scraping.run.logger") as mock_logger:
        main()
        mock_logger.error.assert_called_once_with(
            "No command specified. Use -h for help.")
