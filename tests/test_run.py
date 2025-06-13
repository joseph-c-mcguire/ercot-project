from unittest.mock import patch
import pytest
import sqlite3
from ercot_scraping.run import (
    main,
)
from ercot_scraping.config.config import (
    SETTLEMENT_POINT_PRICES_TABLE_CREATION_QUERY,
    BIDS_TABLE_CREATION_QUERY,
    BID_AWARDS_TABLE_CREATION_QUERY,
    OFFERS_TABLE_CREATION_QUERY,
    OFFER_AWARDS_TABLE_CREATION_QUERY,
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
