import sqlite3
import pytest
import tempfile
import os
from ercot_scraping.database.merge_data import merge_data
from ercot_scraping.config.queries import (
    BID_AWARDS_TABLE_CREATION_QUERY,
    BIDS_TABLE_CREATION_QUERY,
    SETTLEMENT_POINT_PRICES_TABLE_CREATION_QUERY
)


@pytest.fixture
def test_db():
    """Create a temporary in-memory database for testing."""
    conn = sqlite3.connect(':memory:')
    cursor = conn.cursor()

    # Create required tables
    cursor.execute(BID_AWARDS_TABLE_CREATION_QUERY)
    cursor.execute(BIDS_TABLE_CREATION_QUERY)
    cursor.execute(SETTLEMENT_POINT_PRICES_TABLE_CREATION_QUERY)

    # Insert test data
    cursor.execute("""
        INSERT INTO BID_AWARDS (DeliveryDate, HourEnding, SettlementPoint, QSEName, 
                              EnergyOnlyBidAwardMW, SettlementPointPrice, BidId)
        VALUES ('2024-01-19', 1, 'TEST_POINT', 'TEST_QSE', 100.0, 50.0, 'BID001')
    """)

    cursor.execute("""
        INSERT INTO BIDS (DeliveryDate, HourEnding, SettlementPoint, QSEName,
                         EnergyOnlyBidMW1, EnergyOnlyBidPrice1, EnergyOnlyBidID,
                         BlockCurveIndicator)
        VALUES ('2024-01-19', 1, 'TEST_POINT', 'TEST_QSE', 100.0, 45.0, 'BID001', 'V')
    """)

    cursor.execute("""
        INSERT INTO SETTLEMENT_POINT_PRICES (DeliveryDate, DeliveryHour, SettlementPointName,
                                           SettlementPointPrice)
        VALUES ('2024-01-19', 1, 'TEST_POINT', 52.0)
    """)

    conn.commit()
    return conn


def test_merge_data(test_db):
    """Test that data is correctly merged into the FINAL table."""
    merge_data(test_db)  # Pass the connection directly

    # Verify results
    cursor = test_db.cursor()
    cursor.execute("SELECT * FROM FINAL")
    result = cursor.fetchone()

    assert result is not None, "No data was merged into FINAL table"

    # Unpack result into named fields for easier assertion
    (delivery_date, hour_ending, settlement_point, qse_name,
     bid_award_mw, settlement_price, bid_id, mark_price,
     bid_price, bid_size, block_curve) = result

    # Verify each field
    assert delivery_date == '2024-01-19'
    assert hour_ending == 1
    assert settlement_point == 'TEST_POINT'
    assert qse_name == 'TEST_QSE'
    assert bid_award_mw == 100.0
    assert settlement_price == 52.0  # Should take SPP price over bid award price
    assert bid_id == 'BID001'
    assert mark_price == 52.0
    assert bid_price == 45.0
    assert bid_size == 100.0
    assert block_curve == 'V'


def test_merge_data_with_path():
    """Test merge_data with a database path."""
    # Create a temporary database file
    with tempfile.NamedTemporaryFile(delete=False) as temp_db:
        db_path = temp_db.name

    try:
        # Create and populate database
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()

        # Create required tables
        cursor.execute(BID_AWARDS_TABLE_CREATION_QUERY)
        cursor.execute(BIDS_TABLE_CREATION_QUERY)
        cursor.execute(SETTLEMENT_POINT_PRICES_TABLE_CREATION_QUERY)

        # Insert test data
        cursor.execute("""
            INSERT INTO BID_AWARDS (DeliveryDate, HourEnding, SettlementPoint, QSEName, 
                                  EnergyOnlyBidAwardMW, SettlementPointPrice, BidId)
            VALUES ('2024-01-19', 1, 'TEST_POINT', 'TEST_QSE', 100.0, 50.0, 'BID001')
        """)

        # Insert corresponding BIDS record
        cursor.execute("""
            INSERT INTO BIDS (DeliveryDate, HourEnding, SettlementPoint, QSEName,
                            EnergyOnlyBidMW1, EnergyOnlyBidPrice1, EnergyOnlyBidID,
                            BlockCurveIndicator)
            VALUES ('2024-01-19', 1, 'TEST_POINT', 'TEST_QSE', 100.0, 45.0, 'BID001', 'V')
        """)

        # Insert corresponding SPP record
        cursor.execute("""
            INSERT INTO SETTLEMENT_POINT_PRICES (DeliveryDate, DeliveryHour, SettlementPointName,
                                               SettlementPointPrice)
            VALUES ('2024-01-19', 1, 'TEST_POINT', 52.0)
        """)

        conn.commit()
        conn.close()

        # Test merge_data with path
        merge_data(db_path)

        # Verify results
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM FINAL")
        count = cursor.fetchone()[0]
        conn.close()

        assert count > 0, "No data was merged into FINAL table"

    finally:
        # Clean up the temporary file
        if os.path.exists(db_path):
            os.unlink(db_path)


def test_merge_data_missing_bid(test_db):
    """Test merge behavior when bid data is missing."""
    cursor = test_db.cursor()
    cursor.execute("DELETE FROM BIDS")
    test_db.commit()

    merge_data(test_db)

    cursor.execute("SELECT * FROM FINAL")
    result = cursor.fetchone()

    assert result is not None, "No data was merged into FINAL table"
    assert result[8] is None, "BID_PRICE should be NULL when bid is missing"
    assert result[9] is None, "BID_SIZE should be NULL when bid is missing"
    assert result[10] is None, "blockCurve should be NULL when bid is missing"


def test_merge_data_missing_spp(test_db):
    """Test merge behavior when settlement point price data is missing."""
    cursor = test_db.cursor()
    cursor.execute("DELETE FROM SETTLEMENT_POINT_PRICES")
    test_db.commit()

    merge_data(test_db)

    cursor.execute("SELECT * FROM FINAL")
    result = cursor.fetchone()

    assert result is not None, "No data was merged into FINAL table"
    assert result[5] == 50.0, "Should fall back to bid award price when SPP is missing"
    assert result[7] is None, "MARK_PRICE should be NULL when SPP is missing"
