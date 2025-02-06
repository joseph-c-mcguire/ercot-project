import os
import sqlite3
import pytest

from ercot_scraping.create_ercot_tables import (
    create_ercot_tables,
)  # Adjust the import as needed


@pytest.fixture
def test_db():
    # Set up a test database
    db_name = "test_ercot_data.db"
    conn = sqlite3.connect(db_name)
    conn.close()
    yield db_name
    # Ensure the connection is closed before removing the test database
    os.remove(db_name)


def test_initialize_database_tables(test_db):
    # Connect to the test database and initialize tables
    conn = sqlite3.connect(test_db)
    create_ercot_tables(test_db)

    cursor = conn.cursor()

    # Check if SETTLEMENT_POINT_PRICES table exists
    cursor.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name='SETTLEMENT_POINT_PRICES'"
    )
    assert cursor.fetchone() is not None

    # Check if BIDS table exists
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='BIDS'")
    assert cursor.fetchone() is not None

    # Check if BID_AWARDS table exists
    cursor.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name='BID_AWARDS'"
    )
    assert cursor.fetchone() is not None

    # Check if OFFERS table exists
    cursor.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name='OFFERS'"
    )
    assert cursor.fetchone() is not None

    # Check if OFFER_AWARDS table exists
    cursor.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name='OFFER_AWARDS'"
    )
    assert cursor.fetchone() is not None

    # Close the connection
    conn.close()
