import os
import sqlite3
import pytest

from ercot_scraping.database.create_ercot_tables import (
    create_ercot_tables,
)  # Adjust the import as needed


# sourcery skip: dont-import-test-modules
from tests.testconf import TEST_DB


@pytest.fixture(autouse=True)
def cleanup():
    yield
    if os.path.exists(TEST_DB):
        os.remove(TEST_DB)


def test_initialize_database_tables():
    # Connect to the test database and initialize tables
    conn = sqlite3.connect(TEST_DB)
    create_ercot_tables(TEST_DB)

    cursor = conn.cursor()

    # Check if SETTLEMENT_POINT_PRICES table exists
    cursor.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name='SETTLEMENT_POINT_PRICES'"
    )
    assert cursor.fetchone() is not None

    # Check if BIDS table exists
    cursor.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name='BIDS'")
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
