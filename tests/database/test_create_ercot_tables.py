import os
import sqlite3
import tempfile
import pytest
from unittest import mock
from ercot_scraping.database.create_ercot_tables import create_ercot_tables


def test_create_ercot_tables():
    assert True  # Replace with actual test logic for creating ERCOT tables.


@pytest.fixture
def temp_db_path():
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as tf:
        yield tf.name
    os.remove(tf.name)


@pytest.fixture(autouse=True)
def mock_config_queries():
    with mock.patch("ercot_scraping.database.create_ercot_tables.SETTLEMENT_POINT_PRICES_TABLE_CREATION_QUERY", "CREATE TABLE IF NOT EXISTS SETTLEMENT_POINT_PRICES (id INTEGER PRIMARY KEY)"):
        with mock.patch("ercot_scraping.database.create_ercot_tables.BIDS_TABLE_CREATION_QUERY", "CREATE TABLE IF NOT EXISTS BIDS (id INTEGER PRIMARY KEY)"):
            with mock.patch("ercot_scraping.database.create_ercot_tables.BID_AWARDS_TABLE_CREATION_QUERY", "CREATE TABLE IF NOT EXISTS BID_AWARDS (id INTEGER PRIMARY KEY)"):
                with mock.patch("ercot_scraping.database.create_ercot_tables.OFFERS_TABLE_CREATION_QUERY", "CREATE TABLE IF NOT EXISTS OFFERS (id INTEGER PRIMARY KEY)"):
                    with mock.patch("ercot_scraping.database.create_ercot_tables.OFFER_AWARDS_TABLE_CREATION_QUERY", "CREATE TABLE IF NOT EXISTS OFFER_AWARDS (id INTEGER PRIMARY KEY)"):
                        yield


def test_create_ercot_tables_creates_all_tables(temp_db_path):
    create_ercot_tables(save_path=temp_db_path)
    conn = sqlite3.connect(temp_db_path)
    cursor = conn.cursor()
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = set(name for (name,) in cursor.fetchall())
    expected_tables = {
        "SETTLEMENT_POINT_PRICES",
        "BIDS",
        "BID_AWARDS",
        "OFFERS",
        "OFFER_AWARDS"
    }
    assert expected_tables.issubset(tables)
    conn.close()
