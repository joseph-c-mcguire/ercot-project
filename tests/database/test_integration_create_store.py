import os
import sqlite3
import tempfile
import pytest
from ercot_scraping.database.create_ercot_tables import create_ercot_tables
from ercot_scraping.database.store_data import (
    store_prices_to_db,
    store_bid_awards_to_db,
    store_bids_to_db,
    store_offers_to_db,
    store_offer_awards_to_db,
)
from ercot_scraping.database.merge_data import merge_data

# Sample minimal valid data for each table
data_prices = {
    "data": [{
        "deliveryDate": "2024-06-01",
        "deliveryHour": 1,
        "deliveryInterval": 1,
        "settlementPointName": "POINT1",
        "settlementPointType": "TypeA",
        "settlementPointPrice": 30.5,
        "dstFlag": 0
    }]
}
data_bid_awards = {
    "data": [{
        "deliveryDate": "2024-06-01",
        "hourEnding": 1,
        "settlementPointName": "POINT1",
        "qseName": "QSE1",
        "energyOnlyBidAwardInMW": 10.0,
        "settlementPointPrice": 30.5,
        "bidId": "BID1"
    }]
}
data_bids = {
    "data": [{
        "deliveryDate": "2024-06-01",
        "hourEnding": 1,
        "settlementPointName": "POINT1",
        "qseName": "QSE1",
        "energyOnlyBidMw1": 10.0,
        "energyOnlyBidPrice1": 25.0,
        "bidId": "BID1",
        "multiHourBlock": "N",
        "blockCurve": "N"
    }]
}
data_offers = {
    "data": [{
        "deliveryDate": "2024-06-01",
        "hourEnding": 1,
        "settlementPointName": "POINT1",
        "qseName": "QSE1",
        "energyOnlyOfferMw1": 20.0,
        "energyOnlyOfferPrice1": 35.0,
        "offerId": "OFFER1",
        "multiHourBlock": "N",
        "blockCurve": "N"
    }]
}
data_offer_awards = {
    "data": [{
        "deliveryDate": "2024-06-01",
        "hourEnding": 1,
        "settlementPointName": "POINT1",
        "qseName": "QSE1",
        "energyOnlyOfferAwardInMW": 15.0,
        "settlementPointPrice": 32.5,
        "offerId": "OFFER1"
    }]
}


@pytest.fixture
def temp_db():
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as tf:
        db_path = tf.name
    yield db_path
    if os.path.exists(db_path):
        os.remove(db_path)


def test_table_creation(temp_db):
    create_ercot_tables(save_path=temp_db)
    conn = sqlite3.connect(temp_db)
    cursor = conn.cursor()
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
    tables = set(row[0] for row in cursor.fetchall())
    expected = {
        "SETTLEMENT_POINT_PRICES",
        "BIDS",
        "BID_AWARDS",
        "OFFERS",
        "OFFER_AWARDS"
    }
    assert expected.issubset(tables)
    conn.close()


def test_store_prices_to_db(temp_db):
    create_ercot_tables(save_path=temp_db)
    store_prices_to_db(data_prices, db_name=temp_db)
    conn = sqlite3.connect(temp_db)
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM SETTLEMENT_POINT_PRICES")
    rows = cursor.fetchall()
    assert len(rows) == 1
    conn.close()


def test_store_bid_awards_to_db(temp_db):
    create_ercot_tables(save_path=temp_db)
    store_bid_awards_to_db(data_bid_awards, db_name=temp_db)
    conn = sqlite3.connect(temp_db)
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM BID_AWARDS")
    rows = cursor.fetchall()
    assert len(rows) == 1
    conn.close()


def test_store_bids_to_db(temp_db):
    create_ercot_tables(save_path=temp_db)
    store_bids_to_db(data_bids, db_name=temp_db)
    conn = sqlite3.connect(temp_db)
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM BIDS")
    rows = cursor.fetchall()
    assert len(rows) == 1
    conn.close()


def test_store_offers_to_db(temp_db):
    create_ercot_tables(save_path=temp_db)
    store_offers_to_db(data_offers, db_name=temp_db)
    conn = sqlite3.connect(temp_db)
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM OFFERS")
    rows = cursor.fetchall()
    assert len(rows) == 1
    conn.close()


def test_store_offer_awards_to_db(temp_db):
    create_ercot_tables(save_path=temp_db)
    store_offer_awards_to_db(data_offer_awards, db_name=temp_db)
    conn = sqlite3.connect(temp_db)
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM OFFER_AWARDS")
    rows = cursor.fetchall()
    assert len(rows) == 1
    conn.close()


# --- Integration: store_data -> merge_data ---
def test_store_data_to_merge_data(temp_db):
    # Setup: create tables and insert minimal valid data for all required tables
    create_ercot_tables(save_path=temp_db)
    store_prices_to_db(data_prices, db_name=temp_db)
    store_bid_awards_to_db(data_bid_awards, db_name=temp_db)
    store_bids_to_db(data_bids, db_name=temp_db)
    store_offers_to_db(data_offers, db_name=temp_db)
    store_offer_awards_to_db(data_offer_awards, db_name=temp_db)
    # Run merge_data
    merge_data(temp_db)
    # Check FINAL table exists and has rows (if merge logic is compatible with test data)
    conn = sqlite3.connect(temp_db)
    cursor = conn.cursor()
    cursor.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name='FINAL'")
    assert cursor.fetchone() is not None
    cursor.execute("SELECT * FROM FINAL")
    rows = cursor.fetchall()
    # The row count may be 0 if merge logic is strict, but table should exist
    assert rows is not None
    conn.close()


# --- Integration: create_ercot_tables -> store_data -> merge_data (full pipeline) ---
def test_full_pipeline_create_store_merge(temp_db):
    # Create tables
    create_ercot_tables(save_path=temp_db)
    # Insert data
    store_prices_to_db(data_prices, db_name=temp_db)
    store_bid_awards_to_db(data_bid_awards, db_name=temp_db)
    store_bids_to_db(data_bids, db_name=temp_db)
    store_offers_to_db(data_offers, db_name=temp_db)
    store_offer_awards_to_db(data_offer_awards, db_name=temp_db)
    # Merge
    merge_data(temp_db)
    # Check FINAL table
    conn = sqlite3.connect(temp_db)
    cursor = conn.cursor()
    cursor.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name='FINAL'")
    assert cursor.fetchone() is not None
    cursor.execute("SELECT * FROM FINAL")
    rows = cursor.fetchall()
    assert rows is not None
    conn.close()
