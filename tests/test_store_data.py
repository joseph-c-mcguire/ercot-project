import os
import sqlite3
import pytest
from ercot_scraping.store_data import (
    store_prices_to_db,
    store_offer_awards_to_db,
    store_bid_awards_to_db,
    store_bids_to_db,
    store_offers_to_db,
)
from ercot_scraping.data_models import (
    SettlementPointPrice,
    OfferAward,
    BidAward,
    Bid,
    Offer,
)

TEST_DB = "test_ercot.db"

SETTLEMENT_POINT_PRICE_SAMPLE = {
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

SAMPLE_OFFER_AWARDS = {
    "data": [
        {
            "DeliveryDate": "2023-10-01",
            "HourEnding": 1,
            "SettlementPoint": "XYZ",
            "QSEName": "Test QSE",
            "EnergyOnlyOfferAwardMW": 50.0,
            "SettlementPointPrice": 30.5,
            "OfferID": "101",  # Changed to string
        }
    ]
}

SAMPLE_BID_AWARDS = {
    "data": [
        {
            "DeliveryDate": "2023-10-01",
            "HourEnding": 1,
            "SettlementPoint": "XYZ",
            "QSEName": "Test QSE",
            "EnergyOnlyBidAwardMW": 45.0,
            "SettlementPointPrice": 28.5,
            "BidID": "201",  # Changed to string
        }
    ]
}

SAMPLE_BIDS = {
    "data": [
        {
            "DeliveryDate": "2023-10-01",
            "HourEnding": 1,
            "SettlementPoint": "XYZ",
            "QSEName": "Test QSE",
            "EnergyOnlyBidMW1": 10.0,
            "EnergyOnlyBidPrice1": 25.0,
            "EnergyOnlyBidMW2": 15.0,
            "EnergyOnlyBidPrice2": 26.0,
            "EnergyOnlyBidMW3": 0.0,
            "EnergyOnlyBidPrice3": 0.0,
            "EnergyOnlyBidMW4": 0.0,
            "EnergyOnlyBidPrice4": 0.0,
            "EnergyOnlyBidMW5": 0.0,
            "EnergyOnlyBidPrice5": 0.0,
            "EnergyOnlyBidMW6": 0.0,
            "EnergyOnlyBidPrice6": 0.0,
            "EnergyOnlyBidMW7": 0.0,
            "EnergyOnlyBidPrice7": 0.0,
            "EnergyOnlyBidMW8": 0.0,
            "EnergyOnlyBidPrice8": 0.0,
            "EnergyOnlyBidMW9": 0.0,
            "EnergyOnlyBidPrice9": 0.0,
            "EnergyOnlyBidMW10": 0.0,
            "EnergyOnlyBidPrice10": 0.0,
            "EnergyOnlyBidID": "201",  # Changed to string
            "MultiHourBlockIndicator": "N",
            "BlockCurveIndicator": "N",
        }
    ]
}

SAMPLE_OFFERS = {
    "data": [
        {
            "DeliveryDate": "2023-10-01",
            "HourEnding": 1,
            "SettlementPoint": "XYZ",
            "QSEName": "Test QSE",
            "EnergyOnlyOfferMW1": 20.0,
            "EnergyOnlyOfferPrice1": 30.0,
            "EnergyOnlyOfferMW2": 25.0,
            "EnergyOnlyOfferPrice2": 32.0,
            "EnergyOnlyOfferMW3": 0.0,
            "EnergyOnlyOfferPrice3": 0.0,
            "EnergyOnlyOfferMW4": 0.0,
            "EnergyOnlyOfferPrice4": 0.0,
            "EnergyOnlyOfferMW5": 0.0,
            "EnergyOnlyOfferPrice5": 0.0,
            "EnergyOnlyOfferMW6": 0.0,
            "EnergyOnlyOfferPrice6": 0.0,
            "EnergyOnlyOfferMW7": 0.0,
            "EnergyOnlyOfferPrice7": 0.0,
            "EnergyOnlyOfferMW8": 0.0,
            "EnergyOnlyOfferPrice8": 0.0,
            "EnergyOnlyOfferMW9": 0.0,
            "EnergyOnlyOfferPrice9": 0.0,
            "EnergyOnlyOfferMW10": 0.0,
            "EnergyOnlyOfferPrice10": 0.0,
            "EnergyOnlyOfferID": "101",
            "MultiHourBlockIndicator": "N",
            "BlockCurveIndicator": "N",
        }
    ]
}


@pytest.fixture(autouse=True)
def cleanup():
    # Setup - nothing needed
    yield
    # Teardown - ensure any open connections are closed before removing file
    try:
        # Create a temporary connection and close it to ensure no other connections are active
        conn = sqlite3.connect(TEST_DB)
        conn.close()
        if os.path.exists(TEST_DB):
            os.remove(TEST_DB)
    except PermissionError:
        print(f"Warning: Could not remove test database {TEST_DB}. It may be in use.")


def test_store_prices_to_db():
    # Insert sample settlement point price and verify that it exists in the db.
    store_prices_to_db(SETTLEMENT_POINT_PRICE_SAMPLE, db_name=TEST_DB)

    conn = sqlite3.connect(TEST_DB)
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM SETTLEMENT_POINT_PRICES")
    rows = cursor.fetchall()
    conn.close()

    assert len(rows) == 1
    # Verify the row matches the sample data tuple
    sample_instance = SettlementPointPrice(**SETTLEMENT_POINT_PRICE_SAMPLE["data"][0])
    assert rows[0] == sample_instance.as_tuple()


def test_store_offer_awards_to_db():
    # Insert sample offer awards data
    store_offer_awards_to_db(SAMPLE_OFFER_AWARDS, db_name=TEST_DB)

    conn = sqlite3.connect(TEST_DB)
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM OFFER_AWARDS")
    rows = cursor.fetchall()
    conn.close()

    assert len(rows) == 1
    sample_instance = OfferAward(**SAMPLE_OFFER_AWARDS["data"][0])
    assert rows[0] == sample_instance.as_tuple()


def test_store_bid_awards_to_db():
    store_bid_awards_to_db(SAMPLE_BID_AWARDS, db_name=TEST_DB)

    conn = sqlite3.connect(TEST_DB)
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM BID_AWARDS")
    rows = cursor.fetchall()
    conn.close()

    assert len(rows) == 1
    sample_instance = BidAward(**SAMPLE_BID_AWARDS["data"][0])
    assert rows[0] == sample_instance.as_tuple()


def test_store_bids_to_db():
    store_bids_to_db(SAMPLE_BIDS, db_name=TEST_DB)

    conn = sqlite3.connect(TEST_DB)
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM BIDS")
    rows = cursor.fetchall()
    conn.close()

    assert len(rows) == 1
    sample_instance = Bid(**SAMPLE_BIDS["data"][0])
    assert rows[0] == sample_instance.as_tuple()


def test_store_offers_to_db():
    store_offers_to_db(SAMPLE_OFFERS, db_name=TEST_DB)

    conn = sqlite3.connect(TEST_DB)
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM OFFERS")
    rows = cursor.fetchall()
    conn.close()

    assert len(rows) == 1
    sample_instance = Offer(**SAMPLE_OFFERS["data"][0])
    assert rows[0] == sample_instance.as_tuple()


def test_invalid_data():
    invalid_data = {"data": [{"InvalidField": "value"}]}

    with pytest.raises(ValueError):
        store_prices_to_db(invalid_data, db_name=TEST_DB)

    with pytest.raises(ValueError):
        store_bid_awards_to_db(invalid_data, db_name=TEST_DB)

    with pytest.raises(ValueError):
        store_bids_to_db(invalid_data, db_name=TEST_DB)

    with pytest.raises(ValueError):
        store_offers_to_db(invalid_data, db_name=TEST_DB)

    with pytest.raises(ValueError):
        store_offer_awards_to_db(invalid_data, db_name=TEST_DB)
