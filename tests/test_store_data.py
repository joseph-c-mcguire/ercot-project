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
from tests.testconf import TEST_DB

SETTLEMENT_POINT_PRICE_SAMPLE = {
    "data": [
        {
            "deliveryDate": "2023-10-01",
            "deliveryHour": 1,
            "deliveryInterval": 15,
            "settlementPointName": "ABC",
            "settlementPointType": "Type1",
            "settlementPointPrice": 25.5,
            "dstFlag": "N",
        }
    ]
}

SAMPLE_OFFER_AWARDS = {
    "data": [
        {
            "deliveryDate": "2023-10-01",
            "hourEnding": 1,
            "settlementPointName": "XYZ",
            "qseName": "Test QSE",
            "energyOnlyOfferAwardInMW": 50.0,
            "settlementPointPrice": 30.5,
            "offerId": "101",  # Changed to string
        }
    ]
}

SAMPLE_BID_AWARDS = {
    "data": [
        {
            "deliveryDate": "2023-10-01",
            "hourEnding": 1,
            "settlementPointName": "XYZ",
            "qseName": "Test QSE",
            "energyOnlyBidAwardInMW": 45.0,
            "settlementPointPrice": 28.5,
            "bidId": "201",  # Changed to string
        }
    ]
}

SAMPLE_BIDS = {
    "data": [
        {
            "deliveryDate": "2023-10-01",
            "hourEnding": 1,
            "settlementPointName": "XYZ",
            "qseName": "Test QSE",
            "energyOnlyBidMw1": 10.0,
            "energyOnlyBidPrice1": 25.0,
            "energyOnlyBidMw2": 15.0,
            "energyOnlyBidPrice2": 26.0,
            "energyOnlyBidMw3": 0.0,
            "energyOnlyBidPrice3": 0.0,
            "energyOnlyBidMw4": 0.0,
            "energyOnlyBidPrice4": 0.0,
            "energyOnlyBidMw5": 0.0,
            "energyOnlyBidPrice5": 0.0,
            "energyOnlyBidMw6": 0.0,
            "energyOnlyBidPrice6": 0.0,
            "energyOnlyBidMw7": 0.0,
            "energyOnlyBidPrice7": 0.0,
            "energyOnlyBidMw8": 0.0,
            "energyOnlyBidPrice8": 0.0,
            "energyOnlyBidMw9": 0.0,
            "energyOnlyBidPrice9": 0.0,
            "energyOnlyBidMw10": 0.0,
            "energyOnlyBidPrice10": 0.0,
            "bidId": "201",  # Changed to string
            "multiHourBlock": "N",
            "blockCurve": "N",
        }
    ]
}

SAMPLE_OFFERS = {
    "data": [
        {
            "deliveryDate": "2023-10-01",
            "hourEnding": 1,
            "settlementPointName": "XYZ",
            "qseName": "Test QSE",
            "energyOnlyOfferMW1": 20.0,
            "energyOnlyOfferPrice1": 30.0,
            "energyOnlyOfferMW2": 25.0,
            "energyOnlyOfferPrice2": 32.0,
            "energyOnlyOfferMW3": 0.0,
            "energyOnlyOfferPrice3": 0.0,
            "energyOnlyOfferMW4": 0.0,
            "energyOnlyOfferPrice4": 0.0,
            "energyOnlyOfferMW5": 0.0,
            "energyOnlyOfferPrice5": 0.0,
            "energyOnlyOfferMW6": 0.0,
            "energyOnlyOfferPrice6": 0.0,
            "energyOnlyOfferMW7": 0.0,
            "energyOnlyOfferPrice7": 0.0,
            "energyOnlyOfferMW8": 0.0,
            "energyOnlyOfferPrice8": 0.0,
            "energyOnlyOfferMW9": 0.0,
            "energyOnlyOfferPrice9": 0.0,
            "energyOnlyOfferMW10": 0.0,
            "energyOnlyOfferPrice10": 0.0,
            "offerId": "101",
            "multiHourBlock": "N",
            "blockCurve": "N",
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
        print(
            f"Warning: Could not remove test database {TEST_DB}. It may be in use.")


@pytest.mark.parametrize(
    "test_case",
    [
        {
            "name": "prices",
            "data": SETTLEMENT_POINT_PRICE_SAMPLE,
            "store_func": store_prices_to_db,
            "table": "SETTLEMENT_POINT_PRICES",
            "model": SettlementPointPrice,
            "filter_by_awards": False,
        },
        {
            "name": "bid_awards",
            "data": SAMPLE_BID_AWARDS,
            "store_func": store_bid_awards_to_db,
            "table": "BID_AWARDS",
            "model": BidAward,
        },
        {
            "name": "bids",
            "data": SAMPLE_BIDS,
            "store_func": store_bids_to_db,
            "table": "BIDS",
            "model": Bid,
        },
        {
            "name": "offers",
            "data": SAMPLE_OFFERS,
            "store_func": store_offers_to_db,
            "table": "OFFERS",
            "model": Offer,
        },
        {
            "name": "offer_awards",
            "data": SAMPLE_OFFER_AWARDS,
            "store_func": store_offer_awards_to_db,
            "table": "OFFER_AWARDS",
            "model": OfferAward,
        },
    ],
)
def test_store_to_db(test_case):
    """Test storing different types of data to database."""
    kwargs = {"db_name": TEST_DB}
    if "filter_by_awards" in test_case:
        kwargs["filter_by_awards"] = test_case["filter_by_awards"]

    test_case["store_func"](test_case["data"], **kwargs)

    conn = sqlite3.connect(TEST_DB)
    cursor = conn.cursor()
    cursor.execute(f"SELECT * FROM {test_case['table']}")
    rows = cursor.fetchall()
    conn.close()

    assert len(rows) == 1
    sample_instance = test_case["model"](**test_case["data"]["data"][0])
    assert rows[0] == sample_instance.as_tuple()


def test_store_prices_to_db_with_award_filtering():
    """Test storing settlement point prices with award filtering."""
    # First insert some awards to establish settlement points
    store_bid_awards_to_db(SAMPLE_BID_AWARDS, db_name=TEST_DB)
    store_offer_awards_to_db(SAMPLE_OFFER_AWARDS, db_name=TEST_DB)

    # Create sample price data that includes both matching and non-matching settlement points
    test_prices = {
        "data": [
            {
                "deliveryDate": "2023-10-01",
                "deliveryHour": 1,
                "deliveryInterval": 15,
                "settlementPointName": "XYZ",  # Matches award settlement point
                "settlementPointType": "Type1",
                "settlementPointPrice": 25.5,
                "dstFlag": "N",
            },
            {
                "deliveryDate": "2023-10-01",
                "deliveryHour": 1,
                "deliveryInterval": 15,
                "settlementPointName": "ABC",  # Does not match any award
                "settlementPointType": "Type1",
                "settlementPointPrice": 30.5,
                "dstFlag": "N",
            },
        ]
    }

    # Store prices with filtering enabled
    store_prices_to_db(test_prices, db_name=TEST_DB, filter_by_awards=True)

    conn = sqlite3.connect(TEST_DB)
    cursor = conn.cursor()
    cursor.execute("SELECT SettlementPointName FROM SETTLEMENT_POINT_PRICES")
    points = {row[0] for row in cursor.fetchall()}
    conn.close()

    # Should only contain the settlement point that matches awards
    assert points == {"XYZ"}


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


def test_store_bid_awards_with_invalid_data():
    # nested list instead of dict
    invalid_data = {"data": [["not", "a", "dict"]]}
    with pytest.raises(ValueError, match="Invalid data for BidAward: Record must be a dictionary or have fields defined"):
        store_bid_awards_to_db(invalid_data, "test.db", None)


def test_store_bid_awards_with_list_of_dicts():
    # Simulate data with an inner list of records
    valid_record = {"deliveryDate": "2024-01-01",
                    "settlementPointName": "TEST"}
    data_with_list = {"data": [[valid_record, valid_record]]}

    # Expect a ValueError with the correct error message
    with pytest.raises(ValueError, match=r"Invalid data for BidAward: Record must be a dictionary or have fields defined"):
        store_bid_awards_to_db(data_with_list, "test.db", None)
