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
from tests.testconf import TEST_DB, SETTLEMENT_POINT_PRICE_SAMPLE, SAMPLE_BID_AWARDS, SAMPLE_BIDS, SAMPLE_OFFERS, SAMPLE_OFFER_AWARDS
from ercot_scraping.config import (
    SETTLEMENT_POINT_PRICES_TABLE_CREATION_QUERY,
    BIDS_TABLE_CREATION_QUERY,
    BID_AWARDS_TABLE_CREATION_QUERY,
    OFFERS_TABLE_CREATION_QUERY,
    OFFER_AWARDS_TABLE_CREATION_QUERY,
)


@pytest.fixture(autouse=True)
def setup_database():
    """Create test database with required tables."""
    conn = sqlite3.connect(TEST_DB)
    cursor = conn.cursor()

    # Create all required tables
    cursor.execute(SETTLEMENT_POINT_PRICES_TABLE_CREATION_QUERY)
    cursor.execute(BIDS_TABLE_CREATION_QUERY)
    cursor.execute(BID_AWARDS_TABLE_CREATION_QUERY)
    cursor.execute(OFFERS_TABLE_CREATION_QUERY)
    cursor.execute(OFFER_AWARDS_TABLE_CREATION_QUERY)

    conn.commit()
    conn.close()

    yield

    # Cleanup
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

    conn = sqlite3.connect(TEST_DB)
    cursor = conn.cursor()

    # BidAward and OfferAward models use settlementPointName
    cursor.execute("SELECT SettlementPoint FROM BID_AWARDS")
    bid_points = {row[0] for row in cursor.fetchall()}
    cursor.execute("SELECT SettlementPoint FROM OFFER_AWARDS")
    offer_points = {row[0] for row in cursor.fetchall()}
    conn.close()

    print(f"Bid points in DB: {bid_points}")
    print(f"Offer points in DB: {offer_points}")

    # Create sample price data matching SettlementPointPrice model fields
    test_prices = {
        "data": [
            {
                "deliveryDate": "2023-10-01",  # lowercase per model
                "deliveryHour": 1,             # lowercase per model
                "deliveryInterval": 15,         # lowercase per model
                "settlementPointName": "XYZ",   # lowercase per model
                "settlementPointType": "Type1",  # lowercase per model
                "settlementPointPrice": 25.5,   # lowercase per model
                "dstFlag": "N",                # lowercase per model
            },
            {
                "deliveryDate": "2023-10-01",
                "deliveryHour": 1,
                "deliveryInterval": 15,
                "settlementPointName": "ABC",
                "settlementPointType": "Type1",
                "settlementPointPrice": 30.5,
                "dstFlag": "N",
            },
        ]
    }

    # Store prices with filtering enabled
    store_prices_to_db(test_prices, db_name=TEST_DB, filter_by_awards=True)

    # Verify results using the correct field name from the model
    conn = sqlite3.connect(TEST_DB)
    cursor = conn.cursor()
    cursor.execute("SELECT SettlementPointName FROM SETTLEMENT_POINT_PRICES")
    points = {row[0] for row in cursor.fetchall()}
    conn.close()

    print(f"Points stored in settlement prices: {points}")
    assert points == {"XYZ"}, f"Expected only 'XYZ' but got {points}"


def test_invalid_data():
    """Test validation of invalid data for each store function."""
    # Test data with missing required fields
    invalid_data = {"data": [{"InvalidField": "value"}]}

    # Test settlement point prices with specific validation
    with pytest.raises(ValueError) as excinfo:
        store_prices_to_db(invalid_data, db_name=TEST_DB)
    assert "Missing required fields" in str(excinfo.value)

    # Test bid awards with missing required fields
    with pytest.raises(ValueError) as excinfo:
        store_bid_awards_to_db(invalid_data, db_name=TEST_DB)
    assert "Missing required fields for BidAward" in str(excinfo.value)

    # Test bids with missing required fields
    with pytest.raises(ValueError) as excinfo:
        store_bids_to_db(invalid_data, db_name=TEST_DB)
    assert "Missing required fields for Bid" in str(excinfo.value)

    # Test offers with missing required fields
    with pytest.raises(ValueError) as excinfo:
        store_offers_to_db(invalid_data, db_name=TEST_DB)
    assert "Missing required fields for Offer" in str(excinfo.value)

    # Test offer awards with missing required fields
    with pytest.raises(ValueError) as excinfo:
        store_offer_awards_to_db(invalid_data, db_name=TEST_DB)
    assert "Missing required fields for OfferAward" in str(excinfo.value)


def test_store_bid_awards_with_invalid_data():
    """Test handling of invalid data format (list instead of dict)."""
    # nested list instead of dict
    invalid_data = {"data": [["not", "a", "dict"]]}
    with pytest.raises(ValueError, match="Invalid data record format for BidAward"):
        store_bid_awards_to_db(invalid_data, "test.db", None)


def test_store_bid_awards_with_list_of_dicts():
    """Test handling of invalid nested data structure."""
    # Simulate data with an inner list of records
    valid_record = {"deliveryDate": "2024-01-01",
                    "settlementPointName": "TEST"}
    data_with_list = {"data": [[valid_record, valid_record]]}

    with pytest.raises(ValueError, match="Invalid data record format for BidAward"):
        store_bid_awards_to_db(data_with_list, "test.db", None)
