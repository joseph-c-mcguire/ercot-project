import os
import sqlite3
import pytest
from ercot_scraping.store_data import (
    store_prices_to_db,
)
from ercot_scraping.data_models import SettlementPointPrice

TEST_DB = "test_ercot.db"

SAMPLE_DATA = {
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


@pytest.fixture(autouse=True)
def cleanup():
    yield
    if os.path.exists(TEST_DB):
        os.remove(TEST_DB)


def test_store_prices_to_db():
    # Insert sample settlement point price and verify that it exists in the db.
    store_prices_to_db(SAMPLE_DATA, db_name=TEST_DB)

    conn = sqlite3.connect(TEST_DB)
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM SETTLEMENT_POINT_PRICES")
    rows = cursor.fetchall()
    conn.close()

    assert len(rows) == 1
    # Verify the row matches the sample data tuple
    sample_instance = SettlementPointPrice(**SAMPLE_DATA["data"][0])
    assert rows[0] == sample_instance.as_tuple()
