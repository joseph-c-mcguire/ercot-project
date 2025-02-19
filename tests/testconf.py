import os
import sqlite3
from pathlib import Path

# Test configuration
TEST_DB = "test.db"
LOG_FILE = "test.log"
ERCOT_DATA_DB = Path("_data/ercot_data.db")


def close_all_db_connections(db_path: str | Path) -> None:
    """Force close any open connections to the specified database."""
    if not os.path.exists(db_path):
        return

    try:
        # Create a new connection and immediately close it
        # This helps identify if the database is locked
        test_conn = sqlite3.connect(db_path)
        test_conn.close()

        # If we can connect, try to close any remaining connections
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()

        # This pragma helps identify open connections
        cursor.execute("PRAGMA database_list")

        # Force checkpoint and close
        cursor.execute("PRAGMA wal_checkpoint(TRUNCATE)")
        cursor.close()
        conn.commit()
        conn.close()

    except sqlite3.OperationalError as e:
        # If we get a "database is locked" error, try more aggressive cleanup
        if "database is locked" in str(e):
            # Wait for any write operations to complete
            import time
            time.sleep(1)

            try:
                # Try to force close with a timeout
                conn = sqlite3.connect(db_path, timeout=5)
                conn.close()
            except sqlite3.OperationalError:
                # If still locked, log the error but continue
                print(f"Warning: Could not close connections to {db_path}")


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
            "settlementPointName": "XYZ",  # Use this field name consistently
            "qseName": "Test QSE",
            "energyOnlyOfferAwardInMW": 50.0,
            "settlementPointPrice": 30.5,
            "offerId": "101",
        }
    ]
}

SAMPLE_BID_AWARDS = {
    "data": [
        {
            "deliveryDate": "2023-10-01",
            "hourEnding": 1,
            "settlementPointName": "XYZ",  # Use this field name consistently
            "qseName": "Test QSE",
            "energyOnlyBidAwardInMW": 45.0,
            "settlementPointPrice": 28.5,
            "bidId": "201",
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
