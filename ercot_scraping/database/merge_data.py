import logging
import sqlite3
from sqlite3 import Connection
from typing import Union, List, Tuple

from ercot_scraping.config.queries import (
    CREATE_FINAL_TABLE_QUERY,
    MERGE_DATA_QUERY,
)
from ercot_scraping.utils.logging_utils import setup_module_logging

# Configure logging
logger = logging.getLogger(__name__)
per_run_handler = setup_module_logging(__name__)


def get_common_date_hour_pairs(conn: Connection) -> List[Tuple[str, int]]:
    """
    Returns a sorted list of (date, hour) pairs that are present in all required tables.
    If any required table is missing, returns an empty list.
    """
    tables_and_cols = [
        ("BID_AWARDS", "deliveryDate", "hourEnding"),
        ("BIDS", "deliveryDate", "hourEnding"),
        ("SETTLEMENT_POINT_PRICES", "deliveryDate", "deliveryHour"),
        ("OFFERS", "deliveryDate", "hourEnding"),
        ("OFFER_AWARDS", "deliveryDate", "hourEnding"),
    ]
    sets = []
    cursor = conn.cursor()
    for table, date_col, hour_col in tables_and_cols:
        try:
            cursor.execute(f"SELECT {date_col}, {hour_col} FROM {table}")
            pairs = set(cursor.fetchall())
            logger.info("Found %d date-hour pairs in %s", len(pairs), table)
            sets.append(pairs)
        except sqlite3.Error:
            logger.warning(
                "%s table does not exist. Returning empty list.", table)
            return []
    if not sets:
        common = set()
    else:
        common = set.intersection(*sets)
    logger.info("Found %d common date-hour pairs across all tables", len(common))
    return sorted(common)


def create_final_table(conn: Connection) -> None:
    """
    Creates the FINAL table in the database.

    Args:
        conn (Connection): SQLite database connection
    """
    cursor = conn.cursor()
    cursor.execute(CREATE_FINAL_TABLE_QUERY)
    conn.commit()
    logger.info("Created FINAL table successfully")


def merge_data(db: Union[str, Connection], batch_size: int = 50) -> None:
    """
    Efficiently merges data for only those (DeliveryDate, HourEnding) pairs present in all relevant tables.
    For test/simple queries, just run the query as-is (no batching/WHERE logic).
    """
    conn_to_close = None
    try:
        if isinstance(db, str):
            logger.info("Starting merge-data process for database: %s", db)
            conn = sqlite3.connect(db)
            conn_to_close = conn
        else:
            logger.info(
                "Starting merge-data process for provided SQLite connection object")
            conn = db
        create_final_table(conn)
        cursor = conn.cursor()
        # If the query does not use 'ba.' or 'oa.' (test/simple query), just run it once
        if 'ba.' not in MERGE_DATA_QUERY and 'oa.' not in MERGE_DATA_QUERY:
            logger.info(
                "Detected simple/test MERGE_DATA_QUERY, running as-is.")
            cursor.execute(MERGE_DATA_QUERY)
            conn.commit()
            return
        # Otherwise, use batching by (DeliveryDate, HourEnding)
        common_pairs = get_common_date_hour_pairs(conn)
        if not common_pairs:
            logger.warning(
                "No common (DeliveryDate, HourEnding) pairs found across all tables. Nothing to merge.")
            return
        logger.info("Merging data for %d date-hour pairs in batches of %d",
                    len(common_pairs), batch_size)
        # Check for required tables before batching
        required_tables = [
            "BID_AWARDS", "BIDS", "SETTLEMENT_POINT_PRICES", "OFFER_AWARDS", "OFFERS"
        ]
        cursor.execute(
            "SELECT name FROM sqlite_master WHERE type='table';")
        existing_tables = set(row[0] for row in cursor.fetchall())
        if not all(tbl in existing_tables for tbl in required_tables):
            logger.warning(
                "One or more required tables are missing: %s. Skipping merge for these batches.",
                [tbl for tbl in required_tables if tbl not in existing_tables]
            )
            return

        for i in range(0, len(common_pairs), batch_size):
            batch = common_pairs[i:i+batch_size]
            for date, hour in batch:
                # Refactor: filter each branch of UNION ALL by date/hour
                merge_query = """
INSERT INTO FINAL (
    deliveryDate,
    hourEnding,
    settlementPointName,
    qseName,
    settlementPointPrice,
    MARK_PRICE,
    blockCurve,
    sourceType,
    energyOnlyBidAwardInMW,
    bidId,
    BID_PRICE,
    BID_SIZE,
    energyOnlyOfferAwardMW,
    offerId,
    OFFER_PRICE,
    OFFER_SIZE,
    INSERTED_AT
)
SELECT
    ba.deliveryDate,
    ba.HourEnding,
    ba.SettlementPoint as settlementPointName,
    ba.QSEName,
    COALESCE(spp.SettlementPointPrice, ba.SettlementPointPrice) as settlementPointPrice,
    spp.SettlementPointPrice as MARK_PRICE,
    b.BlockCurveIndicator as blockCurve,
    'Bid' as sourceType,
    ba.EnergyOnlyBidAwardInMW as energyOnlyBidAwardInMW,
    ba.BidId,
    CASE
        WHEN b.EnergyOnlyBidMW1 IS NOT NULL THEN b.EnergyOnlyBidPrice1
        WHEN b.EnergyOnlyBidMW2 IS NOT NULL THEN b.EnergyOnlyBidPrice2
        WHEN b.EnergyOnlyBidMW3 IS NOT NULL THEN b.EnergyOnlyBidPrice3
        WHEN b.EnergyOnlyBidMW4 IS NOT NULL THEN b.EnergyOnlyBidPrice4
        WHEN b.EnergyOnlyBidMW5 IS NOT NULL THEN b.EnergyOnlyBidPrice5
        ELSE NULL
    END as BID_PRICE,
    CASE
        WHEN b.EnergyOnlyBidMW1 IS NOT NULL THEN b.EnergyOnlyBidMW1
        WHEN b.EnergyOnlyBidMW2 IS NOT NULL THEN b.EnergyOnlyBidMW2
        WHEN b.EnergyOnlyBidMW3 IS NOT NULL THEN b.EnergyOnlyBidMW3
        WHEN b.EnergyOnlyBidMW4 IS NOT NULL THEN b.EnergyOnlyBidMW4
        WHEN b.EnergyOnlyBidMW5 IS NOT NULL THEN b.EnergyOnlyBidMW5
        ELSE NULL
    END as BID_SIZE,
    NULL as energyOnlyOfferAwardInMW,
    NULL as offerId,
    NULL as OFFER_PRICE,
    NULL as OFFER_SIZE,
    datetime('now') as INSERTED_AT
FROM BID_AWARDS ba
LEFT JOIN BIDS b ON ba.BidId = b.EnergyOnlyBidID
    AND ba.DeliveryDate = b.DeliveryDate
    AND ba.HourEnding = b.HourEnding
LEFT JOIN SETTLEMENT_POINT_PRICES spp ON ba.SettlementPoint = spp.SettlementPointName
    AND ba.DeliveryDate = spp.DeliveryDate
    AND ba.HourEnding = spp.DeliveryHour
WHERE ba.DeliveryDate = ? AND ba.HourEnding = ?
UNION ALL
SELECT
    oa.DeliveryDate,
    oa.HourEnding,
    oa.SettlementPoint as settlementPointName,
    oa.QSEName,
    COALESCE(spp.SettlementPointPrice, oa.SettlementPointPrice) as settlementPointPrice,
    spp.SettlementPointPrice as MARK_PRICE,
    o.BlockCurveIndicator as blockCurve,
    'Offer' as sourceType,
    NULL as energyOnlyBidAwardInMW,
    NULL as bidId,
    NULL as BID_PRICE,
    NULL as BID_SIZE,
    oa.EnergyOnlyOfferAwardMW as energyOnlyOfferAwardMW,
    oa.OfferID as offerId,
    CASE
        WHEN o.EnergyOnlyOfferMW1 IS NOT NULL THEN o.EnergyOnlyOfferPrice1
        WHEN o.EnergyOnlyOfferMW2 IS NOT NULL THEN o.EnergyOnlyOfferPrice2
        WHEN o.EnergyOnlyOfferMW3 IS NOT NULL THEN o.EnergyOnlyOfferPrice3
        WHEN o.EnergyOnlyOfferMW4 IS NOT NULL THEN o.EnergyOnlyOfferPrice4
        WHEN o.EnergyOnlyOfferMW5 IS NOT NULL THEN o.EnergyOnlyOfferPrice5
        ELSE NULL
    END as OFFER_PRICE,
    CASE
        WHEN o.EnergyOnlyOfferMW1 IS NOT NULL THEN o.EnergyOnlyOfferMW1
        WHEN o.EnergyOnlyOfferMW2 IS NOT NULL THEN o.EnergyOnlyOfferMW2
        WHEN o.EnergyOnlyOfferMW3 IS NOT NULL THEN o.EnergyOnlyOfferMW3
        WHEN o.EnergyOnlyOfferMW4 IS NOT NULL THEN o.EnergyOnlyOfferMW4
        WHEN o.EnergyOnlyOfferMW5 IS NOT NULL THEN o.EnergyOnlyOfferMW5
        ELSE NULL
    END as OFFER_SIZE,
    datetime('now') as INSERTED_AT
FROM OFFER_AWARDS oa
LEFT JOIN OFFERS o ON oa.OfferID = o.EnergyOnlyOfferID
    AND oa.DeliveryDate = o.DeliveryDate
    AND oa.HourEnding = o.HourEnding
LEFT JOIN SETTLEMENT_POINT_PRICES spp ON oa.SettlementPoint = spp.SettlementPointName
    AND oa.DeliveryDate = spp.DeliveryDate
    AND oa.HourEnding = spp.DeliveryHour
WHERE oa.DeliveryDate = ? AND oa.HourEnding = ?
"""
                cursor.execute(merge_query, (date, hour, date, hour))
            conn.commit()
        logger.info("merge-data process completed successfully")
    except sqlite3.Error as e:
        logger.error(f"Error merging data: {e}")
        raise
    finally:
        if conn_to_close:
            conn_to_close.close()
