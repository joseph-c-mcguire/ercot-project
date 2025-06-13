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
    Returns a list of (DeliveryDate, HourEnding) pairs that are present in all relevant tables.
    """
    cursor = conn.cursor()
    tables_and_cols = [
        ("BID_AWARDS", "DeliveryDate", "HourEnding"),
        ("BIDS", "DeliveryDate", "HourEnding"),
        ("SETTLEMENT_POINT_PRICES", "DeliveryDate", "DeliveryHour"),
        ("OFFERS", "DeliveryDate", "HourEnding"),
        ("OFFER_AWARDS", "DeliveryDate", "HourEnding"),
    ]
    sets = []
    for table, date_col, hour_col in tables_and_cols:
        try:
            cursor.execute(
                f"SELECT DISTINCT {date_col}, {hour_col} FROM {table}")
            pairs = set(cursor.fetchall())
            logger.info("Found %d date-hour pairs in %s", len(pairs), table)
            sets.append(pairs)
        except sqlite3.Error:
            logger.warning("%s table does not exist. Skipping.", table)
            return []
    if not sets:
        return []
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
        total_inserted = 0
        for i in range(0, len(common_pairs), batch_size):
            batch = common_pairs[i:i+batch_size]
            for date, hour in batch:
                merge_query = MERGE_DATA_QUERY + \
                    " WHERE (ba.DeliveryDate = ? AND ba.HourEnding = ?) OR (oa.DeliveryDate = ? AND oa.HourEnding = ?)"
                cursor.execute(merge_query, (date, hour, date, hour))
            conn.commit()
            cursor.execute("SELECT COUNT(*) FROM FINAL WHERE deliveryDate IN ({}) AND hourEnding IN ({})".format(
                ",".join("?" for _ in batch), ",".join("?" for _ in batch)),
                [d for d, _ in batch] + [h for _, h in batch])
            inserted = cursor.fetchone()[0]
            total_inserted += inserted
            logger.info("Inserted %d rows for batch %d",
                        inserted, (i // batch_size) + 1)
        logger.info("Merged %d rows into FINAL table successfully",
                    total_inserted)
        logger.info("merge-data process completed successfully")
    except sqlite3.Error as e:
        logger.error(f"Error merging data: {e}")
        raise
    finally:
        if conn_to_close:
            conn_to_close.close()
