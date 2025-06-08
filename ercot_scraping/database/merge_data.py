import logging
import sqlite3
from sqlite3 import Connection
from typing import Union

from ercot_scraping.config.queries import (
    CREATE_FINAL_TABLE_QUERY,
    MERGE_DATA_QUERY,
)
from ercot_scraping.utils.logging_utils import setup_module_logging

# Configure logging
logger = logging.getLogger(__name__)
per_run_handler = setup_module_logging(__name__)


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


def merge_data(db: Union[str,
                         Connection],
               start_date: str = None,
               end_date: str = None) -> None:
    """
    Merges data from BID_AWARDS, BIDS, and SETTLEMENT_POINT_PRICES tables into FINAL table.
    Now always merges all available data, regardless of date.

    Args:
        db (Union[str, Connection]): Either a path to SQLite database or an existing connection
        start_date (str, optional): Ignored. Kept for backward compatibility.
        end_date (str, optional): Ignored. Kept for backward compatibility.
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

        cursor = conn.cursor()

        # Check existence and row count of source tables
        for table in ["BID_AWARDS", "BIDS", "SETTLEMENT_POINT_PRICES"]:
            try:
                cursor.execute(f"SELECT COUNT(*) FROM {table}")
                count = cursor.fetchone()[0]
                logger.info("%s table exists with %d rows", table, count)
                if count == 0:
                    logger.warning("%s table is empty.", table)
                # Debug: List available dates if logging level is DEBUG or
                # lower
                if logger.isEnabledFor(logging.DEBUG):
                    try:
                        cursor.execute(
                            f"SELECT DISTINCT DeliveryDate FROM {table} ORDER BY DeliveryDate")
                        dates = [row[0] for row in cursor.fetchall()]
                        logger.debug("Available dates in %s: %s", table, dates)
                    except Exception as e:
                        logger.debug(
                            "Could not fetch available dates for %s: %s", table, e)
            except sqlite3.Error:
                logger.warning("%s table does not exist. Skipping.", table)

        # Create FINAL table if it doesn't exist
        create_final_table(conn)

        # Always merge all available data (no date filter)
        logger.info("Merging all available data (no date filter)")
        cursor.execute(MERGE_DATA_QUERY)

        # Get number of rows inserted
        cursor.execute("SELECT COUNT(*) FROM FINAL")
        row_count = cursor.fetchone()[0]

        conn.commit()
        if row_count == 0:
            logger.warning("No rows were merged into FINAL. Debugging info:")
            # Show unique values from BID_AWARDS
            try:
                cursor.execute(
                    "SELECT COUNT(*), MIN(DeliveryDate), MAX(DeliveryDate) FROM BID_AWARDS")
                count, min_date, max_date = cursor.fetchone()
                logger.warning(
                    "BID_AWARDS: count=%d, min(DeliveryDate)=%s, max(DeliveryDate)=%s", count, min_date, max_date)
                cursor.execute(
                    "SELECT COUNT(DISTINCT SettlementPoint) FROM BID_AWARDS")
                sp_count = cursor.fetchone()[0]
                logger.warning(
                    "BID_AWARDS: unique SettlementPoint count=%d", sp_count)
                cursor.execute("SELECT COUNT(DISTINCT BidId) FROM BID_AWARDS")
                bidid_count = cursor.fetchone()[0]
                logger.warning(
                    "BID_AWARDS: unique BidId count=%d", bidid_count)
            except Exception as e:
                logger.warning("Could not fetch BID_AWARDS diagnostics: %s", e)
            # Show similar info for BIDS
            try:
                cursor.execute(
                    "SELECT COUNT(*), MIN(DeliveryDate), MAX(DeliveryDate) FROM BIDS")
                count, min_date, max_date = cursor.fetchone()
                logger.warning(
                    "BIDS: count=%d, min(DeliveryDate)=%s, max(DeliveryDate)=%s", count, min_date, max_date)
                cursor.execute(
                    "SELECT COUNT(DISTINCT EnergyOnlyBidID) FROM BIDS")
                bidid_count = cursor.fetchone()[0]
                logger.warning(
                    "BIDS: unique EnergyOnlyBidID count=%d", bidid_count)
            except Exception as e:
                logger.warning("Could not fetch BIDS diagnostics: %s", e)
            # Show similar info for SETTLEMENT_POINT_PRICES
            try:
                cursor.execute(
                    "SELECT COUNT(*), MIN(DeliveryDate), MAX(DeliveryDate) FROM SETTLEMENT_POINT_PRICES")
                count, min_date, max_date = cursor.fetchone()
                logger.warning(
                    "SETTLEMENT_POINT_PRICES: count=%d, min(DeliveryDate)=%s, max(DeliveryDate)=%s", count, min_date, max_date)
                cursor.execute(
                    "SELECT COUNT(DISTINCT SettlementPointName) FROM SETTLEMENT_POINT_PRICES")
                sp_count = cursor.fetchone()[0]
                logger.warning(
                    "SETTLEMENT_POINT_PRICES: unique SettlementPointName count=%d", sp_count)
            except Exception as e:
                logger.warning(
                    "Could not fetch SETTLEMENT_POINT_PRICES diagnostics: %s", e)
        logger.info("Merged %d rows into FINAL table successfully", row_count)
        logger.info("merge-data process completed successfully")

    except sqlite3.Error as e:
        logger.error(f"Error merging data: {e}")
        raise
    finally:
        if conn_to_close:
            conn_to_close.close()
