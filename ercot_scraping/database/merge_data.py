import sqlite3
import logging
from typing import Union
from sqlite3 import Connection

from ercot_scraping.config.queries import CREATE_FINAL_TABLE_QUERY, MERGE_DATA_QUERY
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
    Optionally restricts to a date range if start_date and end_date are provided.

    Args:
        db (Union[str, Connection]): Either a path to SQLite database or an existing connection
        start_date (str, optional): Start date in 'YYYY-MM-DD' format. Defaults to None.
        end_date (str, optional): End date in 'YYYY-MM-DD' format. Defaults to None.
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

        # Clear existing data from FINAL table
        logger.info("Clearing existing data from FINAL table")
        cursor.execute("DELETE FROM FINAL")

        # Build merge query with optional date filter
        from ercot_scraping.config.queries import MERGE_DATA_QUERY
        if start_date and end_date:
            logger.info(
                f"Merging data for date range: {start_date} to {end_date}")
            # Log how many BID_AWARDS, BIDS, and SETTLEMENT_POINT_PRICES are in
            # the date range
            for table, date_col in [
                ("BID_AWARDS", "DeliveryDate"),
                ("BIDS", "DeliveryDate"),
                ("SETTLEMENT_POINT_PRICES", "DeliveryDate")
            ]:
                try:
                    cursor.execute(
                        f"SELECT COUNT(*) FROM {table} WHERE {date_col} BETWEEN ? AND ?",
                        (start_date,
                         end_date))
                    count = cursor.fetchone()[0]
                    logger.info(f"%s rows in %s between %s and %s",
                                count, table, start_date, end_date)
                except sqlite3.Error:
                    logger.warning(
                        "%s table does not exist. Skipping date range count.", table)
            merge_query = MERGE_DATA_QUERY.rstrip().rstrip("\n")
            # Add WHERE clause to filter BID_AWARDS by DeliveryDate
            merge_query += "\nWHERE ba.DeliveryDate BETWEEN ? AND ?"
            cursor.execute(merge_query, (start_date, end_date))
        else:
            logger.info("Merging all available data (no date filter)")
            cursor.execute(MERGE_DATA_QUERY)

        # Get number of rows inserted
        cursor.execute("SELECT COUNT(*) FROM FINAL")
        row_count = cursor.fetchone()[0]

        conn.commit()
        logger.info("Merged %d rows into FINAL table successfully", row_count)
        logger.info("merge-data process completed successfully")

    except sqlite3.Error as e:
        logger.error(f"Error merging data: {e}")
        raise
    finally:
        if conn_to_close:
            conn_to_close.close()
