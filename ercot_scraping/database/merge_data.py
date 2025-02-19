import sqlite3
import logging
from typing import Union
from sqlite3 import Connection

from ercot_scraping.config.queries import CREATE_FINAL_TABLE_QUERY, MERGE_DATA_QUERY

logger = logging.getLogger(__name__)


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


def merge_data(db: Union[str, Connection]) -> None:
    """
    Merges data from BID_AWARDS, BIDS, and SETTLEMENT_POINT_PRICES tables into FINAL table.

    Args:
        db (Union[str, Connection]): Either a path to SQLite database or an existing connection
    """
    conn_to_close = None
    try:
        if isinstance(db, str):
            conn = sqlite3.connect(db)
            conn_to_close = conn
        else:
            conn = db

        cursor = conn.cursor()

        # Create FINAL table if it doesn't exist
        create_final_table(conn)

        # Clear existing data from FINAL table
        cursor.execute("DELETE FROM FINAL")

        # Merge data
        cursor.execute(MERGE_DATA_QUERY)

        # Get number of rows inserted
        cursor.execute("SELECT COUNT(*) FROM FINAL")
        row_count = cursor.fetchone()[0]

        conn.commit()
        logger.info(f"Merged {row_count} rows into FINAL table successfully")

    except sqlite3.Error as e:
        logger.error(f"Error merging data: {e}")
        raise
    finally:
        if conn_to_close:
            conn_to_close.close()
