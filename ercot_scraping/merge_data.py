import sqlite3
import logging

from ercot_scraping.queries import CREATE_FINAL_TABLE_QUERY, MERGE_DATA_QUERY

logger = logging.getLogger(__name__)


def create_final_table(db_name: str) -> None:
    """
    Creates the FINAL table in the database.

    Args:
        db_name (str): Path to the SQLite database
    """
    try:
        conn = sqlite3.connect(db_name)
        cursor = conn.cursor()

        cursor.execute(CREATE_FINAL_TABLE_QUERY)
        conn.commit()
        logger.info("Created FINAL table successfully")

    except sqlite3.Error as e:
        logger.error(f"Error creating FINAL table: {e}")
        raise
    finally:
        conn.close()


def merge_data(db_name: str) -> None:
    """
    Merges data from BID_AWARDS, BIDS, and SETTLEMENT_POINT_PRICES tables into FINAL table.

    Args:
        db_name (str): Path to the SQLite database
    """
    try:
        conn = sqlite3.connect(db_name)
        cursor = conn.cursor()

        # Create FINAL table if it doesn't exist
        create_final_table(db_name)

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
        conn.close()
