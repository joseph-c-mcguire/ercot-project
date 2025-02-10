import csv
from typing import Set
import sqlite3

from config import (
    GET_ACTIVE_SETTLEMENT_POINTS_QUERY,
    FETCH_BID_SETTLEMENT_POINTS_QUERY,
    CHECK_EXISTING_TABLES_QUERY,
    FETCH_OFFER_SETTLEMENT_POINTS_QUERY,
)


def load_qse_shortnames(csv_path: str) -> Set[str]:
    """
    Load QSE short names from a CSV file into a set.

    Args:
        csv_path (str): Path to the CSV file containing QSE short names

    Returns:
        Set[str]: Set of QSE short names
    """
    shortnames = set()
    with open(csv_path, "r") as f:
        reader = csv.DictReader(f)
        for row in reader:
            if "SHORT NAME" in row and row["SHORT NAME"]:
                shortnames.add(row["SHORT NAME"].strip())
    return shortnames


def filter_by_qse_names(data: dict, qse_names: Set[str]) -> dict:
    """
    Filter data to keep only records where QSEName matches one in the provided set.

    Args:
        data (dict): Data dictionary with a 'data' key containing list of records
        qse_names (Set[str]): Set of QSE names to filter by

    Returns:
        dict: Filtered data dictionary
    """
    if not data or "data" not in data:
        return data

    filtered_records = [
        record
        for record in data["data"]
        if "QSEName" in record and record["QSEName"] in qse_names
    ]

    return {"data": filtered_records}


def get_active_settlement_points(db_name: str) -> Set[str]:
    """
    Get unique settlement points that appear in either BID_AWARDS or OFFER_AWARDS tables.
    If the tables don't exist, returns an empty set.

    Args:
        db_name (str): Database file path

    Returns:
        Set[str]: Set of unique settlement point names
    """
    conn = sqlite3.connect(db_name)
    cursor = conn.cursor()

    # First check if the tables exist
    cursor.execute(CHECK_EXISTING_TABLES_QUERY)
    existing_tables = {row[0] for row in cursor.fetchall()}

    if not existing_tables:
        conn.close()
        return set()

    # Dynamically build query based on existing tables
    queries = []
    if "BID_AWARDS" in existing_tables:
        queries.append(FETCH_BID_SETTLEMENT_POINTS_QUERY)
    if "OFFER_AWARDS" in existing_tables:
        queries.append(FETCH_OFFER_SETTLEMENT_POINTS_QUERY)

    if not queries:
        conn.close()
        return set()

    query = " UNION ".join(queries)
    UNIQUE_SETTLEMENT_POINTS_QUERY = f"SELECT DISTINCT SettlementPoint FROM ({query})"
    cursor.execute(UNIQUE_SETTLEMENT_POINTS_QUERY)
    points = {row[0] for row in cursor.fetchall()}

    conn.close()
    return points


def filter_by_settlement_points(data: dict, settlement_points: Set[str]) -> dict:
    """
    Filter settlement point price data to keep only records matching active settlement points.

    Args:
        data (dict): Data dictionary with settlement point price records
        settlement_points (Set[str]): Set of settlement point names to keep

    Returns:
        dict: Filtered data dictionary
    """
    if not data or "data" not in data:
        return data

    filtered_records = [
        record
        for record in data["data"]
        if "SettlementPointName" in record
        and record["SettlementPointName"] in settlement_points
    ]

    return {"data": filtered_records}
