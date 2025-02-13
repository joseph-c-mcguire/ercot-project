import csv
from typing import Set
import sqlite3

from ercot_scraping.utils import get_field_name
from ercot_scraping.config import COLUMN_MAPPINGS


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

    points = set()

    # Query both award tables for settlement points
    queries = [
        "SELECT SettlementPoint FROM BID_AWARDS",
        "SELECT SettlementPoint FROM OFFER_AWARDS"
    ]

    for query in queries:
        try:
            cursor.execute(query)
            points.update(row[0] for row in cursor.fetchall())
        except sqlite3.OperationalError:
            # Table might not exist yet, ignore
            pass

    conn.close()
    return points


def filter_by_settlement_points(data: dict, settlement_points: Set[str]) -> dict:
    """
    Filter data to only include records matching the given settlement points.

    Args:
        data (dict): Dictionary containing a 'data' key with records
        settlement_points (Set[str]): Set of settlement point names to filter by

    Returns:
        dict: Filtered data dictionary containing only records matching settlement_points
    """
    # Common variations of settlement point field names
    point_field_names = list(COLUMN_MAPPINGS["settlement_point_prices"].keys())
    filtered_data = {"data": []}
    for record in data["data"]:
        field_name = get_field_name(record, point_field_names)
        if field_name and record[field_name] in settlement_points:
            filtered_data["data"].append(record)

    # Preserve other fields if they exist
    if "fields" in data:
        filtered_data["fields"] = data["fields"]

    return filtered_data


def format_qse_filter_param(qse_names: Set[str]) -> str:
    """
    Format QSE names for API query parameter.

    Args:
        qse_names (Set[str]): Set of QSE short names

    Returns:
        str: Formatted string for API query (e.g., "QABCD,QXYZ1")
    """
    # Filter for only QSE names (starting with 'Q')
    qse_names = {name for name in qse_names if name.startswith('Q')}
    return ','.join(sorted(qse_names))
