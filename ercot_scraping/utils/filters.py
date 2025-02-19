import contextlib
import csv
from typing import Set
import sqlite3
from pathlib import Path


def load_qse_shortnames(csv_file: str | Path) -> Set[str]:
    """
    Load QSE short names from a CSV file.

    Args:
        csv_file: Path to CSV file containing QSE short names

    Returns:
        Set of QSE short names
    """
    qse_names = set()
    try:
        with open(csv_file, 'r', newline='') as f:
            reader = csv.DictReader(f)
            if 'SHORT NAME' not in reader.fieldnames:
                return set()
            for row in reader:
                if name := row['SHORT NAME'].strip():
                    qse_names.add(name)
    except (FileNotFoundError, KeyError):
        return set()
    return qse_names


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
        with contextlib.suppress(sqlite3.OperationalError):
            cursor.execute(query)
            points.update(row[0] for row in cursor.fetchall())
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
    if not data or "data" not in data:
        return {"data": []}

    filtered_records = []
    field_variations = [
        "settlementPointName",
        "SettlementPointName",
        "SettlementPoint",
        "settlementPoint"
    ]

    for record in data["data"]:
        point_name = next(
            (record[field] for field in field_variations if field in record),
            None,
        )
        if point_name and point_name in settlement_points:
            filtered_records.append(record)

    return {"data": filtered_records}


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
