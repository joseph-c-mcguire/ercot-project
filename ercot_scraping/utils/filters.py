"""
Utility functions for filtering and formatting ERCOT data.

This module provides functions to:
- Load QSE short names from a CSV file
- Filter data by QSE names or settlement points
- Retrieve active settlement points from a SQLite database
- Format QSE names for API query parameters

Intended for use in ERCOT data scraping and processing workflows.
"""

import contextlib
import csv
from typing import Set, Union
import sqlite3
from pathlib import Path


def load_qse_shortnames(csv_file: Union[str, Path]) -> Set[str]:
    """
    Load QSE short names from a CSV file.

    Args:
        csv_file: Path to CSV file containing QSE short names

    Returns:
        Set of QSE short names
    """
    qse_names = set()
    try:
        with open(csv_file, 'r', newline='', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            if not reader.fieldnames or 'SHORT NAME' not in reader.fieldnames:
                return set()
            for row in reader:
                name = row.get('SHORT NAME', '').strip()
                if name:
                    qse_names.add(name)
    except (FileNotFoundError, KeyError, UnicodeDecodeError):
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


def filter_by_settlement_points(data, settlement_points):
    """
    Filter data by settlement points, supporting multiple field name variations.
    For each item, match only on the first field found in field_variations.
    """
    if data is None or not isinstance(data, dict):
        return {"data": []}
    items = data.get("data", [])
    if not isinstance(items, list):
        return {"data": []}
    field_variations = [
        "SettlementPoint",
        "settlementPointName",
        "SettlementPointName",
        "settlementPoint",
    ]
    filtered = []
    for item in items:
        for field in field_variations:
            if field in item:
                value = item.get(field)
                if value and value in settlement_points:
                    filtered.append(item)
                break  # Only consider the first matching field
    return {"data": filtered}


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
