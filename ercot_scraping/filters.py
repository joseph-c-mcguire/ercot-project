import csv
from typing import Set, Optional


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
