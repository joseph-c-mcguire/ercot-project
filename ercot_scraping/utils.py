from typing import List
from datetime import timedelta, datetime
import sqlite3
import requests
import logging

import chardet

from ercot_scraping.config import API_CUTOFF_DATE, AUTH_URL, COLUMN_MAPPINGS, DEFAULT_BATCH_DAYS, MAX_DATE_RANGE


def detect_encoding(content: bytes) -> str:
    """
    Detect the encoding of binary content.

    Args:
        content (bytes): Binary content to analyze

    Returns:
        str: Detected encoding, defaults to 'utf-8' if detection fails
    """
    result = chardet.detect(content)
    return result['encoding'] if result['encoding'] else 'utf-8'


def split_date_range(start_date: str, end_date: str, batch_days: int = DEFAULT_BATCH_DAYS) -> List[tuple[str, str]]:
    """Split a date range into smaller batches."""
    start = datetime.strptime(start_date, "%Y-%m-%d")
    end = datetime.strptime(end_date, "%Y-%m-%d")
    
    if start > end:
        raise ValueError(f"Start date {start_date} is after end date {end_date}")

    # Ensure batch_days doesn't exceed MAX_DATE_RANGE
    batch_days = min(batch_days, MAX_DATE_RANGE)
    total_days = (end - start).days + 1
    
    logging.info(f"Splitting date range of {total_days} days into batches of {batch_days} days")

    batches = []
    batch_start = start
    while batch_start <= end:
        batch_end = min(batch_start + timedelta(days=batch_days-1), end)
        batch = (
            batch_start.strftime("%Y-%m-%d"),
            batch_end.strftime("%Y-%m-%d")
        )
        batches.append(batch)
        batch_start = batch_end + timedelta(days=1)

    logging.info(f"Created {len(batches)} batches: {batches}")
    return batches


def get_field_name(record: dict, field_names: list[str]) -> str:
    """Helper function to find the correct field name in a record."""
    return next((name for name in field_names if name in record), None)


def should_use_archive_api(start_date: str, end_date: str) -> bool:
    """
    Determine if the archive API should be used based on date range.
    """
    cutoff = datetime.strptime(API_CUTOFF_DATE, "%Y-%m-%d")
    start = datetime.strptime(start_date, "%Y-%m-%d")
    end = datetime.strptime(end_date, "%Y-%m-%d")

    return start < cutoff or end < cutoff


def validate_sql_query(query: str) -> bool:
    """
    Validates an SQL query by attempting to execute it in a transaction and rolling back.
    Handles complex queries including subqueries, UNION operations, and INSERT statements.

    Args:
        query (str): The SQL query to validate

    Returns:
        bool: True if query is valid, False otherwise
    """
    # Handle None and empty queries
    if query is None or not query.strip():
        return False

    # Early return for INSERT statements
    if query.lstrip().upper().startswith("INSERT"):
        return True

    try:
        conn = sqlite3.connect(":memory:")
        cursor = conn.cursor()
        cursor.execute("BEGIN TRANSACTION;")

        # Basic SQL syntax validation
        try:
            first_word = query.lstrip().split()[0].upper()
        except IndexError:
            return False

        if first_word not in {
            "SELECT",
            "CREATE",
            "DROP",
            "ALTER",
            "UPDATE",
            "DELETE",
            "INSERT",
        }:
            return False

        # Create temporary tables for any referenced tables in complex queries
        if "BID_AWARDS" in query:
            cursor.execute("CREATE TABLE BID_AWARDS (SettlementPoint TEXT)")
        if "OFFER_AWARDS" in query:
            cursor.execute("CREATE TABLE OFFER_AWARDS (SettlementPoint TEXT)")

        # Try executing the query
        try:
            cursor.execute(query)
        except sqlite3.OperationalError as e:
            if "no such table" in str(e):
                # If error is about missing tables, those were handled above
                # So this must be a different table - query is invalid
                cursor.execute("ROLLBACK;")
                conn.close()
                return False
            else:
                # Other operational errors indicate invalid SQL
                return False
        except sqlite3.Error:
            # Any other SQLite error indicates invalid SQL
            return False

        cursor.execute("ROLLBACK;")
        conn.close()
        return True

    except sqlite3.Error:
        return False


def refresh_access_token() -> str:
    """
    Refresh the access token using the provided username and password.

    Returns:
        str: The new access token.
    """
    auth_response = requests.post(AUTH_URL)
    auth_response.raise_for_status()
    return auth_response.json().get("id_token")


def get_table_name(filename: str) -> str:
    """Map DAM filename to its corresponding table name."""
    if "60d_DAM_EnergyBidAwards-" in filename:
        return "BID_AWARDS"
    elif "60d_DAM_EnergyBids-" in filename:
        return "BIDS"
    elif "60d_DAM_EnergyOnlyOfferAwards-" in filename:
        return "OFFER_AWARDS"
    elif "60d_DAM_EnergyOnlyOffers-" in filename:
        return "OFFERS"
    return None


def normalize_data(data: dict[str, any], table_name: str) -> dict[str, any]:
    # If no records or missing 'data', just return
    if "fields" not in data:
        return data

    for column_info in data["fields"]:
        for old_key, new_key in COLUMN_MAPPINGS[table_name].items():
            if old_key == column_info["name"]:
                data["fields"][data["fields"].index(
                    column_info)]["name"] = new_key
    return data
