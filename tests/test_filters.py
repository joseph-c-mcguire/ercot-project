import csv
import sqlite3
import pytest
from ercot_scraping.filters import (
    load_qse_shortnames,
    filter_by_qse_names,
    get_active_settlement_points,
    filter_by_settlement_points,
)
# Changed from ercot_api to utils
from ercot_scraping.utils import validate_sql_query
# Import constants
from ercot_scraping.config import (
    FETCH_BID_SETTLEMENT_POINTS_QUERY,
    CHECK_EXISTING_TABLES_QUERY,
    FETCH_OFFER_SETTLEMENT_POINTS_QUERY,
    SETTLEMENT_POINT_PRICES_INSERT_QUERY,
    BID_AWARDS_INSERT_QUERY,
    BIDS_INSERT_QUERY,
    OFFERS_INSERT_QUERY,
    OFFER_AWARDS_INSERT_QUERY,
    SETTLEMENT_POINT_PRICES_TABLE_CREATION_QUERY,
    BIDS_TABLE_CREATION_QUERY,
    BID_AWARDS_TABLE_CREATION_QUERY,
    OFFERS_TABLE_CREATION_QUERY,
    OFFER_AWARDS_TABLE_CREATION_QUERY,
    GET_ACTIVE_SETTLEMENT_POINTS_QUERY
)


def write_csv(tmp_path, filename, header, rows):
    """Write test data to a CSV file."""
    file_path = tmp_path / filename
    with file_path.open("w", newline="", encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=header)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)
    return file_path


@pytest.mark.parametrize(
    "test_case",
    [
        {
            "name": "valid_data",
            "header": ["SHORT NAME", "OTHER"],
            "rows": [
                # Leading/trailing spaces
                {"SHORT NAME": " QSE1 ", "OTHER": "Value1"},
                {"SHORT NAME": "QSE2", "OTHER": "Value2"},    # No spaces
                {"SHORT NAME": "", "OTHER": "Ignore"},        # Empty name
                {"SHORT NAME": " ", "OTHER": "Ignore"},       # Just whitespace
                {"SHORT NAME": "QSE3", "OTHER": "Value3"},    # Normal entry
            ],
            "expected": {"QSE1", "QSE2", "QSE3"},
        },
        {
            "name": "empty_file",
            "header": ["SHORT NAME", "OTHER"],
            "rows": [],
            "expected": set(),
        },
        {
            "name": "missing_column",
            "header": ["NAME", "OTHER"],
            "rows": [
                {"NAME": "QSE1", "OTHER": "Value1"},
                {"NAME": "QSE2", "OTHER": "Value2"},
            ],
            "expected": set(),
        },
    ],
)
def test_load_qse_shortnames(tmp_path, test_case):
    """Test QSE shortname loading with various input cases."""
    csv_file = write_csv(
        tmp_path, f"{test_case['name']}.csv", test_case["header"], test_case["rows"]
    )
    # Print the contents of the file for debugging
    with open(csv_file, 'r', encoding='utf-8') as f:
        print(f"\nTest file contents for {test_case['name']}:")
        print(f.read())

    result = load_qse_shortnames(str(csv_file))
    assert result == test_case["expected"], \
        f"Failed for {test_case['name']}\nExpected: {test_case['expected']}\nGot: {result}"


@pytest.mark.parametrize(
    "test_case",
    [
        {
            "name": "valid_data",
            "data": {
                "data": [
                    {"QSEName": "QSE1", "value": 10},
                    {"QSEName": "QSE2", "value": 20},
                    {"QSEName": "OTHER", "value": 30},
                    {"value": 40},
                ]
            },
            "qse_names": {"QSE1", "QSE2"},
            "expected": {
                "data": [
                    {"QSEName": "QSE1", "value": 10},
                    {"QSEName": "QSE2", "value": 20},
                ]
            },
        },
        {
            "name": "no_data_key",
            "data": {"something": []},
            "qse_names": {"QSE1", "QSE2"},
            "expected": {"something": []},
        },
        {
            "name": "empty_data_list",
            "data": {"data": []},
            "qse_names": {"QSE1", "QSE2"},
            "expected": {"data": []},
        },
        {
            "name": "empty_qse_names",
            "data": {
                "data": [
                    {"QSEName": "QSE1", "value": 10},
                    {"QSEName": "QSE2", "value": 20},
                ]
            },
            "qse_names": set(),
            "expected": {"data": []},
        },
    ],
)
def test_filter_by_qse_names(test_case):
    result = filter_by_qse_names(test_case["data"], test_case["qse_names"])
    assert result == test_case["expected"]


@pytest.mark.parametrize(
    "sql_query",
    [
        FETCH_BID_SETTLEMENT_POINTS_QUERY,
        CHECK_EXISTING_TABLES_QUERY,
        FETCH_OFFER_SETTLEMENT_POINTS_QUERY,
    ],
)
def test_sql_queries_are_valid(sql_query):
    """Test that SQL queries are valid."""
    assert validate_sql_query(sql_query)


@pytest.mark.parametrize(
    "test_data",
    [
        {
            "bid_points": [("POINT1",), ("POINT2",)],
            "offer_points": [("POINT2",), ("POINT3",)],
            "expected_points": {"POINT1", "POINT2", "POINT3"},
        },
        {
            "bid_points": [("POINT1",), ("POINT1",)],  # Duplicates
            "offer_points": [("POINT2",), ("POINT2",)],
            "expected_points": {"POINT1", "POINT2"},
        },
        {
            "bid_points": [],
            "offer_points": [("POINT1",)],
            "expected_points": {"POINT1"},
        },
    ],
)
def test_settlement_points_queries(tmp_path, test_data):
    db_file = str(tmp_path / "test.db")
    conn = sqlite3.connect(db_file)
    cursor = conn.cursor()

    # Create tables
    cursor.execute("CREATE TABLE BID_AWARDS (SettlementPoint TEXT)")
    cursor.execute("CREATE TABLE OFFER_AWARDS (SettlementPoint TEXT)")

    # Insert data and commit
    cursor.executemany("INSERT INTO BID_AWARDS VALUES (?)",
                       test_data["bid_points"])
    cursor.executemany("INSERT INTO OFFER_AWARDS VALUES (?)",
                       test_data["offer_points"])
    conn.commit()  # Added commit before closing
    conn.close()

    # Test point retrieval after changes are committed
    points = get_active_settlement_points(db_file)
    assert points == test_data["expected_points"]


def test_get_active_settlement_points(tmp_path):
    # Create a temporary test database
    db_file = str(tmp_path / "test.db")
    conn = sqlite3.connect(db_file)
    cursor = conn.cursor()

    # Create test tables
    cursor.execute(
        """
        CREATE TABLE BID_AWARDS (
            SettlementPoint TEXT
        )
    """
    )
    cursor.execute(
        """
        CREATE TABLE OFFER_AWARDS (
            SettlementPoint TEXT
        )
    """
    )

    # Insert test data
    cursor.execute("INSERT INTO BID_AWARDS VALUES (?)", ("POINT1",))
    cursor.execute("INSERT INTO BID_AWARDS VALUES (?)", ("POINT2",))
    cursor.execute("INSERT INTO OFFER_AWARDS VALUES (?)", ("POINT2",))
    cursor.execute("INSERT INTO OFFER_AWARDS VALUES (?)", ("POINT3",))
    conn.commit()
    conn.close()

    # Test the function
    points = get_active_settlement_points(db_file)
    assert points == {"POINT1", "POINT2", "POINT3"}


def test_get_active_settlement_points_missing_tables(tmp_path):
    # Test with a database that doesn't have the required tables
    db_file = str(tmp_path / "empty_test.db")
    conn = sqlite3.connect(db_file)
    conn.close()

    points = get_active_settlement_points(db_file)
    assert points == set()


def test_get_active_settlement_points_partial_tables(tmp_path):
    # Test with a database that only has one of the tables
    db_file = str(tmp_path / "partial_test.db")
    conn = sqlite3.connect(db_file)
    cursor = conn.cursor()

    # Create only BID_AWARDS table
    cursor.execute(
        """
        CREATE TABLE BID_AWARDS (
            SettlementPoint TEXT
        )
    """
    )
    cursor.execute("INSERT INTO BID_AWARDS VALUES (?)", ("POINT1",))
    conn.commit()
    conn.close()

    points = get_active_settlement_points(db_file)
    assert points == {"POINT1"}


def test_filter_by_settlement_points():
    """Test filtering data by settlement points."""
    data = {
        "data": [
            {"settlementPointName": "POINT1", "value": 10},
            {"settlementPointName": "POINT2", "value": 20},
            {"settlementPointName": "OTHER", "value": 30},
            {"value": 40},  # Record missing settlementPointName
        ]
    }
    settlement_points = {"POINT1", "POINT2"}
    result = filter_by_settlement_points(data, settlement_points)
    expected = {
        "data": [
            {"settlementPointName": "POINT1", "value": 10},
            {"settlementPointName": "POINT2", "value": 20},
        ]
    }
    assert result == expected


def test_check_existing_tables_query(tmp_path):
    """Test CHECK_EXISTING_TABLES_QUERY returns correct table names."""
    db_file = str(tmp_path / "test.db")
    conn = sqlite3.connect(db_file)
    cursor = conn.cursor()

    # Create test tables
    cursor.execute("CREATE TABLE BID_AWARDS (SettlementPoint TEXT)")
    cursor.execute("CREATE TABLE OFFER_AWARDS (SettlementPoint TEXT)")
    cursor.execute("CREATE TABLE OTHER_TABLE (Data TEXT)")  # Should be ignored

    cursor.execute(CHECK_EXISTING_TABLES_QUERY)
    tables = {row[0] for row in cursor.fetchall()}

    conn.close()
    assert tables == {"BID_AWARDS", "OFFER_AWARDS"}


def test_fetch_bid_settlement_points_query(tmp_path):
    """Test FETCH_BID_SETTLEMENT_POINTS_QUERY returns correct points."""
    db_file = str(tmp_path / "test.db")
    conn = sqlite3.connect(db_file)
    cursor = conn.cursor()

    cursor.execute("CREATE TABLE BID_AWARDS (SettlementPoint TEXT)")
    test_points = ["POINT1", "POINT2", "POINT1"]  # Duplicate point
    for point in test_points:
        cursor.execute(
            "INSERT INTO BID_AWARDS (SettlementPoint) VALUES (?)", (point,))

    cursor.execute(FETCH_BID_SETTLEMENT_POINTS_QUERY)
    points = {row[0] for row in cursor.fetchall()}

    conn.close()
    assert len(points) == 2
    assert points == {"POINT1", "POINT2"}


def test_fetch_offer_settlement_points_query(tmp_path):
    """Test FETCH_OFFER_SETTLEMENT_POINTS_QUERY returns correct points."""
    db_file = str(tmp_path / "test.db")
    conn = sqlite3.connect(db_file)
    cursor = conn.cursor()

    cursor.execute("CREATE TABLE OFFER_AWARDS (SettlementPoint TEXT)")
    test_points = ["POINT3", "POINT4", "POINT3"]  # Duplicate point
    for point in test_points:
        cursor.execute(
            "INSERT INTO OFFER_AWARDS (SettlementPoint) VALUES (?)", (point,)
        )

    cursor.execute(FETCH_OFFER_SETTLEMENT_POINTS_QUERY)
    points = {row[0] for row in cursor.fetchall()}

    conn.close()
    assert len(points) == 2
    assert points == {"POINT3", "POINT4"}


def test_get_active_settlement_points_uses_constants(tmp_path):
    """Test that get_active_settlement_points uses the defined SQL constants."""
    db_file = str(tmp_path / "test.db")
    conn = sqlite3.connect(db_file)
    cursor = conn.cursor()

    # Create tables using the same schema as in production
    cursor.execute("CREATE TABLE BID_AWARDS (SettlementPoint TEXT)")
    cursor.execute("CREATE TABLE OFFER_AWARDS (SettlementPoint TEXT)")

    # Insert test data
    test_points = {
        "BID_AWARDS": ["POINT1", "POINT2"],
        "OFFER_AWARDS": ["POINT2", "POINT3"],
    }

    for point in test_points["BID_AWARDS"]:
        cursor.execute(
            "INSERT INTO BID_AWARDS (SettlementPoint) VALUES (?)", (point,))
    for point in test_points["OFFER_AWARDS"]:
        cursor.execute(
            "INSERT INTO OFFER_AWARDS (SettlementPoint) VALUES (?)", (point,)
        )

    conn.commit()
    conn.close()

    # Get points using the function that uses our SQL constants
    points = get_active_settlement_points(db_file)
    assert points == {"POINT1", "POINT2", "POINT3"}


def test_combined_queries_integration(tmp_path):
    """Test all SQL queries working together in a realistic scenario."""
    db_file = str(tmp_path / "test.db")
    conn = sqlite3.connect(db_file)
    cursor = conn.cursor()

    # First verify tables don't exist
    cursor.execute(CHECK_EXISTING_TABLES_QUERY)
    assert len(cursor.fetchall()) == 0

    # Create tables and verify they're found
    cursor.execute("CREATE TABLE BID_AWARDS (SettlementPoint TEXT)")
    cursor.execute("CREATE TABLE OFFER_AWARDS (SettlementPoint TEXT)")

    cursor.execute(CHECK_EXISTING_TABLES_QUERY)
    tables = {row[0] for row in cursor.fetchall()}
    assert tables == {"BID_AWARDS", "OFFER_AWARDS"}

    # Insert and verify bid data
    test_data = [("POINT1",), ("POINT2",)]
    cursor.executemany(
        "INSERT INTO BID_AWARDS (SettlementPoint) VALUES (?)", test_data)
    cursor.execute(FETCH_BID_SETTLEMENT_POINTS_QUERY)
    bid_points = {row[0] for row in cursor.fetchall()}
    assert bid_points == {"POINT1", "POINT2"}

    # Insert and verify offer data
    test_data = [("POINT2",), ("POINT3",)]
    cursor.executemany(
        "INSERT INTO OFFER_AWARDS (SettlementPoint) VALUES (?)", test_data
    )
    # Execute the fetch query
    cursor.execute(FETCH_OFFER_SETTLEMENT_POINTS_QUERY)
    offer_points = {row[0] for row in cursor.fetchall()}
    assert offer_points == {"POINT2", "POINT3"}

    # Test combined query
    cursor.execute(GET_ACTIVE_SETTLEMENT_POINTS_QUERY)
    all_points = {row[0] for row in cursor.fetchall()}
    assert all_points == {"POINT1", "POINT2", "POINT3"}

    conn.close()


@pytest.mark.parametrize(
    "query,expected",
    [
        (CHECK_EXISTING_TABLES_QUERY, True),
        (FETCH_BID_SETTLEMENT_POINTS_QUERY, True),
        (FETCH_OFFER_SETTLEMENT_POINTS_QUERY, True),
        (GET_ACTIVE_SETTLEMENT_POINTS_QUERY, True),
        ("SELECT * FROM NONEXISTENT_TABLE", False),
        ("INVALID SQL QUERY", False),
        ("NOT A VALID COMMAND", False),
        ("DROP TABLE IF EXISTS TEST;", True),
        ("SELEC * FROM table", False),  # Misspelled SELECT
        ("", False),  # Empty query
        (None, False),  # None value
        ("SELECT * FROM; BAD SYNTAX", False),  # Bad syntax
    ],
)
def test_validate_sql_query(query, expected):
    """Test SQL query validation with various types of queries."""
    assert validate_sql_query(query) == expected


def test_sql_query_constants_are_valid():
    """Test that all SQL query constants are valid SQL."""

    # Group queries by type for better organization
    filter_queries = [
        GET_ACTIVE_SETTLEMENT_POINTS_QUERY,
        FETCH_BID_SETTLEMENT_POINTS_QUERY,
        CHECK_EXISTING_TABLES_QUERY,
        FETCH_OFFER_SETTLEMENT_POINTS_QUERY,
    ]

    insert_queries = [
        SETTLEMENT_POINT_PRICES_INSERT_QUERY,
        BID_AWARDS_INSERT_QUERY,
        BIDS_INSERT_QUERY,
        OFFERS_INSERT_QUERY,
        OFFER_AWARDS_INSERT_QUERY,
    ]

    create_queries = [
        SETTLEMENT_POINT_PRICES_TABLE_CREATION_QUERY,
        BIDS_TABLE_CREATION_QUERY,
        BID_AWARDS_TABLE_CREATION_QUERY,
        OFFERS_TABLE_CREATION_QUERY,
        OFFER_AWARDS_TABLE_CREATION_QUERY,
    ]

    # Test each query group
    for query in filter_queries:
        assert validate_sql_query(
            query), f"Filter query failed validation: {query}"

    for query in insert_queries:
        assert validate_sql_query(
            query), f"Insert query failed validation: {query}"

    for query in create_queries:
        assert validate_sql_query(
            query), f"Create query failed validation: {query}"


def test_individual_query_functionality(tmp_path):
    """Test each SQL query produces expected results when executed."""
    db_file = str(tmp_path / "test.db")
    conn = sqlite3.connect(db_file)
    cursor = conn.cursor()

    cursor.execute(BID_AWARDS_TABLE_CREATION_QUERY)
    cursor.execute(OFFER_AWARDS_TABLE_CREATION_QUERY)

    # Insert test data
    test_data = [
        ("POINT1",),
        ("POINT2",),
        ("POINT1",),  # Duplicate to test DISTINCT
    ]
    cursor.executemany(
        "INSERT INTO BID_AWARDS (SettlementPoint) VALUES (?)", test_data)
    cursor.executemany(
        "INSERT INTO OFFER_AWARDS (SettlementPoint) VALUES (?)", [("POINT3",)]
    )

    # Test CHECK_EXISTING_TABLES_QUERY
    cursor.execute(CHECK_EXISTING_TABLES_QUERY)
    tables = {row[0] for row in cursor.fetchall()}
    assert tables == {"BID_AWARDS", "OFFER_AWARDS"}

    # Test FETCH_BID_SETTLEMENT_POINTS_QUERY
    cursor.execute(FETCH_BID_SETTLEMENT_POINTS_QUERY)
    bid_points = {row[0] for row in cursor.fetchall()}
    assert bid_points == {"POINT1", "POINT2"}

    # Test FETCH_OFFER_SETTLEMENT_POINTS_QUERY
    cursor.execute(FETCH_OFFER_SETTLEMENT_POINTS_QUERY)
    offer_points = {row[0] for row in cursor.fetchall()}
    assert offer_points == {"POINT3"}

    # Test GET_ACTIVE_SETTLEMENT_POINTS_QUERY
    cursor.execute(GET_ACTIVE_SETTLEMENT_POINTS_QUERY)
    all_points = {row[0] for row in cursor.fetchall()}
    assert all_points == {"POINT1", "POINT2", "POINT3"}

    conn.close()
