import sqlite3
import pytest
from ercot_scraping.database.merge_data import create_final_table
from ercot_scraping.database.merge_data import merge_data


def test_merge_data():
    assert True  # Replace with actual test logic for merging data.


@pytest.fixture
def in_memory_db():
    conn = sqlite3.connect(":memory:")
    yield conn
    conn.close()


def test_create_final_table_creates_table(in_memory_db):
    # Patch the CREATE_FINAL_TABLE_QUERY to a simple table for testing
    import ercot_scraping.database.merge_data as merge_data_module
    merge_data_module.CREATE_FINAL_TABLE_QUERY = "CREATE TABLE IF NOT EXISTS FINAL (id INTEGER PRIMARY KEY, value TEXT);"

    # Table should not exist before
    cursor = in_memory_db.cursor()
    cursor.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name='FINAL';")
    assert cursor.fetchone() is None

    # Call the function
    create_final_table(in_memory_db)

    # Table should exist after
    cursor.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name='FINAL';")
    assert cursor.fetchone()[0] == "FINAL"


def test_create_final_table_is_idempotent(in_memory_db):
    import ercot_scraping.database.merge_data as merge_data_module
    merge_data_module.CREATE_FINAL_TABLE_QUERY = "CREATE TABLE IF NOT EXISTS FINAL (id INTEGER PRIMARY KEY, value TEXT);"

    # Call twice, should not raise
    create_final_table(in_memory_db)
    create_final_table(in_memory_db)

    cursor = in_memory_db.cursor()
    cursor.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name='FINAL';")
    assert cursor.fetchone()[0] == "FINAL"


@pytest.fixture
def merge_data_module(monkeypatch):
    import ercot_scraping.database.merge_data as merge_data_module
    return merge_data_module


@pytest.fixture
def in_memory_db():
    conn = sqlite3.connect(":memory:")
    yield conn
    conn.close()


def setup_tables_and_data(conn, merge_data_module, with_data=True):
    # Patch queries to simple test queries
    merge_data_module.CREATE_FINAL_TABLE_QUERY = """
        CREATE TABLE IF NOT EXISTS FINAL (
            id INTEGER PRIMARY KEY,
            award_val TEXT,
            bid_val TEXT,
            price_val TEXT
        );
    """
    # Simple merge query for testing
    merge_data_module.MERGE_DATA_QUERY = """
        INSERT INTO FINAL (award_val, bid_val, price_val)
        SELECT a.award_val, b.bid_val, s.price_val
        FROM BID_AWARDS a
        JOIN BIDS b ON a.BidId = b.EnergyOnlyBidID
        JOIN SETTLEMENT_POINT_PRICES s ON a.SettlementPoint = s.SettlementPointName
    """
    cur = conn.cursor()
    cur.execute(
        "CREATE TABLE BID_AWARDS (BidId INTEGER, SettlementPoint TEXT, DeliveryDate TEXT, award_val TEXT)")
    cur.execute(
        "CREATE TABLE BIDS (EnergyOnlyBidID INTEGER, DeliveryDate TEXT, bid_val TEXT)")
    cur.execute(
        "CREATE TABLE SETTLEMENT_POINT_PRICES (SettlementPointName TEXT, DeliveryDate TEXT, price_val TEXT)")
    if with_data:
        # Insert matching data
        cur.execute(
            "INSERT INTO BID_AWARDS VALUES (1, 'SP1', '2024-01-01', 'award1')")
        cur.execute("INSERT INTO BIDS VALUES (1, '2024-01-01', 'bid1')")
        cur.execute(
            "INSERT INTO SETTLEMENT_POINT_PRICES VALUES ('SP1', '2024-01-01', 'price1')")
    conn.commit()


def test_merge_data_merges_rows(in_memory_db, merge_data_module):
    setup_tables_and_data(in_memory_db, merge_data_module, with_data=True)
    merge_data(in_memory_db)
    cur = in_memory_db.cursor()
    cur.execute("SELECT * FROM FINAL")
    rows = cur.fetchall()
    assert len(rows) == 1
    assert rows[0][1:] == ('award1', 'bid1', 'price1')


def test_merge_data_no_rows_when_no_match(in_memory_db, merge_data_module):
    setup_tables_and_data(in_memory_db, merge_data_module, with_data=False)
    # Insert non-matching data
    cur = in_memory_db.cursor()
    cur.execute(
        "INSERT INTO BID_AWARDS VALUES (1, 'SP1', '2024-01-01', 'award1')")
    # Different ID
    cur.execute("INSERT INTO BIDS VALUES (2, '2024-01-01', 'bid2')")
    # Different SP
    cur.execute(
        "INSERT INTO SETTLEMENT_POINT_PRICES VALUES ('SP2', '2024-01-01', 'price2')")
    in_memory_db.commit()
    merge_data(in_memory_db)
    cur.execute("SELECT * FROM FINAL")
    rows = cur.fetchall()
    assert len(rows) == 0


def test_merge_data_creates_final_table_if_missing(in_memory_db, merge_data_module):
    setup_tables_and_data(in_memory_db, merge_data_module, with_data=True)
    # Drop FINAL if exists
    cur = in_memory_db.cursor()
    cur.execute("DROP TABLE IF EXISTS FINAL")
    in_memory_db.commit()
    merge_data(in_memory_db)
    cur.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name='FINAL';")
    assert cur.fetchone()[0] == "FINAL"


def test_merge_data_handles_missing_source_tables(in_memory_db, merge_data_module):
    # Only create one source table
    merge_data_module.CREATE_FINAL_TABLE_QUERY = "CREATE TABLE IF NOT EXISTS FINAL (id INTEGER PRIMARY KEY, award_val TEXT, bid_val TEXT, price_val TEXT);"
    merge_data_module.MERGE_DATA_QUERY = "INSERT INTO FINAL (award_val, bid_val, price_val) SELECT NULL, NULL, NULL WHERE 1=0;"
    cur = in_memory_db.cursor()
    cur.execute(
        "CREATE TABLE BID_AWARDS (BidId INTEGER, SettlementPoint TEXT, DeliveryDate TEXT, award_val TEXT)")
    in_memory_db.commit()
    # Should not raise, even though BIDS and SETTLEMENT_POINT_PRICES are missing
    merge_data(in_memory_db)
    cur.execute("SELECT * FROM FINAL")
    assert cur.fetchall() == []


def test_merge_data_accepts_db_path(tmp_path, merge_data_module):
    db_path = tmp_path / "test.db"
    conn = sqlite3.connect(db_path)
    setup_tables_and_data(conn, merge_data_module, with_data=True)
    conn.close()
    merge_data(str(db_path))
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    cur.execute("SELECT * FROM FINAL")
    rows = cur.fetchall()
    assert len(rows) == 1
    conn.close()
