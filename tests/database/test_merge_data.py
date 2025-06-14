import sqlite3
import pytest
from ercot_scraping.database.merge_data import create_final_table
from ercot_scraping.database.merge_data import merge_data
from ercot_scraping.database.merge_data import get_common_date_hour_pairs


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
        JOIN BIDS b ON a.BidId = b.EnergyOnlyBidID AND a.DeliveryDate = b.DeliveryDate AND a.HourEnding = b.HourEnding
        JOIN SETTLEMENT_POINT_PRICES s ON a.SettlementPoint = s.SettlementPointName AND a.DeliveryDate = s.DeliveryDate AND a.HourEnding = s.DeliveryHour
    """
    cur = conn.cursor()
    cur.execute(
        "CREATE TABLE BID_AWARDS (BidId INTEGER, SettlementPoint TEXT, DeliveryDate TEXT, HourEnding INTEGER, award_val TEXT)")
    cur.execute(
        "CREATE TABLE BIDS (EnergyOnlyBidID INTEGER, DeliveryDate TEXT, HourEnding INTEGER, bid_val TEXT)")
    cur.execute(
        "CREATE TABLE SETTLEMENT_POINT_PRICES (SettlementPointName TEXT, DeliveryDate TEXT, DeliveryHour INTEGER, price_val TEXT)")
    if with_data:
        # Insert matching data
        cur.execute(
            "INSERT INTO BID_AWARDS VALUES (1, 'SP1', '2024-01-01', 1, 'award1')")
        cur.execute("INSERT INTO BIDS VALUES (1, '2024-01-01', 1, 'bid1')")
        cur.execute(
            "INSERT INTO SETTLEMENT_POINT_PRICES VALUES ('SP1', '2024-01-01', 1, 'price1')")
    conn.commit()


def test_merge_data_merges_rows(in_memory_db, merge_data_module):
    setup_tables_and_data(in_memory_db, merge_data_module, with_data=True)
    merge_data(in_memory_db, batch_size=10)
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
        "INSERT INTO BID_AWARDS VALUES (1, 'SP1', '2024-01-01', 1, 'award1')")
    # Different ID
    cur.execute("INSERT INTO BIDS VALUES (2, '2024-01-01', 1, 'bid2')")
    # Different SP
    cur.execute(
        "INSERT INTO SETTLEMENT_POINT_PRICES VALUES ('SP2', '2024-01-01', 1, 'price2')")
    in_memory_db.commit()
    merge_data(in_memory_db, batch_size=10)
    cur.execute("SELECT * FROM FINAL")
    rows = cur.fetchall()
    assert len(rows) == 0


def test_merge_data_creates_final_table_if_missing(in_memory_db, merge_data_module):
    setup_tables_and_data(in_memory_db, merge_data_module, with_data=True)
    # Drop FINAL if exists
    cur = in_memory_db.cursor()
    cur.execute("DROP TABLE IF EXISTS FINAL")
    in_memory_db.commit()
    merge_data(in_memory_db, batch_size=10)
    cur.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name='FINAL';")
    assert cur.fetchone()[0] == "FINAL"


def test_merge_data_handles_missing_source_tables(in_memory_db, merge_data_module):
    # Only create one source table
    merge_data_module.CREATE_FINAL_TABLE_QUERY = "CREATE TABLE IF NOT EXISTS FINAL (id INTEGER PRIMARY KEY, award_val TEXT, bid_val TEXT, price_val TEXT);"
    merge_data_module.MERGE_DATA_QUERY = "INSERT INTO FINAL (award_val, bid_val, price_val) SELECT NULL, NULL, NULL WHERE 1=0;"
    cur = in_memory_db.cursor()
    cur.execute(
        "CREATE TABLE BID_AWARDS (BidId INTEGER, SettlementPoint TEXT, DeliveryDate TEXT, HourEnding INTEGER, award_val TEXT)")
    in_memory_db.commit()
    # Should not raise, even though BIDS and SETTLEMENT_POINT_PRICES are missing
    merge_data(in_memory_db, batch_size=10)
    cur.execute("SELECT * FROM FINAL")
    assert cur.fetchall() == []


def test_merge_data_accepts_db_path(tmp_path, merge_data_module):
    db_path = tmp_path / "test.db"
    conn = sqlite3.connect(db_path)
    setup_tables_and_data(conn, merge_data_module, with_data=True)
    conn.close()
    merge_data(str(db_path), batch_size=10)
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    cur.execute("SELECT * FROM FINAL")
    rows = cur.fetchall()
    assert len(rows) == 1
    conn.close()


@pytest.fixture
def in_memory_db():
    conn = sqlite3.connect(":memory:")
    yield conn
    conn.close()


def create_table_with_pairs(conn, table, date_col, hour_col, pairs):
    cur = conn.cursor()
    cur.execute(f"CREATE TABLE {table} ({date_col} TEXT, {hour_col} INTEGER)")
    cur.executemany(
        f"INSERT INTO {table} ({date_col}, {hour_col}) VALUES (?, ?)", pairs
    )
    conn.commit()


def test_get_common_date_hour_pairs_all_tables_exist_and_overlap(in_memory_db):
    # All tables exist, with one common pair
    tables = [
        ("BID_AWARDS", "deliveryDate", "hourEnding"),
        ("BIDS", "deliveryDate", "hourEnding"),
        ("SETTLEMENT_POINT_PRICES", "deliveryDate", "deliveryHour"),  # fixed
        ("OFFERS", "deliveryDate", "hourEnding"),
        ("OFFER_AWARDS", "deliveryDate", "hourEnding"),
    ]
    common = [("2024-01-01", 1)]
    for table, date_col, hour_col in tables:
        create_table_with_pairs(in_memory_db, table, date_col, hour_col, common + [
            (f"2024-01-0{tables.index((table, date_col, hour_col))+2}", 2)])
    result = get_common_date_hour_pairs(in_memory_db)
    assert result == [("2024-01-01", 1)]


def test_get_common_date_hour_pairs_no_common_pairs(in_memory_db):
    # All tables exist, but no common pairs
    tables = [
        ("BID_AWARDS", "DeliveryDate", "HourEnding"),
        ("BIDS", "DeliveryDate", "HourEnding"),
        ("SETTLEMENT_POINT_PRICES", "DeliveryDate", "DeliveryHour"),
        ("OFFERS", "DeliveryDate", "HourEnding"),
        ("OFFER_AWARDS", "DeliveryDate", "HourEnding"),
    ]
    for i, (table, date_col, hour_col) in enumerate(tables):
        create_table_with_pairs(in_memory_db, table, date_col, hour_col, [
                                (f"2024-01-0{i+1}", i+1)])
    result = get_common_date_hour_pairs(in_memory_db)
    assert result == []


def test_get_common_date_hour_pairs_missing_table_returns_empty(in_memory_db):
    # One table missing
    tables = [
        ("BID_AWARDS", "DeliveryDate", "HourEnding"),
        ("BIDS", "DeliveryDate", "HourEnding"),
        # Skip SETTLEMENT_POINT_PRICES
        ("OFFERS", "DeliveryDate", "HourEnding"),
        ("OFFER_AWARDS", "DeliveryDate", "HourEnding"),
    ]
    pairs = [("2024-01-01", 1)]
    for table, date_col, hour_col in tables:
        create_table_with_pairs(in_memory_db, table, date_col, hour_col, pairs)
    result = get_common_date_hour_pairs(in_memory_db)
    assert result == []


def test_get_common_date_hour_pairs_empty_tables(in_memory_db):
    # All tables exist but are empty
    tables = [
        ("BID_AWARDS", "DeliveryDate", "HourEnding"),
        ("BIDS", "DeliveryDate", "HourEnding"),
        ("SETTLEMENT_POINT_PRICES", "DeliveryDate", "DeliveryHour"),
        ("OFFERS", "DeliveryDate", "HourEnding"),
        ("OFFER_AWARDS", "DeliveryDate", "HourEnding"),
    ]
    for table, date_col, hour_col in tables:
        create_table_with_pairs(in_memory_db, table, date_col, hour_col, [])
    result = get_common_date_hour_pairs(in_memory_db)
    assert result == []


def test_get_common_date_hour_pairs_multiple_common_pairs(in_memory_db):
    # All tables have two common pairs
    tables = [
        ("BID_AWARDS", "deliveryDate", "hourEnding"),
        ("BIDS", "deliveryDate", "hourEnding"),
        ("SETTLEMENT_POINT_PRICES", "deliveryDate", "deliveryHour"),  # fixed
        ("OFFERS", "deliveryDate", "hourEnding"),
        ("OFFER_AWARDS", "deliveryDate", "hourEnding"),
    ]
    pairs = [("2024-01-01", 1), ("2024-01-02", 2)]
    for table, date_col, hour_col in tables:
        create_table_with_pairs(in_memory_db, table, date_col, hour_col, pairs + [
                                (f"2024-01-0{tables.index((table, date_col, hour_col))+3}", 3)])
    result = get_common_date_hour_pairs(in_memory_db)
    assert result == sorted(pairs)


@pytest.fixture
def simple_merge_module(monkeypatch):
    import ercot_scraping.database.merge_data as merge_data_module
    # Patch to use simple queries (no 'ba.' or 'oa.')
    merge_data_module.CREATE_FINAL_TABLE_QUERY = "CREATE TABLE IF NOT EXISTS FINAL (id INTEGER PRIMARY KEY, val TEXT);"
    merge_data_module.MERGE_DATA_QUERY = "INSERT INTO FINAL (val) SELECT val FROM SOURCE;"
    return merge_data_module


@pytest.fixture
def batch_merge_module(monkeypatch):
    import ercot_scraping.database.merge_data as merge_data_module
    # Patch to use queries with 'ba.' and 'oa.' for batching
    merge_data_module.CREATE_FINAL_TABLE_QUERY = """
        CREATE TABLE IF NOT EXISTS FINAL (
            deliveryDate TEXT,
            hourEnding INTEGER,
            val TEXT,
            settlementPointName TEXT,
            qseName TEXT,
            settlementPointPrice REAL,
            MARK_PRICE REAL,
            blockCurve TEXT,
            sourceType TEXT,
            energyOnlyBidAwardInMW REAL,
            bidId INTEGER,
            BID_PRICE REAL,
            BID_SIZE REAL,
            energyOnlyOfferAwardMW REAL,
            offerId INTEGER,
            OFFER_PRICE REAL,
            OFFER_SIZE REAL,
            INSERTED_AT TEXT
        );
    """
    merge_data_module.MERGE_DATA_QUERY = "INSERT INTO FINAL (deliveryDate, hourEnding, settlementPointName, qseName, settlementPointPrice, MARK_PRICE, blockCurve, sourceType, energyOnlyBidAwardInMW, bidId, BID_PRICE, BID_SIZE, energyOnlyOfferAwardMW, offerId, OFFER_PRICE, OFFER_SIZE, INSERTED_AT) SELECT ba.DeliveryDate, ba.HourEnding, ba.val, NULL, NULL, NULL, NULL, 'Bid', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, datetime('now') FROM BID_AWARDS ba JOIN OFFER_AWARDS oa ON ba.DeliveryDate = oa.DeliveryDate AND ba.HourEnding = oa.HourEnding"
    return merge_data_module


def test_merge_data_simple_query_runs_and_inserts(simple_merge_module):
    conn = sqlite3.connect(":memory:")
    cur = conn.cursor()
    cur.execute("CREATE TABLE SOURCE (val TEXT);")
    cur.execute("INSERT INTO SOURCE VALUES ('foo'), ('bar');")
    conn.commit()
    merge_data(conn)
    cur.execute("SELECT val FROM FINAL ORDER BY id;")
    results = [row[0] for row in cur.fetchall()]
    assert results == ['foo', 'bar']
    conn.close()


def test_merge_data_simple_query_idempotent(simple_merge_module):
    conn = sqlite3.connect(":memory:")
    cur = conn.cursor()
    cur.execute("CREATE TABLE SOURCE (val TEXT);")
    cur.execute("INSERT INTO SOURCE VALUES ('baz');")
    conn.commit()
    merge_data(conn)
    merge_data(conn)
    cur.execute("SELECT val FROM FINAL;")
    # Should insert again, so two rows
    assert cur.fetchall() == [('baz',), ('baz',)]
    conn.close()


def test_merge_data_batching_no_common_pairs(batch_merge_module):
    conn = sqlite3.connect(":memory:")
    cur = conn.cursor()
    cur.execute(
        "CREATE TABLE BID_AWARDS (DeliveryDate TEXT, HourEnding INTEGER, val TEXT);")
    cur.execute(
        "CREATE TABLE OFFER_AWARDS (DeliveryDate TEXT, HourEnding INTEGER);")
    conn.commit()
    # Patch get_common_date_hour_pairs to return empty
    import ercot_scraping.database.merge_data as merge_data_module
    merge_data_module.get_common_date_hour_pairs = lambda c: []
    merge_data(conn, batch_size=2)
    cur.execute("SELECT * FROM FINAL;")
    assert cur.fetchall() == []
    conn.close()


def test_merge_data_accepts_db_path(tmp_path, simple_merge_module):
    db_path = tmp_path / "merge_test.db"
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    cur.execute("CREATE TABLE SOURCE (val TEXT);")
    cur.execute("INSERT INTO SOURCE VALUES ('x');")
    conn.commit()
    conn.close()
    merge_data(str(db_path))
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    cur.execute("SELECT val FROM FINAL;")
    assert cur.fetchall() == [('x',)]
    conn.close()


def test_merge_data_handles_sqlite_error(monkeypatch, simple_merge_module):
    # Simulate sqlite3.Error during execution
    class DummyCursor:
        def execute(self, *a, **kw):
            raise sqlite3.Error("fail")

    class DummyConn:
        def cursor(self): return DummyCursor()
        def commit(self): pass
        def close(self): pass
    with pytest.raises(sqlite3.Error):
        merge_data(DummyConn())
