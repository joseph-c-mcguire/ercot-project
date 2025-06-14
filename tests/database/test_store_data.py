from ercot_scraping.database.store_data import is_data_empty
from ercot_scraping.database.store_data import normalize_date_string
import sqlite3
import tempfile
import os
from unittest import mock
from ercot_scraping.database.store_data import store_data_to_db
from ercot_scraping.database.store_data import validate_spp_data
from ercot_scraping.database.store_data import _record_to_model
import pandas as pd
from ercot_scraping.database.store_data import aggregate_spp_data
from ercot_scraping.database.store_data import store_prices_to_db
from ercot_scraping.database.store_data import store_bid_awards_to_db
import pytest


def test_store_data():
    assert True  # Replace with actual test logic for store_data functionality.


@pytest.mark.parametrize(
    "input_data,expected",
    [
        (None, True),
        ({}, True),
        ({"foo": []}, True),
        ({"data": []}, True),
        ({"data": None}, True),
        ({"data": [{}]}, False),
        ({"data": [{"a": 1}]}, False),
        ({"data": [1, 2, 3]}, False),
        ({"data": "notalist"}, True),
    ]
)
def test_is_data_empty(input_data, expected):
    assert is_data_empty(input_data) == expected


@pytest.mark.parametrize(
    "input_str,expected",
    [
        ("2024-06-01", "2024-06-01"),         # already normalized
        ("06/01/2024", "2024-06-01"),         # MM/DD/YYYY
        # DD/MM/YYYY (should parse as 1st June)
        ("01/06/2024", "2024-06-01"),
        ("2024/06/01", "2024-06-01"),         # YYYY/MM/DD
        ("2024-13-01", "2024-13-01"),         # invalid month, return as is
        ("notadate", "notadate"),             # invalid string, return as is
        ("", ""),                             # empty string
        (None, None),                         # None input
        (12345, 12345),                       # non-string input
        ("2024-02-29", "2024-02-29"),         # leap year valid date
        ("02/29/2024", "2024-02-29"),         # leap year MM/DD/YYYY
        ("29/02/2024", "2024-02-29"),         # leap year DD/MM/YYYY
    ]
)
def test_normalize_date_string(input_str, expected):
    assert normalize_date_string(input_str) == expected


class DummyModel:
    def __init__(self, a=None, b=None, deliveryDate=None, inserted_at=None):
        self.a = a
        self.b = b
        self.deliveryDate = deliveryDate
        self.inserted_at = inserted_at

    def as_tuple(self):
        # Return tuple in the order expected by the insert query
        return (self.a, self.b, self.deliveryDate, self.inserted_at)


@pytest.fixture
def temp_db():
    # Create a temporary SQLite database file
    fd, path = tempfile.mkstemp(suffix=".db")
    os.close(fd)
    try:
        yield path
    finally:
        if os.path.exists(path):
            os.remove(path)


def create_dummy_table(conn, table_name):
    # Create a table with columns matching DummyModel
    conn.execute(f"""
        CREATE TABLE {table_name} (
            a INTEGER,
            b TEXT,
            deliveryDate TEXT,
            inserted_at TEXT
        )
    """)
    conn.commit()


def test_store_data_to_db_inserts_records(temp_db):
    table_name = "DUMMY"
    insert_query = f"INSERT INTO {table_name} (a, b, deliveryDate, inserted_at) VALUES (?, ?, ?, ?)"
    data = {
        "data": [
            {"a": 1, "b": "foo", "deliveryDate": "2024-06-01"},
            {"a": 2, "b": "bar", "deliveryDate": "2024-06-02"},
        ]
    }
    # Create table first
    conn = sqlite3.connect(temp_db)
    create_dummy_table(conn, table_name)
    conn.close()

    store_data_to_db(
        data=data,
        db_name=temp_db,
        table_name=table_name,
        insert_query=insert_query,
        model_class=DummyModel,
        normalize=False,
        batch_size=100
    )

    # Check records inserted
    conn = sqlite3.connect(temp_db)
    rows = list(conn.execute(
        f"SELECT a, b, deliveryDate FROM {table_name} ORDER BY a"))
    assert rows == [(1, "foo", "2024-06-01"), (2, "bar", "2024-06-02")]
    conn.close()


def test_store_data_to_db_creates_table_if_missing(temp_db):
    table_name = "DUMMY2"
    insert_query = f"INSERT INTO {table_name} (a, b, deliveryDate, inserted_at) VALUES (?, ?, ?, ?)"
    data = {
        "data": [
            {"a": 3, "b": "baz", "deliveryDate": "2024-06-03"},
        ]
    }
    # Patch create_ercot_tables to create our dummy table
    with mock.patch("ercot_scraping.database.store_data.create_ercot_tables") as mock_create_tables:
        def _create_tables(db_name):
            conn = sqlite3.connect(db_name)
            create_dummy_table(conn, table_name)
            conn.close()
        mock_create_tables.side_effect = _create_tables

        store_data_to_db(
            data=data,
            db_name=temp_db,
            table_name=table_name,
            insert_query=insert_query,
            model_class=DummyModel,
            normalize=False,
            batch_size=10
        )

    # Check record inserted
    conn = sqlite3.connect(temp_db)
    rows = list(conn.execute(f"SELECT a, b, deliveryDate FROM {table_name}"))
    assert rows == [(3, "baz", "2024-06-03")]
    conn.close()


def test_store_data_to_db_skips_empty_data(temp_db):
    table_name = "DUMMY3"
    insert_query = f"INSERT INTO {table_name} (a, b, deliveryDate, inserted_at) VALUES (?, ?, ?, ?)"
    data = {"data": []}
    # Create table
    conn = sqlite3.connect(temp_db)
    create_dummy_table(conn, table_name)
    conn.close()

    # Should not raise or insert anything
    store_data_to_db(
        data=data,
        db_name=temp_db,
        table_name=table_name,
        insert_query=insert_query,
        model_class=DummyModel,
        normalize=False,
        batch_size=10
    )
    conn = sqlite3.connect(temp_db)
    rows = list(conn.execute(f"SELECT * FROM {table_name}"))
    assert rows == []
    conn.close()


def test_store_data_to_db_with_list_record(temp_db):
    table_name = "DUMMY5"
    insert_query = f"INSERT INTO {table_name} (a, b, deliveryDate, inserted_at) VALUES (?, ?, ?, ?)"
    # Provide a record as a list, should map to DummyModel fields
    data = {
        "data": [
            [10, "foo", "2024-06-10", "2024-06-10 12:00:00"],
            [11, "bar", "2024-06-11", "2024-06-11 12:00:00"],
        ]
    }
    conn = sqlite3.connect(temp_db)
    create_dummy_table(conn, table_name)
    conn.close()

    store_data_to_db(
        data=data,
        db_name=temp_db,
        table_name=table_name,
        insert_query=insert_query,
        model_class=DummyModel,
        normalize=False,
        batch_size=10
    )
    conn = sqlite3.connect(temp_db)
    rows = list(conn.execute(
        f"SELECT a, b, deliveryDate FROM {table_name} ORDER BY a"))
    assert rows == [(10, "foo", "2024-06-10"), (11, "bar", "2024-06-11")]
    conn.close()


def valid_spp_record():
    return {
        "deliveryDate": "2024-06-01",
        "deliveryHour": 1,
        "deliveryInterval": 1,
        "settlementPointName": "POINT1",
        "settlementPointType": "LZ",
        "settlementPointPrice": 30.5,
        "dstFlag": 0
    }


def test_validate_spp_data_valid():
    data = {"data": [valid_spp_record()]}
    # Should not raise
    validate_spp_data(data)


def test_validate_spp_data_empty_data_raises():
    with pytest.raises(ValueError, match="Invalid or empty data structure"):
        validate_spp_data({})
    with pytest.raises(ValueError, match="Invalid or empty data structure"):
        validate_spp_data({"data": []})
    with pytest.raises(ValueError, match="Invalid or empty data structure"):
        validate_spp_data(None)


def test_validate_spp_data_data_not_list_raises():
    with pytest.raises(ValueError, match="Data must be a list of records"):
        validate_spp_data({"data": "notalist"})


def test_validate_spp_data_first_record_not_dict_raises():
    with pytest.raises(ValueError, match="Invalid data record format for SPP data"):
        validate_spp_data({"data": [123]})


def test_validate_spp_data_missing_fields_raises():
    record = valid_spp_record()
    del record["settlementPointPrice"]
    with pytest.raises(ValueError) as excinfo:
        validate_spp_data({"data": [record]})
    assert "Missing required fields" in str(excinfo.value)
    assert "settlementPointPrice" in str(excinfo.value)


def test_validate_spp_data_first_record_is_list_skips_validation(caplog):
    # Should not raise, should log a warning and return
    data = {"data": [[
        "2024-06-01", 1, 1, "POINT1", "LZ", 30.5, 0
    ]]}
    validate_spp_data(data)
    assert any(
        "First SPP record is a list" in rec.message for rec in caplog.records)


class ModelWithKwargs:
    def __init__(self, x=None, y=None):
        self.x = x
        self.y = y


class ModelWithPositional:
    def __init__(self, x, y):
        self.x = x
        self.y = y


class ModelWithMixedArgs:
    def __init__(self, x, y=None, z=None):
        self.x = x
        self.y = y
        self.z = z


def test_record_to_model_dict_direct_kwargs():
    record = {"x": 1, "y": "test"}
    model = _record_to_model(record, ModelWithKwargs)
    assert isinstance(model, ModelWithKwargs)
    assert model.x == 1
    assert model.y == "test"


def test_record_to_model_dict_fallback_to_positional():
    # ModelWithPositional expects x, y in order
    # The fallback mechanism in _record_to_model will try to get 'x' then 'y'
    record = {"x": 10, "y": "fallback", "z": "extra"}  # 'z' is extra
    model = _record_to_model(record, ModelWithPositional)
    assert isinstance(model, ModelWithPositional)
    assert model.x == 10
    assert model.y == "fallback"


def test_record_to_model_dict_fallback_with_missing_keys():
    # ModelWithPositional expects x, y. 'y' is missing from record.
    # Fallback should use None for missing 'y'.
    record = {"x": 20}
    # This will cause TypeError in ModelWithPositional as y is required
    # Let's use DummyModel which has defaults
    # DummyModel(a=None, b=None, deliveryDate=None, inserted_at=None)
    model = _record_to_model(record, DummyModel)
    assert isinstance(model, DummyModel)
    assert model.a is None  # 'a' is not in record, but DummyModel has 'a' as first arg
    assert model.b is None  # 'b' is not in record
    assert model.deliveryDate is None
    assert model.inserted_at is None

    # Test with a model where positional args are expected and some are missing
    # _record_to_model will fetch 'x', 'y', 'z' in order for ModelWithMixedArgs
    record_for_mixed = {"x": 100, "z": 300}  # y is missing
    model_mixed = _record_to_model(record_for_mixed, ModelWithMixedArgs)
    assert isinstance(model_mixed, ModelWithMixedArgs)
    assert model_mixed.x == 100
    assert model_mixed.y is None  # y was missing, so None is passed
    assert model_mixed.z == 300


def test_record_to_model_list_positional():
    record = [3, "list_test"]
    model = _record_to_model(record, ModelWithPositional)
    assert isinstance(model, ModelWithPositional)
    assert model.x == 3
    assert model.y == "list_test"


def test_record_to_model_unsupported_type():
    record = "not a dict or list"
    model = _record_to_model(record, ModelWithKwargs)
    assert model is None


def test_record_to_model_dict_kwargs_type_error_propagates_if_fallback_fails():
    # ModelWithPositional requires x and y, cannot be instantiated with **{}
    # Fallback will try to get 'x' and 'y', but if they are not present,
    # it will call ModelWithPositional(None, None) which is fine.
    # Let's make a model that will fail even with None.
    class StrictModel:
        def __init__(self, val: int):
            if not isinstance(val, int):
                raise TypeError("val must be an int")
            self.val = val

    # Correct key, wrong type for StrictModel
    record_bad_type = {"val": "string"}
    # Direct kwargs will fail: StrictModel(**{"val": "string"}) -> TypeError
    # Fallback will try: StrictModel("string") -> TypeError
    with pytest.raises(TypeError, match="val must be an int"):
        _record_to_model(record_bad_type, StrictModel)

    record_missing_key = {}  # 'val' is missing
    # Direct kwargs will fail: StrictModel(**{}) -> TypeError (missing 'val')
    # Fallback will try: StrictModel(None) -> TypeError (None is not int)
    with pytest.raises(TypeError, match="val must be an int"):
        _record_to_model(record_missing_key, StrictModel)


def test_record_to_model_list_type_error_propagates():
    record = [1]  # Too few arguments for ModelWithPositional
    # Expected: "ModelWithPositional.__init__() missing 1 required positional argument: 'y'"
    with pytest.raises(TypeError):
        _record_to_model(record, ModelWithPositional)

    record_too_many = [1, "test", "extra"]  # Too many arguments
    # Expected: "ModelWithPositional.__init__() takes 3 positional arguments but 4 were given"
    with pytest.raises(TypeError):
        _record_to_model(record_too_many, ModelWithPositional)


def test_record_to_model_dict_with_non_string_keys_fallback():
    # This tests if the fallback mechanism correctly handles non-string keys
    # if they happen to match parameter names (though unlikely in practice for __init__)
    # The model_fields are derived from co_varnames which are strings.
    # So, non-string keys in the record won't match and will result in None for those fields.
    class ModelForIntKeys:
        def __init__(self, key1=None, key2=None):
            self.key1 = key1
            self.key2 = key2

    record = {1: "val1", "key2": "val2"}  # 'key1' is an int key
    # Direct kwargs will fail due to int key: ModelForIntKeys(**{1: "val1", "key2": "val2"})
    # Fallback will look for 'key1' (str) and 'key2' (str) in record.
    # record.get('key1', None) will be None.
    # record.get('key2', None) will be "val2".
    # So it calls ModelForIntKeys(None, "val2")
    model = _record_to_model(record, ModelForIntKeys)
    assert isinstance(model, ModelForIntKeys)
    assert model.key1 is None
    assert model.key2 == "val2"


@pytest.fixture
def sample_spp_data_dicts():
    return {
        "data": [
            {
                "deliveryDate": "2024-07-01", "deliveryHour": 1, "deliveryInterval": 1,
                "settlementPointName": "SP1", "settlementPointType": "LZ",
                "settlementPointPrice": 20.0, "dstFlag": 0
            },
            {
                "deliveryDate": "2024-07-01", "deliveryHour": 1, "deliveryInterval": 2,  # Same date/hour
                "settlementPointName": "SP1", "settlementPointType": "LZ",
                "settlementPointPrice": 30.0, "dstFlag": 0
            },
            {
                "deliveryDate": "2024-07-01", "deliveryHour": 2, "deliveryInterval": 1,
                "settlementPointName": "SP2", "settlementPointType": "HUB",
                "settlementPointPrice": 25.0, "dstFlag": 0
            },
            {
                "deliveryDate": "2024-07-02", "deliveryHour": 1, "deliveryInterval": 1,
                "settlementPointName": "SP3", "settlementPointType": "LZ",
                "settlementPointPrice": 40.0, "dstFlag": 1
            },
        ]
    }


@pytest.fixture
def sample_spp_data_lists():
    return {
        "data": [
            ["2024-07-01", 1, 1, "SP1", "LZ", 20.0, 0],
            ["2024-07-01", 1, 2, "SP1", "LZ", 30.0, 0],  # Same date/hour
            ["2024-07-01", 2, 1, "SP2", "HUB", 25.0, 0],
            ["2024-07-02", 1, 1, "SP3", "LZ", 40.0, 1],
        ]
    }


def test_aggregate_spp_data_empty_input():
    assert aggregate_spp_data(None) is None
    assert aggregate_spp_data({}) == {}
    assert aggregate_spp_data({"data": []}) == {"data": []}
    assert aggregate_spp_data({"foo": "bar"}) == {
        "foo": "bar"}  # No "data" key


def test_aggregate_spp_data_with_dicts(sample_spp_data_dicts):
    result = aggregate_spp_data(sample_spp_data_dicts)
    assert "data" in result
    aggregated_data = result["data"]
    assert len(aggregated_data) == 3

    # Check aggregation for 2024-07-01 Hour 1
    record1 = next(
        d for d in aggregated_data if d["deliveryDate"] == "2024-07-01" and d["deliveryHour"] == 1)
    assert record1["deliveryInterval"] == 1  # First
    assert record1["settlementPointName"] == "SP1"  # First
    assert record1["settlementPointType"] == "LZ"  # First
    assert record1["settlementPointPrice"] == pytest.approx(
        25.0)  # Mean of 20.0 and 30.0
    assert record1["dstFlag"] == 0  # First

    # Check aggregation for 2024-07-01 Hour 2
    record2 = next(
        d for d in aggregated_data if d["deliveryDate"] == "2024-07-01" and d["deliveryHour"] == 2)
    assert record2["settlementPointPrice"] == pytest.approx(25.0)

    # Check aggregation for 2024-07-02 Hour 1
    record3 = next(
        d for d in aggregated_data if d["deliveryDate"] == "2024-07-02" and d["deliveryHour"] == 1)
    assert record3["settlementPointPrice"] == pytest.approx(40.0)


def test_aggregate_spp_data_with_lists(sample_spp_data_lists, caplog):
    result = aggregate_spp_data(sample_spp_data_lists)
    assert "data" in result
    aggregated_data = result["data"]
    assert len(aggregated_data) == 3
    assert any(
        "SPP data records are lists in aggregate_spp_data" in rec.message for rec in caplog.records)

    # Check aggregation for 2024-07-01 Hour 1
    record1 = next(
        d for d in aggregated_data if d["deliveryDate"] == "2024-07-01" and d["deliveryHour"] == 1)
    assert record1["deliveryInterval"] == 1
    assert record1["settlementPointName"] == "SP1"
    assert record1["settlementPointType"] == "LZ"
    assert record1["settlementPointPrice"] == pytest.approx(25.0)
    assert record1["dstFlag"] == 0


def test_aggregate_spp_data_validation_failure():
    invalid_data = {"data": [{"deliveryDate": "2024-01-01"}]}  # Missing fields
    with pytest.raises(ValueError, match="Missing required fields"):
        aggregate_spp_data(invalid_data)

    invalid_data_format = {"data": "not a list"}
    with pytest.raises(ValueError, match="Data must be a list of records"):
        aggregate_spp_data(invalid_data_format)


def test_aggregate_spp_data_single_record(sample_spp_data_dicts):
    single_record_data = {"data": [sample_spp_data_dicts["data"][0]]}
    result = aggregate_spp_data(single_record_data)
    assert "data" in result
    aggregated_data = result["data"]
    assert len(aggregated_data) == 1
    expected_record = sample_spp_data_dicts["data"][0].copy()
    # Price should remain the same as it's a mean of one value
    assert aggregated_data[0] == expected_record


def test_aggregate_spp_data_all_unique_date_hour(sample_spp_data_dicts):
    unique_data_list = [
        sample_spp_data_dicts["data"][0],  # 2024-07-01 H1
        sample_spp_data_dicts["data"][2],  # 2024-07-01 H2
        sample_spp_data_dicts["data"][3]  # 2024-07-02 H1
    ]
    data = {"data": unique_data_list}
    result = aggregate_spp_data(data)
    assert "data" in result
    aggregated_data = result["data"]
    assert len(aggregated_data) == 3
    # Sort both by date and hour for comparison
    expected_sorted = sorted(unique_data_list, key=lambda x: (
        x["deliveryDate"], x["deliveryHour"]))
    result_sorted = sorted(aggregated_data, key=lambda x: (
        x["deliveryDate"], x["deliveryHour"]))
    assert result_sorted == expected_sorted


def test_aggregate_spp_data_with_nan_values_in_price(caplog):
    # Pandas mean calculation skips NaNs by default.
    # The 'first' aggregator will pick the first non-NaN value if dropna=True (default for agg).
    # Our groupby has dropna=False, but agg's behavior for 'first' might still skip NaNs.
    # Let's test with a NaN price.
    data_with_nan = {
        "data": [
            {
                "deliveryDate": "2024-07-01", "deliveryHour": 1, "deliveryInterval": 1,
                "settlementPointName": "SP1", "settlementPointType": "LZ",
                "settlementPointPrice": 20.0, "dstFlag": 0
            },
            {
                "deliveryDate": "2024-07-01", "deliveryHour": 1, "deliveryInterval": 2,
                "settlementPointName": "SP1", "settlementPointType": "LZ",
                "settlementPointPrice": pd.NA, "dstFlag": 0  # Using pd.NA for missing numeric
            },
            {
                "deliveryDate": "2024-07-01", "deliveryHour": 1, "deliveryInterval": 3,
                "settlementPointName": "SP1", "settlementPointType": "LZ",
                "settlementPointPrice": 40.0, "dstFlag": 0
            },
        ]
    }
    result = aggregate_spp_data(data_with_nan)
    aggregated_data = result["data"]
    assert len(aggregated_data) == 1
    # Mean of 20.0 and 40.0 (pd.NA is skipped)
    assert aggregated_data[0]["settlementPointPrice"] == pytest.approx(30.0)
    assert aggregated_data[0]["deliveryInterval"] == 1  # First non-NA


def test_aggregate_spp_data_preserves_column_order_in_output_dicts(sample_spp_data_dicts):
    # Test that the output dicts have keys in the expected order,
    # which is determined by the groupby_cols + agg_dict keys.
    result = aggregate_spp_data(sample_spp_data_dicts)
    aggregated_data = result["data"]
    if aggregated_data:
        first_record_keys = list(aggregated_data[0].keys())
        expected_keys_order = [
            "deliveryDate", "deliveryHour",  # groupby_cols
            "deliveryInterval", "settlementPointName", "settlementPointType",
            "settlementPointPrice", "dstFlag"  # agg_dict keys
        ]
        assert first_record_keys == expected_keys_order


@pytest.fixture
def temp_db():
    fd, path = tempfile.mkstemp(suffix=".db")
    os.close(fd)
    try:
        yield path
    finally:
        if os.path.exists(path):
            os.remove(path)


@pytest.fixture
def valid_spp_data():
    return {
        "data": [
            {
                "deliveryDate": "2024-07-01",
                "deliveryHour": 1,
                "deliveryInterval": 1,
                "settlementPointName": "SP1",
                "settlementPointType": "LZ",
                "settlementPointPrice": 20.0,
                "dstFlag": 0
            },
            {
                "deliveryDate": "2024-07-01",
                "deliveryHour": 2,
                "deliveryInterval": 1,
                "settlementPointName": "SP2",
                "settlementPointType": "HUB",
                "settlementPointPrice": 25.0,
                "dstFlag": 0
            }
        ]
    }


def test_store_prices_to_db_skips_empty_data(temp_db, caplog):
    # Should not raise or insert anything
    empty_data = {"data": []}
    with mock.patch("ercot_scraping.database.store_data.store_data_to_db") as mock_store:
        store_prices_to_db(empty_data, db_name=temp_db)
        mock_store.assert_not_called()
    assert any(
        "No settlement point prices to store" in rec.message for rec in caplog.records)


def test_store_prices_to_db_valid_inserts(temp_db, valid_spp_data):
    # Patch store_data_to_db to check call and arguments
    with mock.patch("ercot_scraping.database.store_data.store_data_to_db") as mock_store:
        store_prices_to_db(valid_spp_data, db_name=temp_db)
        assert mock_store.called
        args, kwargs = mock_store.call_args
        assert args[1] == temp_db
        assert args[2] == "SETTLEMENT_POINT_PRICES"


def test_store_prices_to_db_list_records_are_mapped(temp_db):
    # Provide data as lists, should be mapped to dicts
    spp_list_data = {
        "data": [
            ["2024-07-01", 1, 1, "SP1", "LZ", 20.0, 0],
            ["2024-07-01", 2, 1, "SP2", "HUB", 25.0, 0]
        ]
    }
    with mock.patch("ercot_scraping.database.store_data.store_data_to_db") as mock_store:
        store_prices_to_db(spp_list_data, db_name=temp_db)
        assert mock_store.called
        # The first record should be a dict after mapping
        called_data = mock_store.call_args[0][0]
        assert isinstance(called_data["data"][0], dict)
        assert called_data["data"][0]["deliveryDate"] == "2024-07-01"


def test_store_prices_to_db_invalid_data_raises(temp_db):
    # Missing required fields
    invalid_data = {"data": [{"deliveryDate": "2024-01-01"}]}
    with pytest.raises(ValueError):
        store_prices_to_db(invalid_data, db_name=temp_db)


def test_store_prices_to_db_filter_by_awards_filters(temp_db, valid_spp_data):
    # Patch get_active_settlement_points and filter_by_settlement_points
    with mock.patch("ercot_scraping.database.store_data.get_active_settlement_points") as mock_active, \
            mock.patch("ercot_scraping.database.store_data.filter_by_settlement_points") as mock_filter, \
            mock.patch("ercot_scraping.database.store_data.store_data_to_db") as mock_store:
        mock_active.return_value = {"SP1"}
        mock_filter.side_effect = lambda data, points: {
            "data": [data["data"][0]]}
        store_prices_to_db(valid_spp_data, db_name=temp_db,
                           filter_by_awards=True)
        mock_active.assert_called_once()
        mock_filter.assert_called_once()
        assert mock_store.called
        called_data = mock_store.call_args[0][0]
        assert len(called_data["data"]) == 1
        assert called_data["data"][0]["settlementPointName"] == "SP1"


def test_store_prices_to_db_filter_by_awards_no_active_points(temp_db, valid_spp_data):
    # If get_active_settlement_points returns empty, should not filter
    with mock.patch("ercot_scraping.database.store_data.get_active_settlement_points") as mock_active, \
            mock.patch("ercot_scraping.database.store_data.filter_by_settlement_points") as mock_filter, \
            mock.patch("ercot_scraping.database.store_data.store_data_to_db") as mock_store:
        mock_active.return_value = set()
        store_prices_to_db(valid_spp_data, db_name=temp_db,
                           filter_by_awards=True)
        mock_active.assert_called_once()
        mock_filter.assert_not_called()
        assert mock_store.called


def test_store_prices_to_db_aggregate_called(temp_db, valid_spp_data):
    # Patch aggregate_spp_data to ensure it's called
    with mock.patch("ercot_scraping.database.store_data.aggregate_spp_data") as mock_agg, \
            mock.patch("ercot_scraping.database.store_data.store_data_to_db") as mock_store:
        mock_agg.side_effect = lambda d: d
        store_prices_to_db(valid_spp_data, db_name=temp_db)
        mock_agg.assert_called_once()
        assert mock_store.called


@pytest.fixture
def valid_bid_award_data():
    # Provide a minimal valid bid award data structure for tests
    return {
        "data": [
            {
                "bidId": 1,
                "qse": "QSE1",
                "deliveryDate": "2024-07-01",
                "deliveryHour": 1,
                "bidMW": 10.0,
                "bidPrice": 30.0,
                "bidType": "ENERGY"
            }
        ]
    }


def test_store_bid_awards_to_db_skips_empty_data(caplog):
    empty_data = {"data": []}
    with mock.patch("ercot_scraping.database.store_data.store_data_to_db") as mock_store:
        store_bid_awards_to_db(empty_data, db_name=":memory:")
        mock_store.assert_not_called()
    assert any("No bid awards to store" in rec.message for rec in caplog.records)


def test_store_bid_awards_to_db_handles_none_data(caplog):
    with mock.patch("ercot_scraping.database.store_data.store_data_to_db") as mock_store:
        store_bid_awards_to_db(None, db_name=":memory:")
        mock_store.assert_not_called()
    assert any("No bid awards to store" in rec.message for rec in caplog.records)


def test_store_bid_awards_to_db_validation_and_logging(caplog):
    from ercot_scraping.database.store_data import store_bid_awards_to_db
    # Valid record
    valid = {
        "data": [
            {
                "deliveryDate": "2024-06-01",
                "hourEnding": 1,
                "settlementPointName": "POINT1",
                "qseName": "QSE1",
                "energyOnlyBidAwardInMW": 10.0,
                "settlementPointPrice": 30.5,
                "bidId": "BID1"
            }
        ]
    }
    store_bid_awards_to_db(valid, db_name=":memory:")
    # Invalid record (missing required field)
    invalid = {
        "data": [
            {
                "deliveryDate": "2024-06-01",
                "hourEnding": 1,
                "settlementPointName": "POINT1",
                "qseName": "QSE1",
                "settlementPointPrice": 30.5,
                "bidId": "BID1"
            }
        ]
    }
    store_bid_awards_to_db(invalid, db_name=":memory:")
    assert any(
        "BidAward validation error" in rec.message for rec in caplog.records)
    assert any(
        "No valid bid awards to store after validation." in rec.message for rec in caplog.records)


def test_store_data_to_db_active_settlement_filter(monkeypatch):
    from ercot_scraping.database import store_data
    # Patch get_active_settlement_points to return only ACTIVE1
    monkeypatch.setattr(
        store_data, "get_active_settlement_points", lambda db: {"ACTIVE1"}
    )
    monkeypatch.setattr(
        store_data, "filter_by_settlement_points",
        lambda data, points: {
            "data": [row for row in data["data"] if row["SettlementPoint"] in points]}
    )
    # Patch normalize_data to pass through
    monkeypatch.setattr(store_data, "normalize_data", lambda d, **k: d)
    # Patch is_data_empty to always return False
    monkeypatch.setattr(store_data, "is_data_empty", lambda d: False)
    # Patch logger to avoid side effects
    monkeypatch.setattr(store_data, "logger", mock.Mock())
    # Patch _insert_batches to capture inserted data
    inserted = {}

    def fake_insert_batches(cursor, insert_query, batch, batch_size):
        inserted["batch"] = batch
    monkeypatch.setattr(store_data, "_insert_batches", fake_insert_batches)
    # Prepare data
    data = {"data": [
        {"SettlementPoint": "ACTIVE1", "Value": 1},
        {"SettlementPoint": "INACTIVE", "Value": 2},
    ]}
    # Call store_data_to_db with filter enabled
    store_data.store_data_to_db(
        data=data,
        db_name="test.db",
        table_name="SETTLEMENT_POINT_PRICES",
        insert_query="INSERT",
        model_class=object,
        filter_by_active_settlement_points=True
    )
    # Only ACTIVE1 should be inserted
    assert len(inserted["batch"]) == 1
    assert inserted["batch"][0]["SettlementPoint"] == "ACTIVE1"


def test_store_data_to_db_no_filter(monkeypatch):
    from ercot_scraping.database import store_data
    monkeypatch.setattr(store_data, "normalize_data", lambda d, **k: d)
    monkeypatch.setattr(store_data, "is_data_empty", lambda d: False)
    monkeypatch.setattr(store_data, "logger", mock.Mock())
    inserted = {}

    def fake_insert_batches(cursor, insert_query, batch, batch_size):
        inserted["batch"] = batch
    monkeypatch.setattr(store_data, "_insert_batches", fake_insert_batches)
    data = {"data": [
        {"SettlementPoint": "ACTIVE1", "Value": 1},
        {"SettlementPoint": "INACTIVE", "Value": 2},
    ]}
    store_data.store_data_to_db(
        data=data,
        db_name="test.db",
        table_name="SETTLEMENT_POINT_PRICES",
        insert_query="INSERT",
        model_class=object,
        filter_by_active_settlement_points=False
    )
    # Both rows should be inserted
    assert len(inserted["batch"]) == 2
    assert {r["SettlementPoint"]
            for r in inserted["batch"]} == {"ACTIVE1", "INACTIVE"}
