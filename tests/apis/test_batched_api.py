from ercot_scraping.apis.batched_api import fetch_in_batches
from unittest import mock
from ercot_scraping.apis.batched_api import rate_limited_request, _MIN_REQUEST_INTERVAL

"""Test module for batched API functionality."""


def test_batched_api():
    """Test the batched API (placeholder)."""
    assert True  # Replace with actual test logic for batched API


def make_fetch_func(return_type="dict", fail_on=None):
    """
    Helper to create a fetch_func for testing.
    - return_type: "dict" returns {"data": [...], "fields": [...]}
                    "list" returns [...]
    - fail_on: set of (batch_start, batch_end) tuples to raise Exception on
    """
    calls = []

    def fetch_func(batch_start, batch_end, **kwargs):
        calls.append((batch_start, batch_end))
        if fail_on and (batch_start, batch_end) in fail_on:
            raise RuntimeError("Simulated fetch error")
        if return_type == "dict":
            return {
                "data": [
                    {"date": batch_start, "value": 1},
                    {"date": batch_end, "value": 2}
                ],
                "fields": ["date", "value"]
            }
        elif return_type == "list":
            return [
                {"date": batch_start, "value": 1},
                {"date": batch_end, "value": 2}
            ]
        else:
            return None
    fetch_func.calls = calls
    return fetch_func


def test_single_batch_dict():
    fetch_func = make_fetch_func("dict")
    result = fetch_in_batches(
        fetch_func,
        start_date="2024-01-01",
        end_date="2024-01-03",
        batch_days=5
    )
    assert isinstance(result, dict)
    assert "data" in result and "fields" in result
    assert len(result["data"]) == 2  # Only one batch, two records
    assert result["fields"] == ["date", "value"]


def test_multiple_batches_dict():
    fetch_func = make_fetch_func("dict")
    result = fetch_in_batches(
        fetch_func,
        start_date="2024-01-01",
        end_date="2024-01-05",
        batch_days=2
    )
    # Batches: [("2024-01-01", "2024-01-01"), ("2024-01-02", "2024-01-03"), ("2024-01-04", "2024-01-05")]
    assert len(result["data"]) == 6  # 3 batches * 2 records each
    assert result["fields"] == ["date", "value"]


def test_multiple_batches_list():
    fetch_func = make_fetch_func("list")
    result = fetch_in_batches(
        fetch_func,
        start_date="2024-01-01",
        end_date="2024-01-04",
        batch_days=2
    )
    assert isinstance(result["data"], list)
    assert len(result["data"]) == 6  # 3 batches * 2 records each
    assert result["fields"] == []


def test_checkpoint_func_called():
    fetch_func = make_fetch_func("dict")
    checkpoints = []

    def checkpoint_func(batch_idx, batch_start, batch_end):
        checkpoints.append((batch_idx, batch_start, batch_end))
    fetch_in_batches(
        fetch_func,
        start_date="2024-01-01",
        end_date="2024-01-04",
        batch_days=2,
        checkpoint_func=checkpoint_func
    )
    assert len(checkpoints) == 3
    assert checkpoints[0][0] == 0  # batch_idx
    assert checkpoints[-1][1] == "2024-01-04"  # batch_start of last batch


def test_fetch_func_exception_handling(caplog):
    fetch_func = make_fetch_func(
        "dict", fail_on={("2024-01-02", "2024-01-03")})
    result = fetch_in_batches(
        fetch_func,
        start_date="2024-01-01",
        end_date="2024-01-04",
        batch_days=2
    )
    # Only 2 batches succeed, so 4 records
    assert len(result["data"]) == 4
    # Should log error for the failed batch
    assert any("Exception in fetch" in rec.message for rec in caplog.records)


def make_response(status_code=200, content=b"ok", text="ok"):
    resp = mock.Mock()
    resp.status_code = status_code
    resp.content = content
    resp.text = text
    return resp


@mock.patch("ercot_scraping.apis.batched_api.requests.request")
def test_rate_limited_request_basic(mock_request):
    mock_resp = make_response()
    mock_request.return_value = mock_resp
    resp = rate_limited_request("GET", "http://test-url")
    assert resp is mock_resp
    mock_request.assert_called_once_with("GET", "http://test-url", timeout=30)


@mock.patch("ercot_scraping.apis.batched_api.requests.request")
def test_rate_limited_request_passes_kwargs(mock_request):
    mock_resp = make_response()
    mock_request.return_value = mock_resp
    headers = {"Authorization": "secret",
               "Ocp-Apim-Subscription-Key": "key", "Other": "val"}
    params = {"foo": "bar"}
    resp = rate_limited_request(
        "POST", "http://test-url", headers=headers, params=params)
    assert resp is mock_resp
    called_args, called_kwargs = mock_request.call_args
    assert called_args[0] == "POST"
    assert called_args[1] == "http://test-url"
    assert called_kwargs["headers"] == headers
    assert called_kwargs["params"] == params
    assert called_kwargs["timeout"] == 30


@mock.patch("ercot_scraping.apis.batched_api.requests.request")
@mock.patch("ercot_scraping.apis.batched_api.time")
def test_rate_limited_request_enforces_min_interval(mock_time, mock_request):
    mock_resp = make_response()
    mock_request.return_value = mock_resp
    # Simulate last call was just now, so should sleep
    mock_time.time.side_effect = [100.0, 100.0, 100.0 + _MIN_REQUEST_INTERVAL]
    mock_time.sleep = mock.Mock()
    # First call sets _last_sync_request_time
    rate_limited_request("GET", "http://test-url")
    # Second call should sleep
    rate_limited_request("GET", "http://test-url")
    assert mock_time.sleep.called
    sleep_arg = mock_time.sleep.call_args[0][0]
    assert abs(sleep_arg - _MIN_REQUEST_INTERVAL) < 1e-6


@mock.patch("ercot_scraping.apis.batched_api.requests.request")
def test_rate_limited_request_logs_and_masks_headers(mock_request, caplog):
    mock_resp = make_response()
    mock_request.return_value = mock_resp
    headers = {
        "Authorization": "secret",
        "Ocp-Apim-Subscription-Key": "key",
        "Other": "visible"
    }
    with caplog.at_level("INFO"):
        rate_limited_request("GET", "http://test-url", headers=headers)
    # Should log masked headers
    log_msgs = [rec.getMessage() for rec in caplog.records]
    assert any("rate_limited_request" in msg for msg in log_msgs)
    assert any("Authorization" in msg and "****" in msg for msg in log_msgs)
    assert any(
        "Ocp-Apim-Subscription-Key" in msg and "****" in msg for msg in log_msgs)
    assert any("Other" in msg and "visible" in msg for msg in log_msgs)


@mock.patch("ercot_scraping.apis.batched_api.requests.request")
def test_rate_limited_request_handles_response_text_exception(mock_request, caplog):
    mock_resp = make_response()
    type(mock_resp).text = mock.PropertyMock(side_effect=Exception("fail"))
    mock_request.return_value = mock_resp
    with caplog.at_level("DEBUG"):
        rate_limited_request("GET", "http://test-url")
    # Should not raise, and should not log response preview
    assert not any("Response preview" in rec.getMessage()
                   for rec in caplog.records)
