import pytest
from unittest.mock import MagicMock
from ercot_scraping.batched_api import fetch_in_batches
from ercot_scraping.config import LOGGER, DEFAULT_BATCH_DAYS, MAX_DATE_RANGE

# Mock the logger to avoid printouts during testing
LOGGER.info = MagicMock()
LOGGER.warning = MagicMock()
LOGGER.error = MagicMock()


@pytest.mark.parametrize(
    "id, start_date, end_date, batch_days, qse_names, expected_batches",
    [
        (
            "single_batch_single_qse",
            "2024-01-01",
            "2024-01-02",
            1,
            {"QSE1"},
            1,
        ),
        (
            "multiple_batches_single_qse",
            "2024-01-01",
            "2024-01-10",
            3,
            {"QSE1"},
            4,  # 1 + 3 + 3 + 3
        ),
        (
            "single_batch_multiple_qses",
            "2024-01-01",
            "2024-01-02",
            1,
            {"QSE1", "QSE2"},
            1,
        ),
        (
            "multiple_batches_multiple_qses",
            "2024-01-01",
            "2024-01-10",
            3,
            {"QSE1", "QSE2"},
            4,
        ),
        (
            "no_qses",
            "2024-01-01",
            "2024-01-02",
            1,
            None,
            1,
        ),
        (
            "batch_days_exceeds_max",
            "2024-01-01",
            "2024-02-10",
            50,
            None,
            1,  # batch_days gets capped to MAX_DATE_RANGE
        ),
    ],
)
def test_fetch_in_batches_happy_path(
    id, start_date, end_date, batch_days, qse_names, expected_batches, mocker
):
    """
    Test fetch_in_batches function with various valid inputs.
    """

    # Arrange
    mock_fetch_func = mocker.MagicMock(
        return_value={"data": [{"value": 1}], "fields": ["value"]}
    )

    # Act
    result = fetch_in_batches(
        fetch_func=mock_fetch_func,
        start_date=start_date,
        end_date=end_date,
        batch_days=batch_days,
        qse_names=qse_names,
    )

    # Assert
    assert len(result["data"]) == expected_batches * \
        (len(qse_names) if qse_names else 1)
    assert result["fields"] == ["value"]
    assert mock_fetch_func.call_count == expected_batches * \
        (len(qse_names) if qse_names else 1)


@pytest.mark.parametrize(
    "id, start_date, end_date, batch_days, qse_names, fetch_func_return, expected_data, expected_fields",
    [
        (
            "empty_response",
            "2024-01-01",
            "2024-01-02",
            1,
            None,
            {},
            [],
            [],
        ),
        (
            "no_data_field",
            "2024-01-01",
            "2024-01-02",
            1,
            None,
            {"fields": ["value"]},
            [],
            ["value"],
        ),
        (
            "empty_data_field",
            "2024-01-01",
            "2024-01-02",
            1,
            None,
            {"data": [], "fields": ["value"]},
            [],
            ["value"],
        ),
        (
            "no_fields_field",
            "2024-01-01",
            "2024-01-02",
            1,
            None,
            {"data": [{"value": 1}]},
            [{"value": 1}],
            [],
        ),
    ],
)
def test_fetch_in_batches_edge_cases(
    id,
    start_date,
    end_date,
    batch_days,
    qse_names,
    fetch_func_return,
    expected_data,
    expected_fields,
    mocker,
):
    """
    Test fetch_in_batches function with edge cases.
    """

    # Arrange
    mock_fetch_func = mocker.MagicMock(return_value=fetch_func_return)

    # Act
    result = fetch_in_batches(
        fetch_func=mock_fetch_func,
        start_date=start_date,
        end_date=end_date,
        batch_days=batch_days,
        qse_names=qse_names,
    )

    # Assert
    assert result["data"] == expected_data
    assert result["fields"] == expected_fields


@pytest.mark.parametrize(
    "id, start_date, end_date, batch_days, qse_names, exception",
    [
        (
            "fetch_func_exception",
            "2024-01-01",
            "2024-01-02",
            1,
            None,
            Exception("Test exception"),
        ),
    ],
)
def test_fetch_in_batches_error_cases(
    id, start_date, end_date, batch_days, qse_names, exception, mocker
):
    """
    Test fetch_in_batches function with error cases.
    """

    # Arrange
    mock_fetch_func = mocker.MagicMock(side_effect=exception)

    # Act
    result = fetch_in_batches(
        fetch_func=mock_fetch_func,
        start_date=start_date,
        end_date=end_date,
        batch_days=batch_days,
        qse_names=qse_names,
    )

    # Assert
    assert result["data"] == []
    assert result["fields"] == []
