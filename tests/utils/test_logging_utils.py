import logging
from ercot_scraping.utils.logging_utils import PerRunLogHandler, setup_module_logging


def test_per_run_log_handler_collects_logs():
    handler = PerRunLogHandler()
    logger = logging.getLogger("test_logger1")
    logger.setLevel(logging.DEBUG)
    logger.addHandler(handler)
    formatter = logging.Formatter('%(levelname)s: %(message)s')
    handler.setFormatter(formatter)

    logger.info("Info message")
    logger.error("Error message")
    logger.debug("Debug message")

    logs = handler.get_all_logs()
    assert any("Info message" in log for log in logs)
    assert any("Error message" in log for log in logs)
    assert any("Debug message" in log for log in logs)
    assert len(handler.records) == 3


def test_get_logs_by_level():
    handler = PerRunLogHandler()
    logger = logging.getLogger("test_logger2")
    logger.setLevel(logging.DEBUG)
    logger.addHandler(handler)
    formatter = logging.Formatter('%(levelname)s: %(message)s')
    handler.setFormatter(formatter)

    logger.info("Info message")
    logger.error("Error message")
    logger.info("Another info")

    info_logs = handler.get_logs_by_level(logging.INFO)
    error_logs = handler.get_logs_by_level(logging.ERROR)

    assert len(info_logs) == 2
    assert all("INFO" in log for log in info_logs)
    assert len(error_logs) == 1
    assert "Error message" in error_logs[0]


def test_clear_records():
    handler = PerRunLogHandler()
    logger = logging.getLogger("test_logger3")
    logger.setLevel(logging.DEBUG)
    logger.addHandler(handler)
    formatter = logging.Formatter('%(levelname)s: %(message)s')
    handler.setFormatter(formatter)

    logger.warning("Warning message")
    assert len(handler.records) == 1
    handler.clear()
    assert handler.records == []


def test_setup_module_logging_adds_handlers():
    logger_name = "test_logger4"
    logger = logging.getLogger(logger_name)
    # Remove all handlers before test
    logger.handlers = []

    per_run_handler = setup_module_logging(logger_name)
    assert any(isinstance(h, PerRunLogHandler) for h in logger.handlers)
    assert any(isinstance(h, logging.StreamHandler) for h in logger.handlers)
    assert per_run_handler in logger.handlers


def test_setup_module_logging_returns_per_run_handler():
    logger_name = "test_logger5"
    logger = logging.getLogger(logger_name)
    logger.handlers = []

    per_run_handler = setup_module_logging(logger_name)
    assert isinstance(per_run_handler, PerRunLogHandler)
