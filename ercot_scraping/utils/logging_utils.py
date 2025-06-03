import logging


class PerRunLogHandler(logging.Handler):
    def __init__(self):
        super().__init__()
        self.records = []

    def emit(self, record):
        self.records.append(record)

    def get_logs_by_level(self, level):
        return [self.format(r) for r in self.records if r.levelno == level]

    def get_all_logs(self):
        return [self.format(r) for r in self.records]

    def clear(self):
        self.records.clear()


# Utility function to set up logging in any module
def setup_module_logging(logger_name: str = None):
    """
    Sets up both a per-run log handler and a stream handler for console output on the given logger.
    Returns the per-run handler for later use (e.g., for dumping logs).
    """
    logger = logging.getLogger(logger_name)
    per_run_handler = PerRunLogHandler()
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    per_run_handler.setFormatter(formatter)
    logger.addHandler(per_run_handler)
    logger.setLevel(logging.DEBUG)

    # Avoid duplicate console handlers
    if not any(isinstance(h, logging.StreamHandler) for h in logger.handlers):
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)
    return per_run_handler
