"""Small shared helpers for bounded, gracefully stopped Kafka generators."""

import os
import signal
import time


_stop_requested = False


def install_shutdown_handlers():
    """Make Airflow's SIGTERM stop the loop and reach its flush/close path."""
    def request_stop(_signum, _frame):
        global _stop_requested
        _stop_requested = True

    signal.signal(signal.SIGTERM, request_stop)
    signal.signal(signal.SIGINT, request_stop)


def duration_deadline():
    raw = os.getenv("SHOPSTREAM_GENERATOR_DURATION_SECONDS", "").strip()
    if not raw:
        return None
    seconds = float(raw)
    if seconds <= 0:
        raise ValueError("SHOPSTREAM_GENERATOR_DURATION_SECONDS must be positive")
    return time.monotonic() + seconds


def should_continue(deadline):
    return not _stop_requested and (deadline is None or time.monotonic() < deadline)


