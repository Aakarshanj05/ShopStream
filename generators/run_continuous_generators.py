"""Run the four bounded Kafka producers as one fail-fast Airflow task."""

import os
import signal
import subprocess
import sys
import time
from pathlib import Path


GENERATOR_SCRIPTS = (
    "orders_producer.py",
    "payment_generator.py",
    "inventory_generator.py",
    "website_activity_generator.py",
)
POLL_SECONDS = 0.25
DEFAULT_GRACE_SECONDS = 20
_requested_signal = None


def _request_shutdown(signum, _frame):
    global _requested_signal
    _requested_signal = signum


def _signal_child_group(process, signum):
    if process.poll() is not None:
        return
    try:
        if os.name == "posix":
            os.killpg(process.pid, signum)
        elif signum == signal.SIGTERM:
            process.terminate()
        else:
            process.kill()
    except ProcessLookupError:
        pass


def stop_children(children, grace_seconds):
    """Gracefully stop, then force-stop and reap every started child."""
    for name, process in children.items():
        if process.poll() is None:
            print(f"Sending SIGTERM to {name} (pid={process.pid})", flush=True)
            _signal_child_group(process, signal.SIGTERM)

    deadline = time.monotonic() + grace_seconds
    while time.monotonic() < deadline:
        if all(process.poll() is not None for process in children.values()):
            break
        time.sleep(POLL_SECONDS)

    for name, process in children.items():
        if process.poll() is None:
            print(f"Grace period expired; force-stopping {name}", flush=True)
            _signal_child_group(process, signal.SIGKILL)

    # wait() reaps each direct child, including one that exited before shutdown.
    for name, process in children.items():
        code = process.wait()
        print(f"Generator stopped: {name}, exit_code={code}", flush=True)


def main():
    global _requested_signal
    signal.signal(signal.SIGTERM, _request_shutdown)
    signal.signal(signal.SIGINT, _request_shutdown)

    grace_seconds = float(os.getenv(
        "SHOPSTREAM_GENERATOR_SHUTDOWN_GRACE_SECONDS",
        str(DEFAULT_GRACE_SECONDS),
    ))
    if grace_seconds <= 0:
        raise ValueError("Generator shutdown grace period must be positive")

    duration = os.getenv("SHOPSTREAM_GENERATOR_DURATION_SECONDS", "300")
    if float(duration) <= 0:
        raise ValueError("Generator duration must be positive")

    env = os.environ.copy()
    env["SHOPSTREAM_GENERATOR_DURATION_SECONDS"] = duration
    generator_dir = Path(__file__).resolve().parent
    children = {}
    failed_name = None

    try:
        for filename in GENERATOR_SCRIPTS:
            path = generator_dir / filename
            print(f"Starting generator: {filename}", flush=True)
            children[filename] = subprocess.Popen(
                [sys.executable, str(path)],
                env=env,
                start_new_session=(os.name == "posix"),
            )

        while True:
            if _requested_signal is not None:
                break

            active = False
            for name, process in children.items():
                code = process.poll()
                if code is None:
                    active = True
                elif code != 0:
                    failed_name = name
                    print(
                        f"Generator failed: {name}, exit_code={code}; "
                        "stopping siblings immediately",
                        flush=True,
                    )
                    break

            if failed_name is not None or not active:
                break
            time.sleep(POLL_SECONDS)
    except BaseException:
        # Also clean up siblings if process creation or this supervisor fails.
        stop_children(children, grace_seconds)
        raise

    stop_children(children, grace_seconds)

    if _requested_signal is not None:
        print(f"Supervisor interrupted by signal {_requested_signal}", flush=True)
        return 128 + int(_requested_signal)
    if failed_name is not None:
        return 1

    failures = [name for name, process in children.items() if process.returncode]
    if failures:
        print(f"Generators exited unsuccessfully: {failures}", flush=True)
        return 1
    print("All continuous generators completed successfully.", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
