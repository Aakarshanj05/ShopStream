"""Capture Kafka high-watermarks before daily generators publish new events."""

import json
import os
import tempfile
import uuid
from pathlib import Path

from confluent_kafka import Consumer, TopicPartition


TOPICS = (
    "customers_topic",
    "products_topic",
    "orders_topic",
    "payments_topic",
    "inventory_topic",
    "website_activity_topic",
)


def _read_offsets(path):
    with path.open("r", encoding="utf-8") as stream:
        offsets = json.load(stream)
    if not isinstance(offsets, dict) or not offsets:
        raise ValueError(f"Invalid Kafka offset snapshot: {path}")
    return offsets


def _write_once(path, offsets):
    path.parent.mkdir(parents=True, exist_ok=True)
    # An initial Bronze offset snapshot is immutable once created. This also
    # keeps a failed first DAG run from advancing the stream's initial start.
    if path.exists():
        _read_offsets(path)
        return False
    fd, temp_name = tempfile.mkstemp(prefix=".offsets-", dir=str(path.parent))
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as stream:
            json.dump(offsets, stream, sort_keys=True)
            stream.flush()
            os.fsync(stream.fileno())
        try:
            os.link(temp_name, path)
            return True
        except FileExistsError:
            _read_offsets(path)
            return False
    finally:
        try:
            os.unlink(temp_name)
        except FileNotFoundError:
            pass


def _ensure_payment_snapshot_guard(snapshot_path, guard_path):
    """Never silently recapture bootstrap offsets after initialization began."""
    if guard_path.exists() and not snapshot_path.is_file():
        raise RuntimeError(
            "The persistent payment offset snapshot is missing after its "
            f"initialization marker was written: {snapshot_path}. Refusing to "
            "recapture newer Kafka offsets and skip unprocessed orders.")
    if guard_path.exists():
        if not guard_path.is_file():
            raise RuntimeError(f"Payment offset guard is not a file: {guard_path}")
        return

    guard_path.parent.mkdir(parents=True, exist_ok=True)
    try:
        fd = os.open(
            guard_path,
            os.O_CREAT | os.O_EXCL | os.O_WRONLY,
            0o664,
        )
    except FileExistsError:
        if not snapshot_path.is_file():
            raise RuntimeError(
                f"Payment offset guard appeared without its snapshot: {snapshot_path}")
        return
    with os.fdopen(fd, "w", encoding="utf-8") as guard:
        guard.write(f"snapshot_path={snapshot_path}\nstate=initialization_started\n")
        guard.flush()
        os.fsync(guard.fileno())


def main():
    bootstrap = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    # Each consumer gets its own immutable, persistent first-start snapshot.
    # Re-reading Kafka high watermarks must never advance either consumer.
    bronze_snapshot_file = Path(os.environ["SHOPSTREAM_BRONZE_INITIAL_OFFSETS_FILE"])
    payment_snapshot_file = Path(os.environ["SHOPSTREAM_PAYMENT_INITIAL_OFFSETS_FILE"])
    payment_guard_file = Path(os.environ["SHOPSTREAM_PAYMENT_INITIAL_OFFSETS_GUARD"])
    _ensure_payment_snapshot_guard(payment_snapshot_file, payment_guard_file)
    consumer = Consumer({
        "bootstrap.servers": bootstrap,
        "group.id": f"shopstream-offset-snapshot-{uuid.uuid4().hex}",
        "enable.auto.commit": False,
        "enable.auto.offset.store": False,
    })
    try:
        metadata = consumer.list_topics(timeout=15)
        snapshot = {}
        for topic in TOPICS:
            topic_metadata = metadata.topics.get(topic)
            if topic_metadata is None or topic_metadata.error is not None:
                raise RuntimeError(f"Kafka topic unavailable: {topic}")
            partitions = {}
            for partition_id in sorted(topic_metadata.partitions):
                _low, high = consumer.get_watermark_offsets(
                    TopicPartition(topic, partition_id), timeout=15, cached=False
                )
                partitions[str(partition_id)] = int(high)
            if not partitions:
                raise RuntimeError(f"Kafka topic has no partitions: {topic}")
            snapshot[topic] = partitions

        bronze_created = _write_once(bronze_snapshot_file, snapshot)
        payment_created = _write_once(payment_snapshot_file, snapshot)
        print(f"Kafka end offsets captured for {len(snapshot)} topics.")
        print(
            "Persistent one-time snapshots: "
            f"Bronze={bronze_snapshot_file} (created={bronze_created}); "
            f"payments={payment_snapshot_file} (created={payment_created})")
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
