"""Create empty isolated fixture topics and publish a deterministic test set.

This script refuses to write unless all six dedicated topics are empty and
single-partitioned. It never resets, deletes, or writes to project topics.
"""
import json
import os
import sys
import time

from confluent_kafka import Consumer, KafkaError, KafkaException, Producer, TopicPartition
from confluent_kafka.admin import AdminClient, NewTopic


BOOTSTRAP_SERVERS = os.getenv("SHOPSTREAM_FIXTURE_BOOTSTRAP_SERVERS", "localhost:9092")
TOPICS = {
    "customers": "shopstream_it_customers",
    "products": "shopstream_it_products",
    "orders": "shopstream_it_orders",
    "payments": "shopstream_it_payments",
    "inventory": "shopstream_it_inventory",
    "website_activity": "shopstream_it_website_activity",
}
EXPECTED_COUNTS = {
    "shopstream_it_customers": 1,
    "shopstream_it_products": 1,
    "shopstream_it_orders": 2,
    "shopstream_it_payments": 1,
    "shopstream_it_inventory": 1,
    "shopstream_it_website_activity": 1,
}
EVENT_TIME = "2026-11-13T00:00:00Z"


def _require_fixture_topics_empty(admin):
    metadata = admin.list_topics(timeout=15)
    missing = [name for name in TOPICS.values() if name not in metadata.topics]
    if missing:
        futures = admin.create_topics([
            NewTopic(name, num_partitions=1, replication_factor=1)
            for name in missing
        ], request_timeout=15)
        for name, future in futures.items():
            try:
                future.result()
            except Exception as exc:
                # A concurrent creator is acceptable only if subsequent
                # metadata inspection confirms the exact safe shape.
                if "TopicExists" not in type(exc).__name__:
                    raise RuntimeError(f"Could not create isolated topic {name}") from exc

    leader_deadline = time.monotonic() + 45
    while True:
        metadata = admin.list_topics(timeout=10)
        unavailable = []
        for name in TOPICS.values():
            topic = metadata.topics.get(name)
            if topic is None or topic.error is not None:
                raise RuntimeError(f"Isolated Kafka topic is unavailable: {name}")
            if set(topic.partitions) != {0}:
                raise RuntimeError(
                    f"Refusing non-isolated topic shape for {name}: "
                    f"partitions={sorted(topic.partitions)}")
            if topic.partitions[0].leader < 0:
                unavailable.append(name)
        if not unavailable:
            break
        if time.monotonic() >= leader_deadline:
            raise RuntimeError(
                f"Timed out waiting for fixture topic leaders: {unavailable}")
        time.sleep(1)

    consumer = Consumer({
        "bootstrap.servers": BOOTSTRAP_SERVERS,
        "group.id": f"shopstream-fixture-preflight-{os.getpid()}",
        "enable.auto.commit": False,
    })
    try:
        watermarks = {}
        for name in TOPICS.values():
            while True:
                try:
                    low, high = consumer.get_watermark_offsets(
                        TopicPartition(name, 0), timeout=10, cached=False)
                    break
                except KafkaException as exc:
                    error = exc.args[0] if exc.args else None
                    if (error is None
                            or error.code() != KafkaError.NOT_LEADER_FOR_PARTITION
                            or time.monotonic() >= leader_deadline):
                        raise
                    time.sleep(1)
            watermarks[name] = {"start": low, "end": high}
            if low != 0 or high != 0:
                raise RuntimeError(
                    f"Refusing to produce into non-empty topic {name}: "
                    f"start={low}, end={high}")
        return watermarks
    finally:
        consumer.close()


def _fixture_records():
    events = [
        ("customers", "customer-fixture-001", {
            "customer_id": "customer-fixture-001", "first_name": "Test",
            "last_name": "Customer", "email": "fixture@example.test",
            "city": "Patna", "membership": "BASIC", "created_at": EVENT_TIME,
        }),
        ("products", "product-fixture-001", {
            "product_id": "product-fixture-001", "product_name": "Fixture Widget",
            "category": "TEST", "brand": "SHOPSTREAM", "price": 9.99,
            "stock_quantity": 8, "created_at": EVENT_TIME,
        }),
        ("orders", "order-fixture-001", {
            "order_id": "order-fixture-001", "customer_id": "customer-fixture-001",
            "product_id": "product-fixture-001", "quantity": 2,
            "unit_price": 9.99, "amount": 19.98, "status": "CONFIRMED",
            "created_at": EVENT_TIME,
        }),
        # This must follow the valid order on the same partition, at offset 1.
        ("orders", None, ""),
        ("payments", "payment-fixture-001", {
            "payment_id": "payment-fixture-001", "order_id": "order-fixture-001",
            "customer_id": "customer-fixture-001", "payment_method": "UPI",
            "payment_status": "SUCCESS", "payment_amount": 19.98,
            "payment_timestamp": EVENT_TIME,
        }),
        ("inventory", "inventory-fixture-001", {
            "inventory_event_id": "inventory-fixture-001",
            "product_id": "product-fixture-001", "event_type": "STOCK_OUT",
            "quantity_changed": -2, "previous_stock": 10,
            "available_stock": 8, "event_timestamp": EVENT_TIME,
        }),
        ("website_activity", "activity-fixture-001", {
            "event_id": "activity-fixture-001", "session_id": "session-fixture-001",
            "customer_id": "customer-fixture-001", "product_id": "product-fixture-001",
            "event_type": "PAGE_VIEW", "device": "DESKTOP", "browser": "CHROME",
            "city": "Patna", "sequence_number": 1, "event_timestamp": EVENT_TIME,
        }),
    ]
    return events


def _verify_fixture_records():
    expected = {}
    next_offsets = {name: 0 for name in TOPICS.values()}
    for entity, key, payload in _fixture_records():
        topic = TOPICS[entity]
        value = (payload if isinstance(payload, str)
                 else json.dumps(payload, sort_keys=True, separators=(",", ":")))
        offset = next_offsets[topic]
        next_offsets[topic] += 1
        expected[(topic, 0, offset)] = (
            key.encode("utf-8") if key is not None else None,
            value.encode("utf-8"))

    consumer = Consumer({
        "bootstrap.servers": BOOTSTRAP_SERVERS,
        "group.id": f"shopstream-fixture-readback-{os.getpid()}",
        "enable.auto.commit": False,
        "enable.partition.eof": False,
    })
    actual = {}
    try:
        consumer.assign([TopicPartition(name, 0, 0) for name in TOPICS.values()])
        deadline = time.monotonic() + 30
        while len(actual) < len(expected):
            if time.monotonic() >= deadline:
                raise RuntimeError(
                    f"Timed out reading fixture records: {len(actual)}/{len(expected)}")
            message = consumer.poll(1)
            if message is None:
                continue
            if message.error():
                raise RuntimeError(f"Fixture read-back failed: {message.error()}")
            identity = (message.topic(), message.partition(), message.offset())
            if identity not in expected:
                raise RuntimeError(f"Unexpected fixture Kafka identity: {identity}")
            observed = (message.key(), message.value())
            if observed != expected[identity]:
                raise RuntimeError(f"Fixture payload/key mismatch at {identity}")
            if identity in actual:
                raise RuntimeError(f"Duplicate fixture Kafka identity: {identity}")
            actual[identity] = observed
    finally:
        consumer.close()

    if set(actual) != set(expected):
        raise RuntimeError("Fixture Kafka read-back identities do not match production")
    malformed = (TOPICS["orders"], 0, 1)
    if expected[malformed] != (None, b""):
        raise RuntimeError("Fixture malformed record is not the explicit empty/null-key record")
    return {
        "records_read_back": len(actual),
        "valid_records": len(actual) - 1,
        "quarantine_records": 1,
        "malformed_identity": {
            "topic": malformed[0], "partition": malformed[1],
            "offset": malformed[2], "key": None, "raw_payload_bytes": 0,
        },
    }


def main():
    if len(sys.argv) != 2 or sys.argv[1] not in ("--produce", "--verify"):
        raise SystemExit(
            "Usage: python publish_shopstream_integration_fixture.py --produce|--verify")

    if sys.argv[1] == "--verify":
        print(json.dumps(_verify_fixture_records(), indent=2, sort_keys=True))
        return

    admin = AdminClient({"bootstrap.servers": BOOTSTRAP_SERVERS})
    before = _require_fixture_topics_empty(admin)
    delivered = []
    producer = Producer({
        "bootstrap.servers": BOOTSTRAP_SERVERS,
        "enable.idempotence": True,
        "acks": "all",
        "client.id": "shopstream-isolated-fixture",
    })

    for entity, key, payload in _fixture_records():
        topic = TOPICS[entity]
        value = (payload if isinstance(payload, str)
                 else json.dumps(payload, sort_keys=True, separators=(",", ":")))
        record = {
            "topic": topic,
            "partition": 0,
            "key": key,
            "value_bytes": len(value.encode("utf-8")),
        }

        def on_delivery(error, message, record=record):
            if error is not None:
                delivered.append({**record, "error": str(error)})
            else:
                delivered.append({**record, "offset": message.offset()})

        producer.produce(
            topic, key=key, value=value.encode("utf-8"), partition=0,
            callback=on_delivery)
        producer.poll(0)

    remaining = producer.flush(30)
    if remaining:
        raise RuntimeError(f"Fixture producer still has {remaining} undelivered records")
    if any("error" in row for row in delivered) or len(delivered) != 7:
        raise RuntimeError(f"Fixture delivery failures: {delivered}")

    expected_bad = [row for row in delivered
                    if row["topic"] == TOPICS["orders"] and row["value_bytes"] == 0]
    if len(expected_bad) != 1 or expected_bad[0].get("offset") != 1:
        raise RuntimeError(
            "Malformed fixture identity was not the expected orders partition 0 offset 1: "
            f"{expected_bad}")

    consumer = Consumer({
        "bootstrap.servers": BOOTSTRAP_SERVERS,
        "group.id": f"shopstream-fixture-postflight-{os.getpid()}",
        "enable.auto.commit": False,
    })
    try:
        after = {}
        for name, expected_count in EXPECTED_COUNTS.items():
            low, high = consumer.get_watermark_offsets(
                TopicPartition(name, 0), timeout=10, cached=False)
            after[name] = {"start": low, "end": high, "records": high - low}
            if low != 0 or high != expected_count:
                raise RuntimeError(
                    f"Post-production offsets differ for {name}: "
                    f"expected start=0,end={expected_count}; got {after[name]}")
    finally:
        consumer.close()

    readback = _verify_fixture_records()
    malformed = expected_bad[0]
    result = {
        "bootstrap_servers": BOOTSTRAP_SERVERS,
        "preflight_empty_offsets": before,
        "topics": after,
        "total_records": sum(EXPECTED_COUNTS.values()),
        "valid_records": sum(EXPECTED_COUNTS.values()) - 1,
        "quarantine_records": 1,
        "malformed_identity": {
            "topic": malformed["topic"],
            "partition": malformed["partition"],
            "offset": malformed["offset"],
            "key": None,
            "raw_payload_bytes": malformed["value_bytes"],
        },
        "delivery_records": delivered,
        "readback": readback,
    }
    print(json.dumps(result, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
