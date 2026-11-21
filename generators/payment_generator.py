
import json
import os
from pathlib import Path
import hashlib

from confluent_kafka import Consumer, Producer
from generator_runtime import duration_deadline, install_shutdown_handlers, should_continue


# --------------------------------------------------
# 1. PROJECT CONFIGURATION
# --------------------------------------------------

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")

ORDER_TOPIC = "orders_topic"

PAYMENT_TOPIC = "payments_topic"

CONSUMER_GROUP = os.getenv("PAYMENT_CONSUMER_GROUP", "payment-generator-group")
_initial_offsets_file = os.getenv("SHOPSTREAM_PAYMENT_INITIAL_OFFSETS_FILE")

ORDER_START_OFFSETS = {}
if _initial_offsets_file:
    with Path(_initial_offsets_file).open("r", encoding="utf-8") as _stream:
        _snapshot = json.load(_stream)
    ORDER_START_OFFSETS = {
        int(partition): int(offset)
        for partition, offset in _snapshot.get(ORDER_TOPIC, {}).items()
    }
    if not ORDER_START_OFFSETS:
        raise ValueError(
            f"No {ORDER_TOPIC} offsets in snapshot {_initial_offsets_file}")


def on_partition_assignment(assigned_consumer, partitions):
    """Resume committed progress, or start a new run group at its snapshot."""
    committed = assigned_consumer.committed(partitions, timeout=10)
    for partition, committed_partition in zip(partitions, committed):
        if committed_partition.error is not None:
            raise RuntimeError(
                f"Unable to read committed offset for {partition.topic}/"
                f"{partition.partition}: {committed_partition.error}")
        if committed_partition.offset >= 0:
            partition.offset = committed_partition.offset
        elif (partition.topic == ORDER_TOPIC
              and partition.partition in ORDER_START_OFFSETS):
            partition.offset = ORDER_START_OFFSETS[partition.partition]
        elif partition.topic == ORDER_TOPIC and _initial_offsets_file:
            raise RuntimeError(
                f"No captured starting offset for {partition.topic}/{partition.partition}")
    assigned_consumer.assign(partitions)


# --------------------------------------------------
# 2. CREATE KAFKA CONSUMER
# --------------------------------------------------

consumer_config = {

    "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,

    "group.id": CONSUMER_GROUP,

    "auto.offset.reset": "latest" if _initial_offsets_file else "earliest",

    "enable.auto.commit": False

}

consumer = Consumer(consumer_config)

consumer.subscribe([ORDER_TOPIC], on_assign=on_partition_assignment)

print("Kafka consumer initialized.")
print(f"Subscribed to topic: {ORDER_TOPIC}")

# --------------------------------------------------
# 3. CREATE KAFKA PRODUCER
# --------------------------------------------------

producer_config = {

    "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS

}

producer = Producer(producer_config)

print("Kafka producer initialized.")
print(f"Payment destination: {PAYMENT_TOPIC}")



# --------------------------------------------------
# 4. PAYMENT MASTER CONFIGURATION
# --------------------------------------------------

PAYMENT_METHODS = [
    "UPI",
    "CREDIT_CARD",
    "DEBIT_CARD",
    "NET_BANKING",
    "COD"
]

PAYMENT_STATUSES = [
    "SUCCESS",
    "SUCCESS",
    "SUCCESS",
    "FAILED",
    "PENDING"
]


# --------------------------------------------------
# 5. GENERATE PAYMENT EVENT
# --------------------------------------------------

def generate_payment(order):
    """Build a stable payment event for delivery/commit retries."""
    order_id = str(order["order_id"])
    digest = hashlib.sha256(order_id.encode("utf-8")).digest()
    payment_method = PAYMENT_METHODS[
        int.from_bytes(digest[:4], "big") % len(PAYMENT_METHODS)]
    payment_status = PAYMENT_STATUSES[
        int.from_bytes(digest[4:8], "big") % len(PAYMENT_STATUSES)]
    if not order.get("created_at"):
        raise ValueError(f"Order {order_id} has no stable created_at timestamp")

    payment = {

        "payment_id": "PAY-" + hashlib.sha256(
            order_id.encode("utf-8")
        ).hexdigest()[:24].upper(),

        "order_id": order["order_id"],

        "customer_id": order["customer_id"],

        "payment_method": payment_method,

        "payment_status": payment_status,

        "payment_amount": order["amount"],

        "payment_timestamp": order["created_at"]

    }

    return payment



# --------------------------------------------------
# 6. DELIVERY CALLBACK
# --------------------------------------------------

def payment_delivery_report(err, message, delivery_result):

    if err is not None:

        delivery_result["error"] = err

        print(f"Payment delivery failed: {err}")

    else:

        print(
            f"Payment delivered | "
            f"Topic: {message.topic()} | "
            f"Partition: {message.partition()} | "
            f"Offset: {message.offset()}"
        )


# --------------------------------------------------
# 7. PUBLISH PAYMENT TO KAFKA
# --------------------------------------------------

def publish_payment(payment):

    delivery_result = {
        "error": None
    }

    producer.produce(

        topic=PAYMENT_TOPIC,

        key=payment["payment_id"],

        value=json.dumps(payment),

        callback=lambda err, msg: payment_delivery_report(
            err, msg, delivery_result
        )

    )

    # Wait until Kafka confirms delivery
    remaining = producer.flush(timeout=10)

    if remaining > 0:

        raise RuntimeError(
            "Payment message delivery timed out."
        )

    if delivery_result["error"] is not None:

        raise RuntimeError(
            f"Kafka delivery failed: {delivery_result['error']}"
        )


# --------------------------------------------------
# 8. PROCESS ORDER AND GENERATE PAYMENT
# --------------------------------------------------

def process_order(order_message):

    try:

        # Convert Kafka message into Python dictionary
        order = json.loads(order_message.value().decode("utf-8"))

        # Validate required order fields
        required_fields = [
            "order_id",
            "customer_id",
            "amount"
        ]

        for field in required_fields:

            if field not in order:

                raise ValueError(
                    f"Missing required order field: {field}"
                )

        # Generate payment using original order
        payment = generate_payment(order)

        print("\nGenerating payment...")

        print(
            f"Order ID: {payment['order_id']} | "
            f"Amount: Rs.{payment['payment_amount']} | "
            f"Method: {payment['payment_method']} | "
            f"Status: {payment['payment_status']}"
        )

        # Publish payment to Kafka
        publish_payment(payment)

        # Commit order offset only after successful payment delivery
        committed_offsets = consumer.commit(
            message=order_message,
            asynchronous=False
        )
        for committed_offset in committed_offsets or []:
            if committed_offset.error is not None:
                raise RuntimeError(
                    "Payment was delivered but the order offset commit failed "
                    f"for {committed_offset.topic}/"
                    f"{committed_offset.partition}: {committed_offset.error}")

        print(
            f"Order processed successfully: "
            f"{order['order_id']}"
        )

        return True

    except (json.JSONDecodeError, UnicodeDecodeError, ValueError) as error:

        print(f"Invalid order message: {error}")

        # Skip malformed messages to avoid repeatedly reading them
        consumer.commit(
            message=order_message,
            asynchronous=False
        )

        return False


# --------------------------------------------------
# 9. MAIN FUNCTION
# --------------------------------------------------

def main():

    print("\n" + "=" * 60)

    print("             PAYMENT GENERATOR STARTED")

    print("=" * 60)

    print(f"Source Topic: {ORDER_TOPIC}")

    print(f"Destination Topic: {PAYMENT_TOPIC}")

    print(f"Consumer Group: {CONSUMER_GROUP}")

    print("Reading existing orders from Kafka...")

    print("Press CTRL + C to stop.\n")

    processed_count = 0
    install_shutdown_handlers()
    deadline = duration_deadline()

    try:

        while should_continue(deadline):

            # Fetch one order from Kafka
            message = consumer.poll(timeout=1.0)

            if message is None:

                continue

            if message.error():

                print(f"Consumer error: {message.error()}")

                continue

            # Process order and publish payment
            successful = process_order(message)

            if successful:

                processed_count += 1

    except KeyboardInterrupt:

        print("\nStopping payment generator...")

    finally:

        # Ensure any outstanding producer messages are flushed
        remaining = producer.flush(timeout=10)
        # Close consumer and release its resources
        consumer.close()
        if remaining:
            raise RuntimeError(f"{remaining} payment messages remain undelivered")

        print("\n" + "=" * 60)

        print("PAYMENT GENERATOR SUMMARY")

        print("=" * 60)

        print(f"Successfully processed orders: {processed_count}")

        print("Payment generator stopped.")


# --------------------------------------------------
# 10. SCRIPT ENTRY POINT
# --------------------------------------------------

if __name__ == "__main__":

    main()
