
import json
import os
import uuid
import random
import time

from datetime import datetime, timezone
from pathlib import Path
from confluent_kafka import Producer
from generator_runtime import duration_deadline, install_shutdown_handlers, should_continue


# --------------------------------------------------
# 1. PROJECT CONFIGURATION
# --------------------------------------------------

BASE_DIR = Path(__file__).resolve().parent.parent

DATA_DIR = Path(os.getenv("SHOPSTREAM_MASTER_DATA_DIR", str(BASE_DIR / "data")))
CUSTOMER_FILE = DATA_DIR / "customers.json"
PRODUCT_FILE = DATA_DIR / "products.json"

KAFKA_CONFIG = {
    "bootstrap.servers": os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
}

TOPIC = "orders_topic"

EVENT_INTERVAL = 4
delivery_errors = []


# --------------------------------------------------
# 2. LOAD MASTER DATASETS
# --------------------------------------------------

def load_json_file(file_path):

    if not file_path.exists():
        raise FileNotFoundError(
            f"Required dataset not found: {file_path}"
        )

    with open(file_path, "r", encoding="utf-8") as file:

        data = json.load(file)

    if not data:
        raise ValueError(
            f"Dataset is empty: {file_path}"
        )

    return data


def load_master_data():

    print("Loading customer and product datasets...")

    customers = load_json_file(CUSTOMER_FILE)

    products = load_json_file(PRODUCT_FILE)

    customer_ids = [
        customer["customer_id"]
        for customer in customers
    ]

    print(f"Customers loaded: {len(customer_ids)}")

    print(f"Products loaded: {len(products)}")

    return customer_ids, products


CUSTOMER_IDS, PRODUCTS = load_master_data()


# --------------------------------------------------
# 3. CREATE KAFKA PRODUCER
# --------------------------------------------------

producer = Producer(KAFKA_CONFIG)


# --------------------------------------------------
# 4. DELIVERY CALLBACK
# --------------------------------------------------

def delivery_report(err, message):

    if err is not None:

        delivery_errors.append(str(err))

        print(f"Message delivery failed: {err}")

    else:

        print(
            f"Delivered | "
            f"Topic: {message.topic()} | "
            f"Partition: {message.partition()} | "
            f"Offset: {message.offset()}"
        )


# --------------------------------------------------
# 5. GENERATE RANDOM ORDER
# --------------------------------------------------

def generate_order():

    # Select an existing customer
    customer_id = random.choice(CUSTOMER_IDS)

    # Select an existing product
    product = random.choice(PRODUCTS)

    # Generate random quantity
    quantity = random.randint(1, 4)

    # Get actual product price
    unit_price = product["price"]

    # Calculate order amount
    amount = round(unit_price * quantity, 2)

    order = {

        "order_id": "ORD-" + uuid.uuid4().hex[:8].upper(),

        "customer_id": customer_id,

        "product_id": product["product_id"],

        "quantity": quantity,

        "unit_price": unit_price,

        "amount": amount,

        "status": random.choice([
            "PLACED",
            "PLACED",
            "PLACED",
            "CONFIRMED"
        ]),

        "created_at": datetime.now(timezone.utc).isoformat()

    }

    return order


# --------------------------------------------------
# 6. MAIN PRODUCER FUNCTION
# --------------------------------------------------

def main():

    print("\n" + "=" * 60)

    print("          ECOMMERCE ORDER GENERATOR STARTED")

    print("=" * 60)

    print(f"Kafka Topic: {TOPIC}")

    print(f"Available Customers: {len(CUSTOMER_IDS)}")

    print(f"Available Products: {len(PRODUCTS)}")

    print(f"Generation Interval: {EVENT_INTERVAL} seconds")

    print("Press CTRL + C to stop.\n")

    order_count = 0
    install_shutdown_handlers()
    deadline = duration_deadline()

    try:

        while should_continue(deadline):

            # Generate order
            order = generate_order()

            # Convert dictionary to JSON
            order_json = json.dumps(order)

            # Publish to Kafka
            producer.produce(

                topic=TOPIC,

                key=order["order_id"],

                value=order_json,

                callback=delivery_report

            )

            # Process delivery callbacks
            producer.poll(0)

            order_count += 1

            print(
                f"Generated Order #{order_count} | "
                f"Order ID: {order['order_id']} | "
                f"Customer: {order['customer_id']} | "
                f"Product: {order['product_id']} | "
                f"Quantity: {order['quantity']} | "
                f"Amount: Rs.{order['amount']}"
            )

            # Wait four seconds
            time.sleep(EVENT_INTERVAL)

    except KeyboardInterrupt:

        print("\nStopping order generator...")

    finally:

        remaining = producer.flush(timeout=10)

        if remaining == 0:

            print("All pending messages delivered.")

        else:

            print(
                f"Warning: {remaining} messages "
                f"remain undelivered."
            )

        if remaining or delivery_errors:
            raise RuntimeError(
                f"Orders delivery failed: pending={remaining}, "
                f"callback_errors={len(delivery_errors)}, "
                f"first_error={delivery_errors[0] if delivery_errors else 'none'}")

        print(f"Total orders generated: {order_count}")

        print("Order generator stopped.")


# --------------------------------------------------
# 7. SCRIPT ENTRY POINT
# --------------------------------------------------

if __name__ == "__main__":

    main()
