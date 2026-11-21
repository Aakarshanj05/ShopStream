
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
PRODUCT_FILE = DATA_DIR / "products.json"

KAFKA_CONFIG = {
    "bootstrap.servers": os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
}

TOPIC = "inventory_topic"

EVENT_INTERVAL = 4


# --------------------------------------------------
# 2. LOAD PRODUCT MASTER DATA
# --------------------------------------------------

def load_products():

    print("Loading product master dataset...")

    if not PRODUCT_FILE.exists():

        raise FileNotFoundError(
            f"Product dataset not found: {PRODUCT_FILE}"
        )

    with open(PRODUCT_FILE, "r", encoding="utf-8") as file:

        products = json.load(file)

    if not products:

        raise ValueError("Product dataset is empty!")

    # Create product inventory dictionary
    inventory = {}

    for product in products:

        product_id = product["product_id"]

        stock_quantity = int(product["stock_quantity"])

        inventory[product_id] = stock_quantity

    print(f"Products loaded: {len(inventory)}")

    return inventory


INVENTORY = load_products()


# --------------------------------------------------
# 3. CREATE KAFKA PRODUCER
# --------------------------------------------------

producer = Producer(KAFKA_CONFIG)


# --------------------------------------------------
# 4. INVENTORY EVENT CONFIGURATION
# --------------------------------------------------

EVENT_TYPES = [
    "STOCK_IN",
    "STOCK_OUT",
    "STOCK_ADJUSTMENT"
]
delivery_errors = []


# --------------------------------------------------
# 5. DELIVERY CALLBACK
# --------------------------------------------------

def delivery_report(err, message):

    if err is not None:

        delivery_errors.append(str(err))

        print(f"Inventory delivery failed: {err}")

    else:

        print(
            f"Delivered | "
            f"Topic: {message.topic()} | "
            f"Partition: {message.partition()} | "
            f"Offset: {message.offset()}"
        )


# --------------------------------------------------
# 6. GENERATE INVENTORY EVENT
# --------------------------------------------------

def generate_inventory_event():

    product_id = random.choice(list(INVENTORY.keys()))

    current_stock = INVENTORY[product_id]

    event_type = random.choice(EVENT_TYPES)

    # Determine stock movement
    if event_type == "STOCK_IN":

        quantity_changed = random.randint(10, 100)

    elif event_type == "STOCK_OUT":

        quantity_changed = -random.randint(
            1,
            min(5, current_stock)
        ) if current_stock > 0 else 0

    else:

        quantity_changed = random.randint(-5, 5)

        # Prevent inventory from becoming negative
        if current_stock + quantity_changed < 0:

            quantity_changed = 0

    # Calculate updated stock
    updated_stock = current_stock + quantity_changed

    # Update in-memory inventory
    INVENTORY[product_id] = updated_stock

    # Create inventory event
    event = {

        "inventory_event_id": (
            "INV-" + uuid.uuid4().hex[:8].upper()
        ),

        "product_id": product_id,

        "event_type": event_type,

        "quantity_changed": quantity_changed,

        "previous_stock": current_stock,

        "available_stock": updated_stock,

        "event_timestamp": datetime.now(
            timezone.utc
        ).isoformat()

    }

    return event


# --------------------------------------------------
# 7. MAIN PRODUCER FUNCTION
# --------------------------------------------------

def main():

    print("\n" + "=" * 60)

    print("          INVENTORY GENERATOR STARTED")

    print("=" * 60)

    print(f"Kafka Topic: {TOPIC}")

    print(f"Products tracked: {len(INVENTORY)}")

    print(f"Generation interval: {EVENT_INTERVAL} seconds")

    print("Press CTRL + C to stop.\n")

    event_count = 0
    install_shutdown_handlers()
    deadline = duration_deadline()

    try:

        while should_continue(deadline):

            # Generate inventory event
            event = generate_inventory_event()

            # Convert dictionary into JSON
            event_json = json.dumps(event)

            # Publish event to Kafka
            producer.produce(

                topic=TOPIC,

                key=event["product_id"],

                value=event_json,

                callback=delivery_report

            )

            # Process delivery callbacks
            producer.poll(0)

            event_count += 1

            print(
                f"Event #{event_count} | "
                f"Product: {event['product_id']} | "
                f"Type: {event['event_type']} | "
                f"Change: {event['quantity_changed']} | "
                f"Previous Stock: {event['previous_stock']} | "
                f"Available Stock: {event['available_stock']}"
            )

            time.sleep(EVENT_INTERVAL)

    except KeyboardInterrupt:

        print("\nStopping inventory generator...")

    finally:

        remaining = producer.flush(timeout=10)

        if remaining == 0:

            print("All pending messages delivered.")

        else:

            print(f"Warning: {remaining} messages remain undelivered.")

        if remaining or delivery_errors:
            raise RuntimeError(
                f"Inventory delivery failed: pending={remaining}, "
                f"callback_errors={len(delivery_errors)}, "
                f"first_error={delivery_errors[0] if delivery_errors else 'none'}")

        print(f"Total events generated: {event_count}")

        print("Inventory generator stopped.")


# --------------------------------------------------
# 8. SCRIPT ENTRY POINT
# --------------------------------------------------

if __name__ == "__main__":

    main()
