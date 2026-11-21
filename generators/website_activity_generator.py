
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

TOPIC = "website_activity_topic"

# Number of seconds between customer sessions
SESSION_INTERVAL = 3

# Number of seconds between events within a session
EVENT_INTERVAL = 1
delivery_errors = []


# --------------------------------------------------
# 2. LOAD CUSTOMER AND PRODUCT DATA
# --------------------------------------------------

def load_json_file(file_path):

    if not file_path.exists():

        raise FileNotFoundError(
            f"Dataset not found: {file_path}"
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

    product_ids = [
        product["product_id"]
        for product in products
    ]

    customer_cities = {
        customer["customer_id"]: customer["city"]
        for customer in customers
    }

    print(f"Customers loaded: {len(customer_ids)}")

    print(f"Products loaded: {len(product_ids)}")

    return customer_ids, product_ids, customer_cities


CUSTOMER_IDS, PRODUCT_IDS, CUSTOMER_CITIES = load_master_data()


# --------------------------------------------------
# 3. CREATE KAFKA PRODUCER
# --------------------------------------------------

producer = Producer(KAFKA_CONFIG)


# --------------------------------------------------
# 4. WEBSITE CONFIGURATION
# --------------------------------------------------

DEVICES = [
    "MOBILE",
    "DESKTOP",
    "TABLET"
]

BROWSERS = [
    "CHROME",
    "EDGE",
    "FIREFOX",
    "SAFARI"
]


# --------------------------------------------------
# 5. DELIVERY CALLBACK
# --------------------------------------------------

def delivery_report(err, message):

    if err is not None:

        delivery_errors.append(str(err))

        print(f"Event delivery failed: {err}")

    else:

        print(
            f"Delivered | "
            f"Topic: {message.topic()} | "
            f"Partition: {message.partition()} | "
            f"Offset: {message.offset()}"
        )


# --------------------------------------------------
# 6. CREATE WEBSITE ACTIVITY EVENT
# --------------------------------------------------

def create_event(
    session_id,
    customer_id,
    product_id,
    event_type,
    device,
    browser,
    city,
    sequence_number
):

    event = {

        "event_id": "WEB-" + uuid.uuid4().hex[:8].upper(),

        "session_id": session_id,

        "customer_id": customer_id,

        "product_id": product_id,

        "event_type": event_type,

        "device": device,

        "browser": browser,

        "city": city,

        "sequence_number": sequence_number,

        "event_timestamp": datetime.now(
            timezone.utc
        ).isoformat()

    }

    return event


# --------------------------------------------------
# 7. PUBLISH EVENT TO KAFKA
# --------------------------------------------------

def publish_event(event):

    producer.produce(

        topic=TOPIC,

        key=event["session_id"],

        value=json.dumps(event),

        callback=delivery_report

    )

    # Process Kafka delivery callbacks
    producer.poll(0)


# --------------------------------------------------
# 8. GENERATE CUSTOMER SESSION
# --------------------------------------------------

def generate_customer_session():

    # Select a real customer
    customer_id = random.choice(CUSTOMER_IDS)

    # Select a real product
    product_id = random.choice(PRODUCT_IDS)

    # Generate unique session ID
    session_id = "SES-" + uuid.uuid4().hex[:8].upper()

    # Select customer device and browser
    device = random.choice(DEVICES)

    browser = random.choice(BROWSERS)

    # Get actual customer city
    city = CUSTOMER_CITIES[customer_id]

    # Track event sequence
    sequence_number = 0

    # Define a helper to create and publish events
    def send_event(event_type):

        nonlocal sequence_number

        sequence_number += 1

        event = create_event(

            session_id=session_id,

            customer_id=customer_id,

            product_id=product_id,

            event_type=event_type,

            device=device,

            browser=browser,

            city=city,

            sequence_number=sequence_number

        )

        publish_event(event)

        print(
            f"Session: {session_id} | "
            f"Event #{sequence_number}: {event_type} | "
            f"Customer: {customer_id} | "
            f"Product: {product_id}"
        )

        return event

    print("\nStarting new customer session...")

    print(
        f"Customer: {customer_id} | "
        f"City: {city} | "
        f"Device: {device} | "
        f"Browser: {browser}"
    )

    # Event 1: Customer visits website
    send_event("PAGE_VIEW")

    time.sleep(EVENT_INTERVAL)

    # Event 2: Customer views product
    send_event("PRODUCT_VIEW")

    time.sleep(EVENT_INTERVAL)

    # Event 3: 35% probability of adding product to cart
    if random.random() > 0.35:

        print("Customer left without adding product to cart.")

        return

    send_event("ADD_TO_CART")

    time.sleep(EVENT_INTERVAL)

    # Event 4: 60% probability of starting checkout
    if random.random() > 0.60:

        print("Customer abandoned the cart.")

        return

    send_event("CHECKOUT_STARTED")

    time.sleep(EVENT_INTERVAL)

    # Event 5: 75% probability of completing purchase
    if random.random() > 0.75:

        print("Customer abandoned during checkout.")

        return

    send_event("PURCHASE")

    print(f"Customer session completed: {session_id}")


# --------------------------------------------------
# 9. MAIN FUNCTION
# --------------------------------------------------

def main():

    print("\n" + "=" * 65)

    print("          SHOPSTREAM WEBSITE ACTIVITY GENERATOR")

    print("=" * 65)

    print(f"Kafka Topic: {TOPIC}")

    print(f"Available Customers: {len(CUSTOMER_IDS)}")

    print(f"Available Products: {len(PRODUCT_IDS)}")

    print(f"Session Interval: {SESSION_INTERVAL} seconds")

    print("Press CTRL + C to stop.\n")

    session_count = 0
    install_shutdown_handlers()
    deadline = duration_deadline()

    try:

        while should_continue(deadline):

            generate_customer_session()

            session_count += 1

            # Wait before starting another session
            print(
                f"\nTotal sessions generated: {session_count}"
            )

            time.sleep(SESSION_INTERVAL)

    except KeyboardInterrupt:

        print("\nStopping website activity generator...")

    finally:

        # Ensure all pending Kafka messages are delivered
        remaining = producer.flush(timeout=10)

        if remaining == 0:

            print("All pending events delivered.")

        else:

            print(
                f"Warning: {remaining} events remain undelivered."
            )

        if remaining or delivery_errors:
            raise RuntimeError(
                f"Website delivery failed: pending={remaining}, "
                f"callback_errors={len(delivery_errors)}, "
                f"first_error={delivery_errors[0] if delivery_errors else 'none'}")

        print(f"Total sessions generated: {session_count}")

        print("Website activity generator stopped.")


# --------------------------------------------------
# 10. SCRIPT ENTRY POINT
# --------------------------------------------------

if __name__ == "__main__":

    main()
