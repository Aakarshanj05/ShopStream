
import json
import os
import random
import time

from datetime import datetime, timezone, timedelta
from pathlib import Path
from confluent_kafka import Producer


# --------------------------------------------------
# 1. CONFIGURATION
# --------------------------------------------------

KAFKA_CONFIG = {
    "bootstrap.servers": os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
}

TOPIC = "customers_topic"

TOTAL_CUSTOMERS = 10000

BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = Path(os.getenv("SHOPSTREAM_MASTER_DATA_DIR", str(BASE_DIR / "data")))

OUTPUT_FILE = DATA_DIR / "customers.json"


# --------------------------------------------------
# 2. CREATE KAFKA PRODUCER
# --------------------------------------------------

producer = Producer(KAFKA_CONFIG)

delivered_count = 0
failed_count = 0


# --------------------------------------------------
# 3. SAMPLE CUSTOMER DATA
# --------------------------------------------------

FIRST_NAMES = [
    "Rahul", "Priya", "Amit", "Neha", "Rohit",
    "Anjali", "Vikas", "Sneha", "Arjun", "Pooja",
    "Karan", "Riya", "Aditya", "Kavya", "Sahil"
]

LAST_NAMES = [
    "Sharma", "Verma", "Jha", "Singh", "Kumar",
    "Gupta", "Mishra", "Das", "Patel", "Roy"
]

CITIES = [
    "Patna", "Kolkata", "Chennai", "Mumbai",
    "Delhi", "Bangalore", "Hyderabad", "Pune",
    "Jaipur", "Lucknow"
]

MEMBERSHIPS = [
    "Basic", "Silver", "Gold", "Premium"
]


# --------------------------------------------------
# 4. DELIVERY CALLBACK
# --------------------------------------------------

def delivery_report(err, message):

    global delivered_count, failed_count

    if err is not None:
        failed_count += 1

    else:
        delivered_count += 1


# --------------------------------------------------
# 5. GENERATE ONE CUSTOMER
# --------------------------------------------------

def generate_customer(customer_number):

    first_name = random.choice(FIRST_NAMES)

    last_name = random.choice(LAST_NAMES)

    customer_id = f"CUST{customer_number:05d}"

    # Generate a registration date within the last 2 years
    registration_date = (
        datetime.now(timezone.utc)
        - timedelta(days=random.randint(0, 730))
    )

    customer = {

        "customer_id": customer_id,

        "first_name": first_name,

        "last_name": last_name,

        "email": (
            f"{first_name.lower()}."
            f"{last_name.lower()}."
            f"{customer_number}@example.com"
        ),

        "city": random.choice(CITIES),

        "membership": random.choice(MEMBERSHIPS),

        "created_at": registration_date.isoformat()

    }

    return customer


# --------------------------------------------------
# 6. GENERATE CUSTOMER MASTER DATASET
# --------------------------------------------------

def generate_customers():

    customers = []

    print("Generating customer records...")

    for number in range(1, TOTAL_CUSTOMERS + 1):

        customer = generate_customer(number)

        customers.append(customer)

    return customers


# --------------------------------------------------
# 7. SAVE CUSTOMERS TO JSON FILE
# --------------------------------------------------

def save_customers(customers):

    DATA_DIR.mkdir(parents=True, exist_ok=True)

    with open(OUTPUT_FILE, "w", encoding="utf-8") as file:

        json.dump(
            customers,
            file,
            indent=4
        )

    print(f"Customer dataset saved: {OUTPUT_FILE}")


# --------------------------------------------------
# 8. PUBLISH CUSTOMERS TO KAFKA
# --------------------------------------------------

def publish_customers(customers):

    print("\nPublishing customers to Kafka...")

    for index, customer in enumerate(customers, start=1):

        customer_json = json.dumps(customer)

        producer.produce(
            topic=TOPIC,
            key=customer["customer_id"],
            value=customer_json,
            callback=delivery_report
        )

        # Process delivery callbacks
        producer.poll(0)

        if index % 1000 == 0:

            print(f"Submitted {index} customer records...")

    # Wait for all pending messages
    remaining = producer.flush(timeout=60)

    print("\nKafka publishing completed.")

    print(f"Successfully delivered: {delivered_count}")

    print(f"Failed deliveries: {failed_count}")

    print(f"Messages still pending: {remaining}")

    if failed_count or remaining:
        raise RuntimeError(
            f"Customer Kafka publish incomplete: failed={failed_count}, pending={remaining}"
        )


# --------------------------------------------------
# 9. MAIN FUNCTION
# --------------------------------------------------

def main():

    start_time = time.time()

    print("=" * 55)
    print("       CUSTOMER MASTER DATA GENERATOR")
    print("=" * 55)

    customers = generate_customers()

    print(f"Generated customers: {len(customers)}")

    save_customers(customers)

    publish_customers(customers)

    end_time = time.time()

    print("\nCustomer generation process completed.")

    print(f"Total customers: {len(customers)}")

    print(f"Total execution time: {end_time - start_time:.2f} seconds")


# --------------------------------------------------
# 10. SCRIPT ENTRY POINT
# --------------------------------------------------

if __name__ == "__main__":

    main()
