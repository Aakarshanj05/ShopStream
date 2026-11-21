
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

TOPIC = "products_topic"

TOTAL_PRODUCTS = 1000

BASE_DIR = Path(__file__).resolve().parent.parent

DATA_DIR = Path(os.getenv("SHOPSTREAM_MASTER_DATA_DIR", str(BASE_DIR / "data")))

OUTPUT_FILE = DATA_DIR / "products.json"


# --------------------------------------------------
# 2. CREATE KAFKA PRODUCER
# --------------------------------------------------

producer = Producer(KAFKA_CONFIG)

delivered_count = 0
failed_count = 0


# --------------------------------------------------
# 3. PRODUCT MASTER DATA
# --------------------------------------------------

PRODUCT_CATALOG = {

    "Electronics": [
        "Wireless Headphones",
        "Smartphone",
        "Laptop",
        "Smart Watch",
        "Bluetooth Speaker",
        "Tablet",
        "Keyboard",
        "Gaming Mouse",
        "Power Bank",
        "Monitor"
    ],

    "Fashion": [
        "Men T-Shirt",
        "Women Jeans",
        "Running Shoes",
        "Formal Shirt",
        "Leather Jacket",
        "Casual Sneakers",
        "Sports Wear",
        "Handbag",
        "Sunglasses",
        "Wrist Watch"
    ],

    "Home & Kitchen": [
        "Mixer Grinder",
        "Electric Kettle",
        "Air Fryer",
        "Coffee Maker",
        "Vacuum Cleaner",
        "Water Bottle",
        "Cookware Set",
        "Table Lamp",
        "Bedsheet",
        "Storage Organizer"
    ],

    "Beauty": [
        "Face Wash",
        "Moisturizer",
        "Sunscreen",
        "Shampoo",
        "Conditioner",
        "Body Lotion",
        "Face Serum",
        "Perfume",
        "Hair Oil",
        "Lip Balm"
    ],

    "Books": [
        "Python Programming",
        "Data Engineering",
        "Machine Learning",
        "System Design",
        "Database Management",
        "Artificial Intelligence",
        "Business Analytics",
        "Cloud Computing",
        "Statistics",
        "Deep Learning"
    ]
}


BRANDS = {
    "Electronics": [
        "Samsung", "Sony", "Dell", "HP", "Lenovo",
        "Boat", "Xiaomi", "Apple"
    ],

    "Fashion": [
        "Nike", "Adidas", "Puma", "Levis",
        "Roadster", "H&M", "Zara"
    ],

    "Home & Kitchen": [
        "Philips", "Prestige", "Havells",
        "Bajaj", "Pigeon", "LG"
    ],

    "Beauty": [
        "Nivea", "Lakme", "Mamaearth",
        "Dove", "Loreal", "Minimalist"
    ],

    "Books": [
        "Penguin", "Pearson", "O'Reilly",
        "McGraw Hill", "Wiley"
    ]
}


PRICE_RANGES = {
    "Electronics": (500, 80000),
    "Fashion": (300, 12000),
    "Home & Kitchen": (200, 25000),
    "Beauty": (100, 5000),
    "Books": (150, 3000)
}


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
# 5. GENERATE ONE PRODUCT
# --------------------------------------------------

def generate_product(product_number):

    category = random.choice(list(PRODUCT_CATALOG.keys()))

    product_name = random.choice(
        PRODUCT_CATALOG[category]
    )

    brand = random.choice(
        BRANDS[category]
    )

    min_price, max_price = PRICE_RANGES[category]

    price = round(
        random.uniform(min_price, max_price),
        2
    )

    stock_quantity = random.randint(0, 500)

    created_at = (
        datetime.now(timezone.utc)
        - timedelta(days=random.randint(0, 730))
    )

    product = {

        "product_id": f"PROD{product_number:05d}",

        "product_name": product_name,

        "category": category,

        "brand": brand,

        "price": price,

        "stock_quantity": stock_quantity,

        "created_at": created_at.isoformat()

    }

    return product


# --------------------------------------------------
# 6. GENERATE PRODUCT MASTER DATASET
# --------------------------------------------------

def generate_products():

    products = []

    print("Generating product records...")

    for number in range(1, TOTAL_PRODUCTS + 1):

        product = generate_product(number)

        products.append(product)

    return products


# --------------------------------------------------
# 7. SAVE PRODUCTS LOCALLY
# --------------------------------------------------

def save_products(products):

    DATA_DIR.mkdir(
        parents=True,
        exist_ok=True
    )

    with open(
        OUTPUT_FILE,
        "w",
        encoding="utf-8"
    ) as file:

        json.dump(
            products,
            file,
            indent=4
        )

    print(f"Product dataset saved: {OUTPUT_FILE}")


# --------------------------------------------------
# 8. PUBLISH PRODUCTS TO KAFKA
# --------------------------------------------------

def publish_products(products):

    print("\nPublishing products to Kafka...")

    for index, product in enumerate(products, start=1):

        product_json = json.dumps(product)

        producer.produce(

            topic=TOPIC,

            key=product["product_id"],

            value=product_json,

            callback=delivery_report

        )

        producer.poll(0)

        if index % 100 == 0:

            print(f"Submitted {index} product records...")

    remaining = producer.flush(timeout=60)

    print("\nKafka publishing completed.")

    print(f"Successfully delivered: {delivered_count}")

    print(f"Failed deliveries: {failed_count}")

    print(f"Messages still pending: {remaining}")

    if failed_count or remaining:
        raise RuntimeError(
            f"Product Kafka publish incomplete: failed={failed_count}, pending={remaining}"
        )


# --------------------------------------------------
# 9. MAIN FUNCTION
# --------------------------------------------------

def main():

    start_time = time.time()

    print("=" * 55)

    print("       PRODUCT MASTER DATA GENERATOR")

    print("=" * 55)

    products = generate_products()

    print(f"Generated products: {len(products)}")

    save_products(products)

    publish_products(products)

    end_time = time.time()

    print("\nProduct generation completed.")

    print(f"Total products: {len(products)}")

    print(
        f"Total execution time: "
        f"{end_time - start_time:.2f} seconds"
    )


# --------------------------------------------------
# 10. SCRIPT ENTRY POINT
# --------------------------------------------------

if __name__ == "__main__":

    main()
