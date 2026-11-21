"""Read-only Silver v2 reconciliation, uniqueness and foreign-key checks."""
import os

from pyspark.sql import SparkSession, functions as F

TOPIC_MAP = {
    "customers": os.getenv("SHOPSTREAM_TOPIC_CUSTOMERS", "customers_topic"),
    "products": os.getenv("SHOPSTREAM_TOPIC_PRODUCTS", "products_topic"),
    "orders": os.getenv("SHOPSTREAM_TOPIC_ORDERS", "orders_topic"),
    "payments": os.getenv("SHOPSTREAM_TOPIC_PAYMENTS", "payments_topic"),
    "inventory": os.getenv("SHOPSTREAM_TOPIC_INVENTORY", "inventory_topic"),
    "website_activity": os.getenv(
        "SHOPSTREAM_TOPIC_WEBSITE_ACTIVITY", "website_activity_topic"),
}
TOPICS = {
    "customers": (TOPIC_MAP["customers"], "customer_id"),
    "products": (TOPIC_MAP["products"], "product_id"),
    "orders": (TOPIC_MAP["orders"], "order_id"),
    "payments": (TOPIC_MAP["payments"], "payment_id"),
    "inventory": (TOPIC_MAP["inventory"], "inventory_event_id"),
    "website_activity": (TOPIC_MAP["website_activity"], "event_id"),
}
ROOT = os.getenv("SHOPSTREAM_DATA_ROOT", "/opt/spark-data")
spark = (SparkSession.builder.appName("ShopStream-Silver-v2-Validation")
         .master("spark://spark-master:7077")
         .config("spark.sql.session.timeZone", "UTC").getOrCreate())
spark.sparkContext.setLogLevel("ERROR")
failures = []
try:
    bronze = spark.read.parquet(ROOT + "/bronze/events")
    bronze_counts = {r["topic"]: r["count"] for r in
                     bronze.groupBy("topic").count().collect()}
    frames = {}
    for entity, (topic, key) in TOPICS.items():
        source_count = bronze_counts.get(topic, 0)
        silver = spark.read.parquet(f"{ROOT}/silver_v2/{entity}")
        quarantine = spark.read.parquet(f"{ROOT}/silver_v2/quarantine/{entity}")
        silver_count, quarantine_count = silver.count(), quarantine.count()
        reconciles = source_count == silver_count + quarantine_count
        unique_key = silver_count == silver.select(key).dropDuplicates().count()
        nonnull_key = silver.filter(F.col(key).isNull()).limit(1).count() == 0
        print(f"RESULT {entity} bronze={source_count} silver={silver_count} "
              f"quarantine={quarantine_count} reconciles={reconciles} "
              f"unique_key={unique_key} nonnull_key={nonnull_key}")
        if not (reconciles and unique_key and nonnull_key):
            failures.append(entity)
        if quarantine_count:
            print(f"REASONS {entity}")
            quarantine.groupBy("rejection_reason").count().orderBy(
                "rejection_reason").show(100, False)
        frames[entity] = silver

    references = [
        ("orders", "customer_id", "customers", "customer_id"),
        ("orders", "product_id", "products", "product_id"),
        ("payments", "order_id", "orders", "order_id"),
        ("payments", "customer_id", "customers", "customer_id"),
        ("inventory", "product_id", "products", "product_id"),
        ("website_activity", "customer_id", "customers", "customer_id"),
        ("website_activity", "product_id", "products", "product_id"),
    ]
    for child, fk, parent, pk in references:
        missing = (frames[child].select(F.col(fk).alias("__foreign_key")).join(
            frames[parent].select(F.col(pk).alias("__primary_key")).dropDuplicates(),
            F.col("__foreign_key") == F.col("__primary_key"), "left_anti").count())
        print(f"REFERENCE {child}.{fk}->{parent}.{pk} unresolved={missing}")
        if missing:
            failures.append(f"{child}.{fk}")
    if failures:
        raise RuntimeError("Silver v2 quality checks failed: " + ", ".join(failures))
    print("SILVER_V2_VALIDATION_PASSED")
finally:
    spark.stop()


