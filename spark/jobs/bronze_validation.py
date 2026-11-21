
import builtins
import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    count,
    min,
    max,
    sum,
    when,
    length,
    trim
)


# =====================================================
# CONFIGURATION
# =====================================================

BRONZE_PATH = os.path.join(
    os.getenv("SHOPSTREAM_DATA_ROOT", "/opt/spark-data"), "bronze", "events")
LANDING_PATH = os.getenv("SHOPSTREAM_BRONZE_LANDING_PATH")
QUARANTINE_PATH = os.getenv("SHOPSTREAM_BRONZE_QUARANTINE_PATH")
IDENTITY_COLUMNS = ["topic", "partition", "offset"]
TOPIC_MAP = {
    "customers": os.getenv("SHOPSTREAM_TOPIC_CUSTOMERS", "customers_topic"),
    "products": os.getenv("SHOPSTREAM_TOPIC_PRODUCTS", "products_topic"),
    "orders": os.getenv("SHOPSTREAM_TOPIC_ORDERS", "orders_topic"),
    "payments": os.getenv("SHOPSTREAM_TOPIC_PAYMENTS", "payments_topic"),
    "inventory": os.getenv("SHOPSTREAM_TOPIC_INVENTORY", "inventory_topic"),
    "website_activity": os.getenv(
        "SHOPSTREAM_TOPIC_WEBSITE_ACTIVITY", "website_activity_topic"),
}
APPROVED_EMPTY_PAYLOAD_IDENTITY = (
    os.getenv("SHOPSTREAM_APPROVED_EMPTY_TOPIC", TOPIC_MAP["orders"]),
    int(os.getenv("SHOPSTREAM_APPROVED_EMPTY_PARTITION", "0")),
    int(os.getenv("SHOPSTREAM_APPROVED_EMPTY_OFFSET", "1")),
)
EXPECTED_TOPICS = set(TOPIC_MAP.values())




def validate_landing_accounting(spark, df, total_records):
    """Reconcile raw landing with valid Bronze and the quarantine output."""
    landing = spark.read.parquet(LANDING_PATH)
    quarantine = spark.read.parquet(QUARANTINE_PATH)
    landing_count = landing.count()
    quarantine_count = quarantine.count()
    print("\nLanding / quarantine reconciliation:")
    print("Raw Kafka landing records:", landing_count)
    print("Valid Bronze event records:", total_records)
    print("Quarantined malformed records:", quarantine_count)

    def duplicate_identity_count(frame):
        return (frame.groupBy(*IDENTITY_COLUMNS).count()
                .filter(col("count") != 1).limit(1).count())

    raw_ids = landing.select(*IDENTITY_COLUMNS)
    valid_ids = df.select(*IDENTITY_COLUMNS)
    quarantine_ids = quarantine.select(*IDENTITY_COLUMNS)
    output_ids = valid_ids.unionByName(quarantine_ids)

    if duplicate_identity_count(landing):
        raise RuntimeError("Bronze accounting failed: duplicate landing identities")
    if duplicate_identity_count(df):
        raise RuntimeError("Bronze accounting failed: duplicate valid Bronze identities")
    if duplicate_identity_count(quarantine):
        raise RuntimeError("Bronze accounting failed: duplicate quarantine identities")
    if landing_count != total_records + quarantine_count:
        raise RuntimeError(
            "Bronze accounting failed: landing rows do not equal valid plus "
            f"quarantine ({landing_count} != {total_records} + {quarantine_count})")

    overlap = valid_ids.join(quarantine_ids, IDENTITY_COLUMNS, "inner").limit(1).count()
    missing = raw_ids.join(output_ids, IDENTITY_COLUMNS, "left_anti").limit(1).count()
    extra = output_ids.join(raw_ids, IDENTITY_COLUMNS, "left_anti").limit(1).count()
    if overlap or missing or extra:
        raise RuntimeError(
            "Bronze accounting failed: valid and quarantined identities must "
            f"partition landing (overlap={overlap}, missing={missing}, extra={extra})")

    approved = (
        (col("topic") == APPROVED_EMPTY_PAYLOAD_IDENTITY[0])
        & (col("partition") == APPROVED_EMPTY_PAYLOAD_IDENTITY[1])
        & (col("offset") == APPROVED_EMPTY_PAYLOAD_IDENTITY[2]))
    raw_blank_or_null = (
        col("event_data").isNull()
        | (length(trim(col("event_data"))) == 0))
    unexpected_source_invalid = (landing.filter(raw_blank_or_null & ~approved)
                                 .select(*IDENTITY_COLUMNS).limit(10).collect())
    if unexpected_source_invalid:
        identities = [row.asDict() for row in unexpected_source_invalid]
        raise RuntimeError(
            "Bronze validation failed: unapproved malformed source records "
            f"remain in raw landing and require review: {identities}")

    approved_source = landing.filter(approved).limit(2).collect()
    if len(approved_source) != 1:
        raise RuntimeError(
            "Bronze quarantine policy expected exactly one source record at "
            f"{APPROVED_EMPTY_PAYLOAD_IDENTITY}")
    approved_row = approved_source[0]
    raw_payload = approved_row["raw_payload"]
    if (approved_row["event_data"] is not None
            and approved_row["event_data"].strip()):
        raise RuntimeError(
            "Bronze quarantine policy source at "
            f"{APPROVED_EMPTY_PAYLOAD_IDENTITY} is no longer blank")
    if raw_payload is None or len(raw_payload) != 0:
        raise RuntimeError(
            "Bronze quarantine policy source at "
            f"{APPROVED_EMPTY_PAYLOAD_IDENTITY} must retain "
            "its original zero-length Kafka value")
    if approved_row["raw_key"] is not None or approved_row["kafka_key"] is not None:
        raise RuntimeError(
            "Bronze quarantine policy source at "
            f"{APPROVED_EMPTY_PAYLOAD_IDENTITY} must retain "
            "its null Kafka key")
    if (approved_row["kafka_timestamp"] is None
            or approved_row["ingestion_timestamp"] is None):
        raise RuntimeError(
            "Bronze quarantine policy source at orders_topic/0/1 lost a timestamp")

    unexpected_quarantine = quarantine.filter(~approved).limit(1).count()
    if unexpected_quarantine or quarantine_count != len(approved_source):
        raise RuntimeError(
            "Bronze quarantine policy permits only orders_topic/0/1; "
            f"found {quarantine_count} quarantined records")

    quarantine_metadata_nulls = quarantine.filter(
        col("topic").isNull()
        | col("partition").isNull()
        | col("offset").isNull()
        | col("raw_payload").isNull()
        | col("kafka_timestamp").isNull()
        | col("ingestion_timestamp").isNull()
        | col("quarantine_reason").isNull()
        | (col("quarantine_reason") != "APPROVED_EMPTY_PAYLOAD")
    ).limit(1).count()
    if quarantine_metadata_nulls:
        raise RuntimeError(
            "Bronze quarantine validation failed: required raw payload, identity, "
            "reason or timestamp is null")

    source_quarantine = landing.alias("source").join(
        quarantine.alias("quarantine"), IDENTITY_COLUMNS, "inner")
    quarantine_mismatch = source_quarantine.filter(
        ~col("source.raw_key").eqNullSafe(col("quarantine.raw_key"))
        | ~col("source.kafka_key").eqNullSafe(col("quarantine.kafka_key"))
        | ~col("source.raw_payload").eqNullSafe(col("quarantine.raw_payload"))
        | ~col("source.event_data").eqNullSafe(col("quarantine.event_data"))
        | ~col("source.kafka_timestamp").eqNullSafe(col("quarantine.kafka_timestamp"))
        | ~col("source.ingestion_timestamp").eqNullSafe(
            col("quarantine.ingestion_timestamp"))
    ).limit(1).count()
    if quarantine_mismatch:
        raise RuntimeError(
            "Bronze quarantine validation failed: source payload, key or "
            "timestamps differ from the raw landing record")

    print("Landing identity reconciliation: PASS")
    print("Quarantine allowlist and raw payload preservation: PASS")

# =====================================================
# SPARK SESSION
# =====================================================

spark = (
    SparkSession.builder
    .appName("ShopStream-Bronze-Validation")
    .master("spark://spark-master:7077")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


try:

    print("\n========== BRONZE VALIDATION ==========\n")

    # Read Bronze Parquet files

    df = spark.read.parquet(BRONZE_PATH)

    print("Bronze Schema:")
    df.printSchema()

    # Total records

    total_records = df.count()

    if total_records == 0:
        raise RuntimeError("Bronze validation failed: no records were ingested")

    print("\nTotal Bronze Records:", total_records)

    # Records by topic

    print("\nRecords Per Kafka Topic:")

    topic_counts = df.groupBy("topic").agg(
        count("*").alias("total_records"),
        min("kafka_timestamp").alias("first_event"),
        max("kafka_timestamp").alias("last_event")
    ).orderBy("topic")
    topic_counts.show(truncate=False)
    actual_topics = {row["topic"] for row in topic_counts.collect()}
    missing_topics = EXPECTED_TOPICS - actual_topics
    if missing_topics:
        raise RuntimeError(f"Bronze validation failed; missing topics: {sorted(missing_topics)}")

    # Blank or null messages

    print("\nBlank / Null Message Validation:")

    blank_rows = df.select(
        sum(
            when(
                col("event_data").isNull()
                | (length(trim(col("event_data"))) == 0),
                1
            ).otherwise(0)
        ).alias("blank_or_null_messages")
    ).first()[0] or 0
    print("Blank / null event_data count:", blank_rows)
    if blank_rows:
        raise RuntimeError(f"Bronze validation failed: {blank_rows} blank/null event payloads")

    # Kafka metadata validation

    print("\nKafka Metadata Validation:")

    metadata_nulls = df.select(
        sum(when(col("topic").isNull(), 1).otherwise(0))
        .alias("null_topic"),

        sum(when(col("partition").isNull(), 1).otherwise(0))
        .alias("null_partition"),

        sum(when(col("offset").isNull(), 1).otherwise(0))
        .alias("null_offset"),

        sum(when(col("kafka_timestamp").isNull(), 1).otherwise(0))
        .alias("null_kafka_timestamp"),

        sum(when(col("ingestion_timestamp").isNull(), 1).otherwise(0))
        .alias("null_ingestion_timestamp")
    ).first()
    metadata_null_count = builtins.sum(
        metadata_nulls[name] or 0 for name in
        ("null_topic", "null_partition", "null_offset", "null_kafka_timestamp",
         "null_ingestion_timestamp"))
    print("Kafka metadata null count:", metadata_null_count)
    if metadata_null_count:
        raise RuntimeError(
            f"Bronze validation failed: {metadata_null_count} required metadata values are null")

    # Raw identity accounting applies when ingestion materialized landing and
    # quarantine outputs. Direct Bronze mode has neither, so the strict checks
    # above remain applicable while this integration-only reconciliation skips.
    if bool(LANDING_PATH) != bool(QUARANTINE_PATH):
        raise RuntimeError(
            "Bronze accounting requires both "
            "SHOPSTREAM_BRONZE_LANDING_PATH and "
            "SHOPSTREAM_BRONZE_QUARANTINE_PATH")
    if LANDING_PATH and QUARANTINE_PATH:
        validate_landing_accounting(spark, df, total_records)
    else:
        print(
            "Landing/quarantine identity accounting skipped: "
            "direct Bronze mode has no raw landing or quarantine outputs.")

    print("\nBronze validation completed!")

finally:

    spark.stop()
