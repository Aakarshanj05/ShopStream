
import logging
import json
import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import col, current_timestamp


# ============================================================
# CONFIGURATION
# ============================================================

APP_NAME = "ShopStream-Bronze-Ingestion"

KAFKA_BOOTSTRAP_SERVERS = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS", "kafka:19092")

TOPIC_MAP = {
    "customers": os.getenv("SHOPSTREAM_TOPIC_CUSTOMERS", "customers_topic"),
    "products": os.getenv("SHOPSTREAM_TOPIC_PRODUCTS", "products_topic"),
    "orders": os.getenv("SHOPSTREAM_TOPIC_ORDERS", "orders_topic"),
    "payments": os.getenv("SHOPSTREAM_TOPIC_PAYMENTS", "payments_topic"),
    "inventory": os.getenv("SHOPSTREAM_TOPIC_INVENTORY", "inventory_topic"),
    "website_activity": os.getenv(
        "SHOPSTREAM_TOPIC_WEBSITE_ACTIVITY", "website_activity_topic"),
}
KAFKA_TOPICS = ",".join(TOPIC_MAP.values())

DATA_ROOT = os.getenv("SHOPSTREAM_DATA_ROOT", "/opt/spark-data")
BRONZE_OUTPUT_PATH = os.getenv(
    "SHOPSTREAM_BRONZE_OUTPUT_PATH",
    os.path.join(DATA_ROOT, "bronze", "events"))
LANDING_PATH = os.getenv("SHOPSTREAM_BRONZE_LANDING_PATH")
QUARANTINE_PATH = os.getenv("SHOPSTREAM_BRONZE_QUARANTINE_PATH")

CHECKPOINT_PATH = os.getenv(
    "SHOPSTREAM_BRONZE_CHECKPOINT_PATH",
    "/opt/spark-checkpoints/bronze_ingestion")
CHECKPOINT_GUARD_PATH = os.getenv("SHOPSTREAM_BRONZE_CHECKPOINT_GUARD_PATH")

MAX_OFFSETS_PER_TRIGGER = 1000
STARTING_OFFSETS_FILE = os.getenv("SHOPSTREAM_BRONZE_STARTING_OFFSETS_FILE")

APPROVED_EMPTY_PAYLOAD_IDENTITY = (
    os.getenv("SHOPSTREAM_APPROVED_EMPTY_TOPIC", TOPIC_MAP["orders"]),
    int(os.getenv("SHOPSTREAM_APPROVED_EMPTY_PARTITION", "0")),
    int(os.getenv("SHOPSTREAM_APPROVED_EMPTY_OFFSET", "1")),
)


# ============================================================
# PATH VALIDATION
# ============================================================

def _validate_integration_paths():

    if bool(LANDING_PATH) != bool(QUARANTINE_PATH):
        raise ValueError(
            "Integration Bronze landing and quarantine paths must be set together")

    if not LANDING_PATH:
        custom_topics = any(
            os.getenv(f"SHOPSTREAM_TOPIC_{entity.upper()}")
            for entity in TOPIC_MAP
        )
        if custom_topics:
            raise ValueError(
                "Custom Kafka topic mapping is allowed only in isolated landing mode")

        return

    if len(set(TOPIC_MAP.values())) != len(TOPIC_MAP):
        raise ValueError("Integration Kafka topics must be unique")

    if (
        APPROVED_EMPTY_PAYLOAD_IDENTITY[0] != TOPIC_MAP["orders"]
        or APPROVED_EMPTY_PAYLOAD_IDENTITY[1] != 0
        or APPROVED_EMPTY_PAYLOAD_IDENTITY[2] < 0
    ):
        raise ValueError(
            "The isolated malformed-record allowlist must target the orders "
            "topic on partition 0 with a nonnegative offset")

    paths = {
        "landing": LANDING_PATH,
        "events": BRONZE_OUTPUT_PATH,
        "quarantine": QUARANTINE_PATH,
        "checkpoint": CHECKPOINT_PATH,
    }

    normalized = {
        name: os.path.abspath(path)
        for name, path in paths.items()
    }

    for index, (left_name, left_path) in enumerate(normalized.items()):

        for right_name, right_path in list(normalized.items())[index + 1:]:

            if (
                left_path == right_path
                or left_path.startswith(right_path + os.sep)
                or right_path.startswith(left_path + os.sep)
            ):
                raise ValueError(
                    f"Integration Bronze path collision: {left_name}={left_path} "
                    f"overlaps {right_name}={right_path}")


def _starting_offsets_option():
    """Use an immutable Kafka high-watermark snapshot only for a fresh checkpoint."""
    if not STARTING_OFFSETS_FILE:
        return "earliest"

    with open(STARTING_OFFSETS_FILE, "r", encoding="utf-8") as stream:
        offsets = json.load(stream)

    for topic in TOPIC_MAP.values():
        partitions = offsets.get(topic)
        if not isinstance(partitions, dict) or not partitions:
            raise ValueError(
                f"Initial Kafka offset snapshot is missing topic {topic}: "
                f"{STARTING_OFFSETS_FILE}")
        for partition, offset in partitions.items():
            if not str(partition).isdigit() or not isinstance(offset, int) or offset < 0:
                raise ValueError(
                    f"Invalid Kafka starting offset {topic}/{partition}={offset!r}")
    return json.dumps(offsets, sort_keys=True)


def _checkpoint_has_state():
    if not os.path.exists(CHECKPOINT_PATH):
        return False
    if not os.path.isdir(CHECKPOINT_PATH):
        raise RuntimeError(
            f"Bronze checkpoint path is not a directory: {CHECKPOINT_PATH}")
    with os.scandir(CHECKPOINT_PATH) as entries:
        return next(entries, None) is not None


def _guard_persistent_checkpoint():
    """Fail closed if the daily checkpoint was initialized and disappeared."""
    if not CHECKPOINT_GUARD_PATH:
        return

    marker_exists = os.path.isfile(CHECKPOINT_GUARD_PATH)
    checkpoint_has_state = _checkpoint_has_state()
    if marker_exists and not checkpoint_has_state:
        raise RuntimeError(
            "The persistent Bronze checkpoint was previously initialized but "
            f"is now missing or empty: {CHECKPOINT_PATH}. Refusing to recreate "
            "it from initial offsets; investigate the checkpoint before recovery.")

    if marker_exists:
        return

    # This marker records that checkpoint initialization is being attempted.
    # Keeping it outside the checkpoint detects later loss, even after a task crash.
    marker_parent = os.path.dirname(CHECKPOINT_GUARD_PATH)
    if marker_parent:
        os.makedirs(marker_parent, exist_ok=True)
    try:
        fd = os.open(
            CHECKPOINT_GUARD_PATH,
            os.O_CREAT | os.O_EXCL | os.O_WRONLY,
            0o664,
        )
    except FileExistsError:
        if not _checkpoint_has_state():
            raise RuntimeError(
                "Bronze checkpoint guard marker exists but checkpoint has no "
                f"state: {CHECKPOINT_PATH}")
        return
    with os.fdopen(fd, "w", encoding="utf-8") as marker:
        marker.write(
            f"checkpoint_path={CHECKPOINT_PATH}\n"
            "state=initialization_started\n"
        )
        marker.flush()
        os.fsync(marker.fileno())


# ============================================================
# INTEGRATION BRONZE MATERIALIZATION
# ============================================================

def _materialize_integration_bronze(spark):
    """Rebuild valid Bronze and the allowlisted quarantine from raw landing."""

    landing = spark.read.parquet(LANDING_PATH)

    blank_or_null = F.coalesce(
        col("event_data").isNull()
        | (F.length(F.trim(col("event_data"))) == 0),
        F.lit(True)
    )

    approved_identity = (
        (col("topic") == APPROVED_EMPTY_PAYLOAD_IDENTITY[0])
        & (col("partition") == APPROVED_EMPTY_PAYLOAD_IDENTITY[1])
        & (col("offset") == APPROVED_EMPTY_PAYLOAD_IDENTITY[2])
    )

    # Avoid collect() and Python-JVM socket data transfer.
    # Count unexpected records directly inside Spark.
    unexpected_count = (
        landing
        .filter(blank_or_null & ~approved_identity)
        .count()
    )

    if unexpected_count:
        logger.error(
            "Unexpected blank/null Kafka payloads remain in raw landing. "
            "They will not enter valid Bronze or quarantine pending review. "
            "Unexpected record count: %s",
            unexpected_count
        )

    valid = landing.filter(~blank_or_null).select(
        "kafka_key",
        "event_data",
        "topic",
        "partition",
        "offset",
        "kafka_timestamp",
        "ingestion_timestamp"
    )

    quarantine = (
        landing
        .filter(blank_or_null & approved_identity)
        .select(
            "raw_key",
            "kafka_key",
            "raw_payload",
            "event_data",
            "topic",
            "partition",
            "offset",
            "kafka_timestamp",
            "ingestion_timestamp"
        )
        .withColumn(
            "quarantine_reason",
            F.lit("APPROVED_EMPTY_PAYLOAD")
        )
    )

    # Landing is append-only under its streaming checkpoint.
    # These deterministic full snapshots make retries safe if a
    # failure occurs between the writes.

    valid.write \
        .mode("overwrite") \
        .partitionBy("topic") \
        .parquet(BRONZE_OUTPUT_PATH)

    quarantine.write \
        .mode("overwrite") \
        .partitionBy("topic") \
        .parquet(QUARANTINE_PATH)

    logger.info(
        "Integration Bronze materialized from %s to valid=%s quarantine=%s",
        LANDING_PATH,
        BRONZE_OUTPUT_PATH,
        QUARANTINE_PATH
    )


# ============================================================
# LOGGING
# ============================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)

logger = logging.getLogger(APP_NAME)


# ============================================================
# MAIN FUNCTION
# ============================================================

def main():

    spark = None

    try:

        logger.info("Starting ShopStream Bronze ingestion")

        _validate_integration_paths()
        _guard_persistent_checkpoint()

        # 1. CREATE SPARK SESSION

        spark = (
            SparkSession.builder
            .appName(APP_NAME)
            .master("spark://spark-master:7077")
            .config("spark.sql.session.timeZone", "UTC")
            .getOrCreate()
        )

        spark.sparkContext.setLogLevel("WARN")

        logger.info("Spark session created successfully")

        # 2. READ DATA FROM KAFKA

        logger.info("Connecting to Kafka topics: %s", KAFKA_TOPICS)

        starting_offsets = _starting_offsets_option()
        logger.info(
            "Kafka starting offsets: %s",
            "immutable snapshot " + STARTING_OFFSETS_FILE
            if STARTING_OFFSETS_FILE else "earliest (existing default)")

        kafka_df = (
            spark.readStream
            .format("kafka")
            .option(
                "kafka.bootstrap.servers",
                KAFKA_BOOTSTRAP_SERVERS
            )
            .option("subscribe", KAFKA_TOPICS)
            .option("startingOffsets", starting_offsets)
            .option(
                "maxOffsetsPerTrigger",
                MAX_OFFSETS_PER_TRIGGER
            )
            .load()
        )

        # 3. EXTRACT RAW DATA AND KAFKA METADATA

        if LANDING_PATH:

            # Integration mode retains Kafka bytes before any validation
            # or routing. Parquet's binary type preserves exact payload/key.

            landing_df = kafka_df.select(
                col("key").alias("raw_key"),
                col("key").cast("string").alias("kafka_key"),
                col("value").alias("raw_payload"),
                col("value").cast("string").alias("event_data"),
                col("topic"),
                col("partition"),
                col("offset"),
                col("timestamp").alias("kafka_timestamp"),
                current_timestamp().alias("ingestion_timestamp")
            )

            stream_path = LANDING_PATH

        else:

            # Preserve original default/baseline schema and behavior.

            landing_df = kafka_df.select(
                col("key").cast("string").alias("kafka_key"),
                col("value").cast("string").alias("event_data"),
                col("topic"),
                col("partition"),
                col("offset"),
                col("timestamp").alias("kafka_timestamp"),
                current_timestamp().alias("ingestion_timestamp")
            )

            stream_path = BRONZE_OUTPUT_PATH

        logger.info(
            "Bronze DataFrame created; streaming to %s",
            stream_path
        )

        # 4. STREAM KAFKA DATA TO PARQUET

        # A single availableNow stream and checkpoint consume every Kafka row.

        query = (
            landing_df.writeStream
            .format("parquet")
            .outputMode("append")
            .option("path", stream_path)
            .option("checkpointLocation", CHECKPOINT_PATH)
            .partitionBy("topic")
            .trigger(availableNow=True)
            .start()
        )

        query.awaitTermination()

        # 5. MATERIALIZE INTEGRATION BRONZE

        if LANDING_PATH:
            _materialize_integration_bronze(spark)

        logger.info("Bronze ingestion completed successfully")
        logger.info("Bronze output: %s", BRONZE_OUTPUT_PATH)

    except Exception:

        logger.exception("Bronze ingestion failed")

        raise

    finally:

        if spark is not None:

            spark.stop()

            logger.info("Spark session stopped")


# ============================================================
# ENTRY POINT
# ============================================================

if __name__ == "__main__":
    main()
