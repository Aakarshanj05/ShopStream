
import logging

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    lit,
    trim,
    lower,
    initcap,
    to_timestamp,
    from_json,
    when,
    concat_ws,
    current_timestamp,
    row_number
)
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType
)
from pyspark.sql.window import Window


# ============================================================
# CONFIGURATION
# ============================================================

APP_NAME = "ShopStream-Customer-Silver"

BRONZE_CUSTOMER_PATH = (
    "/opt/spark-data/bronze/events/topic=customers_topic"
)

SILVER_CUSTOMER_PATH = (
    "/opt/spark-data/silver/customers"
)

QUARANTINE_PATH = (
    "/opt/spark-data/silver/quarantine/customers"
)

VALID_MEMBERSHIPS = [
    "Basic",
    "Silver",
    "Gold",
    "Premium"
]


# ============================================================
# LOGGING
# ============================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)

logger = logging.getLogger(APP_NAME)


# ============================================================
# CUSTOMER JSON SCHEMA
# ============================================================

CUSTOMER_SCHEMA = StructType([
    StructField("customer_id", StringType(), True),
    StructField("first_name", StringType(), True),
    StructField("last_name", StringType(), True),
    StructField("email", StringType(), True),
    StructField("city", StringType(), True),
    StructField("membership", StringType(), True),
    StructField("created_at", StringType(), True)
])


# ============================================================
# MAIN FUNCTION
# ============================================================

def main():

    spark = None

    try:

        logger.info("Starting Customer Silver pipeline")

        # ----------------------------------------------------
        # 1. CREATE SPARK SESSION
        # ----------------------------------------------------

        spark = (
            SparkSession.builder
            .appName(APP_NAME)
            .master("spark://spark-master:7077")
            .config("spark.sql.session.timeZone", "UTC")
            .getOrCreate()
        )

        spark.sparkContext.setLogLevel("WARN")

        logger.info("Spark session created")

        # ----------------------------------------------------
        # 2. READ BRONZE DATA
        # ----------------------------------------------------

        bronze_df = spark.read.parquet(
            BRONZE_CUSTOMER_PATH
        )

        logger.info(
            "Bronze records: %s",
            bronze_df.count()
        )

        # ----------------------------------------------------
        # 3. PARSE RAW JSON USING EXPLICIT SCHEMA
        # ----------------------------------------------------

        parsed_df = bronze_df.withColumn(
            "customer",
            from_json(
                col("event_data"),
                CUSTOMER_SCHEMA
            )
        )

        # ----------------------------------------------------
        # 4. EXTRACT AND STANDARDIZE FIELDS
        # ----------------------------------------------------

        customer_df = parsed_df.select(

            col("customer.customer_id").alias("customer_id"),

            col("customer.first_name").alias("first_name"),

            col("customer.last_name").alias("last_name"),

            col("customer.email").alias("email"),

            col("customer.city").alias("city"),

            col("customer.membership").alias("membership"),

            col("customer.created_at").alias("created_at_raw"),

            col("customer").isNull().alias("malformed_json"),

            col("event_data"),

            col("kafka_key"),

            col("partition").alias("kafka_partition"),

            col("offset").alias("kafka_offset"),

            col("kafka_timestamp"),

            col("ingestion_timestamp")

        )

        customer_df = (
            customer_df

            .withColumn(
                "customer_id",
                trim(col("customer_id"))
            )

            .withColumn(
                "first_name",
                trim(col("first_name"))
            )

            .withColumn(
                "last_name",
                trim(col("last_name"))
            )

            .withColumn(
                "email",
                lower(trim(col("email")))
            )

            .withColumn(
                "city",
                trim(col("city"))
            )

            .withColumn(
                "membership",
                initcap(trim(col("membership")))
            )

            .withColumn(
                "created_at",
                to_timestamp(trim(col("created_at_raw")))
            )
        )

        # ----------------------------------------------------
        # 5. VALIDATE CUSTOMER RECORDS
        # ----------------------------------------------------

        logger.info("Applying customer validation rules")

        validation_reasons = [

            when(
                col("malformed_json"),
                lit("MALFORMED_JSON")
            ),

            when(
                col("customer_id").isNull()
                | (col("customer_id") == ""),
                lit("MISSING_CUSTOMER_ID")
            ),

            when(
                col("first_name").isNull()
                | (col("first_name") == ""),
                lit("MISSING_FIRST_NAME")
            ),

            when(
                col("last_name").isNull()
                | (col("last_name") == ""),
                lit("MISSING_LAST_NAME")
            ),

            when(
                col("email").isNull()
                | (~col("email").rlike(
                    r"^[a-z0-9._%+-]+@[a-z0-9.-]+\.[a-z]{2,}$"
                )),
                lit("INVALID_EMAIL")
            ),

            when(
                col("city").isNull()
                | (col("city") == ""),
                lit("MISSING_CITY")
            ),

            when(
                col("membership").isNull()
                | (~col("membership").isin(
                    *VALID_MEMBERSHIPS
                )),
                lit("INVALID_MEMBERSHIP")
            ),

            when(
                col("created_at").isNull(),
                lit("INVALID_CREATED_AT")
            )
        ]

        customer_df = customer_df.withColumn(
            "validation_reason",
            concat_ws("|", *validation_reasons)
        )

        # ----------------------------------------------------
        # 6. DEDUPLICATE CUSTOMER IDS
        # ----------------------------------------------------

        logger.info(
            "Applying latest-record duplicate strategy"
        )

        dedup_window = (
            Window
            .partitionBy("customer_id")
            .orderBy(
                col("created_at").desc_nulls_last(),

                col("kafka_timestamp").desc_nulls_last(),

                col("kafka_partition").desc_nulls_last(),

                col("kafka_offset").desc_nulls_last()
            )
        )

        customer_df = customer_df.withColumn(
            "dedup_rank",
            when(
                col("customer_id").isNotNull()
                & (col("customer_id") != ""),
                row_number().over(dedup_window)
            ).otherwise(lit(1))
        )

        # ----------------------------------------------------
        # 7. CLASSIFY VALID AND REJECTED RECORDS
        # ----------------------------------------------------

        classified_df = customer_df.withColumn(
            "rejection_reason",

            when(
                col("validation_reason") != "",
                col("validation_reason")
            )

            .when(
                col("dedup_rank") > 1,
                lit("DUPLICATE_SUPERSEDED")
            )

            .otherwise(lit(None).cast("string"))
        )

        # ----------------------------------------------------
        # 8. CREATE SILVER DATAFRAME
        # ----------------------------------------------------

        silver_df = classified_df.filter(
            col("rejection_reason").isNull()
        ).select(

            "customer_id",
            "first_name",
            "last_name",
            "email",
            "city",
            "membership",
            "created_at",

            lit("customers_topic").alias("source_topic"),

            "kafka_key",
            "kafka_partition",
            "kafka_offset",
            "kafka_timestamp",
            "ingestion_timestamp",

            current_timestamp().alias(
                "silver_processed_at"
            )
        )

        # ----------------------------------------------------
        # 9. CREATE QUARANTINE DATAFRAME
        # ----------------------------------------------------

        quarantine_df = classified_df.filter(
            col("rejection_reason").isNotNull()
        ).select(

            "event_data",

            "customer_id",
            "first_name",
            "last_name",
            "email",
            "city",
            "membership",
            "created_at_raw",

            "rejection_reason",

            lit("customers_topic").alias("source_topic"),

            "kafka_key",
            "kafka_partition",
            "kafka_offset",
            "kafka_timestamp",
            "ingestion_timestamp",

            current_timestamp().alias(
                "quarantined_at"
            )
        )

        # ----------------------------------------------------
        # 10. VALIDATE TRANSFORMATION COUNTS
        # ----------------------------------------------------

        silver_count = silver_df.count()

        quarantine_count = quarantine_df.count()

        bronze_count = bronze_df.count()

        logger.info("========== CUSTOMER PIPELINE SUMMARY ==========")

        logger.info("Bronze records: %s", bronze_count)

        logger.info("Silver records: %s", silver_count)

        logger.info(
            "Quarantined records: %s",
            quarantine_count
        )

        if silver_count + quarantine_count != bronze_count:
            raise ValueError(
                "Record reconciliation failed!"
            )

        logger.info(
            "Record reconciliation passed"
        )

        # ----------------------------------------------------
        # 11. WRITE SILVER DATA
        # ----------------------------------------------------

        logger.info("Writing customer Silver data")

        (
            silver_df
            .coalesce(2)
            .write
            .format("parquet")
            .mode("overwrite")
            .save(SILVER_CUSTOMER_PATH)
        )

        # ----------------------------------------------------
        # 12. WRITE QUARANTINE DATA
        # ----------------------------------------------------

        logger.info("Writing customer quarantine data")

        (
            quarantine_df
            .coalesce(1)
            .write
            .format("parquet")
            .mode("overwrite")
            .save(QUARANTINE_PATH)
        )

        logger.info(
            "Customer Silver pipeline completed successfully"
        )

        logger.info(
            "Silver output: %s",
            SILVER_CUSTOMER_PATH
        )

        logger.info(
            "Quarantine output: %s",
            QUARANTINE_PATH
        )

    except Exception:

        logger.exception(
            "Customer Silver pipeline failed"
        )

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
