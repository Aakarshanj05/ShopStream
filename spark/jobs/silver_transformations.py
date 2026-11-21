
"""Build validated Silver v2 datasets from immutable Bronze Parquet.

Outputs are intentionally isolated under data/silver_v2 so the existing
customer Silver output remains untouched.
"""

import logging
import os
from functools import reduce

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DecimalType,
)
from pyspark.sql.functions import col


LOG = logging.getLogger("shopstream.silver")

DATA_ROOT = os.getenv("SHOPSTREAM_DATA_ROOT", "/opt/spark-data")
BRONZE_ROOT = os.path.join(DATA_ROOT, "bronze", "events")
SILVER_ROOT = os.path.join(DATA_ROOT, "silver_v2")
QUARANTINE_ROOT = os.path.join(SILVER_ROOT, "quarantine")


# Topic names can be redirected to dedicated integration topics.
TOPIC_MAP = {
    "customers": os.getenv("SHOPSTREAM_TOPIC_CUSTOMERS", "customers_topic"),
    "products": os.getenv("SHOPSTREAM_TOPIC_PRODUCTS", "products_topic"),
    "orders": os.getenv("SHOPSTREAM_TOPIC_ORDERS", "orders_topic"),
    "payments": os.getenv("SHOPSTREAM_TOPIC_PAYMENTS", "payments_topic"),
    "inventory": os.getenv("SHOPSTREAM_TOPIC_INVENTORY", "inventory_topic"),
    "website_activity": os.getenv(
        "SHOPSTREAM_TOPIC_WEBSITE_ACTIVITY",
        "website_activity_topic",
    ),
}


SCHEMAS = {
    "customers": (
        TOPIC_MAP["customers"],
        "customer_id",
        [
            ("customer_id", StringType()),
            ("first_name", StringType()),
            ("last_name", StringType()),
            ("email", StringType()),
            ("city", StringType()),
            ("membership", StringType()),
            ("created_at", StringType()),
        ],
    ),
    "products": (
        TOPIC_MAP["products"],
        "product_id",
        [
            ("product_id", StringType()),
            ("product_name", StringType()),
            ("category", StringType()),
            ("brand", StringType()),
            ("price", DecimalType(12, 2)),
            ("stock_quantity", IntegerType()),
            ("created_at", StringType()),
        ],
    ),
    "orders": (
        TOPIC_MAP["orders"],
        "order_id",
        [
            ("order_id", StringType()),
            ("customer_id", StringType()),
            ("product_id", StringType()),
            ("quantity", IntegerType()),
            ("unit_price", DecimalType(12, 2)),
            ("amount", DecimalType(14, 2)),
            ("status", StringType()),
            ("created_at", StringType()),
        ],
    ),
    "payments": (
        TOPIC_MAP["payments"],
        "payment_id",
        [
            ("payment_id", StringType()),
            ("order_id", StringType()),
            ("customer_id", StringType()),
            ("payment_method", StringType()),
            ("payment_status", StringType()),
            ("payment_amount", DecimalType(14, 2)),
            ("payment_timestamp", StringType()),
        ],
    ),
    "inventory": (
        TOPIC_MAP["inventory"],
        "inventory_event_id",
        [
            ("inventory_event_id", StringType()),
            ("product_id", StringType()),
            ("event_type", StringType()),
            ("quantity_changed", IntegerType()),
            ("previous_stock", IntegerType()),
            ("available_stock", IntegerType()),
            ("event_timestamp", StringType()),
        ],
    ),
    "website_activity": (
        TOPIC_MAP["website_activity"],
        "event_id",
        [
            ("event_id", StringType()),
            ("session_id", StringType()),
            ("customer_id", StringType()),
            ("product_id", StringType()),
            ("event_type", StringType()),
            ("device", StringType()),
            ("browser", StringType()),
            ("city", StringType()),
            ("sequence_number", IntegerType()),
            ("event_timestamp", StringType()),
        ],
    ),
}


def _reason_expr(reasons):
    nonempty = [
        F.when(expr, F.lit(reason))
        for reason, expr in reasons
    ]
    return F.concat_ws("|", *nonempty)


def _base(topic, fields):
    schema = StructType([
        StructField(name, data_type, True)
        for name, data_type in fields
    ])

    bronze = (
        spark.read
        .parquet(os.path.join(BRONZE_ROOT, "topic=" + topic))
        .withColumn("_parsed", F.from_json(col("event_data"), schema))
    )

    cols = [
        col("_parsed." + name).alias(name)
        for name, _ in fields
    ]

    return bronze.select(
        *cols,
        col("event_data"),
        col("kafka_key"),
        F.lit(topic).alias("source_topic"),
        col("partition").alias("kafka_partition"),
        col("offset").alias("kafka_offset"),
        col("kafka_timestamp"),
        col("ingestion_timestamp"),
    )


def _normalize(df, name):
    for field in df.schema.fieldNames():
        if field in (
            "event_data",
            "source_topic",
            "kafka_key",
            "kafka_partition",
            "kafka_offset",
            "kafka_timestamp",
            "ingestion_timestamp",
        ):
            continue

        if df.schema[field].dataType == StringType():
            df = df.withColumn(field, F.trim(col(field)))

    if "email" in df.columns:
        df = df.withColumn("email", F.lower(col("email")))

    for field in (
        "membership",
        "status",
        "payment_status",
        "payment_method",
        "event_type",
        "device",
        "browser",
    ):
        if field in df.columns:
            df = df.withColumn(field, F.upper(col(field)))

    for field in (
        "created_at",
        "payment_timestamp",
        "event_timestamp",
    ):
        if field in df.columns:
            df = df.withColumn(field, F.to_timestamp(col(field)))

    return df


def _apply_references(df, references):
    for field, reference_df, reference_key, reason in references:
        marker = "__ref_" + field

        lookup = (
            reference_df
            .select(col(reference_key).alias(marker))
            .dropDuplicates()
        )

        df = df.join(
            lookup,
            col(field) == col(marker),
            "left",
        )

        df = (
            df.withColumn("_" + marker, col(marker).isNull())
            .drop(marker)
        )

    return df


def _transform(name, valid_refs=()):
    topic, primary_key, fields = SCHEMAS[name]

    df = _normalize(_base(topic, fields), name)

    reasons = [
        (
            "MALFORMED_JSON",
            col(fields[0][0]).isNull() & col("event_data").isNotNull(),
        )
    ]

    required_by_entity = {
        "customers": [
            "customer_id", "first_name", "last_name", "email",
            "city", "membership", "created_at",
        ],
        "products": [
            "product_id", "product_name", "category", "brand",
            "price", "stock_quantity", "created_at",
        ],
        "orders": [
            "order_id", "customer_id", "product_id", "quantity",
            "unit_price", "amount", "status", "created_at",
        ],
        "payments": [
            "payment_id", "order_id", "customer_id", "payment_method",
            "payment_status", "payment_amount", "payment_timestamp",
        ],
        "inventory": [
            "inventory_event_id", "product_id", "event_type",
            "quantity_changed", "previous_stock", "available_stock",
            "event_timestamp",
        ],
        "website_activity": [
            "event_id", "session_id", "customer_id",
            "product_id", "event_type", "sequence_number",
            "event_timestamp",
        ],
    }

    for field in required_by_entity[name]:
        reasons.append((
            "MISSING_OR_INVALID_" + field.upper(),
            col(field).isNull()
            | (
                (col(field) == "")
                if df.schema[field].dataType == StringType()
                else F.lit(False)
            ),
        ))

    if name == "customers":
        reasons += [
            (
                "INVALID_EMAIL",
                ~col("email").rlike(
                    r"^[a-z0-9._%+-]+@[a-z0-9.-]+\.[a-z]{2,}$"
                ),
            ),
            (
                "INVALID_MEMBERSHIP",
                ~F.upper(col("membership")).isin(
                    "BASIC", "SILVER", "GOLD", "PREMIUM"
                ),
            ),
        ]

    elif name == "products":
        reasons += [
            ("NONPOSITIVE_PRICE", col("price") <= 0),
            ("NEGATIVE_STOCK", col("stock_quantity") < 0),
        ]

    elif name == "orders":
        reasons += [
            ("NONPOSITIVE_QUANTITY", col("quantity") <= 0),
            (
                "NEGATIVE_PRICE_OR_AMOUNT",
                (col("unit_price") < 0) | (col("amount") < 0),
            ),
            (
                "AMOUNT_MISMATCH",
                F.abs(
                    col("amount") - col("unit_price") * col("quantity")
                ) > F.lit(0.02),
            ),
            (
                "INVALID_ORDER_STATUS",
                ~F.upper(col("status")).isin(
                    "PLACED", "CONFIRMED", "CANCELLED"
                ),
            ),
        ]

    elif name == "payments":
        reasons += [
            ("NEGATIVE_PAYMENT_AMOUNT", col("payment_amount") < 0),
            (
                "INVALID_PAYMENT_STATUS",
                ~F.upper(col("payment_status")).isin(
                    "SUCCESS", "FAILED", "PENDING"
                ),
            ),
            (
                "INVALID_PAYMENT_METHOD",
                ~F.upper(col("payment_method")).isin(
                    "UPI", "CREDIT_CARD", "DEBIT_CARD",
                    "NET_BANKING", "COD",
                ),
            ),
        ]

    elif name == "inventory":
        reasons += [
            (
                "INVALID_INVENTORY_EVENT_TYPE",
                ~F.upper(col("event_type")).isin(
                    "STOCK_IN", "STOCK_OUT", "STOCK_ADJUSTMENT"
                ),
            ),
            (
                "NEGATIVE_STOCK",
                (col("previous_stock") < 0)
                | (col("available_stock") < 0),
            ),
            (
                "STOCK_DELTA_MISMATCH",
                col("available_stock")
                != col("previous_stock") + col("quantity_changed"),
            ),
        ]

    elif name == "website_activity":
        reasons += [
            (
                "INVALID_WEBSITE_EVENT_TYPE",
                ~F.upper(col("event_type")).isin(
                    "PAGE_VIEW",
                    "PRODUCT_VIEW",
                    "ADD_TO_CART",
                    "CHECKOUT_STARTED",
                    "PURCHASE",
                ),
            ),
            (
                "INVALID_SEQUENCE_NUMBER",
                col("sequence_number") <= 0,
            ),
        ]

    reference_markers = []

    for reference_field, reference_df, reference_key, rejection in valid_refs:
        marker = "__ref_" + reference_field

        lookup = (
            reference_df
            .select(col(reference_key).alias(marker))
            .dropDuplicates()
        )

        df = df.join(
            lookup,
            col(reference_field) == col(marker),
            "left",
        )

        reasons.append((rejection, col(marker).isNull()))
        reference_markers.append(marker)

    df = df.withColumn("validation_reason", _reason_expr(reasons))

    for marker in reference_markers:
        df = df.drop(marker)

    event_time = next(
        (
            field
            for field in (
                "created_at",
                "event_timestamp",
                "payment_timestamp",
            )
            if field in df.columns
        ),
        None,
    )

    sort_fields = (
        [col(event_time).desc_nulls_last()]
        if event_time
        else []
    )

    sort_fields += [
        col("kafka_timestamp").desc_nulls_last(),
        col("kafka_partition").desc_nulls_last(),
        col("kafka_offset").desc_nulls_last(),
    ]

    window = Window.partitionBy(primary_key).orderBy(*sort_fields)

    df = df.withColumn(
        "_dedup_rank",
        F.row_number().over(window),
    )

    df = df.withColumn(
        "rejection_reason",
        F.when(
            col("validation_reason") != "",
            col("validation_reason"),
        ).when(
            col("_dedup_rank") > 1,
            F.lit("DUPLICATE_SUPERSEDED"),
        ),
    )

    processed = F.current_timestamp()

    silver = (
        df.filter(col("rejection_reason").isNull())
        .drop(
            "event_data",
            "validation_reason",
            "_dedup_rank",
            "rejection_reason",
        )
        .withColumn("silver_processed_at", processed)
    )

    quarantine = (
        df.filter(col("rejection_reason").isNotNull())
        .withColumn("quarantined_at", processed)
    )

    bronze_count = df.count()
    silver_count = silver.count()
    quarantine_count = quarantine.count()

    if bronze_count != silver_count + quarantine_count:
        raise RuntimeError(
            f"{name} reconciliation failed: "
            f"bronze={bronze_count}, "
            f"silver={silver_count}, "
            f"quarantine={quarantine_count}"
        )

    target = os.path.join(SILVER_ROOT, name)
    quarantine_target = os.path.join(QUARANTINE_ROOT, name)

    # Silver output
    (
        silver.write
        .mode("overwrite")
        .option("compression", "snappy")
        .parquet(target)
    )

    # Quarantine output
    (
        quarantine.write
        .mode("overwrite")
        .option("compression", "snappy")
        .parquet(quarantine_target)
    )

    LOG.info(
        "%s rows: bronze=%d silver=%d quarantine=%d",
        name,
        bronze_count,
        silver_count,
        quarantine_count,
    )

    return silver.select(primary_key).dropDuplicates()


def main():
    global spark

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )

    spark = (
        SparkSession.builder
        .appName("ShopStream-Silver-v2")
        .master(
            os.getenv(
                "SPARK_MASTER",
                "spark://spark-master:7077",
            )
        )
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    try:
        customer_keys = _transform("customers")

        product_keys = _transform("products")

        order_keys = _transform(
            "orders",
            [
                (
                    "customer_id",
                    customer_keys,
                    "customer_id",
                    "UNKNOWN_CUSTOMER_ID",
                ),
                (
                    "product_id",
                    product_keys,
                    "product_id",
                    "UNKNOWN_PRODUCT_ID",
                ),
            ],
        )

        _transform(
            "payments",
            [
                (
                    "order_id",
                    order_keys,
                    "order_id",
                    "UNKNOWN_ORDER_ID",
                ),
                (
                    "customer_id",
                    customer_keys,
                    "customer_id",
                    "UNKNOWN_CUSTOMER_ID",
                ),
            ],
        )

        _transform(
            "inventory",
            [
                (
                    "product_id",
                    product_keys,
                    "product_id",
                    "UNKNOWN_PRODUCT_ID",
                ),
            ],
        )

        _transform(
            "website_activity",
            [
                (
                    "customer_id",
                    customer_keys,
                    "customer_id",
                    "UNKNOWN_CUSTOMER_ID",
                ),
                (
                    "product_id",
                    product_keys,
                    "product_id",
                    "UNKNOWN_PRODUCT_ID",
                ),
            ],
        )

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
