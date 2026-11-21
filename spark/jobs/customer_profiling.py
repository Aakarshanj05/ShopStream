
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    trim,
    lower,
    count,
    countDistinct,
    sum,
    when
)


# ============================================================
# CONFIGURATION
# ============================================================

APP_NAME = "ShopStream-Customer-Profiling"

BRONZE_CUSTOMER_PATH = (
    "/opt/spark-data/bronze/events/topic=customers_topic"
)

REQUIRED_COLUMNS = [
    "customer_id",
    "first_name",
    "last_name",
    "email",
    "city",
    "membership",
    "created_at"
]


# ============================================================
# SPARK SESSION
# ============================================================

spark = (
    SparkSession.builder
    .appName(APP_NAME)
    .master("spark://spark-master:7077")
    .config("spark.sql.session.timeZone", "UTC")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


try:

    print("\n========== CUSTOMER DATA PROFILING ==========\n")

    # --------------------------------------------------------
    # 1. READ BRONZE CUSTOMER DATA
    # --------------------------------------------------------

    bronze_df = spark.read.parquet(BRONZE_CUSTOMER_PATH)

    print("Bronze source schema:")
    bronze_df.printSchema()

    print("\nTotal raw Bronze records:", bronze_df.count())

    # --------------------------------------------------------
    # 2. CHECK EMPTY OR NULL JSON MESSAGES
    # --------------------------------------------------------

    valid_json_input = bronze_df.filter(
        col("event_data").isNotNull()
        & (trim(col("event_data")) != "")
    )

    print(
        "\nEmpty or null messages:",
        bronze_df.count() - valid_json_input.count()
    )

    # --------------------------------------------------------
    # 3. PARSE JSON AND INFER CUSTOMER SCHEMA
    # --------------------------------------------------------

    customers_df = spark.read.json(
        valid_json_input.select("event_data")
        .rdd.map(lambda row: row["event_data"])
    )

    customers_df.cache()

    print("\nInferred customer schema:")
    customers_df.printSchema()

    total_customers = customers_df.count()

    print("\nParsed customer records:", total_customers)

    # --------------------------------------------------------
    # 4. CHECK REQUIRED COLUMNS
    # --------------------------------------------------------

    print("\nRequired column availability:")

    for column_name in REQUIRED_COLUMNS:

        if column_name in customers_df.columns:
            print(f"{column_name}: PRESENT")
        else:
            print(f"{column_name}: MISSING")

    # --------------------------------------------------------
    # 5. CHECK NULL AND EMPTY VALUES
    # --------------------------------------------------------

    print("\nNull and empty value counts:")

    available_columns = [
        name for name in REQUIRED_COLUMNS
        if name in customers_df.columns
    ]

    null_checks = []

    for column_name in available_columns:

        null_checks.append(
            sum(
                when(
                    col(column_name).isNull()
                    | (trim(col(column_name).cast("string")) == ""),
                    1
                ).otherwise(0)
            ).alias(column_name)
        )

    customers_df.select(*null_checks).show(truncate=False)

    # --------------------------------------------------------
    # 6. CHECK DUPLICATE CUSTOMER IDS
    # --------------------------------------------------------

    print("\nCustomer ID duplicate analysis:")

    total_ids = customers_df.count()

    unique_ids = customers_df.select(
        countDistinct("customer_id")
    ).first()[0]

    print("Total customer records:", total_ids)
    print("Unique customer IDs:", unique_ids)
    print("Duplicate ID occurrences:", total_ids - unique_ids)

    print("\nDuplicate customer ID samples:")

    customers_df.groupBy("customer_id").count().filter(
        col("count") > 1
    ).show(20, truncate=False)

    # --------------------------------------------------------
    # 7. CHECK EMAIL FORMAT
    # --------------------------------------------------------

    print("\nInvalid email analysis:")

    invalid_emails = customers_df.filter(
        col("email").isNull()
        | (~lower(trim(col("email"))).rlike(
            r"^[a-z0-9._%+-]+@[a-z0-9.-]+\.[a-z]{2,}$"
        ))
    )

    print("Invalid email count:", invalid_emails.count())

    invalid_emails.select(
        "customer_id",
        "email"
    ).show(10, truncate=False)

    # --------------------------------------------------------
    # 8. MEMBERSHIP DISTRIBUTION
    # --------------------------------------------------------

    print("\nMembership distribution:")

    customers_df.groupBy("membership").count().orderBy(
        col("count").desc()
    ).show(truncate=False)

    # --------------------------------------------------------
    # 9. CITY DISTRIBUTION
    # --------------------------------------------------------

    print("\nCity distribution:")

    customers_df.groupBy("city").count().orderBy(
        col("count").desc()
    ).show(truncate=False)

    # --------------------------------------------------------
    # 10. SAMPLE CUSTOMER RECORDS
    # --------------------------------------------------------

    print("\nSample customer records:")

    customers_df.show(10, truncate=False)

    print("\nCustomer profiling completed successfully!")

finally:

    spark.stop()
