"""Publish ShopStream Parquet datasets into a Nessie-backed Iceberg catalog.

The source root, object-store provider, and Nessie namespace prefix are
configurable so integration runs can use isolated paths and table names.
Existing Parquet sources are read only. Re-publishing replaces each target
Iceberg table atomically and retains its prior snapshots.
"""
import logging
import os
import re

from pyspark.sql import SparkSession, functions as F

DATA_ROOT = os.getenv("SHOPSTREAM_DATA_ROOT", "/opt/spark-data")
NESSIE_URI = os.getenv("NESSIE_URI", "http://nessie:19120/api/v2")
NESSIE_BRANCH = os.getenv("NESSIE_BRANCH", "main")
STORE_PROVIDER = os.getenv("OBJECT_STORE_PROVIDER", "minio").lower()
MODE = os.getenv("ICEBERG_CATALOG_MODE", "publish").lower()
NAMESPACE_PREFIX = os.getenv("ICEBERG_NAMESPACE_PREFIX", "integration").strip("_")
INCLUDE_BRONZE_QUARANTINE = (
    os.getenv("ICEBERG_INCLUDE_BRONZE_QUARANTINE", "false").lower() == "true")
ISOLATED_INTEGRATION_PREFIXES = {
    "integration_clean_20261111",
    "integration_run_20261113_01",
    "integration_run_20261113_02",
}
WRITE_DISTRIBUTION_MODE = "range"
WRITE_FANOUT_ENABLED = "false"
LOG = logging.getLogger("shopstream.iceberg")

ENTITIES = ("customers", "products", "orders", "payments", "inventory",
            "website_activity")
GOLD_TABLES = (
    "daily_revenue", "monthly_revenue", "product_performance",
    "category_performance", "customer_lifetime_value",
    "customer_segmentation", "payment_success_failure",
    "inventory_levels_and_movement", "website_conversion_funnel",
    "order_status_analysis",
)


def namespace_name(layer):
    return f"{NAMESPACE_PREFIX}_{layer}" if NAMESPACE_PREFIX else layer


def iceberg_table_name(namespace, table):
    return f"nessie.{namespace_name(namespace)}.{table}"


def _table_specs(spark):
    """Return the existing Parquet inputs and their target Iceberg tables."""
    specs = []
    bronze = spark.read.parquet(os.path.join(DATA_ROOT, "bronze", "events"))
    specs.append(("bronze", "events", bronze, ("topic",)))

    if INCLUDE_BRONZE_QUARANTINE:
        if NAMESPACE_PREFIX not in ISOLATED_INTEGRATION_PREFIXES:
            raise ValueError(
                "Bronze quarantine Iceberg publication is restricted to the "
                f"one of {sorted(ISOLATED_INTEGRATION_PREFIXES)!r}")
        quarantine = spark.read.parquet(
            os.path.join(DATA_ROOT, "bronze", "quarantine", "events"))
        specs.append(("bronze_quarantine", "events", quarantine, ("topic",)))

    for entity in ENTITIES:
        silver = spark.read.parquet(os.path.join(DATA_ROOT, "silver_v2", entity))
        event_time = next((name for name in
                           ("created_at", "payment_timestamp", "event_timestamp")
                           if name in silver.columns), None)
        if event_time:
            silver = silver.withColumn("event_date", F.to_date(event_time))
        specs.append(("silver", entity, silver,
                      ("event_date",) if event_time else ()))

        quarantine = spark.read.parquet(
            os.path.join(DATA_ROOT, "silver_v2", "quarantine", entity))
        specs.append(("quarantine", entity, quarantine, ()))

    for table in GOLD_TABLES:
        gold = spark.read.parquet(os.path.join(DATA_ROOT, "gold_v2", table))
        partition = (("order_date",) if table == "daily_revenue" else
                     ("order_month",) if table == "monthly_revenue" else ())
        specs.append(("gold", table, gold, partition))
    return specs


def _check_name(value):
    if not re.fullmatch(r"[a-z][a-z0-9_]*", value):
        raise ValueError(f"Unsafe Iceberg namespace/table component: {value!r}")


def _verify_table(spark, name, source_count, require_listing=True):
    if not spark.catalog.tableExists(name):
        raise RuntimeError(f"Iceberg table is missing: {name}")
    destination_count = spark.table(name).count()
    if destination_count != source_count:
        raise RuntimeError(
            f"{name}: source rows={source_count}, Iceberg rows={destination_count}")

    snapshots = spark.sql(
        f"SELECT snapshot_id, committed_at FROM {name}.snapshots")
    snapshot_rows = snapshots.orderBy("committed_at").collect()
    metadata_entries = spark.sql(
        f"SELECT file FROM {name}.metadata_log_entries").count()
    if source_count and not snapshot_rows:
        raise RuntimeError(f"Non-empty Iceberg table has no snapshot: {name}")
    if not metadata_entries:
        raise RuntimeError(f"Iceberg table has no metadata log entries: {name}")

    latest_snapshot = None
    if snapshot_rows:
        latest_snapshot = snapshot_rows[-1]["snapshot_id"]
        historical = (spark.read.format("iceberg")
                      .option("snapshot-id", str(latest_snapshot))
                      .load(name))
        historical_count = historical.count()
        if historical_count != source_count:
            raise RuntimeError(
                f"{name}: latest snapshot rows={historical_count}, "
                f"source rows={source_count}")

    if require_listing:
        namespace, table = name.removeprefix("nessie.").split(".", 1)
        listed = {row.tableName for row in
                  spark.sql(f"SHOW TABLES IN nessie.{namespace}").collect()}
        if table not in listed:
            raise RuntimeError(f"Nessie did not list {name}; found {sorted(listed)}")

    LOG.info(
        "ICEBERG_TABLE_VERIFIED table=%s source_rows=%d destination_rows=%d "
        "snapshots=%d latest_snapshot=%s metadata_log_entries=%d",
        name, source_count, destination_count, len(snapshot_rows),
        latest_snapshot, metadata_entries)
    return destination_count


def _publish_table(spark, name, dataframe, partition_fields):
    # Range distribution clusters partition values and avoids Iceberg's
    # Spark 3.5 default fanout writer, which keeps one Parquet writer open per
    # active partition in a task and can exhaust this small executor's heap.
    LOG.info(
        "ICEBERG_TABLE_WRITE_START table=%s source_partitions=%d "
        "partition_fields=%s distribution=%s fanout=%s",
        name, dataframe.rdd.getNumPartitions(), list(partition_fields),
        WRITE_DISTRIBUTION_MODE, WRITE_FANOUT_ENABLED)

    properties = {
        "write.parquet.compression-codec": "snappy",
        "write.parquet.row-group-size-bytes": "8388608",
    }

    exists = spark.catalog.tableExists(name)
    if exists:
        # Update write properties without deleting existing snapshots.
        property_sql = ", ".join(
            f"'{key}'='{value}'" for key, value in properties.items()
        )
        spark.sql(f"ALTER TABLE {name} SET TBLPROPERTIES ({property_sql})")

        # Replace the table contents as a new Iceberg snapshot.
        (dataframe.writeTo(name)
         .option("distribution-mode", WRITE_DISTRIBUTION_MODE)
         .option("fanout-enabled", WRITE_FANOUT_ENABLED)
         .overwrite(F.lit(True)))
    else:
        writer = (dataframe.writeTo(name).using("iceberg")
                  .option("distribution-mode", WRITE_DISTRIBUTION_MODE)
                  .option("fanout-enabled", WRITE_FANOUT_ENABLED))
        writer = writer.tableProperty("format-version", "2")
        writer = writer.tableProperty("write.format.default", "parquet")
        for key, value in properties.items():
            writer = writer.tableProperty(key, value)
        for field in partition_fields:
            writer = writer.partitionedBy(field)
        writer.create()


def _object_store_config():
    if STORE_PROVIDER == "seaweedfs":
        endpoint = os.environ["SEAWEEDFS_ENDPOINT"]
        bucket = os.environ["SEAWEEDFS_BUCKET"]
        access_key = os.environ["SEAWEEDFS_ACCESS_KEY"]
        secret_key = os.environ["SEAWEEDFS_SECRET_KEY"]
    elif STORE_PROVIDER == "minio":
        # Preserve the existing MinIO environment and endpoint behavior.
        endpoint = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
        bucket = os.getenv("MINIO_BUCKET", "shopstream-warehouse")
        access_key = os.environ["MINIO_ACCESS_KEY"]
        secret_key = os.environ["MINIO_SECRET_KEY"]
    else:
        raise ValueError("OBJECT_STORE_PROVIDER must be 'minio' or 'seaweedfs'")
    return endpoint.rstrip("/"), bucket, access_key, secret_key


def main():
    global spark
    if MODE not in ("publish", "verify"):
        raise ValueError("ICEBERG_CATALOG_MODE must be 'publish' or 'verify'")
    if NAMESPACE_PREFIX:
        _check_name(NAMESPACE_PREFIX)

    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)s %(message)s")
    endpoint, bucket, access_key, secret_key = _object_store_config()
    warehouse = f"s3://{bucket}/warehouse/{NAMESPACE_PREFIX or 'shopstream'}"
    spark = (SparkSession.builder.appName("ShopStream-Iceberg-Nessie-Publisher")
             .master(os.getenv("SPARK_MASTER", "spark://spark-master:7077"))
             .config("spark.sql.session.timeZone", "UTC")
             .config("spark.sql.extensions",
                     "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions,"
                     "org.projectnessie.spark.extensions.NessieSparkSessionExtensions")
             .config("spark.sql.catalog.nessie", "org.apache.iceberg.spark.SparkCatalog")
             .config("spark.sql.catalog.nessie.catalog-impl",
                     "org.apache.iceberg.nessie.NessieCatalog")
             .config("spark.sql.catalog.nessie.uri", NESSIE_URI)
             .config("spark.sql.catalog.nessie.ref", NESSIE_BRANCH)
             .config("spark.sql.catalog.nessie.warehouse", warehouse)
             # Nessie’s default HTTP read timeout is short when the server is
             # briefly CPU-bound while committing catalog updates.
             .config("spark.sql.catalog.nessie.nessie.transport.read-timeout",
                     "120000")
             .config("spark.sql.catalog.nessie.io-impl",
                     "org.apache.iceberg.aws.s3.S3FileIO")
             .config("spark.sql.catalog.nessie.s3.endpoint", endpoint)
             .config("spark.sql.catalog.nessie.s3.path-style-access", "true")
             .config("spark.sql.catalog.nessie.s3.access-key-id", access_key)
             .config("spark.sql.catalog.nessie.s3.secret-access-key", secret_key)
             .config("spark.sql.catalog.nessie.client.region", "us-east-1")
             # Keep Hadoop S3A configured for Spark jobs using the same store.
             .config("spark.hadoop.fs.s3a.impl",
                     "org.apache.hadoop.fs.s3a.S3AFileSystem")
             .config("spark.hadoop.fs.s3a.endpoint", endpoint)
             .config("spark.hadoop.fs.s3a.path.style.access", "true")
             .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
             .config("spark.hadoop.fs.s3a.access.key", access_key)
             .config("spark.hadoop.fs.s3a.secret.key", secret_key)
             .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                     "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
             .config("spark.hadoop.fs.s3a.endpoint.region", "us-east-1")
             .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")
    try:
        specs = _table_specs(spark)
        namespaces = sorted({namespace_name(spec[0]) for spec in specs})
        if MODE == "publish":
            for namespace in namespaces:
                _check_name(namespace)
                spark.sql(f"CREATE NAMESPACE IF NOT EXISTS nessie.{namespace}")

        total_tables = 0
        for namespace, table, dataframe, partition_fields in specs:
            _check_name(namespace_name(namespace))
            _check_name(table)
            name = iceberg_table_name(namespace, table)
            source_partitions = dataframe.rdd.getNumPartitions()
            source_count = dataframe.count()
            LOG.info(
                "ICEBERG_TABLE_PROCESSING_START table=%s mode=%s "
                "source_rows=%d source_partitions=%d",
                name, MODE, source_count, source_partitions)
            try:
                if MODE == "publish":
                    _publish_table(spark, name, dataframe, partition_fields)
                _verify_table(spark, name, source_count)
            except Exception:
                LOG.exception(
                    "ICEBERG_TABLE_PROCESSING_FAILED table=%s mode=%s "
                    "source_rows=%d",
                    name, MODE, source_count)
                raise
            total_tables += 1

        LOG.info("ICEBERG_%s_COMPLETE tables=%d data_root=%s prefix=%s",
                 MODE.upper(), total_tables, DATA_ROOT, NAMESPACE_PREFIX)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
