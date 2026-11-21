"""Isolated Spark S3A + Iceberg/Nessie persistence canary.

This job touches only its dedicated Nessie namespace/table and S3A canary prefix.
It never reads or writes the existing Bronze, Silver, Gold, or checkpoint paths.
"""
import logging
import os

from pyspark.sql import SparkSession

NESSIE_URI = os.environ["NESSIE_URI"].rstrip("/")
NESSIE_BRANCH = os.getenv("NESSIE_BRANCH", "main")
S3_ENDPOINT = os.environ["SEAWEEDFS_ENDPOINT"]
S3_BUCKET = os.getenv("SEAWEEDFS_BUCKET", "shopstream-warehouse")
S3_ACCESS_KEY = os.environ["SEAWEEDFS_ACCESS_KEY"]
S3_SECRET_KEY = os.environ["SEAWEEDFS_SECRET_KEY"]
MODE = os.getenv("ICEBERG_CANARY_MODE", "write").lower()
RUN_ID = os.getenv("ICEBERG_CANARY_RUN_ID", "shopstream-persistence")
TABLE = "nessie.integration_canary.persistence_check"
S3A_PATH = f"s3a://{S3_BUCKET}/integration_canary/{RUN_ID}"
LOG = logging.getLogger("shopstream.iceberg.canary")
EXPECTED = [(1, "seaweedfs"), (2, "iceberg"), (3, "nessie")]


def check_s3a(spark):
    """Exercise Hadoop S3A against the new endpoint independently of Iceberg."""
    sc = spark.sparkContext
    path = sc._jvm.org.apache.hadoop.fs.Path(S3A_PATH)
    fs = path.getFileSystem(sc._jsc.hadoopConfiguration())
    if MODE == "write":
        LOG.info("S3A_WRITE_START path=%s", S3A_PATH)
        if fs.exists(path):
            fs.delete(path, True)
        file_path = sc._jvm.org.apache.hadoop.fs.Path(path, "canary.txt")
        output = fs.create(file_path, True)
        output.writeBytes(f"{RUN_ID}:s3a-write-ok\n")
        output.close()
        LOG.info("S3A_WRITE_DONE path=%s", S3A_PATH)
    LOG.info("S3A_READ_START path=%s", S3A_PATH)
    file_path = sc._jvm.org.apache.hadoop.fs.Path(path, "canary.txt")
    input_stream = fs.open(file_path)
    value = input_stream.readLine()
    input_stream.close()
    actual = [value] if value is not None else []
    expected = [f"{RUN_ID}:s3a-write-ok"]
    if actual != expected:
        raise RuntimeError(f"S3A read-back mismatch: expected {expected}, got {actual}")
    LOG.info("S3A_WRITE_READ_PASSED path=s3a://%s rows=%d", S3_BUCKET, len(actual))


def ensure_bucket(spark):
    """Create the isolated warehouse bucket with the same env credentials."""
    jvm = spark._jvm
    credentials = jvm.com.amazonaws.auth.BasicAWSCredentials(
        S3_ACCESS_KEY, S3_SECRET_KEY)
    provider = jvm.com.amazonaws.auth.AWSStaticCredentialsProvider(credentials)
    endpoint = jvm.com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration(
        S3_ENDPOINT, "us-east-1")
    client = (jvm.com.amazonaws.services.s3.AmazonS3ClientBuilder.standard()
              .withEndpointConfiguration(endpoint)
              .withCredentials(provider)
              .withPathStyleAccessEnabled(True)
              .build())
    if not client.doesBucketExistV2(S3_BUCKET):
        client.createBucket(S3_BUCKET)
    LOG.info("SEAWEEDFS_BUCKET_READY bucket=%s", S3_BUCKET)


def check_nessie_listing(spark):
    rows = spark.sql("SHOW TABLES IN nessie.integration_canary").collect()
    names = {row.tableName for row in rows}
    if "persistence_check" not in names:
        raise RuntimeError(f"Nessie catalog did not list {TABLE}: {sorted(names)}")
    LOG.info("NESSIE_TABLE_LIST_PASSED namespace=integration_canary table=persistence_check")


def check_table(spark, expected_snapshot_count=None):
    check_nessie_listing(spark)
    rows = [(row.id, row.label) for row in spark.table(TABLE).orderBy("id").collect()]
    if rows != EXPECTED:
        raise RuntimeError(f"Iceberg rows mismatch: expected {EXPECTED}, got {rows}")

    snapshots = spark.sql(f"SELECT snapshot_id, committed_at FROM {TABLE}.snapshots")
    snapshot_rows = snapshots.orderBy("committed_at").collect()
    if not snapshot_rows:
        raise RuntimeError("Iceberg metadata table contains no snapshots")
    latest = snapshot_rows[-1]["snapshot_id"]
    historical = (spark.read.format("iceberg")
                  .option("snapshot-id", str(latest))
                  .load(TABLE))
    historical_rows = [(row.id, row.label)
                       for row in historical.orderBy("id").collect()]
    if historical_rows != EXPECTED:
        raise RuntimeError("Iceberg snapshot read-back did not match the current table")

    file_count = spark.sql(f"SELECT file_path FROM {TABLE}.files").count()
    metadata_files = spark.sql(
        f"SELECT file FROM {TABLE}.metadata_log_entries").count()
    if file_count < 1 or metadata_files < 1:
        raise RuntimeError(
            f"Iceberg metadata incomplete: files={file_count}, metadata={metadata_files}")
    if expected_snapshot_count and len(snapshot_rows) < expected_snapshot_count:
        raise RuntimeError(
            f"Expected at least {expected_snapshot_count} snapshots, got {len(snapshot_rows)}")
    LOG.info(
        "ICEBERG_READ_SNAPSHOT_PASSED table=%s rows=%d snapshots=%d latest=%s "
        "data_files=%d metadata_log_entries=%d",
        TABLE, len(rows), len(snapshot_rows), latest, file_count, metadata_files)


def main():
    if MODE not in ("write", "verify"):
        raise ValueError("ICEBERG_CANARY_MODE must be write or verify")
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    spark = (SparkSession.builder.appName("ShopStream-Iceberg-Nessie-Persistence-Canary")
             .master(os.getenv("SPARK_MASTER", "spark://spark-master:7077"))
             .config("spark.sql.session.timeZone", "UTC")
             # Spark's Hadoop filesystem access uses S3A.
             .config("spark.hadoop.fs.s3a.impl",
                     "org.apache.hadoop.fs.s3a.S3AFileSystem")
             .config("spark.hadoop.fs.s3a.endpoint", S3_ENDPOINT)
             .config("spark.hadoop.fs.s3a.path.style.access", "true")
             .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
             .config("spark.hadoop.fs.s3a.access.key", S3_ACCESS_KEY)
             .config("spark.hadoop.fs.s3a.secret.key", S3_SECRET_KEY)
             .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                     "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
             .config("spark.hadoop.fs.s3a.endpoint.region", "us-east-1")
             # Iceberg S3FileIO uses the same SeaweedFS S3 endpoint and credentials.
             .config("spark.sql.extensions",
                     "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions,"
                     "org.projectnessie.spark.extensions.NessieSparkSessionExtensions")
             .config("spark.sql.catalog.nessie", "org.apache.iceberg.spark.SparkCatalog")
             .config("spark.sql.catalog.nessie.catalog-impl",
                     "org.apache.iceberg.nessie.NessieCatalog")
             .config("spark.sql.catalog.nessie.uri", NESSIE_URI)
             .config("spark.sql.catalog.nessie.ref", NESSIE_BRANCH)
             .config("spark.sql.catalog.nessie.warehouse", f"s3://{S3_BUCKET}/warehouse")
             .config("spark.sql.catalog.nessie.io-impl",
                     "org.apache.iceberg.aws.s3.S3FileIO")
             .config("spark.sql.catalog.nessie.s3.endpoint", S3_ENDPOINT)
             .config("spark.sql.catalog.nessie.s3.path-style-access", "true")
             .config("spark.sql.catalog.nessie.s3.access-key-id", S3_ACCESS_KEY)
             .config("spark.sql.catalog.nessie.s3.secret-access-key", S3_SECRET_KEY)
             .config("spark.sql.catalog.nessie.client.region", "us-east-1")
             .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")
    try:
        ensure_bucket(spark)
        check_s3a(spark)
        if MODE == "write":
            spark.sql("CREATE NAMESPACE IF NOT EXISTS nessie.integration_canary")
            spark.sql(
                f"CREATE TABLE IF NOT EXISTS {TABLE} "
                "(id BIGINT, label STRING) USING iceberg "
                "TBLPROPERTIES ('format-version'='2')")
            # Idempotently replace only this dedicated canary table.
            spark.sql(f"DELETE FROM {TABLE}")
            spark.sql(
                f"INSERT INTO {TABLE} VALUES "
                "(1, 'seaweedfs'), (2, 'iceberg'), (3, 'nessie')")
            check_table(spark)
            LOG.info("ICEBERG_CANARY_WRITE_PASSED run_id=%s", RUN_ID)
        else:
            check_table(spark, expected_snapshot_count=1)
            LOG.info("ICEBERG_CANARY_PERSISTENCE_PASSED run_id=%s", RUN_ID)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
