"""Validate driver/executor access to the shared Docker data volume.

All writes are confined to one new child of ``/opt/spark-data/_diagnostics``.
The script refuses to reuse an existing path and never removes output.
"""
import os
import socket
from pathlib import Path

from pyspark.sql import SparkSession


DIAGNOSTICS_ROOT = Path("/opt/spark-data/_diagnostics").resolve()
CANARY_PATH = Path(os.getenv(
    "SHOPSTREAM_VOLUME_CANARY_PATH",
    "/opt/spark-data/_diagnostics/parquet_executor_write_canary",
)).resolve()


def _executor_write_probe(partition_index, values, output_root):
    """Prove a worker can write below a directory created by the driver."""
    probe = Path(output_root) / f"executor_partition_{partition_index}"
    probe.mkdir(mode=0o2775)
    probe.chmod(0o2775)
    (probe / "identity.txt").write_text(
        f"uid={os.getuid()} gid={os.getgid()} host={socket.gethostname()}\n",
        encoding="utf-8",
    )
    yield (os.getuid(), os.getgid(), socket.gethostname())


def main():
    if CANARY_PATH.parent != DIAGNOSTICS_ROOT:
        raise ValueError(
            "Canary path must be a direct child of "
            f"{DIAGNOSTICS_ROOT}; got {CANARY_PATH}"
        )
    DIAGNOSTICS_ROOT.mkdir(parents=True, exist_ok=True)
    if os.stat(DIAGNOSTICS_ROOT).st_gid != 185:
        raise PermissionError(
            f"Diagnostics root must be group 185: {DIAGNOSTICS_ROOT}"
        )
    DIAGNOSTICS_ROOT.chmod(0o2775)
    if CANARY_PATH.exists():
        raise FileExistsError(
            f"Refusing to overwrite existing canary path: {CANARY_PATH}"
        )

    # The driver (expected UID 50000 in Airflow) creates the shared parent and
    # a separate marker. Spark workers (expected UID 185) must then write below
    # that same driver-created directory.
    CANARY_PATH.mkdir(mode=0o2775)
    CANARY_PATH.chmod(0o2775)
    if os.stat(CANARY_PATH).st_gid != 185:
        raise PermissionError(
            f"Driver-created canary root did not inherit group 185: {CANARY_PATH}"
        )
    driver_marker = CANARY_PATH / "driver-created.txt"
    driver_marker.write_text(
        f"uid={os.getuid()} host={socket.gethostname()}\n", encoding="utf-8"
    )
    executor_probe = CANARY_PATH / "executor_probe"
    executor_probe.mkdir(mode=0o2775)
    executor_probe.chmod(0o2775)
    if os.stat(executor_probe).st_gid != 185:
        raise PermissionError(
            f"Executor probe directory did not inherit group 185: {executor_probe}"
        )

    spark = (
        SparkSession.builder
        .appName("ShopStream-Shared-Volume-Parquet-Write-Canary")
        .getOrCreate()
    )
    try:
        executor_identity = (
            spark.sparkContext.parallelize(range(4), 4)
            .mapPartitionsWithIndex(
                lambda partition, values: _executor_write_probe(
                    partition, values, str(executor_probe)
                )
            )
            .collect()
        )
        if not executor_identity:
            raise RuntimeError("No Spark executor identity was observed")
        if any(uid != 185 or gid != 185 for uid, gid, _ in executor_identity):
            raise PermissionError(
                f"Expected executor identity 185:185, got {executor_identity}"
            )
        if any(host == socket.gethostname() for _, _, host in executor_identity):
            raise RuntimeError(
                f"Executor probe unexpectedly ran on the Airflow driver: {executor_identity}"
            )

        source = (
            spark.range(8)
            .selectExpr(
                "CAST(id AS INT) AS record_id",
                "CASE WHEN id < 4 THEN 'customers' ELSE 'orders' END AS topic",
            )
            .repartition(2)
        )
        parquet_path = CANARY_PATH / "parquet"
        parquet_path.mkdir(mode=0o2775)
        parquet_path.chmod(0o2775)
        if os.stat(parquet_path).st_gid != 185:
            raise PermissionError(
                f"Parquet target did not inherit group 185: {parquet_path}"
            )
        if any(parquet_path.iterdir()):
            raise RuntimeError(f"New Parquet target is not empty: {parquet_path}")

        source.write.mode("append").partitionBy("topic").parquet(
            str(parquet_path)
        )

        readback = spark.read.parquet(str(parquet_path))
        row_count = readback.count()
        topics = sorted(
            row["topic"] for row in readback.select("topic").distinct().collect()
        )
        expected_paths = [
            parquet_path / "topic=customers",
            parquet_path / "topic=orders",
        ]
        missing_paths = [str(path) for path in expected_paths if not path.is_dir()]

        if row_count != 8 or topics != ["customers", "orders"] or missing_paths:
            raise RuntimeError(
                "Shared-volume Parquet verification failed: "
                f"rows={row_count}, topics={topics}, "
                f"missing_partition_dirs={missing_paths}"
            )

        print(
            "SPARK_VOLUME_PARQUET_WRITE_OK "
            f"driver_uid={os.getuid()} driver_host={socket.gethostname()} "
            f"executors={executor_identity} rows={row_count} topics={topics} "
            f"path={CANARY_PATH}"
        )
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
