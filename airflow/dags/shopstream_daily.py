"""Daily ShopStream Kafka generation, Spark processing and Iceberg publish."""

from datetime import timedelta
import os

import pendulum
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.standard.operators.bash import BashOperator


ROOT = "/opt/spark-data/shopstream_daily"
CHECKPOINT = f"{ROOT}/checkpoints/bronze_ingestion"
MASTER_DATA = f"{ROOT}/master_data"
BRONZE_INITIAL_OFFSETS = f"{ROOT}/control/bronze_initial_offsets.json"
PAYMENT_INITIAL_OFFSETS = f"{ROOT}/control/payment_initial_offsets.json"
PAYMENT_OFFSETS_GUARD = f"{ROOT}/control/payment_initial_offsets.initialized"
CHECKPOINT_GUARD = f"{ROOT}/control/bronze_checkpoint.initialized"
ICEBERG_PREFIX = "daily"
KAFKA = "kafka:19092"
GENERATOR_DIR = "/opt/shopstream/generators"
SPARK_CONN = "spark_default"
JAR_DIR = "/opt/spark/dependencies/shopstream-runtime-1.10.2-0.105.7"
KAFKA_JAR_DIR = "/opt/spark/dependencies/ivy/jars"

DRIVER_NETWORK = {
    "spark.driver.host": "shopstream-airflow-scheduler",
    "spark.driver.bindAddress": "0.0.0.0",
    "spark.driver.port": "37000",
    "spark.blockManager.port": "37001",
}
PYTHON_CONF = {
    "spark.pyspark.python": "/usr/bin/python3.11",
    "spark.pyspark.driver.python": "/home/airflow/.local/bin/python",
    "spark.executorEnv.PYSPARK_PYTHON": "/usr/bin/python3.11",
}
TIME_CONF = {"spark.sql.session.timeZone": "UTC"}
KAFKA_JARS = ",".join([
    f"local:{KAFKA_JAR_DIR}/org.apache.spark_spark-sql-kafka-0-10_2.12-3.5.9.jar",
    f"local:{KAFKA_JAR_DIR}/org.apache.spark_spark-token-provider-kafka-0-10_2.12-3.5.9.jar",
    f"local:{KAFKA_JAR_DIR}/org.apache.kafka_kafka-clients-3.4.1.jar",
    f"local:{KAFKA_JAR_DIR}/org.apache.commons_commons-pool2-2.11.1.jar",
    f"local:{KAFKA_JAR_DIR}/commons-logging_commons-logging-1.1.3.jar",
    f"local:{KAFKA_JAR_DIR}/com.google.code.findbugs_jsr305-3.0.0.jar",
    f"local:{KAFKA_JAR_DIR}/org.lz4_lz4-java-1.8.0.jar",
    f"local:{KAFKA_JAR_DIR}/org.slf4j_slf4j-api-2.0.7.jar",
    f"local:{KAFKA_JAR_DIR}/org.xerial.snappy_snappy-java-1.1.10.5.jar",
    f"local:{KAFKA_JAR_DIR}/org.apache.hadoop_hadoop-client-api-3.3.4.jar",
    f"local:{KAFKA_JAR_DIR}/org.apache.hadoop_hadoop-client-runtime-3.3.4.jar",
])
ICEBERG_JARS = ",".join([
    f"local:{JAR_DIR}/com.amazonaws_aws-java-sdk-bundle-1.12.262.jar",
    f"local:{JAR_DIR}/org.apache.hadoop_hadoop-aws-3.3.4.jar",
    f"local:{JAR_DIR}/org.apache.iceberg_iceberg-aws-bundle-1.10.2.jar",
    f"local:{JAR_DIR}/org.apache.iceberg_iceberg-spark-runtime-3.5_2.12-1.10.2.jar",
    f"local:{JAR_DIR}/org.projectnessie.nessie-integrations_nessie-spark-extensions-3.5_2.12-0.105.7.jar",
    f"local:{JAR_DIR}/org.wildfly.openssl_wildfly-openssl-1.0.7.Final.jar",
])

GENERATOR_ENV = {
    "KAFKA_BOOTSTRAP_SERVERS": KAFKA,
    "SHOPSTREAM_MASTER_DATA_DIR": MASTER_DATA,
}
SPARK_ENV = {
    "KAFKA_BOOTSTRAP_SERVERS": KAFKA,
    "SHOPSTREAM_DATA_ROOT": ROOT,
    "SHOPSTREAM_BRONZE_CHECKPOINT_PATH": CHECKPOINT,
    "SHOPSTREAM_BRONZE_STARTING_OFFSETS_FILE": BRONZE_INITIAL_OFFSETS,
    "SHOPSTREAM_BRONZE_CHECKPOINT_GUARD_PATH": CHECKPOINT_GUARD,
    "SPARK_MASTER": "spark://spark-master:7077",
}
ICEBERG_ENV = {
    **SPARK_ENV,
    "NESSIE_URI": "http://nessie:19120/api/v2",
    "NESSIE_BRANCH": "main",
    "OBJECT_STORE_PROVIDER": "seaweedfs",
    "SEAWEEDFS_ENDPOINT": os.getenv("SEAWEEDFS_ENDPOINT", "http://seaweedfs:8333"),
    "SEAWEEDFS_BUCKET": os.environ["SEAWEEDFS_BUCKET"],
    "SEAWEEDFS_ACCESS_KEY": os.environ["SEAWEEDFS_ACCESS_KEY"],
    "SEAWEEDFS_SECRET_KEY": os.environ["SEAWEEDFS_SECRET_KEY"],
    "ICEBERG_NAMESPACE_PREFIX": ICEBERG_PREFIX,
}


def generator_task(task_id, filename, *, duration=None, extra_env=None, timeout=None):
    env = dict(GENERATOR_ENV)
    if duration is not None:
        env["SHOPSTREAM_GENERATOR_DURATION_SECONDS"] = str(duration)
    if extra_env:
        env.update(extra_env)
    return BashOperator(
        task_id=task_id,
        bash_command=f"exec python {GENERATOR_DIR}/{filename}",
        env=env,
        append_env=True,
        execution_timeout=timedelta(seconds=timeout or 600),
        retries=0,
    )


def spark_task(task_id, job, *, env=None, kafka=False, timeout_hours=2):
    conf = {**DRIVER_NETWORK, **TIME_CONF, **PYTHON_CONF}
    if kafka:
        conf["spark.jars"] = KAFKA_JARS
    conf["spark.hadoop.fs.permissions.umask-mode"] = "0002"
    return SparkSubmitOperator(
        task_id=task_id,
        application=f"/opt/spark-apps/jobs/{job}",
        conn_id=SPARK_CONN,
        name=f"shopstream-daily-{task_id}",
        deploy_mode="client",
        conf=conf,
        env_vars={**SPARK_ENV, **(env or {})},
        execution_timeout=timedelta(hours=timeout_hours),
        retries=0,
        verbose=False,
    )


def iceberg_task(task_id, mode):
    return SparkSubmitOperator(
        task_id=task_id,
        application="/opt/spark-apps/jobs/iceberg_catalog.py",
        conn_id=SPARK_CONN,
        name=f"shopstream-daily-{task_id}",
        deploy_mode="client",
        conf={
            **DRIVER_NETWORK,
            **TIME_CONF,
            **PYTHON_CONF,
            "spark.jars": ICEBERG_JARS,
            "spark.sql.shuffle.partitions": "4",
            "spark.default.parallelism": "8",
        },
        env_vars={
            **ICEBERG_ENV,
            "ICEBERG_CATALOG_MODE": mode,
        },
        execution_timeout=timedelta(hours=2),
        retries=0,
        verbose=False,
    )


with DAG(
    dag_id="shopstream_daily",
    description="Daily five-minute Kafka generation through Nessie Iceberg verification.",
    start_date=pendulum.datetime(2026, 11, 13, tz="Asia/Kolkata"),
    schedule="0 0 * * *",
    catchup=False,
    max_active_runs=1,
    max_active_tasks=8,
    is_paused_upon_creation=True,
    default_args={"owner": "shopstream", "retries": 0},
    tags=["shopstream", "daily", "spark", "iceberg"],
) as dag:
    snapshot_offsets = BashOperator(
        task_id="capture_kafka_start_offsets",
        bash_command=f"exec python {GENERATOR_DIR}/capture_kafka_start_offsets.py",
        env={
            "KAFKA_BOOTSTRAP_SERVERS": KAFKA,
            "SHOPSTREAM_BRONZE_INITIAL_OFFSETS_FILE": BRONZE_INITIAL_OFFSETS,
            "SHOPSTREAM_PAYMENT_INITIAL_OFFSETS_FILE": PAYMENT_INITIAL_OFFSETS,
            "SHOPSTREAM_PAYMENT_INITIAL_OFFSETS_GUARD": PAYMENT_OFFSETS_GUARD,
        },
        append_env=True,
        execution_timeout=timedelta(minutes=2),
        retries=0,
    )

    customers = generator_task("generate_customers", "customer_generator.py")
    products = generator_task("generate_products", "product_generator.py")
    continuous_generators = generator_task(
        "run_continuous_generators",
        "run_continuous_generators.py",
        duration=300,
        extra_env={
            "PAYMENT_CONSUMER_GROUP": "shopstream-payment-daily",
            "SHOPSTREAM_PAYMENT_INITIAL_OFFSETS_FILE": PAYMENT_INITIAL_OFFSETS,
            "SHOPSTREAM_GENERATOR_SHUTDOWN_GRACE_SECONDS": "20",
        },
        timeout=420,
    )

    bronze = spark_task("bronze_ingestion", "bronze_ingestion.py", kafka=True)
    bronze_quality = spark_task("bronze_quality", "bronze_validation.py")
    silver = spark_task("silver_v2", "silver_transformations.py")
    silver_quality = spark_task("silver_quality", "silver_validation.py")
    gold = spark_task("gold_v2", "gold_analytics.py")
    gold_quality = spark_task("gold_quality", "gold_validation.py")
    iceberg_publish = iceberg_task("iceberg_publish", "publish")
    iceberg_verify = iceberg_task("iceberg_readback_verification", "verify")

    snapshot_offsets >> customers >> products
    products >> continuous_generators
    (continuous_generators >> bronze >> bronze_quality >> silver >> silver_quality
     >> gold >> gold_quality >> iceberg_publish >> iceberg_verify)
