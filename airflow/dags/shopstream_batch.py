
"""End-to-end isolated Kafka → Parquet → Iceberg/Nessie workflow.

This DAG deliberately uses a separate data/checkpoint root and integration
Nessie namespaces. It never writes the validated data/bronze, data/silver_v2,
or data/gold_v2 paths. Keep it paused until the isolated validation passes.
"""

from datetime import timedelta
import os

import pendulum
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.standard.operators.python import BranchPythonOperator
from airflow.task.trigger_rule import TriggerRule


SPARK_CONNECTION = "spark_default"

INTEGRATION_ROOT = "/opt/spark-data/lakehouse_integration_run_20261113_02"
INTEGRATION_CHECKPOINT = f"{INTEGRATION_ROOT}/checkpoints/bronze_ingestion"
INTEGRATION_LANDING = f"{INTEGRATION_ROOT}/bronze/raw_events"
INTEGRATION_BRONZE_EVENTS = f"{INTEGRATION_ROOT}/bronze/events"
INTEGRATION_BRONZE_QUARANTINE = f"{INTEGRATION_ROOT}/bronze/quarantine/events"
INTEGRATION_NAMESPACE_PREFIX = "integration_run_20261113_02"

INTEGRATION_TOPIC_MAP = {
    "customers": "shopstream_it_customers",
    "products": "shopstream_it_products",
    "orders": "shopstream_it_orders",
    "payments": "shopstream_it_payments",
    "inventory": "shopstream_it_inventory",
    "website_activity": "shopstream_it_website_activity",
}

INTEGRATION_TOPIC_ENV = {
    f"SHOPSTREAM_TOPIC_{entity.upper()}": topic
    for entity, topic in INTEGRATION_TOPIC_MAP.items()
}

INTEGRATION_ENV = {
    "SHOPSTREAM_DATA_ROOT": INTEGRATION_ROOT,
    "SPARK_MASTER": "spark://spark-master:7077",
    **INTEGRATION_TOPIC_ENV,
    "SHOPSTREAM_APPROVED_EMPTY_TOPIC": INTEGRATION_TOPIC_MAP["orders"],
    "SHOPSTREAM_APPROVED_EMPTY_PARTITION": "0",
    "SHOPSTREAM_APPROVED_EMPTY_OFFSET": "1",
}

DRIVER_NETWORK = {
    "spark.driver.host": "shopstream-airflow-scheduler",
    "spark.driver.bindAddress": "0.0.0.0",
    "spark.driver.port": "37000",
    "spark.blockManager.port": "37001",
}

TIME_ZONE = {
    "spark.sql.session.timeZone": "UTC",
}

SPARK_PYTHON_CONF = {
    "spark.pyspark.python": "/usr/bin/python3.11",
    "spark.pyspark.driver.python": "/home/airflow/.local/bin/python",
    "spark.executorEnv.PYSPARK_PYTHON": "/usr/bin/python3.11",
}

IVY_CONF = {
    "spark.jars.ivy": "/tmp/shopstream-ivy",
    "spark.jars.ivySettings": "/opt/spark-apps/jobs/ivysettings-shopstream.xml",
}

# Explicit JAR paths: avoid passing an unresolved *.jar wildcard to Spark.
JAR_DIR = "/opt/spark/dependencies/shopstream-runtime-1.10.2-0.105.7"

CURATED_ICEBERG_JARS = ",".join([
    f"local:{JAR_DIR}/com.amazonaws_aws-java-sdk-bundle-1.12.262.jar",
    f"local:{JAR_DIR}/org.apache.hadoop_hadoop-aws-3.3.4.jar",
    f"local:{JAR_DIR}/org.apache.iceberg_iceberg-aws-bundle-1.10.2.jar",
    f"local:{JAR_DIR}/org.apache.iceberg_iceberg-spark-runtime-3.5_2.12-1.10.2.jar",
    f"local:{JAR_DIR}/org.projectnessie.nessie-integrations_nessie-spark-extensions-3.5_2.12-0.105.7.jar",
    f"local:{JAR_DIR}/org.wildfly.openssl_wildfly-openssl-1.0.7.Final.jar",
])

# Kafka connector and transitive dependencies are pre-cached in the shared
# Docker volume mounted by both the Airflow scheduler and Spark workers.
KAFKA_JAR_DIR = "/opt/spark/dependencies/ivy/jars"

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

COMMON_ENV = {
    "SHOPSTREAM_DATA_ROOT": INTEGRATION_ROOT,
    "SPARK_MASTER": "spark://spark-master:7077",
}

ICEBERG_ENV = {
    **COMMON_ENV,
    "NESSIE_URI": "http://nessie:19120/api/v2",
    "NESSIE_BRANCH": "main",
    "OBJECT_STORE_PROVIDER": "seaweedfs",
    "SEAWEEDFS_ENDPOINT": os.getenv(
        "SEAWEEDFS_ENDPOINT",
        "http://seaweedfs:8333",
    ),
    "SEAWEEDFS_BUCKET": os.environ["SEAWEEDFS_BUCKET"],
    "SEAWEEDFS_ACCESS_KEY": os.environ["SEAWEEDFS_ACCESS_KEY"],
    "SEAWEEDFS_SECRET_KEY": os.environ["SEAWEEDFS_SECRET_KEY"],
    "ICEBERG_NAMESPACE_PREFIX": INTEGRATION_NAMESPACE_PREFIX,
}


def iceberg_catalog_task(
    task_id,
    mode,
    data_root,
    namespace_prefix,
    retries=None,
    trigger_rule=None,
    include_bronze_quarantine=False,
):
    """Create a publish or read-only verification task for a Parquet root."""

    task_options = {}

    if retries is not None:
        task_options["retries"] = retries

    if trigger_rule is not None:
        task_options["trigger_rule"] = trigger_rule

    spark_conf = {
        **DRIVER_NETWORK,
        **TIME_ZONE,
        **SPARK_PYTHON_CONF,
        **IVY_CONF,
        "spark.jars": CURATED_ICEBERG_JARS,
    }

    if task_id == "iceberg_publish_existing_parquet":
        spark_conf.update({
            "spark.executor.memory": "1400m",
            "spark.executor.cores": "1",
            "spark.sql.shuffle.partitions": "4",
            "spark.default.parallelism": "8",
            "spark.sql.adaptive.enabled": "true",
        })

    return SparkSubmitOperator(
        task_id=task_id,
        application="/opt/spark-apps/jobs/iceberg_catalog.py",
        conn_id=SPARK_CONNECTION,
        name=f"shopstream-iceberg-{task_id}",
        deploy_mode="client",
        conf=spark_conf,
        env_vars={
            **ICEBERG_ENV,
            "SHOPSTREAM_DATA_ROOT": data_root,
            "ICEBERG_NAMESPACE_PREFIX": namespace_prefix,
            "ICEBERG_CATALOG_MODE": mode,
            **(
                {"ICEBERG_INCLUDE_BRONZE_QUARANTINE": "true"}
                if include_bronze_quarantine
                else {}
            ),
        },
        execution_timeout=timedelta(hours=2),
        **task_options,
        verbose=False,
    )


def select_execution_branch(**context):
    """Choose one isolated branch or preserve both branches for default runs."""

    run_mode = (context["dag_run"].conf or {}).get("run_mode")

    if run_mode == "integration_only":
        return "bronze_ingestion_isolated"

    if run_mode == "baseline_only":
        return "iceberg_publish_existing_parquet"

    if run_mode is None:
        return [
            "bronze_ingestion_isolated",
            "iceberg_publish_existing_parquet",
        ]

    raise ValueError(f"Unsupported ShopStream run_mode: {run_mode}")


with DAG(
    dag_id="shopstream_silver_gold_quality",
    description=(
        "Isolated Kafka-to-Iceberg workflow with Bronze/Silver/Gold "
        "quality gates and Nessie read-back verification."
    ),
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    max_active_tasks=1,
    is_paused_upon_creation=True,
    default_args={
        "owner": "shopstream",
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
        "execution_timeout": timedelta(hours=2),
    },
    tags=[
        "shopstream",
        "spark",
        "data-quality",
        "iceberg",
        "isolated",
    ],
    doc_md=(
        "Uses only `/opt/spark-data/lakehouse_integration_run_20261113_02` "
        "and the `integration_run_20261113_02_*` Nessie namespaces. Existing "
        "integration and production Parquet outputs and checkpoints are "
        "not inputs or targets."
    ),
) as dag:

    execution_scope = BranchPythonOperator(
        task_id="select_execution_branch",
        python_callable=select_execution_branch,
    )

    # ---------------------------------------------------------
    # BRONZE: Kafka -> isolated Parquet
    # ---------------------------------------------------------

    bronze = SparkSubmitOperator(
        task_id="bronze_ingestion_isolated",
        application="/opt/spark-apps/jobs/bronze_ingestion.py",
        conn_id=SPARK_CONNECTION,
        name="shopstream-bronze-integration",
        deploy_mode="client",
        conf={
            **DRIVER_NETWORK,
            **TIME_ZONE,
            **SPARK_PYTHON_CONF,
            "spark.jars": KAFKA_JARS,

            # Preserve group-write permission on Hadoop local filesystem paths.
            "spark.hadoop.fs.permissions.umask-mode": "0002",
        },
        env_vars={
            **INTEGRATION_ENV,
            "SHOPSTREAM_BRONZE_OUTPUT_PATH": INTEGRATION_BRONZE_EVENTS,
            "SHOPSTREAM_BRONZE_LANDING_PATH": INTEGRATION_LANDING,
            "SHOPSTREAM_BRONZE_QUARANTINE_PATH": INTEGRATION_BRONZE_QUARANTINE,
            "SHOPSTREAM_BRONZE_CHECKPOINT_PATH": INTEGRATION_CHECKPOINT,
        },
        execution_timeout=timedelta(hours=2),
        verbose=False,
    )

    # ---------------------------------------------------------
    # BRONZE QUALITY
    # ---------------------------------------------------------

    bronze_quality = SparkSubmitOperator(
        task_id="bronze_quality",
        application="/opt/spark-apps/jobs/bronze_validation.py",
        conn_id=SPARK_CONNECTION,
        name="shopstream-bronze-quality-integration",
        deploy_mode="client",
        conf={
            **DRIVER_NETWORK,
            **TIME_ZONE,
            **SPARK_PYTHON_CONF,
        },
        env_vars={
            **INTEGRATION_ENV,
            "SHOPSTREAM_BRONZE_LANDING_PATH": INTEGRATION_LANDING,
            "SHOPSTREAM_BRONZE_QUARANTINE_PATH": INTEGRATION_BRONZE_QUARANTINE,
        },
        verbose=False,
    )

    # ---------------------------------------------------------
    # SILVER V2
    # ---------------------------------------------------------

    silver = SparkSubmitOperator(
        task_id="silver_v2",
        application="/opt/spark-apps/jobs/silver_transformations.py",
        conn_id=SPARK_CONNECTION,
        name="shopstream-silver-v2-integration",
        deploy_mode="client",
        conf={
            **DRIVER_NETWORK,
            **TIME_ZONE,
            **SPARK_PYTHON_CONF,
            "spark.hadoop.fs.permissions.umask-mode": "0002",
        },
        env_vars=INTEGRATION_ENV,
        verbose=False,
    )

    silver_quality = SparkSubmitOperator(
        task_id="silver_quality_and_reconciliation",
        application="/opt/spark-apps/jobs/silver_validation.py",
        conn_id=SPARK_CONNECTION,
        name="shopstream-silver-quality-integration",
        deploy_mode="client",
        conf={
            **DRIVER_NETWORK,
            **TIME_ZONE,
            **SPARK_PYTHON_CONF,
        },
        env_vars=INTEGRATION_ENV,
        verbose=False,
    )

    # ---------------------------------------------------------
    # GOLD V2
    # ---------------------------------------------------------

    gold = SparkSubmitOperator(
        task_id="gold_v2",
        application="/opt/spark-apps/jobs/gold_analytics.py",
        conn_id=SPARK_CONNECTION,
        name="shopstream-gold-v2-integration",
        deploy_mode="client",
        conf={
            **DRIVER_NETWORK,
            **TIME_ZONE,
            **SPARK_PYTHON_CONF,
            "spark.hadoop.fs.permissions.umask-mode": "0002",
        },
        env_vars=INTEGRATION_ENV,
        verbose=False,
    )

    gold_quality = SparkSubmitOperator(
        task_id="gold_quality",
        application="/opt/spark-apps/jobs/gold_validation.py",
        conn_id=SPARK_CONNECTION,
        name="shopstream-gold-quality-integration",
        deploy_mode="client",
        conf={
            **DRIVER_NETWORK,
            **TIME_ZONE,
            **SPARK_PYTHON_CONF,
        },
        env_vars=INTEGRATION_ENV,
        verbose=False,
    )

    # ---------------------------------------------------------
    # ICEBERG PUBLISH + READ-BACK VERIFICATION
    # ---------------------------------------------------------

    iceberg_publish = iceberg_catalog_task(
        "iceberg_publish",
        "publish",
        INTEGRATION_ROOT,
        INTEGRATION_NAMESPACE_PREFIX,
        include_bronze_quarantine=True,
    )

    iceberg_verify = iceberg_catalog_task(
        "iceberg_readback_verification",
        "verify",
        INTEGRATION_ROOT,
        INTEGRATION_NAMESPACE_PREFIX,
        include_bronze_quarantine=True,
    )

    # ---------------------------------------------------------
    # BASELINE BRANCH: already validated Parquet data
    # Do not modify this branch during integration testing.
    # ---------------------------------------------------------

    iceberg_publish_existing = iceberg_catalog_task(
        "iceberg_publish_existing_parquet",
        "publish",
        "/opt/spark-data",
        "baseline",
        retries=0,
    )

    iceberg_verify_existing = iceberg_catalog_task(
        "iceberg_verify_existing_parquet",
        "verify",
        "/opt/spark-data",
        "baseline",
        trigger_rule=TriggerRule.ALL_SUCCESS,
    )

    # ---------------------------------------------------------
    # DAG DEPENDENCIES
    # ---------------------------------------------------------

    execution_scope >> bronze

    (
        bronze
        >> bronze_quality
        >> silver
        >> silver_quality
        >> gold
        >> gold_quality
        >> iceberg_publish
        >> iceberg_verify
    )

    execution_scope >> iceberg_publish_existing >> iceberg_verify_existing
