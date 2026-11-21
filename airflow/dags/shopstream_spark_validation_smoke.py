"""Read-only Airflow-to-Spark integration smoke test."""
import pendulum
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator


with DAG(
    dag_id="shopstream_spark_validation_smoke",
    description="Run the read-only Silver v2 quality validator through SparkSubmit.",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    max_active_tasks=1,
    tags=["shopstream", "smoke", "spark", "read-only"],
) as dag:
    validate_silver = SparkSubmitOperator(
        task_id="silver_v2_read_only_validation",
        application="/opt/spark-apps/jobs/silver_validation.py",
        conn_id="spark_default",
        name="shopstream-airflow-spark-validation-smoke",
        deploy_mode="client",
        conf={
            "spark.driver.host": "shopstream-airflow-scheduler",
            "spark.driver.bindAddress": "0.0.0.0",
            "spark.driver.port": "37002",
            "spark.blockManager.port": "37003",
            "spark.sql.session.timeZone": "UTC",
        },
        verbose=False,
    )
