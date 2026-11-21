"""Side-effect-free smoke check for the isolated ShopStream Airflow deployment."""
import pendulum
from airflow.sdk import dag, task


@dag(
    dag_id="shopstream_airflow_smoke",
    description="Verify Airflow can parse, schedule and run a local no-op task.",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=["shopstream", "smoke"],
)
def shopstream_airflow_smoke():
    @task(task_id="runtime_check")
    def runtime_check():
        print("SHOPSTREAM_AIRFLOW_SMOKE_OK")
        return "ok"

    runtime_check()


shopstream_airflow_smoke()
