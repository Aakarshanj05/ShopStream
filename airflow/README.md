# ShopStream Airflow

The separate Airflow 3.3.1 Compose project uses its own Postgres and logs volumes and UI port 8082. It joins the existing `shopstream_default` network to submit bounded Spark jobs. The legacy Airflow environment on port 8080 is separate and remains operational.

## Verified configuration and bootstrap

Airflow uses `apache-airflow-providers-fab` and sets `AIRFLOW__CORE__AUTH_MANAGER=airflow.providers.fab.auth_manager.fab_auth_manager.FabAuthManager`. Airflow 3 otherwise defaults to SimpleAuthManager; the FAB `airflow users create` command calls `find_role`, which is not present on the Simple manager. Explicitly selecting FAB fixed administrator creation while preserving the migrated metadata DB.

The init command migrates the DB and checks whether the configured admin user already exists before creating it, so reruns are idempotent. Task execution uses `http://airflow-api-server:8080/execution/` inside Docker. `AIRFLOW__API_AUTH__JWT_SECRET` is sourced from the same generated `AIRFLOW_JWT_SECRET` value in the scheduler and API server; both settings are needed for task execution across containers.

1. Copy root `.env.example` to `.env` and set strong unique values for the DB, admin, Fernet and JWT secrets. `.env` is ignored by Git.
2. Validate with `docker compose --env-file .env -f airflow/docker-compose.shopstream.yml config --quiet`.
3. Start with `docker compose --env-file .env -f airflow/docker-compose.shopstream.yml up -d`.
4. Check `docker compose --env-file .env -f airflow/docker-compose.shopstream.yml ps` and open `http://localhost:8082`.

## DAGs and safety

- `shopstream_silver_gold_quality` is the daily Silver → validation/reconciliation → Gold → validation workflow. It is intentionally paused: those jobs write the already validated v2 roots and should only run as an intentional refresh.
- `shopstream_airflow_smoke` checks local task execution without touching project data.
- `shopstream_spark_validation_smoke` submits the read-only `spark/jobs/silver_validation.py` validator to the existing Spark cluster. Fixed client-mode driver ports support container networking.
- Kafka ingestion is not scheduled as a repeated long-running task. Do not regenerate source datasets or restart streaming jobs through Airflow.

## Verification record

Airflow API version returned `3.3.1`; DB migration and idempotent admin bootstrap succeeded; all DAGs parsed with an empty import-error list. Both smoke DAGs completed successfully, including the SparkSubmitOperator task. The production DAG and completed smoke DAGs were paused after verification. The new stack uses port 8082 and separate resources; the old port 8080 environment was not changed.

Never run `down -v` on either Airflow project. See [DOCKER_SETUP.md](../DOCKER_SETUP.md) for service and volume boundaries.
