# ShopStream

ShopStream is an e-commerce data engineering project using Python event generators, Kafka, Spark Structured Streaming and a medallion layout.

## Current state

Kafka → Spark → Bronze and the original Customer Silver were audited without modifying their data. New isolated Silver v2 transformations, data-quality checks, ten Gold analytics outputs and their validators passed on Spark 3.5.9. See [PROJECT_STATUS.md](PROJECT_STATUS.md) for counts and limitations.

A separate ShopStream Airflow 3.3.1 stack is healthy on port 8082 and leaves legacy Airflow on 8080 untouched. Its local smoke DAG and read-only Spark validation DAG succeeded; the production Silver/Gold DAG is paused to protect validated outputs. An isolated SeaweedFS + Spark 3.5.9 + Iceberg + Nessie canary passed writes, reads, snapshot checks, and persistence verification after restarting SeaweedFS and Nessie services. The existing MinIO configuration remains intact. See [Airflow documentation](airflow/README.md), [LAKEHOUSE.md](LAKEHOUSE.md), and [DOCKER_SETUP.md](DOCKER_SETUP.md).

The canary table `nessie.integration_canary.persistence_check` contains three demonstration rows and is isolated from all Bronze, Silver, Gold and Airflow data. Run the write or verification canary with configured values in the ignored `.env` file:

```powershell
docker compose --env-file .env -f docker-compose.lakehouse.yml --profile canary run --rm iceberg-canary
$env:ICEBERG_CANARY_MODE = 'verify'
docker compose --env-file .env -f docker-compose.lakehouse.yml --profile canary run --rm iceberg-canary
Remove-Item Env:ICEBERG_CANARY_MODE
```

The canary uses only its dedicated Nessie namespace and SeaweedFS prefix. Do not use the write mode as a substitute for a production ingestion run.

## Run data jobs

Use `spark/jobs/silver_transformations.py`, then `silver_validation.py`, then `gold_analytics.py` and `gold_validation.py` against the existing Spark cluster. These jobs have already been validated; outputs are isolated under `data/silver_v2` and `data/gold_v2` in the mounted data path. The older Customer Silver job overwrites its destination; avoid rerunning it unless intentionally replacing its output. The Airflow production DAG writes v2 paths too, so it stays paused until an intentional refresh is scheduled.

## Safety

Do not delete Kafka volumes, reset Kafka offsets, or clear Spark checkpoints. The generator and original Bronze/Silver data were preserved. Detailed architecture is in [ARCHITECTURE.md](ARCHITECTURE.md); audit evidence is in [AUDIT_REPORT.md](AUDIT_REPORT.md).

## Known data limitations

Generated data is simulated. Some order product IDs do not match the product master and are quarantined; payments that refer to rejected orders are also quarantined. Website PURCHASE is not proof of a paid order. Payment publishing can duplicate after a crash between publish and offset commit. Customer `created_at` is not guaranteed to be an update timestamp.
