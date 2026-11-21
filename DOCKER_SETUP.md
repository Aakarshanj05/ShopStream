# ShopStream Docker Setup and Operations

## Docker Desktop storage

Docker Desktop's disk image was migrated from `C:\Users\hp\AppData\Local\Docker\wsl` to `E:\DockerData`. The migration is complete; do not repeat it or manually move/delete VHDX files. Check current free space and Docker resource use before image builds or new services:

At the latest read-only check (2026-11-06), C: had about 37.7 GiB free and E: about 326.3 GiB free. Docker 29.9.2 reported about 8.2 GiB memory. Docker listed 23 images (8.678 GB), 19 containers (15 active), 8 volumes (1.71 GB), and 760.5 MB build cache. No cleanup was run.

```powershell
Get-PSDrive C,E | Select-Object Name, @{n='FreeGiB';e={[math]::Round($_.Free/1GB,1)}}
docker info --format 'Docker={{.ServerVersion}} Memory={{.MemTotal}}'
docker system df
```

These are read-only checks. Do not use `docker system prune`, `docker volume prune`, or `docker compose down -v` for routine troubleshooting.

## Existing Kafka and Spark project

The root `docker-compose.yml` owns Kafka, Spark Master and Spark Worker. It uses the named volumes `kafka_data` and `spark_dependencies` and host mounts for `data`, `checkpoints`, and `spark`. Verify existing services without recreating them:

```powershell
docker ps --filter "name=shopstream-kafka" --filter "name=shopstream-spark-"
docker compose ps
```

Kafka host address is `localhost:9092`, Compose network address is `kafka:19092`; Spark Master UI is `http://localhost:8081`. Preserve Kafka topics/offsets, Bronze data and Spark checkpoints.

## Separate ShopStream Airflow

Airflow is a distinct Compose project named `shopstream-airflow`; it uses the external `shopstream_default` network, its own `airflow_db_data` and `airflow_logs` volumes, and host port 8082. The existing Airflow on port 8080 is a separate environment and must remain untouched.

```powershell
docker compose --env-file .env -f airflow/docker-compose.shopstream.yml ps
docker compose --env-file .env -f airflow/docker-compose.shopstream.yml config --quiet
```

The new stack is already initialized. `up -d` may be used for normal service startup after checking disk space; do not add `--build` unless the Airflow Dockerfile/dependencies have changed. Check `http://localhost:8082/api/v2/version` for the API. The production write DAG remains paused until an intentional refresh is needed.

## Lakehouse project (not started)

`docker-compose.lakehouse.yml` has separate Nessie Postgres and MinIO data volumes and shares only the external Spark dependency volume/network. It currently configures licensed MinIO AIStor and is intentionally not running. Do not start it until an S3 object-store choice is approved and the Compose plan is updated. See [LAKEHOUSE.md](LAKEHOUSE.md).

## Volume and service safety

- Never run `docker compose down -v`, `docker system prune`, `docker volume prune`, Docker factory reset, or manual VHDX deletion.
- Do not recreate Kafka/Spark services just to inspect state. Prefer `docker ps`, Compose `ps`, health endpoints and logs.
- Keep Airflow 8080/8082 projects separate. Avoid collisions on ports 9000/9001 until a lakehouse storage option is selected.
- Before starting additional containers, check disk and Docker usage; keep the current Docker disk image on E:.
