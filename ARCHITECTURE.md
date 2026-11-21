# ShopStream Architecture

## Verified data path

```text
Python event generators → Kafka topics → Spark Structured Streaming → Bronze Parquet
                                                                    ├→ original Customer Silver
                                                                    └→ isolated Silver v2 → Gold v2
```

Kafka uses `kafka:19092` inside Docker and `localhost:9092` for host producers. Spark code/data/checkpoint mounts are `/opt/spark-apps`, `/opt/spark-data`, and `/opt/spark-checkpoints`.

## Preserved infrastructure

The original Compose file runs Kafka and Spark. Existing volumes and checkpoints were preserved. The old Customer Silver job overwrites its destination and was not rerun. Older Airflow containers are separate and were not modified.

## New transformations

- Silver v2 applies explicit schemas, type and required-field checks, deduplication, reference validation, lineage fields and multi-reason quarantine.
- Gold v2 publishes ten batch analytics outputs. Paid revenue uses successful payments matched to accepted orders; website funnel events are separate.
- Silver and Gold use separate output roots, avoiding overwrite of the original Customer Silver.

## Orchestration and lakehouse

- A separate Airflow 3.3.1 Compose project is healthy on port 8082, with its own Postgres and logs. It coexists with the older Airflow on port 8080. Admin bootstrap uses the FAB auth manager explicitly; task execution uses the internal API URL and a shared JWT signing key.
- Airflow's local smoke DAG and read-only Silver v2 validation DAG passed. The daily Silver → validation → Gold → validation DAG is registered but paused because it writes validated v2 paths. Airflow does not launch long-running streaming jobs.
- The isolated lakehouse stack uses SeaweedFS 4.52 (`weed mini`) with an S3 gateway on host port 8333. Its `/data` directory is a dedicated named volume; S3 credentials are supplied through the ignored `.env` file. SeaweedFS and Nessie database volumes are separate from MinIO and all application data volumes.
- Spark 3.5.9/Java 11 connects to SeaweedFS through Hadoop S3A 3.3.4 and Iceberg S3FileIO. Iceberg 1.10.2 and Nessie Spark extensions 0.105.7 were selected after verifying Java 11 compatibility. Nessie 0.109.0 uses PostgreSQL 16 for its version store.
- The isolated canary table is `nessie.integration_canary.persistence_check` under the `main` Nessie branch. Three records were inserted and read; the table exposes two snapshots, two data files and three metadata log entries. After restarting SeaweedFS, Nessie PostgreSQL and Nessie, verify-only mode read the records and snapshot again, and Nessie REST still listed the namespace and table.
- The existing MinIO AIStor service definition and named volume remain intact and inactive; the license prerequisite is still unavailable. The canary does not point at MinIO.

## Docker and persistence

- Docker Desktop's disk image is at `E:\DockerData`; the migration is complete. Do not migrate it again.
- The final canary resource sample showed SeaweedFS at about 203 MiB, Nessie at about 317 MiB and Nessie PostgreSQL at about 27 MiB under Docker's 7.679 GiB memory limit. C: had 34.7 GiB free and E: had 324.4 GiB free.
- Kafka/Spark/legacy Airflow and the new Airflow project use distinct Compose resources. Preserve all named volumes and Spark checkpoints; never use volume-removal or prune commands for routine work.
- See [DOCKER_SETUP.md](DOCKER_SETUP.md) for start/health commands and project boundaries.

See [PROJECT_STATUS.md](PROJECT_STATUS.md) and [LAKEHOUSE.md](LAKEHOUSE.md).
