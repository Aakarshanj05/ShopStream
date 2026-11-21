# ShopStream Audit Report

Audit date: 2026-11-06

## Scope and method

Read-only inspection of project code, Docker Compose, services, volumes, and Bronze/Silver Parquet outputs. Spark 3.5.9 was used for a read-only validation. No generators ran, no Kafka offsets/checkpoints reset, and existing datasets were not changed.

## Verified completed components

- Kafka KRaft is running as `shopstream-kafka` on host port 9092 using `apache/kafka:4.4.0`.
- Spark master and worker are running as `shopstream-spark-master` and `shopstream-spark-worker` using `apache/spark:3.5.9-python3`, ports 7077, 8081 and 8084.
- Docker volumes `shopstream_kafka_data` and `shopstream_spark_dependencies` exist. Compose mounts the latter at `/opt/spark/dependencies`.
- Six Python generators exist. Bronze streaming ingestion writes Kafka payload, topic, partition, offset and timestamps to topic-partitioned Parquet with a checkpoint and `availableNow=True`.
- Actual Bronze counts: customers 10,000; products 1,000; orders 26; payments 25; inventory 13; website activity 19. Total 11,083.
- Customer profiling, Bronze validation, and Spark/Kafka connectivity scripts exist.
- Customer Silver has an explicit JSON schema, normalization, validation, latest-record deduplication, lineage fields and count reconciliation.

## Customer Silver investigation

| Dataset | Rows |
| --- | ---: |
| Customer Bronze | 10,000 |
| Customer Silver | 10,000 |
| Customer quarantine | 0 |
| Silver + quarantine | 10,000 |

Reconciliation passes. No rejection reasons exist because quarantine has no rows. Its 1,763-byte Parquet part is an empty output part, not a non-empty dataset. The output shows no rejected or duplicate customers.

`spark/jobs/customer_silver_validation.py` performs read-only counts, rejection grouping, sample display and topic totals.

## Incomplete components

- Silver exists only for customers.
- No Gold jobs or SQL analytics scripts exist.
- No Iceberg configuration, MinIO or Nessie service exists.
- Airflow has only an empty `dags` directory; no Airflow configuration is in this workspace.
- No project documentation, requirements or environment example existed at audit time.
- No repeatable test harness exists beyond Spark validation scripts.

## Actual issues and risks

1. `customer_silver.py` overwrites both output paths. It was not rerun. New work will use separate versioned paths.
2. Customer deduplication prioritizes `created_at`, which is creation time, not a guaranteed update/version timestamp. Kafka timestamp/partition/offset only break ties.
3. Bronze ingestion uses `startingOffsets=earliest` and a persistent checkpoint. Do not reset it. `availableNow` is a bounded batch, not a continuously running service.
4. Bronze has many small Parquet files from trigger batches.
5. Website `PURCHASE` is an independent simulated event, not proof of a paid order.
6. Payment consumer can republish after a crash between payment publish and source offset commit; no durable idempotency store is visible.
7. Older Airflow containers are stopped (several exited 137). They are separate and will not be modified.
8. In this shell, explicit project directory entry and filesystem approval were required. Docker CLI was found outside PATH.

## Architecture gaps

Verified flow: Python producers → Kafka → Spark Structured Streaming → Bronze Parquet; customer Bronze → customer Silver Parquet. Gold, Iceberg, object storage, catalog and orchestration are absent.

## Recommended implementation order and effort

1. Isolated, idempotent Silver transformations for all domains with quality checks (1–2 days).
2. Gold metrics based on successful payments and independent session funnels (1 day).
3. Repeatable quality/reconciliation automation (0.5–1 day).
4. MinIO/Nessie/Iceberg configuration and live table validation (1–2 days).
5. New Airflow configuration/DAGs on port 8082, without touching old Airflow (0.5–1 day).
6. End-to-end validation and documentation (0.5–1 day).

Estimates assume dependencies are available and no compatibility issue blocks integration.

