# Challenges and Resolutions

## Empty customer quarantine Parquet file

The original customer quarantine part file occupied 1,763 bytes, but Spark counted zero rows and found no rejection reasons. Parquet metadata exists even when a file has no rows; file size is not a row count.

## Overwrite behavior in the existing customer job

The original Customer Silver job uses `mode("overwrite")`; rerunning could replace outputs. It was not rerun. New Silver work writes under isolated `silver_v2` paths. The Airflow Silver/Gold DAG also writes those validated v2 roots and remains paused until an intentional refresh is approved.

## Product and order reference mismatches

Some order events refer to product IDs such as `PROD503`, while the product master uses zero-padded IDs such as `PROD00503`. Those orders are quarantined with explicit reasons. Payment events referencing rejected orders are quarantined as unknown-order references. Reconciliation accounts for every Bronze row.

## Customer duplicate timestamp semantics

Deduplication prioritizes `created_at`, then Kafka timestamp/partition/offset. `created_at` is not guaranteed to represent updates, and Kafka metadata cannot determine business recency. This limitation remains pending a reliable update/version field.

## Payment and website events

Payment publish and offset commit are separate operations, so a crash between them can cause duplicate messages. Website purchase events are independent of order/payment records. Gold keeps successful-payment revenue and website funnel counts separate; exactly-once behavior is not claimed.

## Airflow 3 authentication-manager mismatch

**Root cause:** Airflow 3.3.1 defaults to SimpleAuthManager, but initialization called the FAB CLI `airflow users create`. That FAB command calls `find_role`; Airflow's `AirflowSecurityManagerV2` does not implement that FAB method.

**Resolution:** Explicitly configure `airflow.providers.fab.auth_manager.fab_auth_manager.FabAuthManager` with the installed FAB provider. Keep the already migrated metadata DB and make admin creation idempotent by checking users before creating the configured account. Bootstrap then succeeded without DB reset.

## Airflow task execution networking and JWT

The first local task attempted to call `localhost:8080` from inside its task container. Configure the execution API URL as `http://airflow-api-server:8080/execution/`. The next run returned 401 because the scheduler and API server had different generated JWT defaults. Set one generated `AIRFLOW_JWT_SECRET` consistently for both components. The local smoke DAG and read-only SparkSubmit validation DAG then succeeded.

The new Airflow API/scheduler/DAG processor/triggerer are healthy on port 8082, and DAG import errors are empty. The existing Airflow on 8080 was not changed. No Docker volume was removed. The Docker disk image migration to `E:\DockerData` is complete; no second migration was attempted.

## SeaweedFS, Spark S3A, Iceberg and Nessie canary

**Compatibility finding:** the Spark 3.5.9 image runs Java 11. Iceberg 1.12.0 artifacts include Java 17 bytecode (class-file version 61), which the Java 11 runtime cannot load (maximum class-file version 55). The canary therefore uses Iceberg 1.10.2 and Nessie Spark extensions 0.105.7, with Hadoop AWS 3.3.4 matching the Spark image's Hadoop client. Dependencies are loaded before Spark starts through `spark-submit --packages`; placing `spark.jars.packages` on the already-created SparkSession builder was too late for extension class loading.

**Repository and bucket setup:** Maven downloads through the default `repo1.maven.org` route returned a truncated AWS SDK bundle. The canary's Ivy settings use `repo.maven.apache.org`; the downloaded Hadoop AWS dependency was verified as a valid JAR. SeaweedFS `weed mini` started its S3 gateway and recognized credentials from environment variables, but did not provision the requested bucket on startup, so the canary creates the dedicated bucket through the S3 API.

**Canary behavior:** a first distributed S3A text write stalled while its executor performed the probe. The final probe writes and reads a single isolated sentinel with Hadoop's configured S3A filesystem on the Spark driver. Iceberg row scans and snapshot metadata are still executed through Spark on the existing cluster. A subsequent read-back failure was a newline escaping mistake in the sentinel itself; correcting it made S3A write/read pass. Nessie logged a transient commit operation timeout while initializing its namespace, retried successfully, and completed the table commit.

**Verified result:** the write run created `nessie.integration_canary.persistence_check`, inserted and queried three rows, and reported two snapshots (latest ID `7286329699555809498`), two data files and three metadata-log entries. After restarting SeaweedFS, Nessie PostgreSQL and Nessie, verify-only mode read the same three rows and snapshot. Nessie's REST entries endpoint continued to list the namespace and table. Final resource sample: SeaweedFS ~203 MiB, Nessie ~317 MiB, Nessie PostgreSQL ~27 MiB, under the 7.679 GiB Docker memory limit; C: 34.7 GiB free and E: 324.4 GiB free.

The existing MinIO AIStor service definition and named volume remain intact and inactive because its license is unavailable. The canary uses the separate SeaweedFS volume and does not use MinIO. See [LAKEHOUSE.md](LAKEHOUSE.md) for the tested configuration and repeat commands. No Bronze, Silver, Gold, Kafka, Spark checkpoint or Airflow data was modified.
