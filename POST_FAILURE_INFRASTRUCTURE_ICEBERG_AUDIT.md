# ShopStream Post-Failure Infrastructure and Iceberg Audit

**Investigation date:** 2026-11-08  
**Incident run IDs:** manual__baseline_only_20261108_01 and manual__baseline_only_20261108_02  
**Scope:** Read-only investigation. No DAG, service, configuration, metadata, source data, checkpoint, table, snapshot, or volume was changed.

## 1. Executive summary

Neither baseline publication attempt produced any of the 23 expected baseline Iceberg tables. The publisher targets reference **main**, using the warehouse prefix **s3://shopstream-warehouse/warehouse/baseline**. Nessie currently exposes only the main reference, and its catalog contains only the pre-existing integration canary namespace/table. All 23 expected baseline tables are absent; a signed, read-only SeaweedFS listing of the publisher’s baseline prefix returned zero object keys. Therefore, there are no current destination row counts, Iceberg metadata files, or snapshots to verify. “Absent” is not a zero-row table.

Run 02 failed during Spark executor startup. The driver had already resolved and cached the 280,645,251-byte AWS SDK bundle JAR, but Spark’s driver-to-executor file transfer broke with “Broken pipe.” The executor then exited with code 1. Separately, Airflow task heartbeats failed while PostgreSQL was unavailable; Airflow’s task supervisor eventually sent SIGKILL to the task process. Docker did not report an OOM kill. The Spark Master later displayed the application lifecycle as FINISHED, but that status does not establish that the publisher completed: the Airflow publisher attempt is recorded failed, no publisher completion/verification marker exists, its verifier was skipped, and no baseline object or Nessie table exists.

The strongest infrastructure evidence is severe WSL2/Windows memory and CPU pressure during run 02. The monitor recorded only 220 MB Windows available memory at its minimum, WSL swap about 84% used, and elevated CPU, memory, and I/O pressure. Multiple Airflow stacks were consuming substantial resources concurrently. This is strong temporal evidence of resource contention, but it does not prove the cause of either the PostgreSQL backend exit code 2 or the particular socket closure behind the JAR transfer. No Docker OOM, WSL kernel OOM, or retained filesystem I/O error was found.

The DAG is currently paused and remains manual-only in code. Both publisher attempts are terminally failed; neither verifier ran. The baseline-only branch selected only the publisher and skipped the Bronze/Silver/Gold branches. No active Spark application was registered in the Spark Master during the read-only status check.

## 2. Method and limits

The review used the publisher and DAG source, retained Airflow task/run history and logs, Spark driver/worker/master logs, Nessie reference and catalog listings, a read-only SeaweedFS object listing, Docker inspection/statistics, PostgreSQL logs and read-only settings/health queries, and the retained Windows/WSL diagnostic monitor output.

All timestamps below are UTC unless explicitly identified otherwise. The manual run IDs contain a November 8 date in the local UI naming, while their recorded event timestamps fall on November 7 UTC. Source row-count scans were not run: the target tables are absent, and running Spark solely to scan production Parquet sources during an investigation of resource instability would add load. No source count reconciliation has therefore been claimed.

The investigation could establish the current Nessie reference/catalog state and current object prefix. A guessed Nessie commit-history route returned 404, so the report does not claim access to a complete historical commit/deletion ledger. The exact current inventory is supported by exhaustive reference enumeration, entry listings for every available reference, and the empty exact publisher object prefix.

## 3. Exact publisher behavior and target layout

The publisher source is [spark/jobs/iceberg_catalog.py](spark/jobs/iceberg_catalog.py). Its entity and Gold lists and source/target construction are at lines 22–65; create-versus-overwrite behavior is at lines 119–131; the warehouse, Nessie reference, timeout, and object-store configuration are at lines 161–195; and namespace creation plus the table loop are at lines 198–218.

For this DAG task, the source root is /opt/spark-data and the namespace prefix is baseline. The Nessie catalog is named nessie, the URI is http://nessie:19120/api/v2, and the reference is main. The object warehouse is s3://shopstream-warehouse/warehouse/baseline. The prefix yields four namespaces: baseline_bronze, baseline_silver, baseline_quarantine, and baseline_gold.

The implementation first constructs the 23 table specifications, then creates all required namespaces with CREATE NAMESPACE IF NOT EXISTS, then iterates through the table specifications in order. Each table is counted, published, then read back and verified before the next table is handled. This is a sequence of separate per-table Iceberg/Nessie commits, not one transaction spanning all 23 tables. If execution stops partway through the loop, earlier table commits could remain. If a target table is absent, it is created. If it exists, the publisher performs a full-table overwrite into a new snapshot; it does not append. The Parquet source is read-only, and the code comments state prior Iceberg snapshots are retained.

### Expected 23 tables and observed state

All expected tables below were checked against every available Nessie reference. The only reference was main. All are absent there. The exact warehouse prefix listing had zero keys. Since none exists, destination count, metadata, and snapshot are **N/A** for every row.

| # | Nessie namespace.table on main | Parquet source path | Current destination state |
|---:|---|---|---|
| 1 | baseline_bronze.events | /opt/spark-data/bronze/events | Absent; no count/metadata/snapshot |
| 2 | baseline_silver.customers | /opt/spark-data/silver_v2/customers | Absent; no count/metadata/snapshot |
| 3 | baseline_quarantine.customers | /opt/spark-data/silver_v2/quarantine/customers | Absent; no count/metadata/snapshot |
| 4 | baseline_silver.products | /opt/spark-data/silver_v2/products | Absent; no count/metadata/snapshot |
| 5 | baseline_quarantine.products | /opt/spark-data/silver_v2/quarantine/products | Absent; no count/metadata/snapshot |
| 6 | baseline_silver.orders | /opt/spark-data/silver_v2/orders | Absent; no count/metadata/snapshot |
| 7 | baseline_quarantine.orders | /opt/spark-data/silver_v2/quarantine/orders | Absent; no count/metadata/snapshot |
| 8 | baseline_silver.payments | /opt/spark-data/silver_v2/payments | Absent; no count/metadata/snapshot |
| 9 | baseline_quarantine.payments | /opt/spark-data/silver_v2/quarantine/payments | Absent; no count/metadata/snapshot |
| 10 | baseline_silver.inventory | /opt/spark-data/silver_v2/inventory | Absent; no count/metadata/snapshot |
| 11 | baseline_quarantine.inventory | /opt/spark-data/silver_v2/quarantine/inventory | Absent; no count/metadata/snapshot |
| 12 | baseline_silver.website_activity | /opt/spark-data/silver_v2/website_activity | Absent; no count/metadata/snapshot |
| 13 | baseline_quarantine.website_activity | /opt/spark-data/silver_v2/quarantine/website_activity | Absent; no count/metadata/snapshot |
| 14 | baseline_gold.daily_revenue | /opt/spark-data/gold_v2/daily_revenue | Absent; no count/metadata/snapshot |
| 15 | baseline_gold.monthly_revenue | /opt/spark-data/gold_v2/monthly_revenue | Absent; no count/metadata/snapshot |
| 16 | baseline_gold.product_performance | /opt/spark-data/gold_v2/product_performance | Absent; no count/metadata/snapshot |
| 17 | baseline_gold.category_performance | /opt/spark-data/gold_v2/category_performance | Absent; no count/metadata/snapshot |
| 18 | baseline_gold.customer_lifetime_value | /opt/spark-data/gold_v2/customer_lifetime_value | Absent; no count/metadata/snapshot |
| 19 | baseline_gold.customer_segmentation | /opt/spark-data/gold_v2/customer_segmentation | Absent; no count/metadata/snapshot |
| 20 | baseline_gold.payment_success_failure | /opt/spark-data/gold_v2/payment_success_failure | Absent; no count/metadata/snapshot |
| 21 | baseline_gold.inventory_levels_and_movement | /opt/spark-data/gold_v2/inventory_levels_and_movement | Absent; no count/metadata/snapshot |
| 22 | baseline_gold.website_conversion_funnel | /opt/spark-data/gold_v2/website_conversion_funnel | Absent; no count/metadata/snapshot |
| 23 | baseline_gold.order_status_analysis | /opt/spark-data/gold_v2/order_status_analysis | Absent; no count/metadata/snapshot |

The full Spark identifiers are nessie.<namespace>.<table>, for example nessie.baseline_silver.customers. Nessie’s REST entry listing uses the namespace and table components without the Spark catalog prefix.

### Nessie reference and catalog audit

The Nessie reference endpoint returned exactly one reference: **main**, at commit hash 8a940493e580f4085f38a28dc475203b50b8661898bc451c6584b67c93e753fd. Its entry listing contained only integration_canary and integration_canary.persistence_check. No other reference was available to contain a partial baseline commit. All 23 expected baseline tables/namespaces were absent from main. The SeaweedFS ListObjectsV2 request for bucket shopstream-warehouse and prefix warehouse/baseline returned HTTP 200, KeyCount 0, IsTruncated false.

This supports the conclusion that there is no current partial baseline publication on another reference and no persisted baseline table data or metadata at the publisher’s exact warehouse prefix. Because the retained Nessie history endpoint was unavailable, it cannot rule out a transient commit that was later removed through some unobserved external action; no evidence of such an action was found.

## 4. Failure timeline and publication classification

### Attempt 1 — manual__baseline_only_20261108_01

| UTC time | Evidence |
|---|---|
| 2026-11-07 19:02:05 | Airflow run began. The branch task succeeded and selected iceberg_publish_existing_parquet; bronze_ingestion_isolated and the unrelated production/integration tasks were skipped. |
| 19:02:13 | Publisher attempt 1 started. Spark application app-20261107190320-0017 was submitted in client mode. Ivy artifacts were resolved under /tmp/shopstream-ivy. |
| 19:03:22 | Spark Worker launched executor 0 with 2 cores and 1024 MiB executor memory. |
| 19:07:19 | Scheduler logs show psycopg2 OperationalError: PostgreSQL server closed the connection unexpectedly. |
| 19:07:55–19:09:40 | The Airflow task supervisor recorded three execution-heartbeat ReadTimeout failures. |
| 19:09:40–19:09:43 | Supervisor logged “Too many failed heartbeats” and terminated the task process. Airflow recorded spark-submit exit code -9; this was task-supervisor termination evidence, not proof of a Linux OOM kill. |
| 19:09:42–19:09:48 | Spark Master removed the application/worker executor; the executor reported KILLED/status 143. |
| 19:13:12 | Publisher attempt history recorded failed. Its verifier did not run. |
| 20:28:16 | Supported Airflow API action marked the stale DAG run failed; run ended 20:28:19. |

The logs establish a failed publication attempt associated with Airflow metadata/API communication loss and task termination. There is no publisher completion marker, verifier result, or current baseline table/object. No retained diagnostic monitor covered this attempt, so its host pressure cannot be quantified as it can for attempt 2.

### Attempt 2 — manual__baseline_only_20261108_02

| UTC time | Evidence |
|---|---|
| 2026-11-07 20:50:14 | Airflow run began with run_mode=baseline_only. A DAG import timed out once; the branch task later succeeded at 20:52:07, selected only the baseline publisher, and skipped Bronze/Silver/Gold and other tasks. |
| 20:52:09–20:52:12 | Publisher attempt 1 started; SparkSubmit ran in client mode. Ivy used /tmp/shopstream-ivy. |
| 20:52:38–21:07:25 | Nessie received successful GET /api/v2/trees/main requests (HTTP 200). These were reference reads, not baseline commits. |
| 21:00:15 | Spark Master registered application app-20261107210015-0018. |
| 21:00:21–21:00:38 | Worker launched executor 0. The driver-side AWS SDK bundle was present at /tmp/shopstream-ivy/jars/com.amazonaws_aws-java-sdk-bundle-1.12.262.jar. |
| 21:09:26–21:09:36 | Driver reported executor creation failure. At 21:09:27 it logged a Broken pipe while sending the 280,645,251-byte SDK JAR from the driver’s Spark file server to worker address 172.19.0.3:42634. Executor 0 then exited with code 1. |
| 21:10:05–21:10:09 | Spark Master removed executor 0 and tried a second executor; the worker was asked to kill it later. |
| 21:11:27–21:11:54 | PostgreSQL checkpoint took 27.294 seconds, including 19.652 seconds of writes and 3.290 seconds of sync time; this was longer than nearby checkpoints of about 8.164 and 10.177 seconds. This is evidence of elevated I/O latency, not itself a crash cause. |
| 21:14:50–21:15:05 | PostgreSQL logged client reset/broken-pipe messages. At 21:15:05.648 the postmaster reported backend PID 19200 exited with code 2, terminated remaining server processes, and entered recovery. The preceding retained log has no FATAL, PANIC, assertion, stack trace, signal, or core-dump explanation for PID 19200. |
| 21:15:06 onward | PostgreSQL recovery caused connection failures; Airflow logged FATAL “database system is in recovery mode” and scheduler connection errors, including a SQLAlchemy/psycopg2 server-closed connection during orphan-asset cleanup at 21:15:37. |
| 21:15:39–21:17:12 | The publisher task supervisor recorded three execution-heartbeat failures while the metadata service was unavailable. |
| 21:25:04–21:25:14 | Supervisor logged too many failed heartbeats and explicitly sent SIGKILL to task PID 232. This is the known task-runner kill; no Docker OOMKilled evidence accompanies it. |
| 21:26–21:31 | Nessie logs show Vert.x blocked-thread warnings, increasing to as much as 138 seconds, and request timeouts. The onset is after the SDK transfer failure and overlaps severe system pressure; it does not explain the initial JAR broken pipe. |
| 21:27:25 | Spark executor reported KILLED/status 143 after the kill request. |
| 21:48:53 | Airflow task-attempt history recorded publisher failed. The current task instance later displays skipped as part of terminal run reconciliation; the historical attempt is the authoritative execution result. |
| 22:13:43–22:13:44 | Verifier was skipped and the supported API marked the stale DAG run failed; the run became terminally failed. |

The Spark Master UI subsequently displayed app-20261107210015-0018 as FINISHED. That is a Spark standalone application lifecycle state, not a publication-success assertion. The executor creation/transfer failure, failed Airflow publisher history, absent publisher completion marker, skipped verifier, empty Nessie baseline listing, and empty S3 prefix show that this was **not** a successful publication. It was not a successful application with partial table commits, and it was not a full publication. No table among the 23 is currently committed.

## 5. Spark dependency distribution analysis

The DAG builds spark-submit packages for Iceberg runtime 1.10.2, Iceberg AWS bundle 1.10.2, Nessie Spark extensions 0.105.7, Hadoop AWS 3.3.4, and uses an Ivy settings file. The relevant configuration is in [airflow/dags/shopstream_batch.py](airflow/dags/shopstream_batch.py) lines 26–38. Actual run logs show the Ivy cache at /tmp/shopstream-ivy, with the AWS bundle in /tmp/shopstream-ivy/jars.

The failure was **not shown to be a Maven/Ivy download failure**: the large JAR had already arrived in the driver cache. Spark then served that JAR through its driver file server to the executor, and the transfer failed with a broken pipe. The worker-side connection ended before the 280 MB stream completed. Retained logs do not establish whether the immediate cause was the worker process exiting, resource pressure, a network/socket interruption, or a combination.

The Airflow scheduler, Spark Master, and Spark Worker have a shared shopstream_spark_dependencies volume mounted at /opt/spark/dependencies. However, the actual DAG’s run-time Ivy path is /tmp/shopstream-ivy, so the shared dependency volume was not the cache used for this attempt. A shared Ivy cache could avoid repeated downloads, but merely changing the Ivy cache path would **not** guarantee that Spark stops shipping those JARs from driver to executor.

The run used Spark client mode, with the task/driver hosted by the Airflow scheduler container and explicit driver host/ports in the DAG. The worker advertises 2 cores and 2 GiB memory; logs show a 1 GiB executor. No explicit driver memory setting was found in the baseline submit configuration. Compose containers had no hard Docker memory limits. Actual runtime Nessie was image 0.109.0 while the Spark Nessie extension package was 0.105.7; this version drift is real but was not shown to cause these failures.

## 6. Resource, PostgreSQL, and Nessie evidence

### Host and WSL2 monitor

The retained diagnostic directory was diagnostics/diag-baseline-final-20261107T204107Z-c39d3c32. It recorded samples from 20:41:22 to 21:31:11 UTC at 30-second intervals, overlapping run 02.

- Windows available memory fell to a sampled minimum of **220 MB** at 21:16:14.
- Sampled Windows CPU reached 86%.
- The Windows disk latency counter reported 0 ms while the disk queue peaked at 69; the zero-latency result is not credible evidence of an idle disk and should be treated as a measurement limitation.
- WSL2 had about 7.68 GiB total RAM, about 1.68 GiB available at the critical sample, and 2 GiB swap with about 1.69 GiB used (roughly 84%).
- WSL PSI at that sample: CPU some avg10 75.92%; memory some 36.56%, memory full 24.86%; I/O some 11.28%.
- At the same sample, ShopStream Airflow Scheduler used about 253% CPU and 1.05 GiB; DAG Processor about 89% CPU and 286 MiB; Triggerer about 57% CPU; Spark Worker about 166% CPU; Spark Master about 55% CPU. A concurrently running legacy Airflow stack also used substantial CPU and memory: its DAG processor about 191% CPU/571 MiB, scheduler about 194%/608 MiB, worker about 137%/860 MiB, and triggerer about 180%/528 MiB.

These measurements support heavy host/VM resource contention at the time of the PostgreSQL and Airflow connectivity failures. All inspected Docker containers reported OOMKilled=false, and their inspect records had no hard memory limit. Retained WSL kernel logs had no “Out of memory”, “oom-kill”, “Killed process”, “I/O error”, or EXT4 error message. The monitor therefore supports memory/swap/CPU/I/O pressure, but not a confirmed kernel OOM or filesystem fault.

### PostgreSQL

The running database image was postgres:16, PostgreSQL 16.15 (Debian). Read-only settings were shared_buffers 128 MiB, max_connections 100, work_mem 4 MiB, checkpoint_timeout 300 seconds, checkpoint_completion_target 0.9, max_wal_size 1 GiB, min_wal_size 80 MiB, and wal_buffers 4 MiB. These settings do not on their face establish an excessive configuration as the cause.

At 21:15:05.648 UTC the postmaster recorded server process PID 19200 exited with code 2, then terminated other server processes and began crash recovery. No retained PostgreSQL FATAL/PANIC/assertion/stack/core/signal record preceding that exit identifies why the backend returned code 2. The prior client reset/broken pipe messages are symptoms of connection loss and do not identify the backend’s internal cause. Recovery synchronization was slower under the same pressure window, but the exact crash cause remains **unconfirmed**.

The Airflow scheduler’s database connection loss at 21:15 follows directly in time from PostgreSQL recovery and is consistent with the database interruption. The ShopStream scheduler container later started at 21:47:49 and reports restart_count=2; available evidence does not establish the exact restart trigger. Current read-only checks show the Airflow API/metadata health endpoints, PostgreSQL container health, and Nessie reference/catalog requests responding successfully.

### Nessie

Nessie main-reference tree reads returned HTTP 200 during much of run 02. Its event-loop blocked-thread warnings and timeouts became pronounced later, during the same resource-pressure window. There are no observed baseline table commit requests or baseline catalog entries. The evidence supports Nessie responsiveness degradation concurrent with resource contention; it does not show Nessie as the cause of the initial dependency transfer failure.

The running Nessie server image was 0.109.0, while the user-reported server version was 0.105.7 and the DAG’s Spark extension is 0.105.7. The compose declaration confirms the image tag. This should be reconciled before a future publication, but no causal link to the failed runs is established.

## 7. Airflow execution-safety assessment

The DAG ID is shopstream_silver_gold_quality. The source [airflow/dags/shopstream_batch.py](airflow/dags/shopstream_batch.py) sets schedule=None and catchup=False at lines 96–106 and defaults to paused on creation. Read-only Airflow metadata confirmed the DAG is currently paused, with no queued/running DAG runs.

Both run records are terminally failed and used run_mode=baseline_only. Publisher retries are configured as zero; the task history shows a single publisher attempt for each run (try number 1, max tries 0). The baseline verifier uses TriggerRule.ALL_SUCCESS. The branch task logs show it returned only iceberg_publish_existing_parquet in baseline-only mode and skipped bronze_ingestion_isolated. Bronze quality, Silver, Gold, and unrelated integration work were also skipped. The baseline publisher only reads the existing Parquet source paths; code writes to the Nessie/S3 baseline target, not back to those Parquet paths.

For run 02, task-instance history records the publisher attempt as failed and the verifier as skipped. The current task-instance state shown after reconciliation is skipped; that post-reconciliation display does not turn the failed attempt into success. Both runs were explicitly marked failed using the supported single-run Airflow API operation. No new run or other modified run was found. The Spark Master showed no currently active application during status inspection.

## 8. Proven causes versus unresolved hypotheses

### Proven by retained logs and current state

1. Both baseline publisher attempts failed from Airflow’s recorded task-attempt history; neither verifier completed.
2. Run 02 had an actual driver-to-worker transfer failure for the cached 280,645,251-byte AWS SDK bundle, followed by executor creation failure/exit code 1.
3. Airflow’s task supervisor killed the publisher process after repeated execution heartbeat timeouts; run 02 logs explicitly show SIGKILL.
4. PostgreSQL backend PID 19200 exited with code 2 and the postmaster entered crash recovery. Airflow connection errors followed.
5. Current catalog audit finds zero of the 23 expected tables across every available reference; only main exists. The exact baseline object prefix is empty.
6. The retained monitor measured severe Windows/WSL2 memory, swap, CPU and I/O pressure during run 02. Docker did not report OOMKilled.
7. Nessie’s blocked-thread warnings/timeouts occurred later in the pressured interval; no baseline commit was observed.
8. DAG is paused/manual-only, retries are zero, and baseline-only branching skipped all production transformation branches.

### Not established

1. The precise internal cause of PostgreSQL backend exit code 2. No crash dump, assertion, stack, signal detail, or OS-level termination event was retained.
2. The immediate event that closed the Spark JAR transfer socket. Resource pressure is plausible, but the logs do not identify the peer’s termination cause.
3. A confirmed Docker/WSL OOM kill. Container OOM flags and retained kernel logs do not show one.
4. A filesystem/controller error. No retained WSL filesystem I/O errors were found; the Windows latency counter was inadequate.
5. Nessie as the cause of either publisher failure. Nessie degraded later, but no failed baseline commit operation is logged.
6. A complete historical Nessie commit/deletion history. Current references, entries, and object storage were exhaustively checked, but the attempted history route was unavailable.

## 9. Recommended fixes, ranked by priority

These are proposals only; none were applied.

| Priority | Proposed change and location | Expected benefit | Risk / trade-off |
|---|---|---|---|
| 1 | Replace runtime distribution of the large dependency set with a controlled pre-bundled Spark dependency strategy. Candidate locations: airflow/Dockerfile and the Spark image/build in docker-compose.yml (or a shared dependency image); airflow/dags/shopstream_batch.py package/submit options. Pin the tested Iceberg, Nessie, Hadoop AWS, AWS SDK, and Spark versions; ensure both driver and executors use the same classpath. | Removes the large driver-to-worker JAR transfer that failed and avoids repeated Ivy downloads. | Image/build changes and classpath duplication can cause version conflicts or stale jars. Validate a small isolated Spark/Iceberg canary before publication. Changing only spark.jars.ivy to the shared volume may still leave Spark distributing the JAR and is not sufficient by itself. |
| 2 | Reduce contention during publication. In the operational run procedure, ensure only the required Airflow stack is active; review Airflow LocalExecutor parallelism (observed 32) and scheduler/DAG processor concurrency before a controlled run. Keep the diagnostic monitor active. | Lowers CPU/memory pressure and makes PostgreSQL/Airflow/Nessie more likely to remain responsive. | Lower parallelism increases task duration. Arbitrary hard Docker memory limits could create OOMs; do not add limits without measured headroom. |
| 3 | Improve PostgreSQL crash evidence before changing PostgreSQL settings. Preserve complete PostgreSQL, Docker, WSL kernel, and Windows System/Application logs around a controlled run; capture PostgreSQL process exit/coredump details and storage latency with a counter that reports valid values. | Enables a definitive backend crash/storage diagnosis rather than tuning by guesswork. | Adds modest logging/storage overhead; logs must be retained and redacted appropriately. |
| 4 | Reconcile the Nessie version pin between the running server image (0.109.0) and Spark extension package (0.105.7) in docker-compose.lakehouse.yml and airflow/dags/shopstream_batch.py. Select and test one compatible pinned pair. | Removes ambiguity and improves reproducibility. | Version changes can introduce API/serialization differences; this mismatch is not proven to have caused the incidents. |
| 5 | Keep baseline publication isolated and retry-safe. Retain retries=0, verify each of 23 counts/snapshots, and do not run a full overwrite against a populated target until a deliberate replacement policy is approved. | Prevents automatic repetition after infrastructure failure and makes partial progress observable. | A complete publication remains sequential and can stop mid-list; operational validation and safe rerun behavior must be designed. Existing publisher overwrite behavior replaces a target table when it exists. |

## 10. Safe staged recovery plan

1. Keep the DAG paused and manual-only. Do not retry either failed run or alter production paths, Kafka offsets, checkpoints, or existing Iceberg tables.
2. Before any new execution is authorized, pin the runtime dependency strategy and Nessie server/extension versions. Build or stage the driver and executor dependencies together; statically inspect the resulting classpath.
3. Validate the dependency path with a small isolated Spark job that does not read/write production datasets and does not publish to baseline namespaces. Confirm executor startup and class loading without a large dependency transfer.
4. Under stable host conditions, use a read-only source-count method or an authorized isolated Spark scan to record source counts for all 23 paths. Avoid running the publisher as the first diagnostic.
5. Re-enumerate all Nessie references and the isolated warehouse prefix immediately before an authorized publication. Confirm no unexpected target tables or objects exist.
6. Execute one manually authorized publication to an isolated baseline namespace with retries still zero and monitoring active. Since commits are per-table and sequential, capture the publisher’s per-table verification output and stop immediately on the first error.
7. After a successful run, independently verify all 23 Nessie entries, source/destination counts, Iceberg metadata and snapshots, then verify read-back and persistence. Do not infer success from Spark Master FINISHED alone.
8. Test an idempotency/replacement rerun only after the first publication is fully reconciled and explicitly authorized. The current code overwrites a populated target table with a new snapshot; it does not append.
9. Leave the DAG paused after validation. Keep all existing Parquet data, checkpoints, Kafka offsets, and production tables untouched until a separately reviewed migration decision.

## 11. Files/configuration involved and change status

No files or configuration were changed during this investigation.

- Publisher implementation: spark/jobs/iceberg_catalog.py
- Airflow DAG and runtime Ivy/package settings: airflow/dags/shopstream_batch.py
- Spark Worker advertised cores/memory: docker-compose.yml
- Airflow scheduler and shared dependency volume: airflow/docker-compose.shopstream.yml
- Lakehouse Spark/Nessie configuration and Nessie server image pin: docker-compose.lakehouse.yml
- Retained diagnostic evidence: diagnostics/diag-baseline-final-20261107T204107Z-c39d3c32

**Conclusion:** The attempts failed before any baseline table was committed. Run 02’s JAR transfer and executor failure is directly evidenced; host resource pressure is strongly correlated but does not prove the PostgreSQL backend’s internal crash cause. Stabilize and simplify dependency distribution, preserve the paused/manual-only safety state, then validate through a fresh, isolated and explicitly authorized run.

