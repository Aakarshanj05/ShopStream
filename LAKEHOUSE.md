# Iceberg, Nessie and S3-Compatible Object Storage

## Current state

SeaweedFS 4.52 is deployed as an isolated S3-compatible object store with a persistent named volume. Its gateway is published on port 8333, and credentials are supplied through the ignored `.env` file. Nessie 0.109.0 uses PostgreSQL 16 and remains the catalog. The existing MinIO AIStor service configuration and volume are preserved and inactive because its license file is unavailable.

The real canary passed on Spark 3.5.9/Java 11 with Iceberg 1.10.2, Nessie Spark extensions 0.105.7 and Hadoop AWS/S3A 3.3.4. It created `nessie.integration_canary.persistence_check`, inserted and read three rows, checked two Iceberg snapshots, data files and metadata-log entries, and confirmed the Nessie table listing. SeaweedFS, Nessie PostgreSQL and Nessie were restarted; verify-only mode then read the same records and snapshot, and Nessie REST still listed the table. This validates the isolated integration canary; it does not mean the Bronze/Silver/Gold ingestion has been migrated to Iceberg.

## Candidate comparison

| Candidate | License and Docker | S3, Iceberg and Nessie fit | Persistence and local resource fit |
|---|---|---|---|
| **SeaweedFS (deployed)** | Apache-2.0. The isolated service runs `weed mini` and persists `/data` in its own named volume. | S3 gateway on port 8333. S3A and Iceberg S3FileIO were exercised with the Nessie catalog in the persistence canary. SeaweedFS's own Iceberg REST feature is not used. | Single-node mode fits this local project. During the canary it used about 203 MiB RAM at the final sample; actual resource use depends on stored objects and workload. |
| RustFS | Apache-2.0. Official image and Compose examples are available; named-volume persistence should be used here to avoid bind-mount ownership issues. | S3-compatible API; its published matrix describes a tested subset, not full S3 coverage. Spark/Iceberg custom endpoint integration is plausible through S3FileIO, but must be tested. Nessie can remain the catalog. Project announced 1.0 GA in Sep 2026 and release 1.0.4 in Oct 2026, making it newer than SeaweedFS. | Designed as a single server or distributed store; no project-specific local footprint guarantee found. Test against this host before choosing it for the long-lived lakehouse. |
| Ceph RGW | Open-source Ceph, but multi-daemon deployment and administration are substantially heavier than this project needs. | Mature RADOS Gateway provides S3-compatible object access; the same S3FileIO endpoint approach applies. Nessie is independent. | Poor fit for the current Docker Desktop host: Ceph's own guidance sets BlueStore OSD memory target to 4 GiB by default and recommends at least 4 GiB per OSD, before the rest of the stack. |

SeaweedFS is the recommendation for this local, single-node portfolio environment because its official project supports a compact single-process mode, Docker, a persistent data directory and an S3 endpoint under Apache-2.0. RustFS is the closest second choice when a MinIO-like server/console experience matters more, but its 1.0 release is recent. Neither is declared compatible with the actual ShopStream job until the Iceberg canary passes.

Sources: [SeaweedFS README and license](https://github.com/seaweedfs/seaweedfs), [SeaweedFS Docker guide](https://github.com/seaweedfs/seaweedfs/blob/master/docker/README.md), [RustFS project and license](https://github.com/rustfs/rustfs), [RustFS S3 compatibility matrix](https://github.com/rustfs/docs.rustfs.com/blob/main/content/en/reference/s3-compatibility.md), [RustFS releases](https://github.com/rustfs/rustfs/releases), [Ceph sizing guidance](https://docs.ceph.com/en/latest/start/hardware-cpu-memory/), and [Iceberg S3FileIO properties](https://iceberg.apache.org/javadoc/1.4.1/org/apache/iceberg/aws/s3/S3FileIOProperties.html).

## Deployed design

- S3-compatible object store holds Iceberg data files in a persistent named volume.
- Nessie 0.109.0 uses PostgreSQL 16 for its version store and remains the Iceberg catalog.
- Spark 3.5.9/Java 11 uses Iceberg 1.10.2, Nessie Spark extensions 0.105.7 and Hadoop AWS 3.3.4, resolved under `/opt/spark/dependencies`. Iceberg 1.12.0 was incompatible with this Java 11 runtime because it contains Java 17 bytecode.
- Configure Iceberg's `S3FileIO` with the internal object-store endpoint, path-style access, and credentials from environment variables. Keep S3 storage endpoint separate from Nessie catalog URI.
- Namespaces: `bronze`, `silver`, `quarantine`, `gold`, `audit`.
- The catalog job checks counts, snapshot metadata and reads the latest snapshot by ID. Optional flags test branch creation and additive schema evolution.
- Snapshot expiration and orphan removal remain manual. They can delete files and require a retention policy.

## Canary commands

Run a write canary using `.env` values:

```powershell
docker compose --env-file .env -f docker-compose.lakehouse.yml --profile canary run --rm iceberg-canary
```

Run verify-only mode after service restarts:

```powershell
$env:ICEBERG_CANARY_MODE = 'verify'
docker compose --env-file .env -f docker-compose.lakehouse.yml --profile canary run --rm iceberg-canary
Remove-Item Env:ICEBERG_CANARY_MODE
```

Only the canary namespace/table and its `integration_canary` object prefix are written. Do not direct this small test toward the existing Bronze, Silver or Gold data.

The integration canary does not bulk-copy or rewrite historical data. The existing MinIO AIStor service was not started, and its volume was not reused.

## Current validation

`docker-compose.lakehouse.yml` passes Compose config validation. The canary passed S3A write/read, Nessie namespace/table creation, three-row Iceberg write/query, snapshot and metadata inspection, and verify-only reads after restarting SeaweedFS and Nessie services. Nessie REST continued listing the table. Final resource sample was SeaweedFS ~203 MiB, Nessie ~317 MiB and Nessie PostgreSQL ~27 MiB under a 7.679 GiB Docker memory limit. C: had 34.7 GiB free; E: had 324.4 GiB free. No Docker volumes were removed, and Kafka, Spark and Airflow services stayed running.
