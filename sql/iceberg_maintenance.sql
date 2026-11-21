-- Execute only against the configured Nessie Iceberg catalog.
-- Snapshot listing and time travel are read-only.
SELECT committed_at, snapshot_id, parent_id, operation
FROM nessie.gold.daily_revenue.snapshots
ORDER BY committed_at DESC;

-- Example time-travel query; replace <snapshot_id> with an ID from snapshots.
-- SELECT * FROM nessie.gold.daily_revenue VERSION AS OF <snapshot_id>;

-- Schema evolution example. Adds an optional field without rewriting existing rows.
ALTER TABLE nessie.gold.order_status_analysis
ADD COLUMN IF NOT EXISTS quality_note STRING;

-- Snapshot expiration and orphan cleanup remove historical data files.
-- Review retention and confirm backups before manually running maintenance:
-- CALL nessie.system.expire_snapshots(
--   table => 'gold.daily_revenue',
--   older_than => TIMESTAMP '2026-01-01 00:00:00'
-- );
-- Do not automate expiration or orphan removal until the retention policy is approved.

