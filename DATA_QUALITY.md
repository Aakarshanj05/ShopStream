# Data Quality and Metric Definitions

## Silver v2

Each topic is parsed with an explicit Spark schema. String fields are trimmed; email is lowercased; enum-like values are uppercased; ISO event timestamps are converted to Spark timestamps in UTC. Invalid/missing required fields, invalid email/membership/status, negative values, order amount mismatch, and missing references generate pipe-delimited `rejection_reason` values.

Each entity output has one row per accepted primary key after deterministic ranking. Customer ranking is `created_at DESC, kafka_timestamp DESC, kafka_partition DESC, kafka_offset DESC`. For other events, the event timestamp leads, followed by Kafka timestamp/partition/offset. These are deterministic tie breakers; they do not create a source-provided update sequence. Quarantine keeps the original event payload and Kafka metadata.

For each entity the job checks `bronze_count = silver_count + quarantine_count` before writing. Silver and quarantine v2 outputs are overwritten idempotently on rerun at their isolated paths. No checkpoint or source data is changed.

## Gold v2 metrics

- Daily/monthly revenue: sum of successful payment amounts by successful payment timestamp, joined to accepted Silver orders. It represents observed successful payment attempts, not accounting revenue: the data has no refunds, chargebacks, fees, taxes or payment idempotency guarantee.
- Product/category performance: accepted orders with at least one successful payment, grouped by product/category; captured payment amount is summed and order quantities are reported from each accepted order.
- Customer lifetime value: cumulative successful payment amount per customer in the observed dataset; it is not predictive CLV.
- Customer segmentation: only customers with successful payments are included. HIGH_VALUE is at/above the observed payer 75th percentile; MID_VALUE is at/above the observed payer median; remaining payers are LOW_VALUE. Threshold values are persisted with rows.
- Payment analysis: counts/amounts by method and outcome. Multiple successful payment IDs for one order are summed as successful payment attempts.
- Inventory levels: latest event snapshot by event timestamp and Kafka metadata, plus sum of observed quantity changes. This is not an authoritative stock ledger because generator state is in-memory and can diverge from the original product master.
- Website funnel: counts of distinct sessions reaching each event stage; website PURCHASE remains a simulated web event and is never joined as a paid order.
- Order status: accepted order count and order value by status; it is not recognized revenue.

Gold checks that daily and monthly successful-payment totals match.

