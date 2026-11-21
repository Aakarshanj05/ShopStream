-- Spark SQL examples. Run after silver_transformations.py and gold_analytics.py.
-- Paths are the mounted container paths. Amount is the generator's currency unit.

-- Daily revenue: successful payment records linked to valid Silver orders.
SELECT order_date, successful_payment_revenue, paid_order_count, paying_customer_count
FROM parquet.`/opt/spark-data/gold_v2/daily_revenue`
ORDER BY order_date;

-- Monthly rollup. Daily and monthly successful-payment amounts reconcile.
SELECT order_month, successful_payment_revenue, paid_order_count, paying_customer_count
FROM parquet.`/opt/spark-data/gold_v2/monthly_revenue`
ORDER BY order_month;

-- Product and category performance based on orders with successful payments.
SELECT product_id, product_name, category, paid_order_count, units_in_paid_orders,
       successful_payment_revenue
FROM parquet.`/opt/spark-data/gold_v2/product_performance`
ORDER BY successful_payment_revenue DESC;

SELECT category, paid_order_count, units_in_paid_orders, successful_payment_revenue
FROM parquet.`/opt/spark-data/gold_v2/category_performance`
ORDER BY successful_payment_revenue DESC;

-- Customer lifetime successful-payment amount and payer segments.
SELECT customer_id, lifetime_successful_payment_amount, paid_order_count,
       last_successful_payment_at
FROM parquet.`/opt/spark-data/gold_v2/customer_lifetime_value`
ORDER BY lifetime_successful_payment_amount DESC;

SELECT segment, COUNT(*) AS paying_customers,
       MIN(lifetime_successful_payment_amount) AS min_amount,
       MAX(lifetime_successful_payment_amount) AS max_amount
FROM parquet.`/opt/spark-data/gold_v2/customer_segmentation`
GROUP BY segment
ORDER BY segment;

-- Attempts are grouped by method and outcome, not treated as unique paid orders.
SELECT payment_method, payment_status, payment_count, order_count, payment_amount
FROM parquet.`/opt/spark-data/gold_v2/payment_success_failure`
ORDER BY payment_method, payment_status;

-- Latest reported stock comes from inventory event snapshots; net movement is the
-- sum of observed quantity_changed events, not an inferred warehouse inventory.
SELECT product_id, product_name, latest_reported_stock, net_quantity_movement,
       movement_event_count, latest_inventory_event_at
FROM parquet.`/opt/spark-data/gold_v2/inventory_levels_and_movement`
ORDER BY product_id;

-- This funnel is based on distinct sessions and is independent from payments.
SELECT * FROM parquet.`/opt/spark-data/gold_v2/website_conversion_funnel`;

-- Order status counts and order values are order intent/status metrics, not paid revenue.
SELECT status, order_count, customer_count, order_value
FROM parquet.`/opt/spark-data/gold_v2/order_status_analysis`
ORDER BY status;

