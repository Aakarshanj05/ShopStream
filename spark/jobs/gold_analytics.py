"""Create 10 analytics-ready Gold v2 Parquet datasets from Silver v2."""
import logging
import os

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F

DATA_ROOT = os.getenv("SHOPSTREAM_DATA_ROOT", "/opt/spark-data")
SILVER_ROOT = os.path.join(DATA_ROOT, "silver_v2")
GOLD_ROOT = os.path.join(DATA_ROOT, "gold_v2")
LOG = logging.getLogger("shopstream.gold")


def read(name):
    return spark.read.parquet(os.path.join(SILVER_ROOT, name))


def write(name, df):
    path = os.path.join(GOLD_ROOT, name)
    df.write.mode("overwrite").parquet(path)
    count = df.count()
    LOG.info("%s rows=%d path=%s", name, count, path)
    return df


def main():
    global spark
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    spark = (SparkSession.builder.appName("ShopStream-Gold-v2")
             .master(os.getenv("SPARK_MASTER", "spark://spark-master:7077"))
             .config("spark.sql.session.timeZone", "UTC").getOrCreate())
    spark.sparkContext.setLogLevel("WARN")
    try:
        orders = read("orders")
        payments = read("payments")
        products = read("products")
        customers = read("customers")
        inventory = read("inventory")
        activity = read("website_activity")

        successful = payments.filter(F.col("payment_status") == "SUCCESS")
        successful_by_order = (successful.groupBy("order_id")
                               .agg(F.sum("payment_amount").alias("captured_revenue"),
                                    F.max("payment_timestamp").alias("last_successful_payment_at"),
                                    F.countDistinct("payment_id").alias("successful_payment_count")))
        paid_orders = (orders.join(successful_by_order, "order_id", "inner")
                       .withColumn("order_date", F.to_date("last_successful_payment_at"))
                       .withColumn("order_month", F.date_trunc("month", "last_successful_payment_at").cast("date")))

        daily = (paid_orders.groupBy("order_date")
                 .agg(F.sum("captured_revenue").alias("successful_payment_revenue"),
                      F.countDistinct("order_id").alias("paid_order_count"),
                      F.countDistinct("customer_id").alias("paying_customer_count")))
        monthly = (paid_orders.groupBy("order_month")
                   .agg(F.sum("captured_revenue").alias("successful_payment_revenue"),
                        F.countDistinct("order_id").alias("paid_order_count"),
                        F.countDistinct("customer_id").alias("paying_customer_count")))
        write("daily_revenue", daily)
        write("monthly_revenue", monthly)

        order_product = (paid_orders.join(products.select("product_id", "product_name", "category", "brand"),
                                          "product_id", "left"))
        product_perf = (order_product.groupBy("product_id", "product_name", "category", "brand")
                        .agg(F.countDistinct("order_id").alias("paid_order_count"),
                             F.sum("quantity").alias("units_in_paid_orders"),
                             F.sum("captured_revenue").alias("successful_payment_revenue")))
        category_perf = (order_product.groupBy("category")
                         .agg(F.countDistinct("order_id").alias("paid_order_count"),
                              F.sum("quantity").alias("units_in_paid_orders"),
                              F.sum("captured_revenue").alias("successful_payment_revenue")))
        write("product_performance", product_perf)
        write("category_performance", category_perf)

        clv = (paid_orders.groupBy("customer_id")
               .agg(F.sum("captured_revenue").alias("lifetime_successful_payment_amount"),
                    F.countDistinct("order_id").alias("paid_order_count"),
                    F.max("last_successful_payment_at").alias("last_successful_payment_at")))
        write("customer_lifetime_value", clv)

        if clv.limit(1).count():
            q50, q75 = clv.approxQuantile("lifetime_successful_payment_amount", [0.50, 0.75], 0.01)
            segments = (clv.withColumn(
                "segment",
                F.when(F.col("lifetime_successful_payment_amount") >= F.lit(q75), "HIGH_VALUE")
                 .when(F.col("lifetime_successful_payment_amount") >= F.lit(q50), "MID_VALUE")
                 .otherwise("LOW_VALUE"))
                .withColumn("segment_q50_threshold", F.lit(float(q50)))
                .withColumn("segment_q75_threshold", F.lit(float(q75))))
        else:
            segments = (clv.withColumn("segment", F.lit(None).cast("string"))
                        .withColumn("segment_q50_threshold", F.lit(None).cast("double"))
                        .withColumn("segment_q75_threshold", F.lit(None).cast("double")))
        write("customer_segmentation", segments)

        payment_analysis = (payments.groupBy("payment_method", "payment_status")
                            .agg(F.countDistinct("payment_id").alias("payment_count"),
                                 F.countDistinct("order_id").alias("order_count"),
                                 F.sum("payment_amount").alias("payment_amount")))
        write("payment_success_failure", payment_analysis)

        movement = (inventory.groupBy("product_id")
                    .agg(F.sum("quantity_changed").alias("net_quantity_movement"),
                         F.countDistinct("inventory_event_id").alias("movement_event_count")))
        latest_window = Window.partitionBy("product_id").orderBy(
            F.col("event_timestamp").desc_nulls_last(),
            F.col("kafka_timestamp").desc_nulls_last(),
            F.col("kafka_partition").desc_nulls_last(),
            F.col("kafka_offset").desc_nulls_last())
        latest = (inventory.withColumn("_rank", F.row_number().over(latest_window))
                  .filter(F.col("_rank") == 1)
                  .select("product_id", F.col("available_stock").alias("latest_reported_stock"),
                          F.col("event_timestamp").alias("latest_inventory_event_at")))
        inv_gold = (latest.join(movement, "product_id", "inner")
                    .join(products.select("product_id", "product_name", "category"), "product_id", "left"))
        write("inventory_levels_and_movement", inv_gold)

        session = (activity.groupBy("session_id")
                   .agg(F.max(F.when(F.col("event_type") == "PAGE_VIEW", 1).otherwise(0)).alias("page_view"),
                        F.max(F.when(F.col("event_type") == "PRODUCT_VIEW", 1).otherwise(0)).alias("product_view"),
                        F.max(F.when(F.col("event_type") == "ADD_TO_CART", 1).otherwise(0)).alias("add_to_cart"),
                        F.max(F.when(F.col("event_type") == "CHECKOUT_STARTED", 1).otherwise(0)).alias("checkout_started"),
                        F.max(F.when(F.col("event_type") == "PURCHASE", 1).otherwise(0)).alias("website_purchase"),
                        F.countDistinct("event_id").alias("activity_event_count")))
        page_sessions = session.filter(F.col("page_view") == 1).count()
        funnel = (session.agg(F.sum("page_view").alias("page_view_sessions"),
                              F.sum("product_view").alias("product_view_sessions"),
                              F.sum("add_to_cart").alias("add_to_cart_sessions"),
                              F.sum("checkout_started").alias("checkout_sessions"),
                              F.sum("website_purchase").alias("website_purchase_sessions"),
                              F.sum("activity_event_count").alias("activity_events"))
                  .withColumn("product_view_rate_from_page_view",
                              F.when(F.col("page_view_sessions") > 0,
                                     F.col("product_view_sessions") / F.col("page_view_sessions")))
                  .withColumn("add_to_cart_rate_from_page_view",
                              F.when(F.col("page_view_sessions") > 0,
                                     F.col("add_to_cart_sessions") / F.col("page_view_sessions")))
                  .withColumn("checkout_rate_from_page_view",
                              F.when(F.col("page_view_sessions") > 0,
                                     F.col("checkout_sessions") / F.col("page_view_sessions")))
                  .withColumn("website_purchase_rate_from_page_view",
                              F.when(F.col("page_view_sessions") > 0,
                                     F.col("website_purchase_sessions") / F.col("page_view_sessions")))
                  .withColumn("purchase_is_payment_confirmation", F.lit(False)))
        write("website_conversion_funnel", funnel)

        order_status = (orders.groupBy("status")
                        .agg(F.countDistinct("order_id").alias("order_count"),
                             F.sum("amount").alias("order_value"),
                             F.countDistinct("customer_id").alias("customer_count")))
        write("order_status_analysis", order_status)

        # The daily and monthly successful-payment totals must agree.
        daily_total = daily.agg(F.sum("successful_payment_revenue")).first()[0] or 0
        monthly_total = monthly.agg(F.sum("successful_payment_revenue")).first()[0] or 0
        if abs(float(daily_total) - float(monthly_total)) > 0.01:
            raise RuntimeError(f"Revenue reconciliation failed: daily={daily_total}, monthly={monthly_total}")
        LOG.info("Revenue reconciliation passed: daily=%s monthly=%s", daily_total, monthly_total)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()

