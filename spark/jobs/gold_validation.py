"""Read-only checks for Gold v2 outputs and revenue reconciliation."""
import os

from pyspark.sql import SparkSession, functions as F

spark = (SparkSession.builder.appName("ShopStream-Gold-v2-Validation")
         .master("spark://spark-master:7077")
         .config("spark.sql.session.timeZone", "UTC").getOrCreate())
spark.sparkContext.setLogLevel("ERROR")
root = os.path.join(os.getenv("SHOPSTREAM_DATA_ROOT", "/opt/spark-data"), "gold_v2")
try:
    daily = spark.read.parquet(root + "/daily_revenue")
    monthly = spark.read.parquet(root + "/monthly_revenue")
    daily_total = daily.agg(F.sum("successful_payment_revenue")).first()[0] or 0
    monthly_total = monthly.agg(F.sum("successful_payment_revenue")).first()[0] or 0
    if abs(float(daily_total) - float(monthly_total)) > 0.01:
        raise RuntimeError(f"Daily/monthly revenue mismatch: {daily_total} != {monthly_total}")
    funnel = spark.read.parquet(root + "/website_conversion_funnel")
    if funnel.filter(F.col("purchase_is_payment_confirmation") != F.lit(False)).limit(1).count():
        raise RuntimeError("Website purchase must not be treated as payment confirmation")
    expected = [
        "daily_revenue", "monthly_revenue", "product_performance",
        "category_performance", "customer_lifetime_value",
        "customer_segmentation", "payment_success_failure",
        "inventory_levels_and_movement", "website_conversion_funnel",
        "order_status_analysis",
    ]
    for name in expected:
        count = spark.read.parquet(f"{root}/{name}").count()
        print(f"RESULT {name} rows={count}")
    print(f"REVENUE_RECONCILIATION_PASSED total={daily_total}")
finally:
    spark.stop()

