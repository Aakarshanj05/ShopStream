from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("ShopStream-Customer-Silver-ReadOnly-Audit").master("local[2]").config("spark.ui.enabled", "false").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
try:
    bronze = spark.read.parquet("/opt/spark-data/bronze/events/topic=customers_topic")
    silver = spark.read.parquet("/opt/spark-data/silver/customers")
    quarantine = spark.read.parquet("/opt/spark-data/silver/quarantine/customers")
    bronze_count, silver_count, quarantine_count = bronze.count(), silver.count(), quarantine.count()
    print(f"CUSTOMER_COUNTS bronze={bronze_count} silver={silver_count} quarantine={quarantine_count} reconciles={silver_count + quarantine_count == bronze_count}")
    print("QUARANTINE_REASONS")
    quarantine.groupBy("rejection_reason").count().orderBy("rejection_reason").show(100, False)
    print("QUARANTINE_RECORDS")
    quarantine.select("customer_id", "email", "membership", "created_at_raw", "rejection_reason", "kafka_partition", "kafka_offset", "event_data").show(50, False)
    print("BRONZE_TOPIC_COUNTS")
    spark.read.parquet("/opt/spark-data/bronze/events").groupBy("topic").count().orderBy("topic").show(20, False)
finally:
    spark.stop()



