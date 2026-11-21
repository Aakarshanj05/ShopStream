from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Create Spark session
spark = (
    SparkSession.builder
    .appName("ShopStream-Kafka-Integration-Test")
    .master("spark://spark-master:7077")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

print("Starting Spark-Kafka integration test...")

# Read streaming data from Kafka
kafka_df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "kafka:19092")
    .option("subscribe", "customers_topic")
    .option("startingOffsets", "earliest")
    .option("maxOffsetsPerTrigger", 1000)
    .load()
)

# Extract Kafka message details
customers_df = kafka_df.select(
    col("key").cast("string").alias("kafka_key"),
    col("value").cast("string").alias("customer_json"),
    col("topic"),
    col("partition"),
    col("offset"),
    col("timestamp").alias("kafka_timestamp")
)

# Display records in the console
query = (
    customers_df.writeStream
    .format("console")
    .outputMode("append")
    .option("truncate", False)
    .option("numRows", 5)
    .option(
        "checkpointLocation",
        "/opt/spark-checkpoints/kafka_stream_test"
    )
    .trigger(availableNow=True)
    .start()
)

query.awaitTermination()

print("Spark-Kafka integration test completed!")

spark.stop()