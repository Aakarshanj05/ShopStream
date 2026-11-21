
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = (
    SparkSession.builder
    .appName("ShopStream-Spark-Test")
    .master("spark://spark-master:7077")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

print("\n===== SPARK CLUSTER TEST =====")

data = [
    (1, "Laptop", 50000),
    (2, "Mobile", 20000),
    (3, "Headphones", 3000),
    (4, "Keyboard", 1500),
]

columns = ["product_id", "product_name", "price"]

df = spark.createDataFrame(data, columns)

print("\nOriginal DataFrame:")
df.show()

print("\nProducts costing more than Rs. 5000:")
df.filter(col("price") > 5000).show()

print("\nTotal products:", df.count())

print("\nSpark test completed successfully!")

spark.stop()
