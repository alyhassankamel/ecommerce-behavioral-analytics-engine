import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Setup paths relative to script
base_path = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
csv_path = os.path.join(base_path, "ecommerce_logs.csv")
output_path = os.path.join(base_path, "phase1_output", "market_basket_counts")

# Initialize Spark
spark = SparkSession.builder \
    .appName("MarketBasketCoOccurrence") \
    .master("local[*]") \
    .getOrCreate()

# Load CSV
logs_df = spark.read.csv(csv_path, header=True, inferSchema=True)

# Filter only purchase events
purchase_df = logs_df.filter(col("event_type") == "purchase") \
                     .select("session_id", "product_id") \
                     .distinct()

# Self-join to find pairs of products in the same session
# We use p1.product_id < p2.product_id to avoid duplicates like (A,B) and (B,A) and self-pairs (A,A)
pairs_df = purchase_df.alias("p1").join(
    purchase_df.alias("p2"),
    (col("p1.session_id") == col("p2.session_id")) & (col("p1.product_id") < col("p2.product_id"))
).select(
    col("p1.product_id").alias("item_a"),
    col("p2.product_id").alias("item_b")
)

# Count co-occurrences
result_df = pairs_df.groupBy("item_a", "item_b") \
                    .count() \
                    .withColumnRenamed("count", "cooccurrence_count")

# Show top 20 most frequent co-occurring pairs
result_df.orderBy(col("cooccurrence_count").desc()).show(20, truncate=False)

# Save output using Pandas (bypasses Hadoop winutils requirement on Windows)
# We add .csv extension manually as Pandas writes a single file
result_df.toPandas().to_csv(output_path + ".csv", index=False)

spark.stop()
