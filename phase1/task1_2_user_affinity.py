import os
from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import col, when, sum as _sum, lit, collect_list, explode, split, get_json_object, row_number

# Setup paths relative to script
base_path = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
csv_path = os.path.join(base_path, "ecommerce_logs.csv")
output_path = os.path.join(base_path, "phase1_output", "user_affinity")

# Weight mapping as per requirements
WEIGHTS = {"view": 1, "cart": 3, "purchase": 5}

spark = SparkSession.builder\
    .appName("UserAffinityAggregation")\
    .master("local[*]")\
    .getOrCreate()

# Load CSV (with escape='"' to properly parse the double-quoted JSON strings)
df = spark.read.csv(csv_path, header=True, inferSchema=True, escape='"')

# Add weight column based on event_type
weight_expr = when(col("event_type") == "view", lit(WEIGHTS["view"])) \
    .when(col("event_type") == "cart", lit(WEIGHTS["cart"])) \
    .when(col("event_type") == "purchase", lit(WEIGHTS["purchase"])) \
    .otherwise(lit(0))

df = df.withColumn("weight", weight_expr)

# Join with product metadata to get category and drop nulls
df = df.withColumn("category", get_json_object(col("product_metadata"), "$.category"))
df = df.filter(col("category").isNotNull())

# Aggregate weighted scores per user-category
agg_df = df.groupBy("user_id", "category")\
    .agg(_sum("weight").alias("affinity_score"))

# For each user, collect categories sorted by descending affinity
window_spec = Window.partitionBy("user_id").orderBy(col("affinity_score").desc())

ranked = agg_df.withColumn("rn", row_number().over(window_spec))

# Keep top N categories per user (e.g., top 3)
TOP_N = 3
top_affinity = ranked.filter(col("rn") <= TOP_N)

# Output as JSON for downstream usage
# We use Pandas to bypass Hadoop winutils requirement on Windows
os.makedirs(output_path, exist_ok=True)
result_df = top_affinity.select(col("user_id"), col("category"), col("affinity_score"))
result_df.toPandas().to_json(os.path.join(output_path, "affinity_results.json"), orient="records", lines=True)

spark.stop()
