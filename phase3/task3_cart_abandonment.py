import os
import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, get_json_object, udf, lit, collect_set
from pyspark.sql.types import StringType
from pymongo import MongoClient

# Setup paths relative to script
base_path = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
csv_path = os.path.join(base_path, "ecommerce_logs.csv")
output_path = os.path.join(base_path, "phase3_output", "campaign_results")

# Initialize Spark
spark = SparkSession.builder \
    .appName("CartAbandonmentRecovery") \
    .master("local[*]") \
    .getOrCreate()

# 1. Read raw logs and filter for Cart Abandonment
# We want users who added to cart but did NOT purchase in that session
# Use escape='"' to properly parse the double-quoted JSON strings in product_metadata
logs_df = spark.read.csv(csv_path, header=True, inferSchema=True, escape='"')

cart_sessions = logs_df.filter(col("event_type") == "cart").select("session_id", "user_id", "product_id", "product_metadata").distinct()
purchase_sessions = logs_df.filter(col("event_type") == "purchase").select("session_id").distinct()

# Left anti join to find sessions with cart but no purchase
abandoned_cart_df = cart_sessions.join(purchase_sessions, "session_id", "left_anti")

# Extract category from product_metadata and filter out NULLs (dirty data)
abandoned_cart_df = abandoned_cart_df.withColumn("category", get_json_object(col("product_metadata"), "$.category"))
abandoned_cart_df = abandoned_cart_df.filter(col("category").isNotNull())

# 2. Load historical User Profiles
# We read the JSON produced in Phase 1 directly into Spark. 
# This is much faster and avoids serialization errors with large datasets.
affinity_json_path = os.path.join(base_path, "phase1_output", "user_affinity", "affinity_results.json")

# Read the JSON and group by user to get the list of categories
profiles_df = spark.read.json(affinity_json_path) \
    .groupBy("user_id") \
    .agg(collect_set("category").alias("top_categories_list"))

# 3. Join the datasets
final_df = abandoned_cart_df.join(profiles_df, "user_id", "left")

from pyspark.sql.functions import when, array_contains

# 4. Flag logic using native Spark functions (avoids Python UDF PicklingError)
result_df = final_df.withColumn(
    "campaign_flag",
    when(
        col("top_categories_list").isNotNull() & array_contains(col("top_categories_list"), col("category")),
        lit("High_Discount")
    ).otherwise(lit("Standard_Reminder"))
)

# Show and save
result_df.select("user_id", "session_id", "category", "campaign_flag").show(20)

# We use Pandas to bypass Hadoop winutils requirement on Windows
# Pandas requires the directory to exist before writing
os.makedirs(os.path.dirname(output_path), exist_ok=True)
result_df.select("user_id", "session_id", "category", "campaign_flag").toPandas().to_csv(output_path + ".csv", index=False)

spark.stop()
