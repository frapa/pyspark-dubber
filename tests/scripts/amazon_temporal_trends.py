from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# Initialize Spark Session
spark = SparkSession.builder.appName("TemporalTrendsAnalysis").getOrCreate()

# --- Data Loading ---
reviews_path = "data/amazon_reviews.jsonl.gz"
meta_path = "data/amazon_reviews_meta.jsonl.gz"

print("Loading data...")

# Read reviews data
reviews_df = spark.read.json(reviews_path).select("parent_asin", "rating", "timestamp")

# Read metadata (only needed for the join mandate, we'll keep the ASIN)
meta_df = spark.read.json(meta_path).select(
    F.col("parent_asin").alias("meta_parent_asin")
)

# --- Data Merging ---
# Join on the common parent_asin key
joined_df = (
    reviews_df.join(
        meta_df, reviews_df["parent_asin"] == meta_df["meta_parent_asin"], "inner"
    )
    .drop("meta_parent_asin")
    .cache()
)  # Drop redundant column and cache

print(f"Total joined records: {joined_df.count()}")

# --- Analysis 2: Monthly Temporal Trends ---

# 1. Convert timestamp (milliseconds) to date format
# 1602133857705 ms / 1000 = 1602133857.705 seconds
df_with_date = joined_df.withColumn(
    "review_date", (F.col("timestamp") / 1000).cast("timestamp")
).withColumn(
    # Extract year and month for grouping (e.g., "2020-10")
    "year_month",
    F.date_format("review_date", "yyyy-MM"),
)

# 2. Group by Year-Month and calculate aggregations
monthly_trends = (
    df_with_date.groupBy("year_month")
    .agg(
        F.count("*").alias("total_reviews"),
        F.round(F.avg("rating"), 4).alias("avg_monthly_rating"),
    )
    .orderBy("year_month")
)

print("\n--- Monthly Review Volume and Average Rating Trends ---")
monthly_trends.show(50, truncate=False)  # Show more rows for temporal data

# --- Output to File (Parquet) ---
output_path = "output/monthly_trends_parquet"
print(f"\nWriting monthly trend results to Parquet at: {output_path}")

# Parquet is often preferred in Spark environments due to its efficiency
monthly_trends.write.mode("overwrite").parquet(output_path)

# Stop Spark Session
spark.stop()
