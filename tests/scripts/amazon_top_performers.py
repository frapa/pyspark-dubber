from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# Initialize Spark Session
spark = SparkSession.builder.appName("TopBottomPerformersAnalysis").getOrCreate()

# --- Data Loading ---
# NOTE: Replace these paths with the actual paths to your .jsonl.gz files
reviews_path = Path("data/amazon_reviews.jsonl.gz").absolute()
meta_path = Path("data/amazon_reviews_meta.jsonl.gz").absolute()

print("Loading data...")

# Read reviews data
reviews_df = spark.read.json(str(reviews_path))

# Read metadata (product features)
meta_df = spark.read.json(str(meta_path)).select(
    F.col("parent_asin").alias("meta_parent_asin"), "main_category", "store"
)

# --- Data Merging ---
# Join on the common parent_asin key
joined_df = reviews_df.join(
    meta_df, reviews_df["parent_asin"] == meta_df["meta_parent_asin"], "inner"
).cache()  # Cache the joined DF for multiple aggregations

print(f"Total joined records: {joined_df.count()}")

# --- Analysis 1: Top/Bottom Performers (Product, Store, Category) ---

# 1. Product Level Aggregation
product_agg = (
    joined_df.groupBy("parent_asin")
    .agg(F.avg("rating").alias("average_rating"), F.count("*").alias("review_count"))
    .filter(F.col("review_count") >= 10)
)  # Filter out products with too few reviews

print("\n--- Top 5 Products by Average Rating ---")
top_products = product_agg.orderBy(F.col("average_rating").desc(), "parent_asin").limit(
    5
)
top_products.show(truncate=False)

print("\n--- Bottom 5 Products by Average Rating ---")
bottom_products = product_agg.orderBy(F.col("average_rating").asc()).limit(5)
bottom_products.show(truncate=False)

# 2. Store Level Aggregation
store_agg = (
    joined_df.groupBy("store")
    .agg(F.avg("rating").alias("average_rating"), F.count("*").alias("review_count"))
    .filter(F.col("review_count") >= 50)
)  # Filter out stores with low volume

print("\n--- Top 5 Stores by Average Rating ---")
store_agg.orderBy(F.col("average_rating").desc()).limit(5).show(truncate=False)

# 3. Category Level Aggregation
category_agg = (
    joined_df.groupBy("main_category")
    .agg(F.avg("rating").alias("average_rating"), F.count("*").alias("review_count"))
    .filter(F.col("review_count") >= 100)
)  # Filter out small categories

print("\n--- Top 5 Categories by Average Rating ---")
category_agg.orderBy(F.col("average_rating").desc()).limit(5).show(truncate=False)


# --- Output to File (CSV) ---
# Combine all results into one DF for the output file
final_df = (
    product_agg.withColumn("level", F.lit("product"))
    .unionByName(
        store_agg.withColumn("level", F.lit("store")).withColumnRenamed(
            "store", "identifier"
        ),
        allowMissingColumns=True,
    )
    .unionByName(
        category_agg.withColumn("level", F.lit("category")).withColumnRenamed(
            "main_category", "identifier"
        ),
        allowMissingColumns=True,
    )
    .fillna("N/A", subset=["parent_asin", "identifier"])
    .orderBy("parent_asin", "average_rating")
)

output_path = Path("output").absolute()
print(f"\nWriting combined performance results to CSV")

# Write as CSV, overwriting if needed, with a header
final_df.write.mode("overwrite").option("header", "true").csv(str(output_path))

# Stop Spark Session
spark.stop()
