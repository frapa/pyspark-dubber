from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# Initialize Spark Session
spark = SparkSession.builder.appName("TopBottomPerformersAnalysis").getOrCreate()

# --- Data Loading ---
# NOTE: Replace these paths with the actual paths to your .jsonl.gz files
reviews_path = "data/amazon_reviews.jsonl.gz"
meta_path = "data/amazon_reviews_meta.jsonl.gz"

print("Loading data...")

# Read reviews data
reviews_df = spark.read.json(reviews_path)

# Read metadata (product features)
meta_df = spark.read.json(meta_path).select(
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
top_products = product_agg.orderBy(F.col("average_rating").desc()).limit(5)
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
)

output_path = "output/performance_summary_csv"
print(f"\nWriting combined performance results to CSV at: {output_path}")

# Write as CSV, overwriting if needed, with a header
final_df.write.mode("overwrite").option("header", "true").csv(output_path)

# Stop Spark Session
spark.stop()
