from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import FloatType

# Initialize Spark Session
spark = SparkSession.builder.appName("CorrelationAnalysis").getOrCreate()

# --- Data Loading ---
reviews_path = "data/amazon_reviews.jsonl.gz"
meta_path = "data/amazon_reviews_meta.jsonl.gz"

print("Loading data...")

# Read reviews data
reviews_df = (
    spark.read.json(reviews_path)
    .select("parent_asin", "rating", "text")
    .filter(F.col("text").isNotNull())
)  # Filter out reviews with no text

# Read metadata (only needed for the join mandate)
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
)

print(f"Total joined records with text: {joined_df.count()}")

# --- Analysis 3: Correlate Rating and Text Length ---

# 1. Calculate Text Length (number of characters)
# We use length of the text string
df_with_length = joined_df.withColumn("text_length", F.length(F.col("text")))

# 2. Calculate the Pearson Correlation Coefficient
# The 'rating' column is already numeric, 'text_length' is an integer.
# Cast rating to Float for the correlation function to be safe.
correlation_value = df_with_length.select(
    F.corr(
        F.col("rating").cast(FloatType()), F.col("text_length").cast(FloatType())
    ).alias("rating_length_correlation")
).collect()[0]["rating_length_correlation"]

print("\n--- Correlation Results ---")
print(f"Pearson Correlation (Rating vs. Text Length): {correlation_value:.4f}")
print(
    "A value close to +1 indicates a positive correlation (longer text -> higher rating)."
)
print(
    "A value close to -1 indicates a negative correlation (longer text -> lower rating)."
)
print("A value close to 0 indicates no linear correlation.")

# Show a sample of the data used for correlation
print("\n--- Sample of Data Used for Correlation ---")
df_with_length.select("rating", "text_length", "text").limit(5).show(truncate=50)


# --- Output to File (JSON) ---
# For this script, we'll write the intermediate DF used for calculation to JSON
output_path = "output/rating_length_data_json"
print(f"\nWriting intermediate rating/length data to JSON at: {output_path}")

# Write the data frame as JSON, containing the rating and the calculated length
df_with_length.select("parent_asin", "rating", "text_length").write.mode(
    "overwrite"
).json(output_path)

# Stop Spark Session
spark.stop()
