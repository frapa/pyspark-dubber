from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# -------------------------------------
# Create Spark session
# -------------------------------------
spark = SparkSession.builder.appName("DrugDevelopmentStageCount").getOrCreate()

# -------------------------------------
# Read multiple parquet files
# Replace with your real paths
# -------------------------------------
input_paths = ["data/drugs.parquet"]

df = spark.read.parquet(*input_paths)

# Optional: inspect schema
df.printSchema()

# -------------------------------------
# Count number of drugs per clinical phase
# -------------------------------------
phase_counts = df.groupBy("phase").count().orderBy(col("phase"))

# -------------------------------------
# Print results
# -------------------------------------
print("Number of drugs by clinical phase:")
phase_counts.show(truncate=False)

spark.stop()
