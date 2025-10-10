from minio import Minio
from io import BytesIO
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, LongType, DoubleType, IntegerType
from pyspark.sql.functions import col, from_unixtime, unix_timestamp, first, max, min, last, sum, row_number, floor
import os
import shutil

from minio_api.client import sign_in  # Import the sign_in function from minio/client.py
from utils.minio import get_minio_csv  # Import the get_ninio_csv function from utils/minio.py

from pyspark.sql.functions import col, from_unixtime, window, first, max, min, last, sum
from pyspark.sql.types import TimestampType
from pyspark.sql.window import Window
import duckdb
#=========================MinIO Part=========================
# Get MinIO client using sign_in function
minio_client = sign_in()

# Fetch CSV data from MinIO
csv_lines = get_minio_csv(minio_client, bucket_name="minio-ngrok-bucket", file_name="input.csv")
#=========================PySpark Part=========================

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("MinIO to Spark DataFrame") \
    .config("spark.driver.memory", "4g") \
    .config("spark.executor.memory", "4g") \
    .getOrCreate()

# Define the schema based on the provided metadata
schema = StructType([
    StructField("Open time", LongType(), True),
    StructField("Open", DoubleType(), True),
    StructField("High", DoubleType(), True),
    StructField("Low", DoubleType(), True),
    StructField("Close", DoubleType(), True),
    StructField("Volume", DoubleType(), True),
    StructField("Close time", LongType(), True),
    StructField("Quote asset volume", DoubleType(), True),
    StructField("Number of trades", IntegerType(), True),
    StructField("Taker buy base asset volume", DoubleType(), True),
    StructField("Taker buy quote asset volume", DoubleType(), True),
    StructField("Ignore", IntegerType(), True)
])


# Check if csv_lines contains a header
# Expected header: "Open time,Open,High,Low,Close,Volume,Close time,Quote asset volume,Number of trades,Taker buy base asset volume,Taker buy quote asset volume,Ignore"
if csv_lines and csv_lines[0].startswith("Open time,"):
    # Skip the header row
    data_lines = csv_lines[1:]
else:
    data_lines = csv_lines  # No header present

# Process data_lines in chunks

temp_parquet_path = "temp/temp_parquet_chunks"  # Replace with your desired output directory

# Clear the output directory if it exists
if os.path.exists(temp_parquet_path):
    shutil.rmtree(temp_parquet_path)

chunk_size = 100000  # Adjust based on memory (start with 100,000 rows)
# Process each chunk
for i in range(0, len(data_lines), chunk_size):
    chunk = data_lines[i:i + chunk_size]
    # Create RDD for the chunk
    rdd_chunk = spark.sparkContext.parallelize(chunk)
    # Convert to DataFrame with explicit schema
    df_chunk = spark.read.schema(schema).csv(rdd_chunk, header=False)  # Header already skipped
    # Write chunk to Parquet in append mode
    df_chunk.write.mode("append").parquet(temp_parquet_path)

# Read the full DataFrame from Parquet
df = spark.read.parquet(temp_parquet_path)

#=========================Resampling Part=========================
keep_cols = ["Open time", "Open", "High", "Low", "Close", "Number of trades"]
df = df.select(keep_cols)

# Add a row number to group every track_each rows
track_each = 3600 * 1  # Example: Group every 5 rows (can be changed to 10, 12, etc.)
window_spec = Window.orderBy("Open time")  # Ensure rows are ordered by Open time
df = df.withColumn("row_number", row_number().over(window_spec))

# Create a group identifier for every track_each rows
df = df.withColumn("group_id", floor((col("row_number") - 1) / track_each))

# Define aggregation rules for OHLC and Number of trades
aggregations = [
    first("Open time").alias("Open time"),  # First Open time in the group
    first("Open").alias("Open"),           # First open price
    max("High").alias("High"),             # Max high
    min("Low").alias("Low"),               # Min low
    last("Close").alias("Close"),          # Last close price
    sum("Number of trades").alias("Number of trades")  # Sum of trades
]

# Group by the group_id and apply aggregations
aggregated_df = df.groupBy("group_id").agg(*aggregations)

# Reorder columns to match the desired output and drop group_id
aggregated_df = aggregated_df.select("Open time", "Open", "High", "Low", "Close", "Number of trades")



#=========================Clear Temp Files and PySpark Stop=========================

# Clean up the temporary Parquet file
if os.path.exists(temp_parquet_path):
    shutil.rmtree(temp_parquet_path)

# Step 6: Save the aggregated DataFrame to a new Parquet file
# output_aggregated_path = "output_aggregated_financial_data"
# aggregated_df.write.mode("overwrite").parquet(output_aggregated_path)


# Stop the SparkSession
spark.stop()