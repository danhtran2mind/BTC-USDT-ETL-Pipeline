from minio import Minio
from io import BytesIO
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, LongType, DoubleType, IntegerType
from pyspark.sql.functions import col, row_number, floor, first, max, min, last, sum
from pyspark.sql.window import Window
import os
import shutil

from minio_api.client import sign_in
from utils.minio import get_minio_data

def initialize_spark_session(app_name="MinIO to Spark DataFrame", driver_memory="4g", executor_memory="4g"):
    return SparkSession.builder \
        .appName(app_name) \
        .config("spark.driver.memory", driver_memory) \
        .config("spark.executor.memory", executor_memory) \
        .getOrCreate()

def create_dataframe_from_csv(spark, csv_lines, temp_parquet_path="temp/temp_parquet_chunks", chunk_size=100000):
    """
    Convert CSV lines to a Spark DataFrame using a defined schema and write to temporary Parquet.
    
    Args:
        spark: SparkSession instance
        csv_lines (list): List of CSV lines
        temp_parquet_path (str): Path for temporary Parquet files
        chunk_size (int): Number of rows per chunk
    
    Returns:
        DataFrame: Spark DataFrame created from CSV data
    """
    # Define schema
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

    # Check for header and skip if present
    if csv_lines and csv_lines[0].startswith("Open time,"):
        data_lines = csv_lines[1:]
    else:
        data_lines = csv_lines

    # Clear temp directory if exists
    if os.path.exists(temp_parquet_path):
        shutil.rmtree(temp_parquet_path)

    # Process data in chunks
    for i in range(0, len(data_lines), chunk_size):
        chunk = data_lines[i:i + chunk_size]
        rdd_chunk = spark.sparkContext.parallelize(chunk)
        df_chunk = spark.read.schema(schema).csv(rdd_chunk, header=False)
        df_chunk.write.mode("append").parquet(temp_parquet_path)

    # Read full DataFrame from Parquet
    return spark.read.parquet(temp_parquet_path)

def resample_dataframe(df, track_each=3600):
    """
    Resample DataFrame by grouping rows and applying OHLC aggregations.
    
    Args:
        df: Input Spark DataFrame
        track_each (int): Number of seconds for grouping (default: 3600 for 1 hour)
    
    Returns:
        DataFrame: Aggregated DataFrame with OHLC and Number of trades
    """
    keep_cols = ["Open time", "Open", "High", "Low", "Close", "Number of trades"]
    df = df.select(keep_cols)

    # Add row number for ordering
    window_spec = Window.orderBy("Open time")
    df = df.withColumn("row_number", row_number().over(window_spec))

    # Create group identifier
    df = df.withColumn("group_id", floor((col("row_number") - 1) / track_each))

    # Define aggregations
    aggregations = [
        first("Open time").alias("Open time"),
        first("Open").alias("Open"),
        max("High").alias("High"),
        min("Low").alias("Low"),
        last("Close").alias("Close"),
        sum("Number of trades").alias("Number of trades")
    ]

    # Apply aggregations
    aggregated_df = df.groupBy("group_id").agg(*aggregations)
    return aggregated_df.select("Open time", "Open", "High", "Low", "Close", "Number of trades")

def process_financial_data(bucket_name="minio-ngrok-bucket", file_name="input.csv", temp_parquet_path="temp/temp_parquet_chunks"):
    """
    Main function to process financial data from MinIO and return aggregated DataFrame.
    
    Args:
        bucket_name (str): MinIO bucket name
        file_name (str): CSV file name
        temp_parquet_path (str): Path for temporary Parquet files
    
    Returns:
        DataFrame: Aggregated Spark DataFrame
    """
    # Initialize components
    minio_client = sign_in()
    spark = initialize_spark_session()
    
    try:
        # Fetch and process data
        csv_lines = get_minio_data(minio_client, bucket_name, file_name)
        df = create_dataframe_from_csv(spark, csv_lines, temp_parquet_path)
        aggregated_df = resample_dataframe(df)
        
        return aggregated_df
    
    finally:
        # Clean up
        if os.path.exists(temp_parquet_path):
            shutil.rmtree(temp_parquet_path)
        spark.stop()

if __name__ == "__main__":
    # Execute the main processing function and display the result
    aggregated_df = process_financial_data()
    aggregated_df.show()