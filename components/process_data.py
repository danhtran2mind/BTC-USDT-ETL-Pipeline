# scripts/process_data.py
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, LongType, DoubleType, IntegerType
from pyspark.sql.functions import col, row_number, floor, first, max, min, last, sum
from pyspark.sql.window import Window
from minio_api.client import sign_in
import os
import sys
import shutil
import pandas as pd

# Add the project root directory to the Python path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from minio_api.minio_utils import get_minio_data

def initialize_spark_session(app_name="MinIO to Spark DataFrame", 
                             driver_memory="4g", executor_memory="4g"):
    return SparkSession.builder \
        .appName(app_name) \
        .config("spark.driver.memory", driver_memory) \
        .config("spark.executor.memory", executor_memory) \
        .getOrCreate()

def create_dataframe_from_csv(spark, csv_lines, temp_parquet_path="temp/temp_parquet_chunks", 
                              chunk_size=int(3e+6)):
    os.makedirs(temp_parquet_path, exist_ok=True)
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

    if csv_lines and csv_lines[0].startswith("Open time,"):
        data_lines = csv_lines[1:]
    else:
        data_lines = csv_lines

    if os.path.exists(temp_parquet_path):
        shutil.rmtree(temp_parquet_path)

    for i in range(0, len(data_lines), chunk_size):
        chunk = data_lines[i:i + chunk_size]
        rdd_chunk = spark.sparkContext.parallelize(chunk).repartition(8)
        df_chunk = spark.read.schema(schema).csv(rdd_chunk, header=False)
        df_chunk.write.mode("append").parquet(temp_parquet_path)

    return spark.read.parquet(temp_parquet_path)

def resample_dataframe(df, track_each=3600):
    keep_cols = ["Open time", "Open", "High", "Low", "Close", "Number of trades"]
    df = df.select(keep_cols)
    window_spec = Window.orderBy("Open time")
    df = df.withColumn("row_number", row_number().over(window_spec))
    df = df.withColumn("group_id", floor((col("row_number") - 1) / track_each))
    aggregations = [
        first("Open time").alias("Open time"),
        first("Open").alias("Open"),
        max("High").alias("High"),
        min("Low").alias("Low"),
        last("Close").alias("Close"),
        sum("Number of trades").alias("Number of trades")
    ]
    aggregated_df = df.groupBy("group_id").agg(*aggregations)
    return aggregated_df.select("Open time", "Open", "High", "Low", "Close", "Number of trades")

def extract_from_minio(bucket_name="minio-ngrok-bucket", 
                   file_name="BTCUSDT-1s-2025-09.csv"):
    minio_client = sign_in()
    csv_lines = get_minio_data(minio_client, bucket_name, file_name)
    return csv_lines

def transform_financial_data(csv_lines,                    
                   temp_parquet_path="temp/temp_parquet_chunks", 
                   output_parquet_path="temp/aggregated_output"):
    # minio_client = sign_in()
    spark = initialize_spark_session()
    
    try:
        df = create_dataframe_from_csv(spark, csv_lines, temp_parquet_path)
        print("Created Spark DataFrame from CSV data.")
        aggregated_df = resample_dataframe(df)
        print("Resampled DataFrame with OHLC aggregations.")
        
        # Save aggregated DataFrame to a temporary Parquet directory
        os.makedirs(os.path.dirname(output_parquet_path), exist_ok=True)
        aggregated_df.write.mode("overwrite").parquet(output_parquet_path)
        print(f"Saved aggregated DataFrame to {output_parquet_path}")
        
        # Verify that the Parquet directory exists
        if not os.path.exists(output_parquet_path) or not os.path.isdir(output_parquet_path):
            raise FileNotFoundError(f"Parquet directory {output_parquet_path} was not created or is not a directory.")
        else:
            print(f"Verified: Parquet directory exists at {output_parquet_path}")
        
        return output_parquet_path
    
    except Exception as e:
        print(f"Error in process_financial_data: {e}")
        raise
    finally:
        spark.stop()

if __name__ == "__main__":
    # Example usage
    minio_client = sign_in()
    extracted_csv_lines = extract_from_minio(minio_client)
    output_parquet_path = transform_financial_data()
    print(output_parquet_path)