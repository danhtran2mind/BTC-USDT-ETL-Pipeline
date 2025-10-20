from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, LongType, DoubleType, IntegerType
from pyspark.sql.functions import col, row_number, floor, first, max, min, last, sum
from pyspark.sql.window import Window

import os
import sys
import shutil
import pandas as pd
import ast
import io

# Add the project root directory to the Python path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from minio_api.minio_utils import get_minio_data
from minio_api.client import sign_in


def initialize_spark_session(app_name="MinIO to Spark DataFrame", 
                             driver_memory="4g", executor_memory="4g"):
    return SparkSession.builder \
        .appName(app_name) \
        .config("spark.driver.memory", driver_memory) \
        .config("spark.executor.memory", executor_memory) \
        .getOrCreate()

def create_dataframe_from_csv(spark, parquet_file_path, schema, temp_parquet_path="temp/temp_parquet_chunks", 
                              chunk_size=int(3e+6)):
    os.makedirs(temp_parquet_path, exist_ok=True)
    
    # Clear the temporary Parquet path if it exists
    if os.path.exists(temp_parquet_path):
        shutil.rmtree(temp_parquet_path)
    
    # Read Parquet file directly with Spark, applying the schema
    df = spark.read.schema(schema).parquet(parquet_file_path)
    
    # Write the DataFrame to the temporary Parquet path (for consistency with original logic)
    df.write.mode("append").parquet(temp_parquet_path)
    
    # Read back the Parquet data from the temporary path
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
                       file_names=["BTCUSDT-1s-2025-09.csv"]):
    minio_client = sign_in()
    out_pqrquet_file_paths = []
    for file_name in file_names:
        csv_lines = get_minio_data(minio_client, bucket_name, file_name)
        if not csv_lines:
            raise ValueError(f"No data retrieved from MinIO for bucket {bucket_name}, file {file_name}")
        temp_parquet_path = f"temp/extracted_from_minio/{os.path.splitext(os.path.basename(file_name))[0]}.parquet"
        os.makedirs(os.path.dirname(temp_parquet_path), exist_ok=True)
        # with open(temp_file_path, 'w') as f:
        #     f.write('\n'.join(csv_lines))

        # Convert CSV lines to DataFrame and write to parquet
        df = pd.read_csv(io.StringIO('\n'.join(csv_lines)))
        # print("df.columns========================", df.columns)
        df.to_parquet(temp_parquet_path, index=False)  

        out_pqrquet_file_paths.append(temp_parquet_path)
    
    return out_pqrquet_file_paths#, out_temp_file_paths

def transform_financial_data(csv_file_paths, 
                            temp_parquet_path="temp/temp_parquet_chunks", 
                            output_parquet_path="temp/aggregated_output"):
    spark = initialize_spark_session()
    
    try:
        # Define the schema
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

        # output_parquet_paths = []
        if isinstance(csv_file_paths, str):
            try:
                csv_file_paths = ast.literal_eval(csv_file_paths)
            except (ValueError, SyntaxError) as e:
                raise ValueError(f"Failed to parse server_files as a list: {csv_file_paths}, error: {e}")

        for csv_file_path in csv_file_paths:
            # Create DataFrame using create_dataframe_from_csv
            df = create_dataframe_from_csv(spark, csv_file_path, schema, temp_parquet_path)
            print("Created Spark DataFrame from CSV file.")
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
            # output_parquet_paths.append(output_parquet_path)

        # name_output_parquet_paths = [os.path.basename(path) for path in output_parquet_paths]
        
        return output_parquet_path#, name_output_parquet_paths
    
    except Exception as e:
        print(f"Error in transform_financial_data: {e}")
        raise
    finally:
        spark.stop()

if __name__ == "__main__":
    # Example usage
    extracted_parquet_path = extract_from_minio()
    output_parquet_path, name_output_parquet_paths = transform_financial_data(extracted_parquet_path)
    print(output_parquet_path)
    print(name_output_parquet_paths)