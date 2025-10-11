import os
import duckdb
import shutil

def push_to_duckdb(duckdb_path, aggregated_df, temp_parquet_path="temp/duckdb_temp_parquet"):
    """
    Push the aggregated Spark DataFrame to DuckDB via a temporary Parquet file.
    
    Args:
        duckdb_path (str): Path to the DuckDB database file
        aggregated_df: Spark DataFrame to be pushed
        temp_parquet_path (str): Temporary path for storing Parquet files
    """
    # Ensure the directory for the temporary Parquet file exists
    os.makedirs(temp_parquet_path, exist_ok=True)
    
    # Write Spark DataFrame to Parquet
    aggregated_df.write.mode("overwrite").parquet(temp_parquet_path)
    
    # Connect to DuckDB
    directory = os.path.dirname(duckdb_path)
    if directory and not os.path.exists(directory):
        os.makedirs(directory)
    
    con = duckdb.connect(duckdb_path)  # Create or connect to a DuckDB database file
    
    # Create or replace the table in DuckDB by reading only .parquet files
    con.execute(f"""
        CREATE OR REPLACE TABLE aggregated_financial_data AS
        SELECT * FROM parquet_scan('{temp_parquet_path}/*.parquet')
    """)
    
    # Close the DuckDB connection
    con.close()
    
    # Clean up temporary Parquet files
    if os.path.exists(temp_parquet_path):
        shutil.rmtree(temp_parquet_path)

if __name__ == "__main__":
    from process_data import process_financial_data
    # Example usage
    duckdb_path = "duckdb_databases/financial_data.db"
    
    # Get Spark session and aggregated DataFrame
    spark, aggregated_df = process_financial_data()
    
    try:
        push_to_duckdb(duckdb_path, aggregated_df)
    finally:
        # Clean up Spark session
        spark.stop()