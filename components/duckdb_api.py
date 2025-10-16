import os
import duckdb
import shutil
import pandas as pd

def push_to_duckdb(duckdb_path, parquet_path, temp_parquet_path="temp/duckdb_temp_parquet"):
    """
    Push the aggregated data from a Parquet directory to DuckDB.
    
    Args:
        duckdb_path (str): Path to the DuckDB database file
        parquet_path (str): Path to the Parquet directory containing the aggregated data
        temp_parquet_path (str): Temporary path for storing Parquet files
    """
    # Validate input parquet_path
    if not isinstance(parquet_path, str):
        raise ValueError(f"parquet_path must be a string, got {type(parquet_path)}: {parquet_path}")
    if not os.path.exists(parquet_path):
        raise FileNotFoundError(f"Parquet directory does not exist at {parquet_path}")
    if not os.path.isdir(parquet_path):
        raise ValueError(f"parquet_path must be a directory, got a file at {parquet_path}")

    # Ensure the temporary directory is clean before copying
    if os.path.exists(temp_parquet_path):
        shutil.rmtree(temp_parquet_path)
    os.makedirs(temp_parquet_path, exist_ok=True)

    # Copy the input Parquet directory to the temporary directory
    try:
        shutil.copytree(parquet_path, temp_parquet_path, dirs_exist_ok=True)
        print(f"Copied Parquet directory from {parquet_path} to {temp_parquet_path}")
    except Exception as e:
        raise RuntimeError(f"Failed to copy Parquet directory from {parquet_path} to {temp_parquet_path}: {e}")

    # Connect to DuckDB
    directory = os.path.dirname(duckdb_path)
    if directory and not os.path.exists(directory):
        os.makedirs(directory)
    
    con = duckdb.connect(duckdb_path)
  
    # Create or replace the table in DuckDB by reading the Parquet files
    try:
        con.execute(f"""
            CREATE OR REPLACE TABLE aggregated_financial_data AS
            SELECT * FROM parquet_scan('{temp_parquet_path}/*.parquet')
        """)
        print(f"Successfully loaded data into DuckDB table from {temp_parquet_path}")
    except Exception as e:
        raise RuntimeError(f"Failed to load Parquet files into DuckDB: {e}")
    finally:
        con.close()
    
    # Clean up temporary Parquet directory
    if os.path.exists(temp_parquet_path):
        shutil.rmtree(temp_parquet_path)
        print(f"Cleaned up temporary directory {temp_parquet_path}")

if __name__ == "__main__":
    from process_data import process_financial_data
    duckdb_path = "duckdb_databases/financial_data.db"
    parquet_path = process_financial_data()
    
    try:
        push_to_duckdb(duckdb_path, parquet_path)
    except Exception as e:
        print(f"Error pushing to DuckDB: {e}")
    
