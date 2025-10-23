import duckdb
import pandas as pd
import logging

def duckdb_to_csv(duckdb_path, output_csv_path):
    try:
        # Connect to DuckDB
        con = duckdb.connect(duckdb_path)
        # Query data
        df = con.execute("SELECT * FROM aggregated_financial_data").fetchdf()
        if df.empty:
            raise ValueError("No data found in table 'aggregated_financial_data'")
        # Save to CSV
        df.to_csv(output_csv_path, index=False)
        logging.info(f"Successfully exported data to {output_csv_path}")
    except Exception as e:
        logging.error(f"Error in duckdb_to_csv: {str(e)}")
        raise

if __name__ == "__main__":
    duckdb_to_csv("duckdb_databases/financial_data.db",
                      "analytics/financial_data.csv")