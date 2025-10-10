import duckdb
import pandas as pd

def duckdb_to_gsheets(duckdb_path, output_csv_path):
    # Connect to DuckDB
    con = duckdb.connect(duckdb_path)  # Or ':memory:' for in-memory

    # Query data
    df = con.execute("SELECT * FROM aggregated_financial_data").fetchdf()

    df.to_csv(output_csv_path, index=False)

if __name__ == "__main__":
    duckdb_to_gsheets("duckdb_databases/financial_data.db",
                      "analytics/financial_data.csv")