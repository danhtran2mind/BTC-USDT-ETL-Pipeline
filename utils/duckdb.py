import duckdb

def push_to_duckdb(duckdb_path, aggregated_df):
    """Push the aggregated DataFrame to DuckDB."""
    # Connect to DuckDB
    # con = duckdb.connect("duckdb_databases/financial_data.db")
    con = duckdb.connect(duckdb_path)  # Create or connect to a DuckDB database file
    # Register the Pandas DataFrame
    con.register("temp_view", aggregated_df.toPandas())

    # Create or replace the table in DuckDB
    con.execute("""
        CREATE OR REPLACE TABLE aggregated_financial_data AS
        SELECT * FROM temp_view
    """)

    # Close the DuckDB connection
    con.close()

if __name__ == "__main__":
    # Example usage
    duckdb_path = "duckdb_databases/financial_data.db"
    # Assume aggregated_df is defined elsewhere in your code
    push_to_duckdb(duckdb_path, aggregated_df)
