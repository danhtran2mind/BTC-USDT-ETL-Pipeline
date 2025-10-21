import requests
import zipfile
import io
import os
import yaml
import pandas as pd
from datetime import datetime
from pathlib import Path
from typing import Optional

def add_column_headers(csv_data: bytes, expected_columns: list) -> bytes:

    try:
        # Read CSV data into a DataFrame without headers
        df = pd.read_csv(io.BytesIO(csv_data), header=None)
        
        # Validate column count
        if len(df.columns) != len(expected_columns):
            raise ValueError(
                f"CSV has {len(df.columns)} columns, expected {len(expected_columns)}"
            )
        
        # Assign column names
        df.columns = expected_columns
        
        # Convert DataFrame back to CSV bytes with headers
        csv_buffer = io.StringIO()
        df.to_csv(csv_buffer, index=False)
        return csv_buffer.getvalue().encode('utf-8')
    
    except pd.errors.ParserError as e:
        raise Exception(f"Failed to parse CSV data: {e}")
    except Exception as e:
        raise Exception(f"Failed to process CSV with headers: {e}")

def download_and_extract_binance_data(url: str, output_path: str = "temp/input.csv") -> pd.DataFrame:
    expected_columns = [
        "Open time", "Open", "High", "Low", "Close", "Volume",
        "Close time", "Quote asset volume", "Number of trades",
        "Taker buy base asset volume", "Taker buy quote asset volume", "Ignore"
    ]
    
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()

        with io.BytesIO(response.content) as zip_file:
            with zipfile.ZipFile(zip_file, 'r') as zip_ref:
                csv_file_name = zip_ref.namelist()[0]
                csv_data = zip_ref.read(csv_file_name)
                # csv_data = add_column_headers(csv_data, expected_columns)
                with open(output_path, 'wb') as output_file:
                    output_file.write(csv_data)

        print(f"Successfully downloaded and extracted data to {output_path}")

        # Load the saved CSV into a DataFrame
        df = pd.read_csv(output_path)
        
        # Print the first few rows to inspect the data
        print("First few rows of the CSV:")
        print(df.head())
        print("Open time and Close time dtypes:", df["Open time"].dtype, df["Close time"].dtype)

        # Check for invalid or out-of-range timestamps
        try:
            df["Open time"] = pd.to_datetime(df["Open time"], unit="ms", errors="coerce")
            df["Close time"] = pd.to_datetime(df["Close time"], unit="ms", errors="coerce")
        except Exception as e:
            print(f"Timestamp conversion error: {e}")
            print("Invalid Open time values:", df[df["Open time"].isna()])
            print("Invalid Close time values:", df[df["Close time"].isna()])
            raise

        print("CSV structure validated and headers added successfully")
        return df

    except requests.RequestException as e:
        raise Exception(f"Failed to download file from {url}: {e}")
    except zipfile.BadZipFile as e:
        raise Exception(f"Invalid ZIP file: {e}")
    except IOError as e:
        raise Exception(f"Failed to write to {output_path}: {e}")
    except Exception as e:
        raise Exception(f"Failed during processing: {e}")

def crawl_data_from_sources():
    try:
        # Load data sources configuration
        sources_path = Path("configs/data_sources.yml")
        with open(sources_path, 'r') as file:
            data_sources = yaml.safe_load(file)
            if not data_sources or not isinstance(data_sources, list):
                raise ValueError("Invalid or empty data_sources configuration file")

        # Load data limit configuration
        limits_path = Path("configs/data_limit.yml")
        with open(limits_path, 'r') as file:
            data_limits = yaml.safe_load(file)
            if not data_limits or not isinstance(data_limits, list):
                raise ValueError("Invalid or empty data_limit configuration file")

        # Create a dictionary of limits for each data source
        limits_dict = {limit['name']: limit['limit'] for limit in data_limits if isinstance(limit, dict) and 'name' in limit and 'limit' in limit}
        
        output_paths = []
        # Process each data source
        for data_source in data_sources:
            try:
                if not isinstance(data_source, dict) or 'name' not in data_source or 'url' not in data_source:
                    print(f"Skipping invalid data source: {data_source}")
                    continue

                # Get allowed periods for this data source
                allowed_periods = limits_dict.get(data_source['name'], [])

                # Process each allowed period
                for period in allowed_periods:
                    try:
                        # Validate period format
                        try:
                            datetime.strptime(period, '%Y-%m')
                        except ValueError:
                            print(f"Invalid period format for {data_source['name']}: {period}")
                            continue

                        # Construct unique output path
                        output_path = f"temp/{data_source['name']}-{period}.csv"

                        # Create the directory if it doesn't exist
                        os.makedirs(os.path.dirname(output_path), exist_ok=True)

                        # Construct URL
                        url = f"{data_source['url']}{data_source['name']}-{period}.zip"

                        # Download and extract data
                        download_and_extract_binance_data(url, output_path)

                    except Exception as e:
                        print(f"Failed to process period {period} for {data_source['name']}: {e}")
                        continue
                    output_paths.append(output_path)
                
            except Exception as e:
                print(f"Failed to process data source {data_source.get('name', 'unknown')}: {e}")
                continue
            
        return output_paths
    
    except (yaml.YAMLError, FileNotFoundError) as e:
        raise Exception(f"Failed to load configuration: {e}")
    except Exception as e:
        raise Exception(f"Script execution failed: {e}")

if __name__ == "__main__":
    out_paths = crawl_data_from_sources()
    print("Downloaded files:", out_paths)