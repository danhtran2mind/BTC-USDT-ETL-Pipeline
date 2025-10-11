from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import os
import sys

# Add the project root directory to the Python path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))

from utils import process_data
from utils.duckdb_api import push_to_duckdb
from utils.process_data import process_financial_data
from utils.btcusdt_ingest_data import crawl_data_from_sources
from utils.datalake_cr import up_to_datalake

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 10, 7) + timedelta(hours=20),
}

dag_1 = DAG(
    'crawl_data_from_sources_pipeline',
    default_args=default_args,
    schedule_interval='@monthly',
    catchup=False
)

dag_2 = DAG(
    'etl_pipeline',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False
)

# Download Binance BTCUSDT 1s kline CSV and unzip
download_binance_csv = PythonOperator(
    dag=dag_1,
    task_id='download_binance_csv',
    python_callable=crawl_data_from_sources,
)

# Move ingested data and other files to HDFS
upload_to_datalake = PythonOperator(
    dag=dag_1,
    task_id='upload_to_datalake',
    python_callable=up_to_datalake,
    op_kwargs={
        'client_file': 'temp/BTCUSDT-1s-2025-09.csv', 
        'server_file': 'BTCUSDT-1s-2025-09.csv',
        'bucket_name': 'minio-ngrok-bucket'

    }
)

# Spark job to process data
# extract_and_transform_data = BashOperator(
#     dag=dag_2,
#     task_id='extract_and_transform_data',
#     bash_command='spark-submit spark/process_data.py /data/raw/input.csv /data/processed/output.csv'
# )


extract_and_transform_data = PythonOperator(
    dag=dag_2,
    task_id='extract_and_transform_data',
    python_callable=process_financial_data
)

# Export processed data to DuckDB
# export_duckdb = BashOperator(
#     task_id='export_duckdb',
#     bash_command='duckdb /tmp/export.duckdb "COPY (SELECT * FROM read_csv_auto(\'/data/processed/output.csv\')) TO \'/tmp/output.duckdb\' (FORMAT PARQUET);"'
# )

# Task to push to DuckDB
push_to_warehouse = PythonOperator(
    task_id='export_duckdb',
    python_callable=push_to_duckdb,
    op_kwargs={
        'duckdb_path': 'duckdb_databases/financial_data.db',
        'aggregated_df': '{{ ti.xcom_pull(task_ids="transform_data") }}'
    },
    dag=dag_2
)

# (Optional) Move DuckDB file to Google Locker
# move_to_google_locker = BashOperator(
#     dag=dag,
#     task_id='move_to_google_locker',
#     bash_command='gsutil cp /tmp/output.duckdb gs://my-locker/output.duckdb'
# )

# download_binance_csv >> ingest_json_task >> move_to_hdfs >> process_data >> export_duckdb >> move_to_google_locker

download_binance_csv >> upload_to_datalake
extract_and_transform_data >> push_to_warehouse