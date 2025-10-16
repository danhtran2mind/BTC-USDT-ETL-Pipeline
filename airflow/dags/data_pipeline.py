# airflow/dags/data_pipeline.py
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import os
import sys

# Add the project root directory to the Python path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))

from components.duckdb_api import push_to_duckdb
from components.process_data import process_financial_data
from components.btcusdt_ingest_data import crawl_data_from_sources
from components.datalake_cr import up_to_datalake

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 10, 7) + timedelta(hours=20),
}

dag_1 = DAG(
    'crawl_data_from_sources_pipeline',
    default_args=default_args,
    schedule_interval='@monthly',
    max_active_runs=1,
    catchup=False
)

dag_2 = DAG(
    'etl_pipeline',
    default_args=default_args,
    schedule_interval='@monthly',
    max_active_runs=1,
    catchup=False
)

download_binance_csv = PythonOperator(
    dag=dag_1,
    task_id='download_binance_csv',
    python_callable=crawl_data_from_sources,
)

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

extract_and_transform_data = PythonOperator(
    dag=dag_2,
    task_id='extract_and_transform_data',
    python_callable=process_financial_data,
    op_kwargs={
        'bucket_name': 'minio-ngrok-bucket',
        'file_name': 'BTCUSDT-1s-2025-09.csv',
        'temp_parquet_path': 'temp/temp_parquet_chunks',
        'output_parquet_path': 'temp/aggregated_output'
    }
)

push_to_warehouse = PythonOperator(
    task_id='export_duckdb',
    python_callable=push_to_duckdb,
    op_kwargs={
        'duckdb_path': 'duckdb_databases/financial_data.db',
        'parquet_path': '{{ ti.xcom_pull(task_ids="extract_and_transform_data") }}'
    },
    dag=dag_2
)

download_binance_csv >> upload_to_datalake
extract_and_transform_data >> push_to_warehouse