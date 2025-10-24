from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta, timezone
import os
import sys

# Add project root to path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))

from components.btcusdt_ingest_data import crawl_data_from_sources
from components.datalake_cr import up_to_minio
from components.process_data import extract_from_minio, transform_financial_data
from components.duckdb_api import push_to_duckdb
from components.duckdb2csv import duckdb_to_csv
from components.model.training import train_lstm_model
from components.model.evaluation import metric_and_predict_lstm_model
from components.utils.file_utils import (
    load_extract_config, 
    define_server_filenames, 
    load_pipeline_config
)

# DAG default arguments
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 10, 7, 20, 0),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define DAGs
dag_1 = DAG('crawl_to_minio', default_args=default_args, 
            schedule_interval='@monthly', max_active_runs=1, catchup=False)
dag_2 = DAG('etl_to_duckdb', default_args=default_args, 
            schedule_interval='@monthly', max_active_runs=1, catchup=False)
dag_3 = DAG('lstm_forecast', default_args=default_args, 
            schedule_interval='@monthly', max_active_runs=1, catchup=False)
dag_4 = DAG('duckdb_to_csv_export', default_args=default_args, 
            schedule_interval='@monthly', max_active_runs=1, catchup=False)

# Load pipeline configuration
pipeline_config = load_pipeline_config()

# DAG 1: Crawl to MinIO
download_binance_csv = PythonOperator(
    task_id='download_binance_csv',
    python_callable=crawl_data_from_sources,
    dag=dag_1
)

extract_filenames_task = PythonOperator(
    task_id='extract_filenames',
    python_callable=define_server_filenames,
    dag=dag_1
)

upload_to_minio_storage = PythonOperator(
    task_id='upload_to_minio',
    python_callable=up_to_minio,
    op_kwargs={
        'client_files': '{{ ti.xcom_pull(task_ids="download_binance_csv") }}',
        'server_files': '{{ ti.xcom_pull(task_ids="extract_filenames") }}',
        'bucket_name': pipeline_config['minio']['bucket_name']
    },
    dag=dag_1
)

# DAG 2: MinIO to DuckDB
extract_data = PythonOperator(
    task_id='extract_data',
    python_callable=extract_from_minio,
    op_kwargs={
        'bucket_name': pipeline_config['minio']['bucket_name'],
        'file_names': load_extract_config("extract_data.yml")["files"]
    },
    dag=dag_2
)

transform_data = PythonOperator(
    task_id='transform_data',
    python_callable=transform_financial_data,
    op_kwargs={
        'parquet_file_paths': '{{ ti.xcom_pull(task_ids="extract_data") }}',
        'temp_parquet_path': pipeline_config['paths']['temp_parquet_path'],
        'output_parquet_path': pipeline_config['paths']['output_parquet_path']
    },
    dag=dag_2
)

push_to_warehouse = PythonOperator(
    task_id='export_duckdb',
    python_callable=push_to_duckdb,
    op_kwargs={
        'duckdb_path': pipeline_config['paths']['duckdb_path'],
        'parquet_path': '{{ ti.xcom_pull(task_ids="transform_data") }}'
    },
    dag=dag_2
)

# DAG 3: LSTM Forecasting
train_lstm = PythonOperator(
    task_id='train_lstm_model',
    python_callable=train_lstm_model,
    dag=dag_3
)

# metric_and_predict_lstm = PythonOperator(
#     task_id='metric_and_predict_lstm',
#     python_callable=metric_and_predict_lstm_model,
#     dag=dag_3
# )

metric_and_predict_lstm = PythonOperator(
    task_id='metric_and_predict_lstm',
    python_callable=metric_and_predict_lstm_model,
    op_kwargs={'ti': '{{ ti }}'},  # Explicitly pass task instance
    provide_context=True,  # Ensure context is provided for XCom
    dag=dag_3
)

# DAG 4: DuckDB to CSV
export_duckdb_to_csv = PythonOperator(
    task_id='export_duckdb_to_csv',
    python_callable=duckdb_to_csv,
    op_kwargs={
        'duckdb_path': pipeline_config['paths']['duckdb_path'],
        'output_csv_path': pipeline_config['paths']['output_csv_path']
    },
    dag=dag_4
)

# Dependencies
download_binance_csv >> extract_filenames_task >> upload_to_minio_storage
extract_data >> transform_data >> push_to_warehouse
train_lstm >> metric_and_predict_lstm
export_duckdb_to_csv