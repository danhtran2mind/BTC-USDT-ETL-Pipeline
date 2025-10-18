from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import os
import sys

import uuid
import pandas as pd
import numpy as np
from sklearn.preprocessing import MinMaxScaler
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_squared_error
from tensorflow import keras
import pickle

# Add the project root directory to the Python path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))

from components.duckdb_api import push_to_duckdb
from components.process_data import extract_from_minio, transform_financial_data
from components.btcusdt_ingest_data import crawl_data_from_sources
from components.datalake_cr import up_to_datalake
from components.duckdb2csv import duckdb_to_csv

from components.utils.lstm_utils import create_sequences
from components.model import build_lstm_model

# ========================================================================== #
#                                Define DAGs                                 #
# ========================================================================== #
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

dag_3 = DAG(
    'lstm_pipeline',
    default_args=default_args,
    schedule_interval='@monthly',
    max_active_runs=1,
    catchup=False
)

dag_4 = DAG(
    'duckdb_to_csv',
    default_args=default_args,
    schedule_interval='@monthly',
    max_active_runs=1,
    catchup=False
)

# ========================================================================== #
#                       Download and Save to MinIO DAG                       #
# ========================================================================== #

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
        # 'client_files': 'temp/BTCUSDT-1s-2025-09.csv',
        'client_files': '{{ ti.xcom_pull(task_ids="download_binance_csv") }}', 
        # 'server_files': 'BTCUSDT-1s-2025-09.csv',
        'server_files': '{{ [path.split("/")[-1] for path in ti.xcom_pull(task_ids="download_binance_csv")] }}',
        'bucket_name': 'minio-ngrok-bucket'
    }
)

# ========================================================================== #
#                                  ETL DAG                                   #
# ========================================================================== #

extract_data = PythonOperator(
    dag=dag_2,
    task_id='extract_data',
    python_callable=extract_from_minio,
    op_kwargs={
        'bucket_name': 'minio-ngrok-bucket',
        'file_name': 'BTCUSDT-1s-2025-09.csv',
        'temp_file_path': 'temp/minio_extracted.csv'
    }
)

transform_data = PythonOperator(
    dag=dag_2,
    task_id='transform_data',
    python_callable=transform_financial_data,
    op_kwargs={
        'csv_file_path': '{{ ti.xcom_pull(task_ids="extract_data") }}',
        'temp_parquet_path': 'temp/temp_parquet_chunks',
        'output_parquet_path': 'temp/aggregated_output'
    }
)

push_to_warehouse = PythonOperator(
    task_id='export_duckdb',
    python_callable=push_to_duckdb,
    op_kwargs={
        'duckdb_path': 'duckdb_databases/financial_data.db',
        'parquet_path': '{{ ti.xcom_pull(task_ids="transform_data") }}'
    },
    dag=dag_2
)

# ========================================================================== #
#                              LSTM Pipeline DAG                             #
# ========================================================================== #

def train_lstm_model(**kwargs):
    ti = kwargs['ti']
    parquet_path = ti.xcom_pull(task_ids='transform_data', dag_id='etl_pipeline')
    
    # Generate unique file names
    unique_id = str(uuid.uuid4())
    model_ckpt_path = f'ckpts/lstm_checkpoint_{unique_id}.keras'
    scaler_path = f'ckpts/scaler_{unique_id}.pkl'
    os.makedirs('ckpts', exist_ok=True)
    
    # Load and preprocess data
    df = pd.read_parquet(parquet_path)
    prices = df['Close'].astype(float).values.reshape(-1, 1)
    
    # Scale data
    scaler = MinMaxScaler()
    prices_scaled = scaler.fit_transform(prices)
    
    # Split data into training and validation sets
    seq_length = 60
    train_split_idx = int(len(prices_scaled) * 0.8)
    val_split_idx = int(len(prices_scaled) * 0.9)
    
    train_data = prices_scaled[:train_split_idx]
    val_data = prices_scaled[train_split_idx - seq_length:val_split_idx]
    
    # Create sequences for LSTM
    X_train, y_train = create_sequences(train_data, seq_length)
    X_val, y_val = create_sequences(val_data, seq_length)
    
    # Build and train model
    model = build_lstm_model(seq_length)
    checkpoint_cb = keras.callbacks.ModelCheckpoint(
        model_ckpt_path,
        save_best_only=True,
        monitor='val_loss',
        verbose=0
    )
    
    model.fit(
        X_train, y_train,
        epochs=5,
        batch_size=64,
        validation_data=(X_val, y_val),
        callbacks=[checkpoint_cb],
        verbose=2
    )
    
    # Save scaler
    with open(scaler_path, 'wb') as f:
        pickle.dump(scaler, f)
    
    return {'model_path': model_ckpt_path, 'scaler_path': scaler_path}

def metric_and_predict_lstm_model(**kwargs):
    ti = kwargs['ti']
    parquet_path = ti.xcom_pull(task_ids='transform_data', dag_id='etl_pipeline')
    paths = ti.xcom_pull(task_ids='train_lstm_model')
    model_path = paths['model_path']
    scaler_path = paths['scaler_path']
    
    # Load data
    df = pd.read_parquet(parquet_path)
    prices = df['Close'].astype(float).values.reshape(-1, 1)
    
    # Load scaler
    if not os.path.exists(scaler_path):
        raise FileNotFoundError(f"Scaler file not found at {scaler_path}")
    with open(scaler_path, 'rb') as f:
        scaler = pickle.load(f)
    
    prices_scaled = scaler.transform(prices)
    
    # Split data into train, validation, and test sets
    seq_length = 60
    train_split_idx = int(len(prices_scaled) * 0.8)
    val_split_idx = int(len(prices_scaled) * 0.9)
    
    train_data = prices_scaled[:train_split_idx]
    val_data = prices_scaled[train_split_idx - seq_length:val_split_idx]
    test_data = prices_scaled[val_split_idx - seq_length:]
    
    # Create sequences
    X_train, y_train = create_sequences(train_data, seq_length)
    X_val, y_val = create_sequences(val_data, seq_length)
    X_test, y_test = create_sequences(test_data, seq_length)
    
    # Load and evaluate model
    model = build_lstm_model(seq_length)
    model.load_weights(model_path)
    
    # Compute RMSE for train, validation, and test sets
    train_pred = model.predict(X_train, verbose=0)
    train_rmse = np.sqrt(mean_squared_error(
        scaler.inverse_transform(y_train),
        scaler.inverse_transform(train_pred)
    ))
    
    val_pred = model.predict(X_val, verbose=0)
    val_rmse = np.sqrt(mean_squared_error(
        scaler.inverse_transform(y_val),
        scaler.inverse_transform(val_pred)
    ))
    
    test_pred = model.predict(X_test, verbose=0)
    test_rmse = np.sqrt(mean_squared_error(
        scaler.inverse_transform(y_test),
        scaler.inverse_transform(test_pred)
    ))
    
    # Save metrics
    metrics_path = f'ckpts/lstm_metrics_{str(uuid.uuid4())}.csv'
    os.makedirs(os.path.dirname(metrics_path), exist_ok=True)
    metrics_df = pd.DataFrame({
        'Metric': ['Train RMSE', 'Validation RMSE', 'Test RMSE'],
        'Value': [train_rmse, val_rmse, test_rmse]
    })
    metrics_df.to_csv(metrics_path, index=False)
    
    # Predict next close price
    last_seq = prices_scaled[-seq_length:]
    next_pred = model.predict(last_seq.reshape(1, seq_length, 1), verbose=0)
    next_price = scaler.inverse_transform(next_pred)[0][0]
    
    # Save prediction
    prediction_path = f'metrics/metric_prediction_output_{str(uuid.uuid4())}.csv'
    os.makedirs(os.path.dirname(prediction_path), exist_ok=True)
    with open(prediction_path, 'w') as f:
        f.write(f"Predicted next close price: {next_price}")
    
    return prediction_path

train_lstm = PythonOperator(
    task_id='train_lstm_model',
    python_callable=train_lstm_model,
    dag=dag_3
)

predict_lstm = PythonOperator(
    task_id='predict_lstm_model',
    python_callable=metric_and_predict_lstm_model,
    dag=dag_3
)

# ========================================================================== #
#                               DuckDB to CSV                                #
# ========================================================================== #

export_duckdb_to_csv = PythonOperator(
    task_id='export_duckdb_to_csv',
    python_callable=duckdb_to_csv,
    dag=dag_3
)

download_binance_csv >> upload_to_datalake
extract_data >> transform_data >> push_to_warehouse
train_lstm >> predict_lstm
export_duckdb_to_csv