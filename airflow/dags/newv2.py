from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import os
import sys

import uuid
import yaml
import pandas as pd
import numpy as np
from sklearn.preprocessing import MinMaxScaler
# from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_squared_error
from tensorflow import keras
import pickle
import logging
import ast

# Add the project root directory to the Python path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))

from components.duckdb_api import push_to_duckdb
from components.process_data import extract_from_minio, transform_financial_data
from components.btcusdt_ingest_data import crawl_data_from_sources
from components.datalake_cr import up_to_minio
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

def define_server_filenames(**kwargs):
    """Extract base file names from the client file paths."""
    ti = kwargs['ti']
    client_files = ti.xcom_pull(task_ids='download_binance_csv')
    # logger.info(f"extract_filenames received client_files: {client_files}")
    if not isinstance(client_files, list):
        client_files = [client_files]
    server_files = [os.path.basename(path) for path in client_files]
    # logger.info(f"extract_filenames returning server_files: {server_files}")
    return server_files

download_binance_csv = PythonOperator(
    dag=dag_1,
    task_id='download_binance_csv',
    python_callable=crawl_data_from_sources,
)

extract_filenames_task = PythonOperator(
    dag=dag_1,
    task_id='extract_filenames',
    python_callable=define_server_filenames,
)

upload_to_minio_storage = PythonOperator(
    dag=dag_1,
    task_id='upload_to_minio',
    python_callable=up_to_minio,
    op_kwargs={
        'client_files': '{{ ti.xcom_pull(task_ids="download_binance_csv") }}',
        'server_files': '{{ ti.xcom_pull(task_ids="extract_filenames") }}',
        'bucket_name': 'minio-ngrok-bucket'
    }
)

# ========================================================================== #
#                                  ETL DAG                                   #
# ========================================================================== #

def load_extract_config(storage_folder='temp'):
    """Load file names from extract_data.yml and generate UUID-based temp file paths."""
    config_path = os.path.join(os.path.dirname(__file__), '..', '..', 'configs', 'extract_data.yml')
    with open(config_path, 'r') as file:
        config = yaml.safe_load(file)
    file_names = config.get('files', [])
    storage_folder = config.get('storage_folder', storage_folder)
    # temp_file_paths = [os.path.join(storage_folder, f"{str(uuid.uuid4())}.csv") for _ in file_names]
    return file_names#, temp_file_paths

def load_extract_config_2(storage_folder='temp'):
    """Load file names from extract_data.yml and generate UUID-based temp file paths."""
    config_path = os.path.join(os.path.dirname(__file__), '..', '..', 'configs', 'extract_data.yml')
    with open(config_path, 'r') as file:
        config = yaml.safe_load(file)
    file_names = config.get('files', [])
    storage_folder = config.get('storage_folder', storage_folder)
    temp_file_paths = [os.path.join(storage_folder, "extracted_from_minio", el.replace(".csv", ".parquet")) for el in file_names]

    return file_names#, temp_file_paths

extract_data = PythonOperator(
    dag=dag_2,
    task_id='extract_data',
    python_callable=extract_from_minio,
    op_kwargs={
        'bucket_name': 'minio-ngrok-bucket',
        'file_names': load_extract_config(),
        # 'temp_file_paths': load_extract_config()[1]
    }
)

transform_data = PythonOperator(
    dag=dag_2,
    task_id='transform_data',
    python_callable=transform_financial_data,
    op_kwargs={
        'parquet_file_paths': '{{ ti.xcom_pull(task_ids="extract_data") }}',
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
    # ti = kwargs['ti']
    # parquet_paths = ti.xcom_pull(task_ids='extract_data', dag_id='etl_pipeline')
    # parquet_paths = ti.xcom_pull(task_ids='transform_data', dag_id='etl_pipeline')[1]
    parquet_paths = load_extract_config_2()
    parquet_paths = [f"temp/extracted_from_minio/{el.split('.')[0]}" + '.parquet' for el in parquet_paths]
    if isinstance(parquet_paths, str):
        try:
            parquet_paths = ast.literal_eval(parquet_paths)
        except (ValueError, SyntaxError) as e:
            raise ValueError(f"Failed to parse server_files as a list: {parquet_paths}, error: {e}")

    print("parquet_paths: ", parquet_paths)
    
    # model_ckpt_path = "ckpts/lstm_checkpoint_1c458a58-2e92-4bb4-8f4a-71237d674b7d.keras"
    # scaler_path = "ckpts/scaler_1c458a58-2e92-4bb4-8f4a-71237d674b7d.pkl"
    # return {'model_path': model_ckpt_path, 'scaler_path': scaler_path}
    # all_df = pd.DataFrame()

    for parquet_path in parquet_paths:
        # Generate unique file names
        # Load and preprocess data
        df = pd.read_parquet(parquet_path)
        all_df = pd.concat([all_df, df], ignore_index=True)

    prices = df['Close'].astype(float).values.reshape(-1, 1)
    
    unique_id = str(uuid.uuid4())
    model_ckpt_path = f'ckpts/lstm_checkpoint_{unique_id}.keras'
    scaler_path = f'ckpts/scaler_{unique_id}.pkl'
    os.makedirs('ckpts', exist_ok=True)

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

# --------------------------------------------------------------------------- #
#  metric_and_predict_lstm_model  –  BATCHED + RMSE + MAE
# --------------------------------------------------------------------------- #
def metric_and_predict_lstm_model(**kwargs):
    ti = kwargs['ti']

    # ------------------------------------------------------------------- #
    #  1. Pull model & scaler paths from the training task
    # ------------------------------------------------------------------- #
    train_result = ti.xcom_pull(task_ids='train_lstm_model')
    if not train_result:
        raise ValueError("No result from train_lstm_model – cannot evaluate.")
    model_path  = train_result['model_path']
    scaler_path = train_result['scaler_path']

    # ------------------------------------------------------------------- #
    #  2. Build list of Parquet files
    # ------------------------------------------------------------------- #
    file_names = load_extract_config_2()
    parquet_paths = [
        f"temp/extracted_from_minio/{el.split('.')[0]}.parquet"
        for el in file_names
    ]
    logging.info(f"Evaluation will read: {parquet_paths}")

    # ------------------------------------------------------------------- #
    #  3. Hyper-parameters
    # ------------------------------------------------------------------- #
    SEQ_LENGTH  = 60
    BATCH_SIZE  = 64
    VAL_RATIO   = 0.10
    TEST_RATIO  = 0.10

    # ------------------------------------------------------------------- #
    #  4. Scaled generator (uses same scaler as training)
    # ------------------------------------------------------------------- #
    def _scaled_generator():
        with open(scaler_path, 'rb') as f:
            scaler = pickle.load(f)

        for path in parquet_paths:
            if not os.path.exists(path):
                logging.warning(f"Skipping missing file: {path}")
                continue

            for chunk in pd.read_parquet(path, columns=['Close'], chunksize=10_000):
                if chunk.empty or 'Close' not in chunk.columns:
                    continue

                prices = chunk['Close'].astype('float32').values.reshape(-1, 1)
                prices_scaled = scaler.transform(prices)

                for i in range(len(prices_scaled) - SEQ_LENGTH):
                    X = prices_scaled[i:i + SEQ_LENGTH]
                    y = prices_scaled[i + SEQ_LENGTH]
                    yield X, y

    dataset = tf.data.Dataset.from_generator(
        _scaled_generator,
        output_types=(tf.float32, tf.float32),
        output_shapes=((SEQ_LENGTH, 1), (1,))
    )
    dataset = dataset.batch(BATCH_SIZE).prefetch(tf.data.AUTOTUNE)

    # ------------------------------------------------------------------- #
    #  5. Count total sequences → split
    # ------------------------------------------------------------------- #
    total_seqs = sum(
        max(0, len(pd.read_parquet(p, columns=['Close'])) - SEQ_LENGTH)
        for p in parquet_paths if os.path.exists(p)
    )
    if total_seqs == 0:
        raise ValueError("Not enough data to form sequences.")

    steps_total = (total_seqs + BATCH_SIZE - 1) // BATCH_SIZE
    steps_train = int(steps_total * (1 - VAL_RATIO - TEST_RATIO))
    steps_val   = int(steps_total * VAL_RATIO)
    steps_test  = steps_total - steps_train - steps_val

    train_ds = dataset.take(steps_train)
    val_ds   = dataset.skip(steps_train).take(steps_val)
    test_ds  = dataset.skip(steps_train + steps_val).take(steps_test)

    # ------------------------------------------------------------------- #
    #  6. Load model
    # ------------------------------------------------------------------- #
    model = build_lstm_model(SEQ_LENGTH)
    model.load_weights(model_path)

    # ------------------------------------------------------------------- #
    #  7. Evaluation: RMSE + MAE (on original scale)
    # ------------------------------------------------------------------- #
    def _evaluate(ds):
        y_true_list, y_pred_list = [], []
        for X_batch, y_batch in ds:
            pred = model.predict(X_batch, verbose=0)
            y_true_list.append(y_batch.numpy())
            y_pred_list.append(pred)

        y_true = np.concatenate(y_true_list, axis=0)
        y_pred = np.concatenate(y_pred_list, axis=0)

        with open(scaler_path, 'rb') as f:
            sc = pickle.load(f)

        y_true_orig = sc.inverse_transform(y_true)
        y_pred_orig = sc.inverse_transform(y_pred)

        rmse = np.sqrt(mean_squared_error(y_true_orig, y_pred_orig))
        mae  = mean_absolute_error(y_true_orig, y_pred_orig)

        return rmse, mae

    train_rmse, train_mae = _evaluate(train_ds)
    val_rmse,   val_mae   = _evaluate(val_ds)
    test_rmse,  test_mae  = _evaluate(test_ds)

    # ------------------------------------------------------------------- #
    #  8. Save metrics (RMSE + MAE)
    # ------------------------------------------------------------------- #
    metrics_id = str(uuid.uuid4())
    metrics_path = f'metrics/lstm_metrics_{metrics_id}.csv'
    os.makedirs('metrics', exist_ok=True)

    metrics_df = pd.DataFrame({
        'Split': ['Train', 'Train', 'Validation', 'Validation', 'Test', 'Test'],
        'Metric': ['RMSE', 'MAE', 'RMSE', 'MAE', 'RMSE', 'MAE'],
        'Value': [train_rmse, train_mae, val_rmse, val_mae, test_rmse, test_mae]
    })
    metrics_df.to_csv(metrics_path, index=False)

    # ------------------------------------------------------------------- #
    #  9. Predict next close price
    # ------------------------------------------------------------------- #
    last_chunk = None
    for path in reversed(parquet_paths):
        if os.path.exists(path):
            df_tail = pd.read_parquet(path, columns=['Close']).tail(SEQ_LENGTH)
            if len(df_tail) >= SEQ_LENGTH:
                last_chunk = df_tail
                break
    if last_chunk is None or len(last_chunk) < SEQ_LENGTH:
        raise ValueError("Not enough recent data for prediction.")

    last_prices = last_chunk['Close'].astype('float32').values.reshape(-1, 1)
    with open(scaler_path, 'rb') as f:
        scaler = pickle.load(f)
    last_scaled = scaler.transform(last_prices)
    next_scaled = model.predict(last_scaled.reshape(1, SEQ_LENGTH, 1), verbose=0)
    next_price = scaler.inverse_transform(next_scaled)[0][0]

    # ------------------------------------------------------------------- #
    # 10. Save prediction + summary
    # ------------------------------------------------------------------- #
    pred_path = f'metrics/prediction_{metrics_id}.txt'
    with open(pred_path, 'w') as f:
        f.write(f"Predicted next close price: {next_price:.6f}\n")
        f.write(f"Based on last {SEQ_LENGTH} timesteps.\n")
        f.write(f"\nEvaluation Metrics:\n")
        f.write(f"  Train  → RMSE: {train_rmse:8.4f} | MAE: {train_mae:8.4f}\n")
        f.write(f"  Val    → RMSE: {val_rmse:8.4f}   | MAE: {val_mae:8.4f}\n")
        f.write(f"  Test   → RMSE: {test_rmse:8.4f}  | MAE: {test_mae:8.4f}\n")

    logging.info(
        f"Evaluation complete | "
        f"Test RMSE: {test_rmse:.4f}, Test MAE: {test_mae:.4f} | "
        f"Next price: {next_price:.2f}"
    )

    # ------------------------------------------------------------------- #
    # 11. Return results
    # ------------------------------------------------------------------- #
    return pred_path

train_lstm = PythonOperator(
    task_id='train_lstm_model',
    python_callable=train_lstm_model,
    dag=dag_3
)

metric_and_predict_lstm = PythonOperator(
    task_id='metric_and_predict_lstm',
    python_callable=metric_and_predict_lstm_model,
    dag=dag_3
)

# ========================================================================== #
#                               DuckDB to CSV                                #
# ========================================================================== #

export_duckdb_to_csv = PythonOperator(
    task_id='export_duckdb_to_csv',
    python_callable=duckdb_to_csv,
    dag=dag_4
)

download_binance_csv >> extract_filenames_task >> upload_to_minio_storage
# download_binance_csv >> extract_filenames_task >> upload_to_minio_storage
extract_data >> transform_data >> push_to_warehouse
# train_lstm >> metric_and_predict_lstm
metric_and_predict_lstm

export_duckdb_to_csv