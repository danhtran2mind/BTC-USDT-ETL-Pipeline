# ========================================================================== #
#                                new3.py
#  BTC/USDT Forecasting – ISO Datetime Checkpoints (2025-10-22-14-30-22)
# ========================================================================== #

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import os
import sys
import yaml
import pandas as pd
import numpy as np
from sklearn.preprocessing import MinMaxScaler
from sklearn.metrics import mean_squared_error, mean_absolute_error
import pickle
import logging
import tensorflow as tf
from tensorflow import keras

# Add project root to path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))

from components.duckdb_api import push_to_duckdb
from components.process_data import extract_from_minio, transform_financial_data
from components.btcusdt_ingest_data import crawl_data_from_sources
from components.datalake_cr import up_to_minio
from components.duckdb2csv import duckdb_to_csv
from components.utils.lstm_utils import create_sequences

# ========================================================================== #
#                               CONFIG LOADER
# ========================================================================== #

def load_config():
    config_path = os.path.join(os.path.dirname(__file__), '..', '..', 'configs', 'model_config.yml')
    with open(config_path, 'r') as f:
        return yaml.safe_load(f)

# ========================================================================== #
#                              MODEL BUILDER
# ========================================================================== #

def build_model_from_config(seq_length, cfg):
    arch = cfg['model']['architecture']
    units = cfg['model']['units']
    layers = cfg['model'].get('layers', 1)
    dropout = cfg['model'].get('dropout', 0.2)
    activation = cfg['model'].get('activation', 'tanh')

    inputs = keras.layers.Input(shape=(seq_length, 1))
    x = inputs

    for i in range(layers):
        return_seq = i < layers - 1
        if arch == 'lstm':
            x = keras.layers.LSTM(
                units, return_sequences=return_seq, activation=activation,
                dropout=dropout, recurrent_dropout=0.1
            )(x)
        elif arch == 'bilstm':
            x = keras.layers.Bidirectional(
                keras.layers.LSTM(
                    units, return_sequences=return_seq, activation=activation,
                    dropout=dropout, recurrent_dropout=0.1
                )
            )(x)
        elif arch == 'gru':
            x = keras.layers.GRU(
                units, return_sequences=return_seq, activation=activation,
                dropout=dropout, recurrent_dropout=0.1
            )(x)
        elif arch == 'custom':
            x = keras.layers.LSTM(units, return_sequences=True)(x)
            x = keras.layers.LSTM(units // 2, return_sequences=False)(x)
            x = keras.layers.Dense(50, activation='relu')(x)
            x = keras.layers.Dropout(dropout)(x)
        else:
            raise ValueError(f"Unsupported architecture: {arch}")

    if arch != 'custom':
        x = keras.layers.Dense(1)(x)

    model = keras.Model(inputs, x)
    model.compile(
        optimizer=cfg['model'].get('optimizer', 'adam'),
        loss=cfg['model'].get('loss', 'mse'),
        metrics=['mae']
    )
    return model

# ========================================================================== #
#                                DAG DEFINITIONS
# ========================================================================== #

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 10, 7, 20, 0),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag_1 = DAG('crawl_to_minio', default_args=default_args, schedule_interval='@monthly', max_active_runs=1, catchup=False)
dag_2 = DAG('etl_to_duckdb', default_args=default_args, schedule_interval='@monthly', max_active_runs=1, catchup=False)
dag_3 = DAG('lstm_forecast', default_args=default_args, schedule_interval='@monthly', max_active_runs=1, catchup=False)
dag_4 = DAG('duckdb_to_csv_export', default_args=default_args, schedule_interval='@monthly', max_active_runs=1, catchup=False)

# ========================================================================== #
#                          DAG 1: Crawl to MinIO
# ========================================================================== #

def define_server_filenames(**kwargs):
    ti = kwargs['ti']
    client_files = ti.xcom_pull(task_ids='download_binance_csv')
    if not isinstance(client_files, list):
        client_files = [client_files]
    return [os.path.basename(p) for p in client_files]

download_binance_csv = PythonOperator(task_id='download_binance_csv', python_callable=crawl_data_from_sources, dag=dag_1)
extract_filenames_task = PythonOperator(task_id='extract_filenames', python_callable=define_server_filenames, dag=dag_1)
upload_to_minio_storage = PythonOperator(
    task_id='upload_to_minio',
    python_callable=up_to_minio,
    op_kwargs={
        'client_files': '{{ ti.xcom_pull(task_ids="download_binance_csv") }}',
        'server_files': '{{ ti.xcom_pull(task_ids="extract_filenames") }}',
        'bucket_name': 'minio-ngrok-bucket'
    },
    dag=dag_1,
)

# ========================================================================== #
#                          DAG 2: MinIO to DuckDB
# ========================================================================== #

def load_extract_config():
    config_path = os.path.join(os.path.dirname(__file__), '..', '..', 'configs', 'extract_data.yml')
    with open(config_path, 'r') as f:
        config = yaml.safe_load(f)
    return config.get('files', []), config.get('storage_folder', 'temp')

def load_extract_config_2():
    config_path = os.path.join(os.path.dirname(__file__), '..', '..', 'configs', 'extract_data.yml')
    with open(config_path, 'r') as f:
        config = yaml.safe_load(f)
    files = config.get('files', [])
    return [el.replace(".csv", ".parquet") for el in files]

extract_data = PythonOperator(
    task_id='extract_data',
    python_callable=extract_from_minio,
    op_kwargs={'bucket_name': 'minio-ngrok-bucket', 'file_names': load_extract_config()[0]},
    dag=dag_2,
)

transform_data = PythonOperator(
    task_id='transform_data',
    python_callable=transform_financial_data,
    op_kwargs={
        'parquet_file_paths': '{{ ti.xcom_pull(task_ids="extract_data") }}',
        'temp_parquet_path': 'temp/temp_parquet_chunks',
        'output_parquet_path': 'temp/aggregated_output'
    },
    dag=dag_2,
)

push_to_warehouse = PythonOperator(
    task_id='export_duckdb',
    python_callable=push_to_duckdb,
    op_kwargs={
        'duckdb_path': 'duckdb_databases/financial_data.db',
        'parquet_path': '{{ ti.xcom_pull(task_ids="transform_data") }}'
    },
    dag=dag_2,
)

# ========================================================================== #
#                          DAG 3: LSTM Forecasting
# ========================================================================== #

def train_lstm_model(**kwargs):
    cfg = load_config()
    model_cfg = cfg['model']
    train_cfg = cfg['training']
    data_cfg = cfg['data']
    out_cfg = cfg['output']
    ver_cfg = cfg['versioning']

    # === Generate ISO-like datetime: 2025-10-22-14-30-22 ===
    dt_str = datetime.now().strftime(ver_cfg['datetime_format'])

    model_path = out_cfg['checkpoints']['model_path_template'].format(datetime=dt_str)
    scaler_path = out_cfg['checkpoints']['scaler_path_template'].format(datetime=dt_str)

    os.makedirs(os.path.dirname(model_path), exist_ok=True)

    # === Load data ===
    file_names = load_extract_config_2()
    parquet_paths = [f"temp/extracted_from_minio/{el}" for el in file_names]

    all_df = pd.DataFrame()
    for path in parquet_paths:
        if os.path.exists(path):
            df = pd.read_parquet(path)
            all_df = pd.concat([all_df, df], ignore_index=True)

    if all_df.empty:
        raise ValueError("No data loaded from Parquet files.")

    prices = all_df['Close'].astype(float).values.reshape(-1, 1)

    # === Scale ===
    scaler = MinMaxScaler()
    prices_scaled = scaler.fit_transform(prices)

    # === Sequences ===
    seq_length = data_cfg['seq_length']
    X, y = create_sequences(prices_scaled, seq_length)

    # === Split ===
    train_idx = int(len(X) * data_cfg['train_ratio'])
    val_idx = int(len(X) * (data_cfg['train_ratio'] + data_cfg['val_ratio']))
    X_train, X_val, X_test = X[:train_idx], X[train_idx:val_idx], X[val_idx:]
    y_train, y_val, y_test = y[:train_idx], y[train_idx:val_idx], y[val_idx:]

    # === Build model ===
    model = build_model_from_config(seq_length, cfg)

    # === Callbacks ===
    checkpoint_cb = keras.callbacks.ModelCheckpoint(
        model_path, save_best_only=True, monitor='val_loss', verbose=0
    )
    early_stop = keras.callbacks.EarlyStopping(
        monitor='val_loss', patience=train_cfg['patience'], restore_best_weights=True
    )

    # === Train ===
    model.fit(
        X_train, y_train,
        epochs=train_cfg['epochs'],
        batch_size=train_cfg['batch_size'],
        validation_data=(X_val, y_val),
        callbacks=[checkpoint_cb, early_stop],
        verbose=2
    )

    # === Save scaler ===
    with open(scaler_path, 'wb') as f:
        pickle.dump(scaler, f)

    # === Test eval ===
    pred_scaled = model.predict(X_test, verbose=0)
    pred = scaler.inverse_transform(pred_scaled)
    true = scaler.inverse_transform(y_test)
    test_rmse = np.sqrt(mean_squared_error(true, pred))
    test_mae = mean_absolute_error(true, pred)

    logging.info(f"Test RMSE: {test_rmse:.4f}, MAE: {test_mae:.4f}")

    return {
        'model_path': model_path,
        'scaler_path': scaler_path,
        'datetime': dt_str,
        'test_rmse': float(test_rmse),
        'test_mae': float(test_mae)
    }


def metric_and_predict_lstm_model(**kwargs):
    ti = kwargs['ti']
    train_result = ti.xcom_pull(task_ids='train_lstm_model')
    if not train_result:
        raise ValueError("No training result.")

    cfg = load_config()
    model_cfg = cfg['model']
    data_cfg = cfg['data']
    out_cfg = cfg['output']
    dt_str = train_result['datetime']
    model_filename = train_result['model_filename']
    dataset_merge = train_result['dataset_merge']

    model_path = train_result['model_path']
    scaler_path = train_result['scaler_path']
    SEQ_LENGTH = data_cfg['seq_length']
    BATCH_SIZE = cfg['training'].get('batch_size', 64)

    # === Load scaler & model ===
    with open(scaler_path, 'rb') as f:
        scaler = pickle.load(f)
    model = build_model_from_config(SEQ_LENGTH, cfg)
    model.load_weights(model_path)

    # === Generator for batched evaluation ===
    def _scaled_generator():
        for path in [f"temp/extracted_from_minio/{el}" for el in load_extract_config_2()]:
            if not os.path.exists(path):
                continue
            for chunk in pd.read_parquet(path, columns=['Close'], chunksize=10_000):
                prices = chunk['Close'].astype('float32').values.reshape(-1, 1)
                scaled = scaler.transform(prices)
                for i in range(len(scaled) - SEQ_LENGTH):
                    yield scaled[i:i + SEQ_LENGTH], scaled[i + SEQ_LENGTH]

    dataset = tf.data.Dataset.from_generator(
        _scaled_generator,
        output_types=(tf.float32, tf.float32),
        output_shapes=((SEQ_LENGTH, 1), (1,))
    ).batch(BATCH_SIZE).prefetch(tf.data.AUTOTUNE)

    total_seqs = sum(
        max(0, len(pd.read_parquet(p, columns=['Close'])) - SEQ_LENGTH)
        for p in [f"temp/extracted_from_minio/{el}" for el in load_extract_config_2()]
        if os.path.exists(p)
    )
    if total_seqs == 0:
        raise ValueError("Not enough sequences.")

    steps_total = (total_seqs + BATCH_SIZE - 1) // BATCH_SIZE
    steps_train = int(steps_total * data_cfg['train_ratio'])
    steps_val = int(steps_total * data_cfg['val_ratio'])
    steps_test = steps_total - steps_train - steps_val

    train_ds = dataset.take(steps_train)
    val_ds = dataset.skip(steps_train).take(steps_val)
    test_ds = dataset.skip(steps_train + steps_val)

    def _evaluate(ds):
        y_true, y_pred = [], []
        for X, y in ds:
            pred = model.predict(X, verbose=0)
            y_true.append(y.numpy())
            y_pred.append(pred)
        y_true = np.concatenate(y_true)
        y_pred = np.concatenate(y_pred)
        y_true_orig = scaler.inverse_transform(y_true)
        y_pred_orig = scaler.inverse_transform(y_pred)
        return np.sqrt(mean_squared_error(y_true_orig, y_pred_orig)), mean_absolute_error(y_true_orig, y_pred_orig)

    train_rmse, train_mae = _evaluate(train_ds)
    val_rmse, val_mae = _evaluate(val_ds)
    test_rmse, test_mae = _evaluate(test_ds)

    # === Save Metrics CSV with Header: model_path, dataset_merge, Split, Metric, Value ===
    metrics_path = os.path.join(
        out_cfg['metrics']['metrics_dir'],
        f"{out_cfg['metrics']['metrics_prefix']}_{dt_str}{out_cfg['metrics']['metrics_ext']}"
    )
    os.makedirs(out_cfg['metrics']['metrics_dir'], exist_ok=True)

    metrics_data = [
        [model_filename, dataset_merge, "Train", "RMSE", train_rmse],
        [model_filename, dataset_merge, "Train", "MAE",  train_mae],
        [model_filename, dataset_merge, "Val",   "RMSE", val_rmse],
        [model_filename, dataset_merge, "Val",   "MAE",  val_mae],
        [model_filename, dataset_merge, "Test",  "RMSE", test_rmse],
        [model_filename, dataset_merge, "Test",  "MAE",  test_mae],
    ]

    metrics_df = pd.DataFrame(
        metrics_data,
        columns=['model_path', 'dataset_merge', 'Split', 'Metric', 'Value']
    )
    metrics_df.to_csv(metrics_path, index=False)

    # === Predict next close price ===
    last_chunk = None
    for path in reversed([f"temp/extracted_from_minio/{el}" for el in load_extract_config_2()]):
        if os.path.exists(path):
            df_tail = pd.read_parquet(path).tail(SEQ_LENGTH)
            if len(df_tail) >= SEQ_LENGTH:
                last_chunk = df_tail['Close'].values.astype('float32').reshape(-1, 1)
                break
    if last_chunk is None:
        raise ValueError("Not enough recent data for prediction.")

    last_scaled = scaler.transform(last_chunk)
    next_scaled = model.predict(last_scaled.reshape(1, SEQ_LENGTH, 1), verbose=0)
    next_price = scaler.inverse_transform(next_scaled)[0][0]

    # === Save prediction TXT ===
    pred_path = os.path.join(
        out_cfg['predictions']['pred_dir'],
        f"{out_cfg['predictions']['pred_prefix']}_{dt_str}{out_cfg['predictions']['pred_ext']}"
    )
    with open(pred_path, 'w') as f:
        f.write(f"Model Run: {dt_str}\n")
        f.write(f"Model File: {model_filename}\n")
        f.write(f"Dataset Merged: {dataset_merge}\n")
        f.write(f"Architecture: {model_cfg['architecture'].upper()}\n")
        f.write(f"Predicted Next Close: {next_price:.6f}\n")
        f.write(f"Based on last {SEQ_LENGTH} timesteps.\n\n")
        f.write("Evaluation Metrics:\n")
        f.write(f"  Train  -> RMSE: {train_rmse:8.4f} | MAE: {train_mae:8.4f}\n")
        f.write(f"  Val    -> RMSE: {val_rmse:8.4f}   | MAE: {val_mae:8.4f}\n")
        f.write(f"  Test   -> RMSE: {test_rmse:8.4f}  | MAE: {test_mae:8.4f}\n")

    logging.info(f"Next price: {next_price:.2f} | Test RMSE: {test_rmse:.4f} | Dataset: {dataset_merge}")

    return {
        'metrics_path': metrics_path,
        'prediction_path': pred_path,
        'next_price': float(next_price)
    }


train_lstm = PythonOperator(task_id='train_lstm_model', python_callable=train_lstm_model, dag=dag_3)
metric_and_predict_lstm = PythonOperator(task_id='metric_and_predict_lstm', python_callable=metric_and_predict_lstm_model, dag=dag_3)

# ========================================================================== #
#                          DAG 4: DuckDB to CSV
# ========================================================================== #

export_duckdb_to_csv = PythonOperator(task_id='export_duckdb_to_csv', python_callable=duckdb_to_csv, dag=dag_4)

# ========================================================================== #
#                               DAG Dependencies
# ========================================================================== #

# DAG 1
download_binance_csv >> extract_filenames_task >> upload_to_minio_storage
# DAG 2
extract_data >> transform_data >> push_to_warehouse
# DAG 3
train_lstm >> metric_and_predict_lstm
# DAG 4
export_duckdb_to_csv