import os
import logging
import pandas as pd
import numpy as np
from sklearn.preprocessing import MinMaxScaler
import pickle
from datetime import datetime
import tensorflow as tf
from tensorflow import keras
from sklearn.metrics import mean_squared_error, mean_absolute_error
from typing import Dict, List

from components.utils.file_utils import load_config, get_parquet_file_names
from components.utils.lstm_utils import create_data_loader, build_model_from_config

def train_lstm_model(**kwargs) -> Dict:
    """Train an LSTM model for BTC/USDT forecasting and save model and scaler.

    Args:
        kwargs: Airflow task instance arguments.

    Returns:
        Dict: Training metadata including model path, scaler path, metrics, and dataset info.
    """
    # Verify GPU availability
    gpus = tf.config.list_physical_devices('GPU')
    if not gpus:
        logging.warning("No GPU detected. Training on CPU, which may be slower.")
    else:
        logging.info(f"GPUs detected: {len(gpus)}. Using CUDA for training.")
        for gpu in gpus:
            tf.config.experimental.set_memory_growth(gpu, True)

    cfg = load_config('model_config.yml')
    model_cfg = cfg['model']
    train_cfg = cfg['training']
    data_cfg = cfg['data']
    out_cfg = cfg['output']
    ver_cfg = cfg['versioning']

    # Generate datetime string with timezone
    dt = datetime.now().astimezone()
    dt_str = dt.strftime(ver_cfg['datetime_format']) + f"-({dt.strftime('%z').replace('+', '+').replace('-', '-')[:3]})"
    model_path = os.path.join(out_cfg['checkpoints']['model_dir'], f"model_{dt_str}.h5")
    scaler_path = os.path.join(out_cfg['checkpoints']['scaler_dir'], f"scaler_{dt_str}.pkl")
    os.makedirs(os.path.dirname(model_path), exist_ok=True)

    # Load data
    file_names = get_parquet_file_names()
    parquet_paths = [f"temp/extracted_from_minio/{el}" for el in file_names]
    all_df = pd.DataFrame()
    used_files = []

    for path, name in zip(parquet_paths, file_names):
        if os.path.exists(path):
            df = pd.read_parquet(path)
            all_df = pd.concat([all_df, df], ignore_index=True)
            clean_name = name.replace(".parquet", "").replace(".csv", "")
            used_files.append(clean_name)

    if all_df.empty:
        raise ValueError("No data loaded from Parquet files.")

    dataset_merge = " + ".join(used_files) if used_files else "none"

    # Scale data
    scaler = MinMaxScaler()
    prices = all_df['Close'].astype(float).values.reshape(-1, 1)
    prices_scaled = scaler.fit_transform(prices)

    # Create dataset
    seq_length = data_cfg['seq_length']
    batch_size = train_cfg['batch_size']
    dataset = create_data_loader(parquet_paths, scaler, seq_length, batch_size)

    # Calculate splits
    total_seqs = sum(max(0, len(pd.read_parquet(path, columns=['Close'])) - seq_length)
                     for path in parquet_paths if os.path.exists(path))
    if total_seqs == 0:
        raise ValueError("Not enough sequences for training.")

    steps_total = (total_seqs + batch_size - 1) // batch_size
    steps_train = int(steps_total * data_cfg['train_ratio'])
    steps_val = int(steps_total * data_cfg['val_ratio'])
    steps_test = steps_total - steps_train - steps_val

    train_ds = dataset.take(steps_train)
    val_ds = dataset.skip(steps_train).take(steps_val)
    test_ds = dataset.skip(steps_train + steps_val)

    # Build and train model
    model = build_model_from_config(seq_length, cfg)
    checkpoint_cb = keras.callbacks.ModelCheckpoint(
        model_path, save_best_only=True, monitor='val_loss', verbose=0
    )
    early_stop = keras.callbacks.EarlyStopping(
        monitor='val_loss', patience=train_cfg['patience'], restore_best_weights=True
    )

    model.fit(
        train_ds,
        epochs=train_cfg['epochs'],
        validation_data=val_ds,
        callbacks=[checkpoint_cb, early_stop],
        verbose=2
    )

    # Save scaler
    with open(scaler_path, 'wb') as f:
        pickle.dump(scaler, f)

    # Test evaluation
    y_true, y_pred = [], []
    for X, y in test_ds:
        pred = model.predict(X, verbose=0)
        y_true.append(y.numpy())
        y_pred.append(pred)
    y_true = np.concatenate(y_true)
    y_pred = np.concatenate(y_pred)
    y_true_orig = scaler.inverse_transform(y_true)
    y_pred_orig = scaler.inverse_transform(y_pred)
    test_rmse = np.sqrt(mean_squared_error(y_true_orig, y_pred_orig))
    test_mae = mean_absolute_error(y_true_orig, y_pred_orig)

    logging.info(f"Test RMSE: {test_rmse:.4f}, MAE: {test_mae:.4f}")

    return {
        'model_path': model_path,
        'model_filename': os.path.basename(model_path),
        'scaler_path': scaler_path,
        'datetime': dt_str,
        'dataset_merge': dataset_merge,
        'test_rmse': float(test_rmse),
        'test_mae': float(test_mae)
    }