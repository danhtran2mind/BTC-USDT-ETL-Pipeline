import os
import logging
import pandas as pd
import numpy as np
from sklearn.preprocessing import MinMaxScaler
import pickle
from datetime import datetime, timezone, timedelta
import tensorflow as tf
from tensorflow import keras
from sklearn.metrics import mean_squared_error, mean_absolute_error
from typing import Dict, List
import sys

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))
from components.utils.file_utils import load_config, get_parquet_file_names
from components.utils.lstm_utils import create_data_loader, build_model_from_config

# Configure logging with +07:00 timezone
tz = timezone(timedelta(hours=7))
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)s: %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S %Z'
)
logger = logging.getLogger(__name__)

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
        logger.warning("No GPU detected. Training on CPU, which may be slower.")
    else:
        logger.info(f"GPUs detected: {len(gpus)}. Using CUDA for training.")
        for gpu in gpus:
            tf.config.experimental.set_memory_growth(gpu, True)

    cfg = load_config('model_config.yml')
    model_cfg = cfg['model']
    train_cfg = cfg['training']
    data_cfg = cfg['data']
    out_cfg = cfg['output']
    ver_cfg = cfg['versioning']

    # Generate datetime string with +07:00 timezone
    dt = datetime.now(tz)
    dt_str = dt.strftime(ver_cfg['datetime_format']) + f"-({dt.strftime('%z')[:3]})"
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
            logger.info(f"Loaded {path} with {len(df)} rows")
            all_df = pd.concat([all_df, df], ignore_index=True)
            clean_name = name.replace(".parquet", "").replace(".csv", "")
            used_files.append(clean_name)
        else:
            logger.warning(f"File not found: {path}")

    if all_df.empty:
        logger.error("No data loaded from Parquet files")
        raise ValueError("No data loaded from Parquet files")

    dataset_merge = " + ".join(used_files) if used_files else "none"
    logger.info(f"Dataset merged: {dataset_merge}, total rows: {len(all_df)}")

    # Scale data
    scaler = MinMaxScaler()
    prices = all_df['Close'].astype(float).values.reshape(-1, 1)
    if prices.size <= data_cfg['seq_length']:
        logger.error(f"Total data size {prices.size} is insufficient for seq_length {data_cfg['seq_length']}")
        raise ValueError(f"Total data size {prices.size} is insufficient for seq_length {data_cfg['seq_length']}")
    prices_scaled = scaler.fit_transform(prices)

    # Create dataset with smaller batch size
    seq_length = data_cfg['seq_length']
    batch_size = train_cfg.get('batch_size', 64)  # Default to 64 if not specified
    if batch_size > 8192:
        logger.warning(f"Batch size {batch_size} is large; reducing to 64 to avoid memory issues")
        batch_size = 64

    dataset = create_data_loader(parquet_paths, scaler, seq_length, batch_size)

    # Calculate exact number of sequences
    total_seqs = 0
    for path in parquet_paths:
        if os.path.exists(path):
            df = pd.read_parquet(path, columns=['Close'])
            seqs = max(0, len(df) - seq_length)
            total_seqs += seqs
            logger.info(f"File {path}: {len(df)} rows, {seqs} sequences")
    
    if total_seqs == 0:
        logger.error("Not enough sequences for training")
        raise ValueError("Not enough sequences for training")

    # Calculate steps for training, validation, and test
    steps_total = (total_seqs + batch_size - 1) // batch_size
    train_ratio = data_cfg.get('train_ratio', 0.7)
    val_ratio = data_cfg.get('val_ratio', 0.2)
    steps_train = max(1, int(steps_total * train_ratio))
    steps_val = max(1, int(steps_total * val_ratio))
    steps_test = max(1, steps_total - steps_train - steps_val)
    logger.info(f"Dataset splits: total_steps={steps_total}, train={steps_train}, val={steps_val}, test={steps_test}")

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

    # Train with exact steps_per_epoch
    model.fit(
        train_ds,
        epochs=train_cfg['epochs'],
        steps_per_epoch=steps_train,
        validation_data=val_ds,
        validation_steps=steps_val,
        callbacks=[checkpoint_cb, early_stop],
        verbose=1
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

    logger.info(f"Test RMSE: {test_rmse:.4f}, MAE: {test_mae:.4f}")

    return {
        'model_path': model_path,
        'model_filename': os.path.basename(model_path),
        'scaler_path': scaler_path,
        'datetime': dt_str,
        'dataset_merge': dataset_merge,
        'test_rmse': float(test_rmse),
        'test_mae': float(test_mae)
    }

if __name__ == "__main__":
    logger.info("Running standalone training test")
    result = train_lstm_model()
    print("Training completed successfully!")
    print(result)
    logger.info("Standalone training run completed")