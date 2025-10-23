import os
import logging
import pandas as pd
import numpy as np
from sklearn.preprocessing import MinMaxScaler
import pickle
from sklearn.metrics import mean_squared_error, mean_absolute_error
from typing import Dict, List, Tuple
import tensorflow as tf

from components.utils.file_utils import load_config, get_parquet_file_names
from components.utils.lstm_utils import create_data_loader, build_model_from_config

def model_evaluate(model, scaler: MinMaxScaler, ds: tf.data.Dataset) -> Tuple[float, float]:
    """Evaluate a model on a dataset and return RMSE and MAE.

    Args:
        model: Trained Keras model.
        scaler (MinMaxScaler): Scaler used for data normalization.
        ds (tf.data.Dataset): Dataset to evaluate on.

    Returns:
        Tuple[float, float]: RMSE and MAE metrics.
    """
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

def metric_and_predict_lstm_model(**kwargs) -> Dict:
    """Evaluate the trained LSTM model and predict the next price.

    Args:
        kwargs: Airflow task instance arguments.

    Returns:
        Dict: Evaluation metrics and prediction metadata.
    """
    ti = kwargs['ti']
    train_result = ti.xcom_pull(task_ids='train_lstm_model')
    if not train_result:
        raise ValueError("No training result.")

    cfg = load_config('model_config.yml')
    parquet_folder = load_config('pipeline_config.yml')['paths']['parquet_folder']
    os.makedirs(parquet_folder, exist_ok=True)

    model_cfg = cfg['model']
    data_cfg = cfg['data']
    out_cfg = cfg['output']
    dt_str = train_result['datetime']
    model_filename = train_result['model_filename']
    dataset_merge = train_result['dataset_merge']

    model_path = train_result['model_path']
    scaler_path = train_result['scaler_path']
    seq_length = data_cfg['seq_length']
    batch_size = cfg['evaluation'].get('eval_batch_size', 64)

    # Load scaler and model
    with open(scaler_path, 'rb') as f:
        scaler = pickle.load(f)
    model = build_model_from_config(seq_length, cfg)
    model.load_weights(model_path)

    # Create dataset
    parquet_paths = [parquet_folder for el in get_parquet_file_names()]
    dataset = create_data_loader(parquet_paths, scaler, seq_length, batch_size)

    # Calculate splits
    total_seqs = sum(max(0, len(pd.read_parquet(path, columns=['Close'])) - seq_length)
                     for path in parquet_paths if os.path.exists(path))
    if total_seqs == 0:
        raise ValueError("Not enough sequences for evaluation.")

    steps_total = (total_seqs + batch_size - 1) // batch_size
    steps_train = int(steps_total * data_cfg['train_ratio'])
    steps_val = int(steps_total * data_cfg['val_ratio'])
    steps_test = steps_total - steps_train - steps_val

    train_ds = dataset.take(steps_train)
    val_ds = dataset.skip(steps_train).take(steps_val)
    test_ds = dataset.skip(steps_train + steps_val)

    # Evaluate model
    train_rmse, train_mae = model_evaluate(model, scaler, train_ds)
    val_rmse, val_mae = model_evaluate(model, scaler, val_ds)
    test_rmse, test_mae = model_evaluate(model, scaler, test_ds)

    # Save metrics
    metrics_path = os.path.join(out_cfg['metrics']['metrics_dir'], f"metrics_{dt_str}.csv")
    os.makedirs(out_cfg['metrics']['metrics_dir'], exist_ok=True)

    metrics_data = [
        [model_filename, dataset_merge, "Train", "RMSE", train_rmse],
        [model_filename, dataset_merge, "Train", "MAE", train_mae],
        [model_filename, dataset_merge, "Val", "RMSE", val_rmse],
        [model_filename, dataset_merge, "Val", "MAE", val_mae],
        [model_filename, dataset_merge, "Test", "RMSE", test_rmse],
        [model_filename, dataset_merge, "Test", "MAE", test_mae],
    ]

    metrics_df = pd.DataFrame(
        metrics_data,
        columns=['model_path', 'dataset_merge', 'Split', 'Metric', 'Value']
    )
    metrics_df.to_csv(metrics_path, index=False)

    # Predict next price
    last_chunk = None
    for path in reversed(parquet_paths):
        if os.path.exists(path):
            df_tail = pd.read_parquet(path).tail(seq_length)
            if len(df_tail) >= seq_length:
                last_chunk = df_tail['Close'].values.astype('float32').reshape(-1, 1)
                break
    if last_chunk is None:
        raise ValueError("Not enough recent data for prediction.")

    last_scaled = scaler.transform(last_chunk)
    next_scaled = model.predict(last_scaled.reshape(1, seq_length, 1), verbose=0)
    next_price = scaler.inverse_transform(next_scaled)[0][0]

    # Save prediction
    pred_path = os.path.join(out_cfg['predictions']['pred_dir'], f"prediction_{dt_str}.txt")
    with open(pred_path, 'w') as f:
        f.write(f"Model Run: {dt_str}\n")
        f.write(f"Model File: {model_filename}\n")
        f.write(f"Dataset Merged: {dataset_merge}\n")
        f.write(f"Architecture: {model_cfg['architecture'].upper()}\n")
        f.write(f"Predicted Next Close: {next_price:.6f}\n")
        f.write(f"Based on last {seq_length} timesteps.\n\n")
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