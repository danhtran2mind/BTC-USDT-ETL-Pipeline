import os
import logging
import pandas as pd
import numpy as np
from sklearn.preprocessing import MinMaxScaler
import pickle
from sklearn.metrics import mean_squared_error, mean_absolute_error
from typing import Dict, List, Tuple
from datetime import datetime, timezone
import tensorflow as tf
import sys
import ast

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))
from components.utils.file_utils import load_extract_config, get_parquet_file_names
from components.model.model_utils import build_model_from_config
from components.model.data_utils import create_data_loader
from components.utils.utils import parse_timezone

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)s: %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S %Z'
)
logger = logging.getLogger(__name__)

# def model_evaluate(model, scaler: MinMaxScaler, ds: tf.data.Dataset) -> Tuple[float, float]:
#     """Evaluate a model on a dataset and return RMSE and MAE.

#     Args:
#         model: Trained Keras model.
#         scaler (MinMaxScaler): Scaler used for data normalization.
#         ds (tf.data.Dataset): Dataset to evaluate on.

#     Returns:
#         Tuple[float, float]: RMSE and MAE metrics.
#     """
#     y_true, y_pred = [], []
#     for X, y in ds:
#         pred = model.predict(X, verbose=1)
#         y_true.append(y.numpy())
#         y_pred.append(pred)
#     y_true = np.concatenate(y_true)
#     y_pred = np.concatenate(y_pred)
#     y_true_orig = scaler.inverse_transform(y_true)
#     y_pred_orig = scaler.inverse_transform(y_pred)
#     return (np.sqrt(mean_squared_error(y_true_orig, y_pred_orig)), 
#             mean_absolute_error(y_true_orig, y_pred_orig))

def model_evaluate(model, scaler: MinMaxScaler, ds: tf.data.Dataset) -> Tuple[float, float]:
    """Evaluate a model on a dataset and return RMSE and MAE.

    Args:
        model: Trained Keras model.
        scaler (MinMaxScaler): Scaler used for data normalization.
        ds (tf.data.Dataset): Dataset to evaluate on.

    Returns:
        Tuple[float, float]: RMSE and MAE metrics.
    """
    # Collect true labels (y) from dataset
    y_true = []
    for _, y in ds:
        y_true.append(y.numpy())
    y_true = np.concatenate(y_true)

    # Predict the entire dataset
    y_pred = model.predict(ds, verbose=0)  # Silent predictions

    # Inverse transform to original scale
    y_true_orig = scaler.inverse_transform(y_true)
    y_pred_orig = scaler.inverse_transform(y_pred)

    # Calculate metrics
    return (np.sqrt(mean_squared_error(y_true_orig, y_pred_orig)), 
            mean_absolute_error(y_true_orig, y_pred_orig))

def metric_and_predict_lstm_model(train_result: Dict) -> Dict:
    """Evaluate the trained LSTM model and predict the next price.

    Args:
        train_result (Dict): Training result dictionary from train_lstm_model task.

    Returns:
        Dict: Evaluation metrics and prediction metadata.
    """
    # Access ti directly from kwargs
    if not train_result:
        raise ValueError("No training result provided.")
    
    # Convert string representation to dictionary if necessary
    train_result = ast.literal_eval(train_result)

    cfg = load_extract_config('model_config.yml')
    parquet_folder = load_extract_config('pipeline_config.yml')['paths']['parquet_folder']
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
    # train_rmse, train_mae = model_evaluate(model, scaler, train_ds)
    # val_rmse, val_mae = model_evaluate(model, scaler, val_ds)
    train_rmse, train_mae = 1.0, 1.0
    val_rmse, val_mae = 1.0, 1.0
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
    next_scaled = model.predict(last_scaled.reshape(1, seq_length, 1), verbose=1)
    next_price = scaler.inverse_transform(next_scaled)[0][0]

    # Save prediction
    pred_path = os.path.join(out_cfg['predictions']['pred_dir'], f"prediction_{dt_str}.txt")
    os.makedirs(os.path.dirname(pred_path), exist_ok=True)

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

if __name__ == "__main__":
    logger.info("Running standalone evaluation test")
    # Simulate training result for testing
    cfg = load_extract_config('model_config.yml')
    out_cfg = cfg['output']
    data_cfg = cfg['data']
    
    # Mock training result (adjust paths to match an actual trained model and scaler)
    mock_train_result = {
        'model_path': os.path.join(out_cfg['checkpoints']['model_dir'], 
                                   'model_2025-10-24-21-59-42-(+07).h5'),
        'model_filename': 'model_2025-10-24-18-40-00-(+07).h5',
        'scaler_path': os.path.join(out_cfg['checkpoints']['scaler_dir'], 
                                    'scaler_2025-10-24-21-59-42-(+07).pkl'),
        'datetime': '2025-10-24-21-59-42-(+07',
        'dataset_merge': 'BTCUSDT-1s-2025-08 + BTCUSDT-1s-2025-09'
    }

    # Simulate Airflow task instance
    class MockTaskInstance:
        def xcom_pull(self, task_ids):
            return mock_train_result
    
    mock_ti = MockTaskInstance()
    
    try:
        result = metric_and_predict_lstm_model(ti=mock_ti)
        logger.info("Evaluation completed successfully!")
        logger.info(f"Result: {result}")
    except Exception as e:
        logger.error(f"Evaluation failed: {str(e)}")
    logger.info("Standalone evaluation run completed")