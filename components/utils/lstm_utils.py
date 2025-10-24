import os
import logging
import numpy as np
import tensorflow as tf
import pyarrow.parquet as pq
import pandas as pd
from sklearn.preprocessing import MinMaxScaler
from tensorflow import keras
from typing import Tuple
from datetime import datetime, timezone, timedelta

# Configure logging with +07:00 timezone
tz = timezone(timedelta(hours=7))
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)s: %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S %Z'
)
logger = logging.getLogger(__name__)

def create_sequences(data: np.ndarray, seq_length: int) -> Tuple[np.ndarray, np.ndarray]:
    """Create sequences of data for LSTM model training and prediction.

    Args:
        data (np.ndarray): Input time series data (scaled), shape (n_samples, n_features).
        seq_length (int): Length of each sequence.

    Returns:
        Tuple[np.ndarray, np.ndarray]: (X, y) where X is input sequences (n_samples, seq_length, n_features)
                                      and y is target values (n_samples, n_features).

    Raises:
        ValueError: If data is empty, seq_length is invalid, or data has insufficient length.
    """
    if not isinstance(data, np.ndarray):
        logger.error("Input data must be a numpy array")
        raise ValueError("Input data must be a numpy array")
    if data.size == 0:
        logger.error("Input data is empty")
        raise ValueError("Input data is empty")
    if not isinstance(seq_length, int) or seq_length <= 0:
        logger.error(f"Invalid seq_length: {seq_length}")
        raise ValueError("seq_length must be a positive integer")
    if len(data) <= seq_length:
        logger.error(f"Data length {len(data)} is insufficient for seq_length {seq_length}")
        raise ValueError(f"Data length {len(data)} is insufficient for seq_length {seq_length}")

    X, y = [], []
    for i in range(len(data) - seq_length):
        sequence = data[i:i + seq_length]
        target = data[i + seq_length]
        X.append(sequence)
        y.append(target)

    X = np.array(X)
    y = np.array(y)

    if len(X.shape) == 2:
        X = X.reshape(X.shape[0], X.shape[1], 1)

    logger.info(f"Created {X.shape[0]} sequences: X shape {X.shape}, y shape {y.shape}")
    return X, y

def create_data_loader(parquet_paths: list, scaler: MinMaxScaler, seq_length: int, batch_size: int) -> tf.data.Dataset:
    """Create a tf.data.Dataset from Parquet files for LSTM training or evaluation.

    Args:
        parquet_paths (list): List of paths to Parquet files.
        scaler (MinMaxScaler): Scaler fitted on the data.
        seq_length (int): Length of input sequences.
        batch_size (int): Batch size for the dataset.

    Returns:
        tf.data.Dataset: Dataset yielding (sequence, target) pairs with shapes (batch_size, seq_length, 1) and (batch_size, 1).

    Raises:
        ValueError: If inputs are invalid or no valid data is found.
    """
    if not parquet_paths:
        logger.error("No parquet paths provided")
        raise ValueError("parquet_paths cannot be empty")
    if not isinstance(scaler, MinMaxScaler):
        logger.error("Invalid scaler provided")
        raise ValueError("scaler must be an instance of MinMaxScaler")
    if not isinstance(seq_length, int) or seq_length <= 0:
        logger.error(f"Invalid seq_length: {seq_length}")
        raise ValueError("seq_length must be a positive integer")
    if not isinstance(batch_size, int) or batch_size <= 0:
        logger.error(f"Invalid batch_size: {batch_size}")
        raise ValueError("batch_size must be a positive integer")

    total_sequences = 0
    def _scaled_generator():
        nonlocal total_sequences
        for path in parquet_paths:
            if not os.path.exists(path):
                logger.warning(f"Parquet file not found, skipping: {path}")
                continue
            try:
                file_size = os.path.getsize(path) / (1024 * 1024)  # Size in MB
                if file_size < 100:  # Load small files into memory
                    df = pd.read_parquet(path, columns=['Close'])
                    logger.debug(f"Loaded {path} into memory, size: {file_size:.2f} MB")
                    if 'Close' not in df.columns or df['Close'].isna().any():
                        logger.warning(f"Invalid or missing 'Close' column in {path}")
                        continue
                    prices = df['Close'].astype('float32').values.reshape(-1, 1)
                    if prices.size <= seq_length:
                        logger.warning(f"File {path} has {prices.size} rows, insufficient for seq_length {seq_length}")
                        continue
                    scaled = scaler.transform(prices)
                    for j in range(len(scaled) - seq_length):
                        total_sequences += 1
                        yield scaled[j:j + seq_length], scaled[j + seq_length]
                else:
                    parquet_file = pq.ParquetFile(path)
                    for batch in parquet_file.iter_batches(batch_size=10_000, columns=['Close']):
                        chunk = batch.to_pandas()
                        if 'Close' not in chunk.columns or chunk['Close'].isna().any():
                            logger.warning(f"Invalid or missing 'Close' column in {path}")
                            continue
                        prices = chunk['Close'].astype('float32').values.reshape(-1, 1)
                        scaled = scaler.transform(prices)
                        logger.debug(f"Processing batch from {path}, scaled shape: {scaled.shape}")
                        for j in range(len(scaled) - seq_length):
                            total_sequences += 1
                            yield scaled[j:j + seq_length], scaled[j + seq_length]
            except Exception as e:
                logger.error(f"Error processing parquet file {path}: {e}")
                continue

        if total_sequences == 0:
            logger.error("No valid sequences generated from any Parquet file")
            raise ValueError("No valid sequences generated from any Parquet file")

    dataset = tf.data.Dataset.from_generator(
        _scaled_generator,
        output_types=(tf.float32, tf.float32),
        output_shapes=((seq_length, 1), (1,))
    ).batch(batch_size).prefetch(tf.data.AUTOTUNE)
    
    logger.info(f"Created data loader with seq_length={seq_length}, batch_size={batch_size}, total_sequences={total_sequences}")
    return dataset

def build_model_from_config(seq_length: int, cfg: dict) -> keras.Model:
    """Build an LSTM-based model based on configuration.

    Args:
        seq_length (int): Length of input sequences.
        cfg (dict): Model configuration dictionary with 'model' key containing architecture, units, etc.

    Returns:
        keras.Model: Compiled Keras model.

    Raises:
        ValueError: If configuration is invalid or architecture is unsupported.
    """
    if not isinstance(cfg, dict) or 'model' not in cfg:
        logger.error("Invalid configuration: 'model' key missing")
        raise ValueError("Configuration must be a dictionary with a 'model' key")

    model_cfg = cfg['model']
    arch = model_cfg.get('architecture')
    units = model_cfg.get('units')
    layers = model_cfg.get('layers', 1)
    dropout = model_cfg.get('dropout', 0.2)
    activation = model_cfg.get('activation', 'tanh')

    if not isinstance(units, int) or units <= 0:
        logger.error(f"Invalid units: {units}")
        raise ValueError("units must be a positive integer")
    if not isinstance(layers, int) or layers <= 0:
        logger.error(f"Invalid layers: {layers}")
        raise ValueError("layers must be a positive integer")
    if not isinstance(dropout, float) or not 0 <= dropout < 1:
        logger.error(f"Invalid dropout: {dropout}")
        raise ValueError("dropout must be a float between 0 and 1")
    if arch not in ['lstm', 'bilstm', 'gru', 'custom']:
        logger.error(f"Unsupported architecture: {arch}")
        raise ValueError(f"Unsupported architecture: {arch}")
    if not isinstance(seq_length, int) or seq_length <= 0:
        logger.error(f"Invalid seq_length: {seq_length}")
        raise ValueError("seq_length must be a positive integer")

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

    if arch != 'custom':
        x = keras.layers.Dense(1)(x)

    model = keras.Model(inputs, x)
    model.compile(
        optimizer=model_cfg.get('optimizer', 'adam'),
        loss=model_cfg.get('loss', 'mse'),
        metrics=['mae']
    )
    
    logger.info(f"Built model: architecture={arch}, units={units}, layers={layers}")
    return model

if __name__ == "__main__":
    import pandas as pd
    from components.utils.file_utils import load_config

    logger.info("Running standalone tests for lstm_utils.py")
    # Test create_sequences
    data = np.array([[10000], [10050], [10100], [10150], [10200]])
    seq_length = 3
    X, y = create_sequences(data, seq_length)
    print(f"create_sequences: X shape {X.shape}, y shape {y.shape}")
    print(f"Sample sequence: {X[0]}, target: {y[0]}")

    # Test create_data_loader
    scaler = MinMaxScaler()
    scaler.fit(data)
    parquet_paths = ['temp/extracted_from_minio/btcusdt_1h.parquet']
    if not os.path.exists(parquet_paths[0]):
        os.makedirs(os.path.dirname(parquet_paths[0]), exist_ok=True)
        pd.DataFrame({'Close': [10000, 10050, 10100, 10150, 10200]}).to_parquet(parquet_paths[0])
    
    dataset = create_data_loader(parquet_paths, scaler, seq_length=3, batch_size=2)
    for x, y in dataset.take(1):
        print(f"create_data_loader: x shape {x.shape}, y shape {y.shape}")

    # Test build_model_from_config
    config = load_config('model_config.yml')
    model = build_model_from_config(seq_length=3, cfg=config)
    model.summary()
    logger.info("Standalone tests completed successfully.")