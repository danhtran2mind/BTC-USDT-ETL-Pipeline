import os
import logging
import tensorflow as tf
import pandas as pd
import pyarrow.parquet as pq
from sklearn.preprocessing import MinMaxScaler

# Configure logging
logger = logging.getLogger(__name__)

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