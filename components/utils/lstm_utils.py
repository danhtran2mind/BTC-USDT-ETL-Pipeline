import os
import numpy as np
import tensorflow as tf
import pyarrow.parquet as pq
from sklearn.preprocessing import MinMaxScaler
from tensorflow import keras


def create_sequences(data, seq_length):
    """
    Create sequences of data for LSTM model training and prediction.
    
    Args:
        data (numpy.ndarray): Input time series data (scaled)
        seq_length (int): Length of each sequence
        
    Returns:
        tuple: (X, y) where X is the input sequences and y is the target values
    """
    X, y = [], []
    
    for i in range(len(data) - seq_length):
        # Extract sequence of length seq_length
        sequence = data[i:i + seq_length]
        # Target is the next value after the sequence
        target = data[i + seq_length]
        X.append(sequence)
        y.append(target)
    
    # Convert to numpy arrays and ensure correct shape
    X = np.array(X)
    y = np.array(y)
    
    # Reshape X to (samples, seq_length, features)
    if len(X.shape) == 2:
        X = X.reshape(X.shape[0], X.shape[1], 1)
    
    return X, y

def create_data_loader(parquet_paths: list, scaler: MinMaxScaler, 
                       seq_length: int, batch_size: int) -> tf.data.Dataset:
    """Create a tf.data.Dataset from Parquet files for LSTM training or evaluation.

    Args:
        parquet_paths (list): List of paths to Parquet files.
        scaler (MinMaxScaler): Scaler fitted on the data.
        seq_length (int): Length of input sequences.
        batch_size (int): Batch size for the dataset.

    Returns:
        tf.data.Dataset: Dataset yielding (sequence, target) pairs.
    """
    def _scaled_generator():
        for path in parquet_paths:
            if not os.path.exists(path):
                continue
            parquet_file = pq.ParquetFile(path)
            for batch in parquet_file.iter_batches(batch_size=10_000, columns=['Close']):
                chunk = batch.to_pandas()
                prices = chunk['Close'].astype('float32').values.reshape(-1, 1)
                scaled = scaler.transform(prices)
                for j in range(len(scaled) - seq_length):
                    yield scaled[j:j + seq_length], scaled[j + seq_length]

    return tf.data.Dataset.from_generator(
        _scaled_generator,
        output_types=(tf.float32, tf.float32),
        output_shapes=((seq_length, 1), (1,))
    ).batch(batch_size).prefetch(tf.data.AUTOTUNE)

def build_model_from_config(seq_length: int, cfg: dict) -> keras.Model:
    """Build an LSTM-based model based on configuration.

    Args:
        seq_length (int): Length of input sequences.
        cfg (dict): Model configuration dictionary.

    Returns:
        keras.Model: Compiled Keras model.
    """
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