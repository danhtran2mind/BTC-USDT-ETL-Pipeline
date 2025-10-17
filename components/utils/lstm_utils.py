import numpy as np

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