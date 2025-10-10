from tensorflow import keras

def build_lstm_model(seq_length):
    model = keras.Sequential([
        keras.layers.LSTM(50, return_sequences=True, input_shape=(seq_length, 1)),
        keras.layers.LSTM(50),
        keras.layers.Dense(1)
    ])
    model.compile(optimizer='adam', loss='mse')
    return model
