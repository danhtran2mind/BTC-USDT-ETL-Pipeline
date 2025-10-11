import pandas as pd
import numpy as np
from utils import create_sequences
from model import build_lstm_model
from sklearn.preprocessing import MinMaxScaler
from tensorflow import keras
import os

df = pd.read_csv('/tmp/BTCUSDT-1s-2024-05.csv', header=None)
df.columns = [
    "open_time", "open", "high", "low", "close", "volume",
    "close_time", "quote_asset_volume", "number_of_trades",
    "taker_buy_base_asset_volume", "taker_buy_quote_asset_volume", "ignore"
]

prices = df['close'].astype(float).values.reshape(-1, 1)
scaler = MinMaxScaler()
prices_scaled = scaler.fit_transform(prices)

split_idx = int(len(prices_scaled) * 0.8)
train_data = prices_scaled[:split_idx]
test_data = prices_scaled[split_idx - 60:]

seq_length = 60
X_train, y_train = create_sequences(train_data, seq_length)
X_test, y_test = create_sequences(test_data, seq_length)

model = build_lstm_model(seq_length)

os.makedirs('./ckpts', exist_ok=True)
checkpoint_cb = keras.callbacks.ModelCheckpoint(
    './ckpts/lstm_checkpoint.keras', save_best_only=True, monitor='val_loss'
)

model.fit(
    X_train, y_train,
    epochs=5,
    batch_size=64,
    validation_data=(X_test, y_test),
    callbacks=[checkpoint_cb],
    verbose=2
)
