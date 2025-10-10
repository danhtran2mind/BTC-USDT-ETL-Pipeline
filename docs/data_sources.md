# Data Source: Binance BTCUSDT 1s Kline

- **Source:** [Binance Data Public](https://data.binance.vision/?prefix=data/spot/monthly/klines/BTCUSDT/1s/)
- **Example file:** `BTCUSDT-1s-2024-05.csv.gz`
- **Columns:**
  1. Open time
  2. Open
  3. High
  4. Low
  5. Close
  6. Volume
  7. Close time
  8. Quote asset volume
  9. Number of trades
  10. Taker buy base asset volume
  11. Taker buy quote asset volume
  12. Ignore

## Usage

- The pipeline downloads and processes this data for deep learning time series forecasting.
- The DAG can be modified to use a different month by changing the filename in the download step.

## Acknowledgment
You can refer to https://github.com/binance/binance-public-data?tab=readme-ov-file#klines to get more infromation

