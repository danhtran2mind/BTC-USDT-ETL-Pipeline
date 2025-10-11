# Data Engineering Project: Binance BTCUSDT 1s Kline Pipeline

This project demonstrates a data engineering pipeline using Airflow, Hadoop/HDFS, Spark, and deep learning (LSTM) for time series forecasting on Binance BTCUSDT 1-second kline data.

## Pipeline Overview

- **Airflow** orchestrates the workflow:
  1. Download Binance BTCUSDT 1s kline CSV (latest or specified month).
  2. Move data to HDFS for storage.
  3. (Optional) Spark job for data cleaning or transformation (`spark/process_data.py`).
  4. Train LSTM model (`spark/lstm_train.py`) and save checkpoint to `./ckpts`.
  5. Predict with LSTM model (`spark/lstm_predict.py`) using the latest checkpoint.

- **Hadoop/HDFS** stores raw and processed data.
- **Spark** can be used for data cleaning or transformation.
<!-- - **TensorFlow/Keras** for LSTM deep learning. -->

## Quickstart

1. **Install dependencies**  
   See [docs/dependencies.md](docs/dependencies.md).

2. **Set up Hadoop and Airflow**  
   See [docs/services.md](docs/services.md).

3. **Run the pipeline**  
   ```bash
   airflow dags trigger data_engineering_pipeline
   ```

4. **Check model checkpoints**  
   After training, model checkpoints are saved in `./ckpts/lstm_checkpoint.keras`.

5. **Manual execution (for development)**  
   You can run individual scripts for development:
   ```bash
   python3 spark/lstm_train.py
   python3 spark/lstm_predict.py
   ```

## Files

- `spark/lstm_train.py`: Trains the LSTM model and saves checkpoint.
- `spark/lstm_predict.py`: Loads the checkpoint, evaluates, and predicts.
- `spark/utils.py`, `spark/model.py`: Utilities and model definition.
- `spark/process_data.py`: (Optional) Spark data processing.
- `airflow/dags/data_pipeline.py`: Airflow DAG definition.

## Documentation

See the [docs/](docs/) folder for more details.

## Troubleshooting

- Ensure all Python dependencies are installed (see [docs/dependencies.md](docs/dependencies.md)).
- Make sure Hadoop/HDFS and Airflow services are running before triggering the DAG.
- If you encounter file permission or path issues, check that the workspace and HDFS directories exist and are writable.

## MinIO
## Server
Read and run code follow guide at (MinIO Server)[docs/minio_server.md]
or run this bash for faster running:
```bash
python minio_api/server.py
```

## Client



######################

## Usage

### Step 1: Download and Run MinIO Server

#### 1.1 Download and Install MinIO
```bash
wget https://dl.min.io/server/minio/release/linux-amd64/minio
chmod +x minio
mkdir -p ~/minio-data
```

#### 1.2 Set MinIO Credentials and Start Server
Create a Python script (e.g., `start_minio.py`) with the following:
```python
import os
import subprocess

os.environ['MINIO_ROOT_USER'] = 'username'
os.environ['MINIO_ROOT_PASSWORD'] = 'username_password'

address_port = 9123
web_port = 9124
command = f'./minio server ~/minio-data --address ":{address_port}" --console-address ":{web_port}" &'

try:
    subprocess.run(command, shell=True, check=True)
    print(f"MinIO started with API on :{address_port} and WebUI on :{web_port}")
except subprocess.CalledProcessError as e:
    print(f"Failed to start MinIO: {e}")
```

### Step 2: Download and Setup Spark

```bash
wget https://downloads.apache.org/spark/spark-3.5.6/spark-3.5.6-bin-hadoop3.tgz
tar -xzf spark-3.5.6-bin-hadoop3.tgz
sudo mv spark-3.5.6-bin-hadoop3 /opt/spark
sudo ln -s /opt/spark /opt/spark-latest  # Optional symlink for version management

export SPARK_HOME=/opt/spark
export PATH=$SPARK_HOME/bin:$SPARK_HOME/sbin:$PATH
export PYSPARK_PYTHON=python3
```

### Step 3: Download and Run Airflow

```bash
pip install "apache-airflow[async,celery,postgres,cncf.kubernetes]" --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.9.3/constraints-3.12.txt"
```

#### 3.1 Initialize Airflow Database and Create Admin User

```python
import os

current_dir_path = os.getcwd()
os.environ['AIRFLOW_HOME'] = f"{current_dir_path}/airflow"
```
```bash
airflow db init
nohup airflow scheduler > airflow/scheduler.log 2>&1 &
airflow users create --username admin --firstname Admin --lastname User --role Admin --email admin@example.com --password admin
```

### Step 4: Install Project Dependencies

```bash
pip install -r requirements.txt
```