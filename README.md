# BTC-USDT ETL Pipeline

A modern data engineering pipeline for BTC-USDT, leveraging Apache Airflow, MinIO, and Apache Spark.

## Overview

This project extracts, transforms, and loads BTC-USDT trading data using scalable, cloud-native frameworks.

## Key Features

- Configure DAGs for BTC-USDT ETL in Airflow.
- Store raw/processed data in MinIO.
- Use Spark for transformation and analytics.


## Architecture

- **MinIO**: Object storage for raw and processed data.
- **Apache Spark**: Distributed data processing.
- **Apache Airflow**: Workflow orchestration.

## Technologies

- Python 3
- Apache Airflow
- Apache Spark
- MinIO

## Quickstart

### 1. MinIO Setup

#### Install & Run MinIO
You can easy download and run **MinIO** server with below code
```bash
python installations/install_mino.py\
      --address-port <address_port>\
      --web-port <web_port>
```

#### Start MinIO Server

```python
# start_minio.py
import os, subprocess
os.environ['MINIO_ROOT_USER'] = 'username'
os.environ['MINIO_ROOT_PASSWORD'] = 'username_password'
subprocess.run('./minio server ~/minio-data --address ":9123" --console-address ":9124" &', shell=True, check=True)
```

### 2. Spark Setup

```bash
wget https://downloads.apache.org/spark/spark-3.5.6/spark-3.5.6-bin-hadoop3.tgz
tar -xzf spark-3.5.6-bin-hadoop3.tgz
sudo mv spark-3.5.6-bin-hadoop3 /opt/spark
export SPARK_HOME=/opt/spark
export PATH=$SPARK_HOME/bin:$SPARK_HOME/sbin:$PATH
export PYSPARK_PYTHON=python3
```

### 3. Airflow Setup

```bash
pip install "apache-airflow[async,celery,postgres,cncf.kubernetes]" --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.9.3/constraints-3.12.txt"
```

#### Initialize Airflow

```python
import os
os.environ['AIRFLOW_HOME'] = f"{os.getcwd()}/airflow"
```
```bash
airflow db init
nohup airflow scheduler > airflow/scheduler.log 2>&1 &
airflow users create --username admin --firstname Admin --lastname User --role Admin --email admin@example.com --password admin
```

### 4. Install Dependencies

```bash
pip install -r requirements.txt
```
### 5. Setup `Secret`

Create `minio.env` with below content, note that the `MINIO_HOST` and `MINIO_BORWSER` refer to `address_port` and `web_port` at [MinIO Installation](#install--run-minio):
```markdown
MINIO_ROOT_USER=<minio_username>
MINIO_ROOT_PASSWORD=<username_password>
MINIO_HOST=<minio_host_server>
MINIO_BORWSER=<minio_host_web>
```

### 6. Start Airflow

Run below code to start `Airflow` to control pipelines:
```bash
airflow webserver --port <your_airflow_port>
```
Then you access `localhost:<your_airflow_port>` to start workflows.

## License

MIT