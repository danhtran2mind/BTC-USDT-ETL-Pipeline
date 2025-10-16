
# BTC-USDT ETL Pipeline

A modern data engineering pipeline for extracting, transforming, and loading BTC-USDT trading data using Apache Airflow, MinIO, and Apache Spark.

## Overview

This project builds a scalable, cloud-native pipeline to process BTC-USDT trading data, leveraging industry-standard tools for orchestration, storage, and distributed processing.

## Key Features

- **Workflow Orchestration**: Configurable Directed Acyclic Graphs (DAGs) in Apache Airflow for BTC-USDT ETL processes.
- **Data Storage**: MinIO for storing raw and processed data.
- **Data Processing**: Apache Spark for efficient transformation and analytics.

## Architecture

- **MinIO**: Object storage for raw and processed data.
- **Apache Spark**: Distributed data processing for large-scale transformations.
- **Apache Airflow**: Workflow orchestration for scheduling and managing ETL pipelines.

## Technologies

- Apache Airflow
- Apache Spark
- MinIO

## Quickstart Guide

Follow these steps to set up and run the BTC-USDT ETL pipeline.

### 1. Clone the Repository

```bash
git clone https://github.com/danhtran2mind/BTC-USDT-ETL-Pipeline.git
cd BTC-USDT-ETL-Pipeline
```

### 2. Install Dependencies

Install required Python packages:

```bash
pip install -r requirements.txt
```

### 3. Set Up MinIO

#### Install MinIO

Run the provided script to download and install MinIO:

```bash
python installations\install_mino.py
```



#### Configure MinIO Secrets

Create a `minio.env` file in the project root with the following content:

```bash
MINIO_ROOT_USER=<your_username>
MINIO_ROOT_PASSWORD=<your_password>
MINIO_HOST=localhost:9192
MINIO_BROWSER=localhost:9193
```

Replace `<your_username>` and `<your_password>` with your chosen credentials.

#### Start MinIO Server

Set environment variables and start the MinIO server:

```bash
export MINIO_ROOT_USER=<your_username>
export MINIO_ROOT_PASSWORD=<your_password>
# Start server with credentials
MINIO_ROOT_USER=$MINIO_ROOT_USER MINIO_ROOT_PASSWORD=$MINIO_ROOT_PASSWORD \
./minio server ~/minio-data --address :9192 --console-address :9193 > logs/minio_server.log 2>&1 &
```

### 4. Set Up Apache Spark

Download and configure Spark:

```bash
wget https://downloads.apache.org/spark/spark-3.5.6/spark-3.5.6-bin-hadoop3.tgz
tar -xzf spark-3.5.6-bin-hadoop3.tgz
sudo mv spark-3.5.6-bin-hadoop3 /opt/spark
export SPARK_HOME=/opt/spark
export PATH=$SPARK_HOME/bin:$SPARK_HOME/sbin:$PATH
export PYSPARK_PYTHON=python3
```

Add the `export` commands to your shell configuration file (e.g., `~/.bashrc` or `~/.zshrc`) to persist them.

### 5. Set Up Apache Airflow

#### Install Airflow

Install Airflow with required dependencies:

```bash
pip install "apache-airflow[async,celery,postgres,cncf.kubernetes]" --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.9.3/constraints-3.12.txt"
```

#### Configure Airflow Home

Set the Airflow home directory:
```bash
python installations/installation_airflow.py
```

#### Start Airflow Webserver

Run the webserver (default port: 8081):

```bash
airflow webserver --port 8081 > airflow /airflow.log 2>&1 &
```

### 6. Access Airflow

Open your browser and navigate to `http://localhost:8081`. Log in with the credentials:
- Username: `admin`
- Password: `admin`

From the Airflow UI, you can trigger and monitor the BTC-USDT ETL workflows.

## Troubleshooting

- **MinIO not starting**: Ensure the ports `9192` and `9193` are not in use. Check `logs/minio_server.log` for errors.
- **Airflow UI inaccessible**: Verify the webserver is running and the port is open. Check `logs/airflow.log` for details.
- **Spark errors**: Confirm `SPARK_HOME` and `PYSPARK_PYTHON` are set correctly.

## License

MIT

