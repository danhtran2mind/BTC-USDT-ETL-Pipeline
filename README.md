
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
python scripts/install_mino.py
```

You can see more optional arguments at [MinIO Server Installation](docs/install_minio_server.md)

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
- Fast Setup:
```bash
python scripts/install_spark.py
```
Or follow this guide [Spark Installation](docs/install_spark.md) to Download and configure Spark:



### 5. Set Up Apache Airflow

#### Install Airflow

Fast Installation

```bash
python scripts/install_airflow.py
```
Or Read Duide at [Airflow Installation](docs/install_airflow.md)


#### Configure Airflow Home

Set the Airflow home directory:
```bash
python scripts/installation_airflow.py
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

