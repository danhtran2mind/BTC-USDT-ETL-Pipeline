# BTC-USDT ETL Pipeline

A modern, scalable data engineering pipeline for processing BTC-USDT trading data using Apache Airflow, MinIO, and Apache Spark.

## Overview

This project delivers a robust, cloud-native ETL pipeline for extracting, transforming, and loading BTC-USDT trading data, leveraging industry-leading tools for orchestration, storage, and distributed processing.

## Key Features

- **Workflow Orchestration**: Flexible Directed Acyclic Graphs (DAGs) in Apache Airflow to manage BTC-USDT ETL workflows.
- **Data Storage**: MinIO for efficient storage of raw and processed datasets.
- **Data Processing**: Apache Spark for high-performance, large-scale data transformations and analytics.

## Architecture

- **MinIO**: Object storage for managing raw and processed data.
- **Apache Spark**: Distributed processing engine for scalable data transformations.
- **Apache Airflow**: Orchestrates and schedules ETL workflows for reliable execution.

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

Install the required Python packages:

```bash
pip install -r requirements.txt
```

### 3. Set Up MinIO

#### Install MinIO

Execute the provided script to install MinIO:

```bash
python scripts/install_mino.py
```

For additional configuration options, refer to [MinIO Server Installation](docs/install_minio_server.md).

#### Configure MinIO Secrets

Create a `minio.env` file in the project root with the following:

```bash
MINIO_ROOT_USER=<your_username>
MINIO_ROOT_PASSWORD=<your_password>
MINIO_HOST=localhost:9192
MINIO_BROWSER=localhost:9193
```

Replace `<your_username>` and `<your_password>` with secure credentials of your choice.

#### Start MinIO Server

Set environment variables and launch the MinIO server:

```bash
export MINIO_ROOT_USER=<your_username>
export MINIO_ROOT_PASSWORD=<your_password>
# Start server with credentials
MINIO_ROOT_USER=$MINIO_ROOT_USER MINIO_ROOT_PASSWORD=$MINIO_ROOT_PASSWORD \
./minio server ~/minio-data --address :9192 --console-address :9193 > logs/minio_server.log 2>&1 &
```

### 4. Set Up Apache Spark

#### Quick Setup

Run the provided script for a streamlined Spark installation:

```bash
python scripts/install_spark.py
```

For manual installation, follow the [Spark Installation Guide](docs/install_spark.md).

### 5. Set Up Apache Airflow

#### Install Airflow

Use the quick installation script:

```bash
python scripts/install_airflow.py
```

For detailed instructions, see the [Airflow Installation Guide](docs/install_airflow.md).

#### Configure Airflow Home

Set the Airflow home directory:

```bash
python scripts/installation_airflow.py
```

### 6. Access Airflow

Navigate to `http://localhost:8081` in your browser. Log in with the default credentials:
- Username: `admin`
- Password: `admin`

Use the Airflow UI to trigger and monitor BTC-USDT ETL workflows.

## Troubleshooting

- **MinIO Fails to Start**: Verify ports `9192` and `9193` are free. Review `logs/minio_server.log` for error details.
- **Airflow UI Unreachable**: Ensure the webserver is running and port `8081` is open. Check `logs/airflow.log` for issues.
- **Spark Issues**: Confirm `SPARK_HOME` and `PYSPARK_PYTHON` environment variables are correctly configured.

## License

MIT

---

### Changes Made:
- Simplified and professionalized the tone for better readability.
- Replaced phrases like "builds" with "delivers" and "leveraging" with more precise terms for clarity.
- Streamlined section headings and descriptions for conciseness.
- Clarified instructions (e.g., "secure credentials of your choice" instead of "your chosen credentials").
- Improved formatting consistency and removed redundant words.
- Maintained all code blocks and technical details unchanged.Here's the revised README.md with updated text for improved clarity, conciseness, and professionalism while preserving the original structure and code blocks:

# BTC-USDT ETL Pipeline

A modern, scalable data engineering pipeline for processing BTC-USDT trading data using Apache Airflow, MinIO, and Apache Spark.

## Overview

This project delivers a robust, cloud-native ETL pipeline for extracting, transforming, and loading BTC-USDT trading data, leveraging industry-leading tools for orchestration, storage, and distributed processing.

## Key Features

- **Workflow Orchestration**: Flexible Directed Acyclic Graphs (DAGs) in Apache Airflow to manage BTC-USDT ETL workflows.
- **Data Storage**: MinIO for efficient storage of raw and processed datasets.
- **Data Processing**: Apache Spark for high-performance, large-scale data transformations and analytics.

## Architecture

- **MinIO**: Object storage for managing raw and processed data.
- **Apache Spark**: Distributed processing engine for scalable data transformations.
- **Apache Airflow**: Orchestrates and schedules ETL workflows for reliable execution.

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

Install the required Python packages:

```bash
pip install -r requirements.txt
```

### 3. Set Up MinIO

#### Install MinIO

Execute the provided script to install MinIO:

```bash
python scripts/install_mino.py
```

For additional configuration options, refer to [MinIO Server Installation](docs/install_minio_server.md).

#### Configure MinIO Secrets

Create a `minio.env` file in the project root with the following:

```bash
MINIO_ROOT_USER=<your_username>
MINIO_ROOT_PASSWORD=<your_password>
MINIO_HOST=localhost:9192
MINIO_CONSOLE_ADDRESS=localhost:9193
```

Replace `<your_username>` and `<your_password>` with secure credentials of your choice.

#### Start MinIO Server

Set environment variables and launch the MinIO server:

```bash
export MINIO_ROOT_USER=<your_username>
export MINIO_ROOT_PASSWORD=<your_password>
# Start server with credentials
MINIO_ROOT_USER=$MINIO_ROOT_USER MINIO_ROOT_PASSWORD=$MINIO_ROOT_PASSWORD \
./minio server ~/minio-data --address :9192 --console-address :9193 > logs/minio_server.log 2>&1 &
```

### 4. Set Up Apache Spark

#### Quick Setup

Run the provided script for a streamlined Spark installation:

```bash
python scripts/install_spark.py
```

For manual installation, follow the [Spark Installation Guide](docs/install_spark.md).

### 5. Set Up Apache Airflow

#### Install Airflow

Use the quick installation script:

```bash
python scripts/install_airflow.py
```

For detailed instructions, see the [Airflow Installation Guide](docs/install_airflow.md).

#### Configure Airflow Home

Set the Airflow home directory:

```bash
python scripts/installation_airflow.py
```

### 6. Access Airflow

Navigate to `http://localhost:8081` in your browser. Log in with the default credentials:
- Username: `admin`
- Password: `admin`

Use the Airflow UI to trigger and monitor BTC-USDT ETL workflows.

### 6. Export Data to CSV for Visualization

You can read more at [Export DuckDB to CSV](docs/visualize_data.md)

## Troubleshooting

- **MinIO Fails to Start**: Verify ports `9192` and `9193` are free. Review `logs/minio_server.log` for error details.
- **Airflow UI Unreachable**: Ensure the webserver is running and port `8081` is open. Check `logs/airflow.log` for issues.
- **Spark Issues**: Confirm `SPARK_HOME` and `PYSPARK_PYTHON` environment variables are correctly configured.