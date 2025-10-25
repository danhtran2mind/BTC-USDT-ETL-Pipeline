# Installing and Setting Up Apache Airflow

This guide provides detailed instructions for installing and configuring Apache Airflow with support for asynchronous tasks, Celery, PostgreSQL, and Kubernetes. The steps below ensure a proper setup for running Airflow, initializing its database, creating an admin user, and starting the scheduler and webserver. This setup is suitable for a local development environment or a scalable production setup with the specified backends.

## Prerequisites
Before proceeding, ensure you have the following:
- **Python 3.12**: Airflow 2.10.3 is compatible with Python 3.12, as specified in the constraint file.
- **pip**: The Python package manager to install Airflow and its dependencies.
- **PostgreSQL**: If using PostgreSQL as the metadata database (recommended for production).
- **Celery**: For distributed task execution (optional, included in the installation).
- **Kubernetes**: For running Airflow in a Kubernetes cluster (optional, included in the installation).
- **Sufficient permissions**: To create directories and run background processes.
- **Virtual environment** (recommended): To isolate dependencies. Create one with:
  ```bash
  python -m venv venv
  source venv/bin/activate  # On Windows: venv\Scripts\activate
  ```

## Installation Steps

### 1. Install Apache Airflow
Install Airflow with the required extras for async, Celery, PostgreSQL, and Kubernetes support. The `--constraint` flag ensures compatibility with Python 3.12 by using the official Airflow constraint file for version 2.10.3.

```bash
pip install "apache-airflow[async,celery,postgres,cncf.kubernetes]" --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.10.3/constraints-3.12.txt"
```

This command installs:
- **apache-airflow**: The core Airflow package.
- **async**: Support for asynchronous task execution.
- **celery**: Support for Celery as a distributed task queue.
- **postgres**: PostgreSQL database connector for the metadata database.
- **cncf.kubernetes**: Kubernetes executor for running tasks in a Kubernetes cluster.

### 2. Set Up the Airflow Home Directory
Airflow requires a home directory to store its configuration, logs, and DAGs. The following Python script sets the `AIRFLOW_HOME` environment variable and creates the directory if it doesn't exist.

```python
import os
import time

# Ensure environment
os.environ['AIRFLOW_HOME'] = '<your_project_path>/airflow'
os.makedirs('airflow', exist_ok=True)
```

Replace `<your_project_path>` with the absolute path to your project directory (e.g., `/home/user/BTC-USDT-ETL-Pipeline`). For example:
```python
os.environ['AIRFLOW_HOME'] = '/home/user/BTC-USDT-ETL-Pipeline/airflow'
```

This script ensures the `airflow` directory is created in your project path to store Airflow's configuration files, logs, and SQLite database (if not using PostgreSQL).

### 3. Initialize the Airflow Database
Initialize the Airflow metadata database, which stores DAG runs, task instances, and other metadata. This step is required before starting the scheduler or webserver.

```bash
# Re-init the database (resets metadata but keeps DAGs if any)
airflow db init
```

**Note**:
- This command creates a default `airflow.cfg` configuration file in `AIRFLOW_HOME`.
- If using PostgreSQL, ensure the database is running and update the `sql_alchemy_conn` in `airflow.cfg` to point to your PostgreSQL instance (e.g., `postgresql+psycopg2://user:password@localhost:5432/airflow`).
- Running `airflow db init` resets metadata but preserves any DAGs in the `dags` folder.

### 4. Create an Admin User
The Airflow webserver requires at least one admin user for login. Create an admin user with the following command:

```bash
# Create admin user (critical—webserver needs this for login)
airflow users create \
    --username admin \
    --firstname Admin \
    --lastname User \
    --role Admin \
    --email admin@example.com \
    --password admin
```

This command creates a user with:
- Username: `admin`
- Password: `admin` (change this in production for security)
- Role: `Admin` (grants full access to the Airflow UI)

To verify the user was created successfully, list all users:

```bash
# Verify user creation
airflow users list
```

### 5. Start the Airflow Scheduler
The scheduler is responsible for scheduling and executing DAGs. Start it in the background using `nohup` to ensure it continues running.

```bash
# Start scheduler first (it needs DB)
nohup airflow scheduler > airflow/scheduler.log 2>&1 &
```

**Notes**:
- The scheduler requires the database to be initialized first.
- Logs are redirected to `scheduler.log` in the specified directory.
- Replace `airflow` with your `AIRFLOW_HOME` path if different.

### 6. Start the Airflow Webserver
The webserver provides the Airflow UI for managing DAGs, viewing task logs, and monitoring runs. Start it on port 8081 (or another port if needed).

```bash
airflow webserver --port 8081 > airflow/airflow.log 2>&1 &
```

**Notes**:
- The webserver runs on `http://localhost:8081` by default.
- Logs are redirected to `airflow.log` in the `AIRFLOW_HOME` directory.
- Access the UI by navigating to `http://localhost:8081` in your browser and logging in with the admin credentials (username: `admin`, password: `admin`).

## Additional Notes
- **Configuration**: After running `airflow db init`, review and modify `airflow.cfg` in the `AIRFLOW_HOME` directory to customize settings (e.g., executor type, database connection, or Celery broker).
- **Celery Setup**: If using the Celery executor, ensure a message broker (e.g., Redis or RabbitMQ) is running and configured in `airflow.cfg`.
- **Kubernetes Executor**: For Kubernetes, configure the Kubernetes executor in `airflow.cfg` and ensure your Kubernetes cluster is accessible.
- **Security**: Change the default admin password and secure the database connection in production environments.
- **Logs**: Check `scheduler.log` and `airflow.log` for troubleshooting.

## Next Steps
- Place your DAGs in the `AIRFLOW_HOME/dags` folder to start defining workflows.
- Explore the Airflow UI to monitor and manage your DAGs.
- Refer to the [Apache Airflow documentation](https://airflow.apache.org/docs/apache-airflow/stable/) for advanced configurations.
