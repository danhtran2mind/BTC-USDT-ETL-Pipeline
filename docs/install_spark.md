# Install Apache Spark

This guide provides step-by-step instructions to download, install, and configure Apache Spark 3.5.6 with Hadoop 3 support on a Linux-based system. Apache Spark is a powerful open-source data processing engine designed for big data and machine learning workloads. The following commands will help you set up Spark and configure the environment variables to run Spark applications, including PySpark with Python 3.

## Prerequisites
- A Linux-based operating system (e.g., Ubuntu, CentOS).
- `wget` and `tar` utilities installed.
- `sudo` privileges for moving files to system directories.
- Python 3 installed (for PySpark).

## Installation Steps

```bash
# Download Apache Spark 3.5.6 with Hadoop 3 support
wget https://downloads.apache.org/spark/spark-3.5.6/spark-3.5.6-bin-hadoop3.tgz

# Extract the downloaded tarball
tar -xzf spark-3.5.6-bin-hadoop3.tgz

# Move the extracted folder to /opt/spark
sudo mv spark-3.5.6-bin-hadoop3 /opt/spark

# Set environment variables for Spark
export SPARK_HOME=/opt/spark
export PATH=$SPARK_HOME/bin:$SPARK_HOME/sbin:$PATH
export PYSPARK_PYTHON=python3
```