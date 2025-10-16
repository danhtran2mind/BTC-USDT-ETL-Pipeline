#!/usr/bin/env python3
import subprocess
import os

def run(cmd):
    subprocess.run(cmd, shell=True, check=True)

# Download and install Spark
spark_ver = "3.5.6"
url = f"https://downloads.apache.org/spark/spark-{spark_ver}/spark-{spark_ver}-bin-hadoop3.tgz"
run(f"wget {url}")
run("tar -xzf spark-*.tgz")
run("sudo mv spark-* /opt/spark")

# Set environment variables
bashrc = f"""
export SPARK_HOME=/opt/spark
export PATH=$SPARK_HOME/bin:$SPARK_HOME/sbin:$PATH
export PYSPARK_PYTHON=python3
"""
with open(os.path.expanduser("~/.bashrc"), "a") as f:
    f.write(bashrc)

run("rm spark-*.tgz")
print("Spark installed! Run: source ~/.bashrc")