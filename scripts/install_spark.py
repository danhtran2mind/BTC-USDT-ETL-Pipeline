#!/usr/bin/env python3
import subprocess
import os
import urllib.request
import tarfile
from pathlib import Path

def run(cmd):
    """Run command and capture output for debugging"""
    print(f"Running: {cmd}")
    result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
    if result.returncode != 0:
        print(f"Error: {result.stderr}")
        raise subprocess.CalledProcessError(result.returncode, cmd)
    print(f"Success: {result.stdout}")
    return result

def install_spark_colab():
    spark_ver = "3.5.6"
    spark_dir = f"spark-{spark_ver}-bin-hadoop3"
    url = f"https://downloads.apache.org/spark/spark-{spark_ver}/spark-{spark_ver}-bin-hadoop3.tgz"
    archive = f"{spark_dir}.tgz"
    
    # Create working directory
    work_dir = Path.home() / "spark_install"
    work_dir.mkdir(exist_ok=True)
    os.chdir(work_dir)
    
    # Download if not exists
    if not Path(archive).exists():
        print(f"Downloading Spark {spark_ver}...")
        urllib.request.urlretrieve(url, archive)
    
    # Extract
    print("Extracting archive...")
    with tarfile.open(archive, "r:gz") as tar:
        tar.extractall(path=".")
    
    # Create symlink in home directory (no sudo needed)
    spark_home = Path.home() / "spark"
    if spark_home.exists():
        spark_home.unlink()
    spark_home.symlink_to(work_dir / spark_dir)
    
    # Set environment variables for current session
    os.environ['SPARK_HOME'] = str(spark_home)
    os.environ['PATH'] = f"{spark_home}/bin:{spark_home}/sbin:{os.environ.get('PATH', '')}"
    os.environ['PYSPARK_PYTHON'] = 'python3'
    
    # Clean up
    Path(archive).unlink(missing_ok=True)
    
    print(f"Spark installed to: {spark_home}")
    print("Environment variables set for this session!")
    print("\nTo verify installation:")
    print("!ls $SPARK_HOME")
    print("!spark-submit --version")
    
    return str(spark_home)

if __name__ == "__main__":
    spark_path = install_spark_colab()