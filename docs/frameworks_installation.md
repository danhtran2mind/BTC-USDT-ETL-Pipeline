## Spark
### Install Java 8 (required for Spark)
!apt-get update -qq
!apt-get install openjdk-8-jdk-headless -qq > /dev/null

### Download and extract Spark (use the latest version; this is 3.5.6 with Hadoop 3)
!wget -q https://downloads.apache.org/spark/spark-3.5.6/spark-3.5.6-bin-hadoop3.tgz
!tar xf spark-3.5.6-bin-hadoop3.tgz

### Install PySpark and findspark (helps locate Spark)
!pip install -q pyspark findspark duckdb  # duckdb for your script

### Set environment variables
import os
os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-8-openjdk-amd64"
os.environ["SPARK_HOME"] = "/content/spark-3.5.6-bin-hadoop3"

### Initialize findspark
import findspark
findspark.init()


## Hadoop

!wget https://downloads.apache.org/hadoop/common/hadoop-3.4.2/hadoop-3.4.2.tar.gz
!tar -xzvf hadoop-3.4.2.tar.gz && cp -r hadoop-3.4.2/ /usr/local/


JAVA_HOME = !readlink -f /usr/bin/java | sed "s:bin/java::"
java_home_text = JAVA_HOME[0]
java_home_text_command = f"$ {JAVA_HOME[0]} "
!echo export JAVA_HOME=$java_home_text >>/usr/local/hadoop-3.4.2/etc/hadoop/hadoop-env.sh

# Set environment variables
import os
os.environ['HADOOP_HOME']="/usr/local/hadoop-3.4.2"
os.environ['JAVA_HOME']=java_home_text

!alias hadoop="/usr/local/hadoop-3.4.2/bin/hadoop"
!alias hdfs="/usr/local/hadoop-3.4.2/bin/hdfs"
!source ~/.bashrc   # or source ~/.zshrc
!sudo ln -s /usr/local/hadoop-3.4.2/bin/hadoop /usr/local/bin/hadoop
!sudo ln -s /usr/local/hadoop-3.4.2/bin/hdfs /usr/local/bin/hdfs
!hadoop
!hdfs
## Airflow

pip install apache-airflow

airflow db init

airflow webserver -p 8080 &
airflow scheduler &

## Ngrok

## MinIO
### Client
```bash
pip install minio
```
### Server
# Install MinIO binary
!wget https://dl.min.io/server/minio/release/linux-amd64/minio
!chmod +x minio
!mkdir -p ~/minio-data

import os
os.environ['MINIO_ROOT_USER'] = 'username'
os.environ['MINIO_ROOT_PASSWORD'] = 'username_password'

!./minio server ~/minio-data --address ":12390" --console-address ":12391" &
