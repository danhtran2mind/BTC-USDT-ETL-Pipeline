!wget https://downloads.apache.org/spark/spark-3.5.6/spark-3.5.6-bin-hadoop3.tgz
!tar -xzf spark-3.5.6-bin-hadoop3.tgz
!sudo mv spark-3.5.6-bin-hadoop3 /opt/spark
!export SPARK_HOME=/opt/spark
!export PATH=$SPARK_HOME/bin:$SPARK_HOME/sbin:$PATH
!export PYSPARK_PYTHON=python3


```bash
wget https://downloads.apache.org/spark/spark-3.5.6/spark-3.5.6-bin-hadoop3.tgz
tar -xzf spark-3.5.6-bin-hadoop3.tgz
sudo mv spark-3.5.6-bin-hadoop3 /opt/spark
export SPARK_HOME=/opt/spark
export PATH=$SPARK_HOME/bin:$SPARK_HOME/sbin:$PATH
export PYSPARK_PYTHON=python3
```

Add the `export` commands to your shell configuration file (e.g., `~/.bashrc` or `~/.zshrc`) to persist them.