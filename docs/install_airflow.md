!pip install "apache-airflow[async,celery,postgres,cncf.kubernetes]" --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.9.3/constraints-3.12.txt"

import os
import time

# Ensure environment
os.environ['AIRFLOW_HOME'] = '/content/BTC-USDT-ETL-Pipeline/airflow'
# os.makedirs('/content/airflow', exist_ok=True)

# # Kill any lingering processes (safe)
# os.system('pkill -f "airflow"')
# time.sleep(5)

# Re-init the database (resets metadata but keeps DAGs if any)
!airflow db init

# Create admin user (critical—webserver needs this for login)
!airflow users create \
    --username admin \
    --firstname Admin \
    --lastname User \
    --role Admin \
    --email admin@example.com \
    --password admin

# Verify user creation
!airflow users list

# Start scheduler first (it needs DB)
!nohup airflow scheduler > /content/BTC-USDT-ETL-Pipeline/airflow/scheduler.log 2>&1 &