import os
import subprocess
import time

PROJECT_PATH = os.getcwd()
AIRFLOW_HOME = os.path.join(PROJECT_PATH, "airflow")

def run(cmd, cwd=None):
    p = subprocess.Popen(cmd, shell=True, cwd=cwd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True)
    stdout, stderr = p.communicate()
    if p.returncode != 0: raise subprocess.CalledProcessError(p.returncode, cmd)
    return stdout

os.environ['AIRFLOW_HOME'] = AIRFLOW_HOME
os.makedirs(AIRFLOW_HOME, exist_ok=True)

# Install Airflow
run('pip install "apache-airflow[async,celery,postgres,cncf.kubernetes]" '
    '--constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.9.3/constraints-3.12.txt"')

run('airflow db init', AIRFLOW_HOME)
run('airflow users create --username admin --firstname Admin --lastname User '
    '--role Admin --email admin@example.com --password admin', AIRFLOW_HOME)
run('airflow users list', AIRFLOW_HOME)

# Background scheduler
subprocess.Popen(f"nohup airflow scheduler > {AIRFLOW_HOME}/scheduler.log 2>&1 &", shell=True, cwd=AIRFLOW_HOME)
time.sleep(2)
# Background webserver run on port 8081
subprocess.Popen(f"airflow webserver --port 8081 > {AIRFLOW_HOME}/airflow.log 2>&1 &", shell=True, cwd=AIRFLOW_HOME)
time.sleep(2)

print(f"Setup at {AIRFLOW_HOME}, Login: admin/admin at http://localhost:8081")